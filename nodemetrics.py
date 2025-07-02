from fastapi import FastAPI, Query, HTTPException, WebSocket, WebSocketDisconnect
import requests
from typing import Optional
from urllib.parse import quote
import asyncio
import json
from pydantic import BaseModel
from typing import List, Optional
import time
import datetime

app = FastAPI()

# Base VictoriaMetrics URL
VICTORIA_BASE_URL = "http://34.131.24.129:8428"


# ------------------------------------------------
# Helper: Build dynamic PromQL with filter options
# ------------------------------------------------
def build_advanced_promql(
    metric: str,
    org: str,
    region: str,
    instance: Optional[str],
    name_regex: Optional[str],
    job: Optional[str],
    use_rate: bool = False,
    window: Optional[str] = "5m",
    agg: Optional[str] = "sum",
    group_by: Optional[str] = None,
    multiply: Optional[int] = None,
    custom_expression: Optional[str] = None,
    filters: Optional[dict] = None
) -> str:
    labels = [f'org="{org}"', f'region="{region}"']
    if instance:
        labels.append(f'instance=~"{instance}"')
    if name_regex:
        labels.append(f'name=~"{name_regex}"')
    if job:
        labels.append(f'job="{job}"')

    label_str = ",".join(labels)
    base = f"{metric}{{{label_str}}}" if label_str else metric

    if use_rate:
        base = f"irate({base}[{window}])"

    if agg:
        if group_by:
            base = f"{agg}({base}) by ({group_by})"
        else:
            base = f"{agg}({base})"

    if custom_expression:
        # Replace all filters from config
        for k, v in (filters or {}).items():
            custom_expression = custom_expression.replace(f"${k}", str(v))
        base = custom_expression.replace("$base", base)

    if multiply:
        base = f"{base} * {multiply}"

    return base


def query(promql: str) -> dict:
    url = f"{VICTORIA_BASE_URL}/api/v1/query?query={quote(promql)}"
    res = requests.get(url)
    return res.json()

# ------------------------------------------------
# Helper: Query VictoriaMetrics (instant/range)
# ------------------------------------------------
def query_prometheus(promql: str, mode: str, start: Optional[str] = None, end: Optional[str] = None, step: Optional[str] = "60s") -> dict:
    encoded = quote(promql)

    if mode == "range":
        if not start or not end:
            raise HTTPException(status_code=422, detail="start and end time required for range queries")

        url = f"{VICTORIA_BASE_URL}/api/v1/query_range"
        params = {
            "query": promql,
            "start": start,
            "end": end,
            "step": step
        }
    else:
        url = f"{VICTORIA_BASE_URL}/api/v1/query"
        params = {"query": promql}

    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()

# ------------------------------------------------
# ✅ Unified Metrics Query API
# ------------------------------------------------
@app.get("/metrics/query")
def dynamic_metrics_query(
    metric: str = Query(..., description="Prometheus metric name"),
    org: str = Query(..., description="Org label (required)"),
    region: str = Query(..., description="Region label (required)"),
    job: Optional[str] = Query(None, description="Job label filter"),
    instance: Optional[str] = Query(None, description="Instance filter"),
    name_regex: Optional[str] = Query(None, description="Regex for container or pod name"),
    use_rate: bool = Query(False, description="Use rate() function"),
    window: str = Query("5m", description="Rate window duration (default: 5m)"),
    agg: Optional[str] = Query("sum", description="Aggregation function"),
    group_by: Optional[str] = Query(None, description="Label to group by"),
    multiply: Optional[int] = Query(None, description="Multiply result by a factor (e.g., 100 for %)"),
    mode: str = Query("instant", description="Query type: instant or range"),
    start: Optional[str] = Query(None, description="Start time (RFC3339 or unix)"),
    end: Optional[str] = Query(None, description="End time (RFC3339 or unix)"),
    step: str = Query("60s", description="Step for range queries")
):
    try:
        promql = build_advanced_promql(
            metric, org, region, instance, name_regex, job,
            use_rate, window, agg, group_by, multiply
        )
        data = query_prometheus(promql, mode, start, end, step)

        return {
            "filters_used": {
                "metric": metric,
                "org": org,
                "region": region,
                "job": job,
                "instance": instance,
                "name_regex": name_regex,
                "agg": agg,
                "group_by": group_by,
                "use_rate": use_rate,
                "window": window,
                "multiply": multiply
            },
            "query": promql,
            "mode": mode,
            "result": data.get("data", {})
        }

    except Exception as e:
        return {"error": str(e)}



@app.get("/metrics/status")
def get_job_status(
    org: str = Query(..., description="Organization label"),
    region: Optional[str] = Query(None, description="Region label (optional, omit to fetch all regions)"),
    job: Optional[str] = Query(None),
    instance: Optional[str] = Query(None),
    env: Optional[str] = Query(None),
    status_filter: Optional[str] = Query("all", description="Filter by status: up, down, or all")
):
    # --- Label Filtering Logic ---
    labels = [f'org="{org}"']

    if region:
        # Regex or literal
        if "*" in region or "." in region or region.startswith("("):
            labels.append(f'region=~"{region}"')
        else:
            labels.append(f'region="{region}"')

    if job:
        labels.append(f'job="{job}"')
    if instance:
        labels.append(f'instance=~"{instance}"')
    if env:
        labels.append(f'env="{env}"')

    promql = f'up{{{",".join(labels)}}}'

    # --- Fetch status data ---
    try:
        response = requests.get(f"{VICTORIA_BASE_URL}/api/v1/query", params={"query": promql}, timeout=5)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=502, detail=f"Request failed: {str(e)}")
    except ValueError:
        raise HTTPException(status_code=500, detail="Invalid JSON from VictoriaMetrics")

    result = {}
    region_set = set()

    for item in data.get("data", {}).get("result", []):
        metric = item.get("metric", {})
        job_label = metric.get("job", "unknown")
        instance_label = metric.get("instance", "unknown")
        region_label = metric.get("region", "unknown")
        status = item.get("value", [])[1]

        region_set.add(region_label)

        if job_label not in result:
            result[job_label] = {"up": [], "down": []}

        if status == "1":
            result[job_label]["up"].append(instance_label)
        else:
            result[job_label]["down"].append(instance_label)

    # --- Status Filtering Logic ---
    if status_filter.lower() == "up":
        for job_name in result:
            result[job_name]["down"] = []
    elif status_filter.lower() == "down":
        for job_name in result:
            result[job_name]["up"] = []

    # --- Optional: Fetch containers per instance if job is cadvisor ---
    container_names = {}
    if job == "cadvisor":
        container_labels = [f'org="{org}"']
        if region:
            if "*" in region or "." in region or region.startswith("("):
                container_labels.append(f'region=~"{region}"')
            else:
                container_labels.append(f'region="{region}"')
        if instance:
            container_labels.append(f'instance=~"{instance}"')
        container_labels.append('job="cadvisor"')

        match_str = f'{{{",".join(container_labels)}}}'

        try:
            series_response = requests.get(
                f"{VICTORIA_BASE_URL}/api/v1/series",
                params={"match[]": f'container_cpu_usage_seconds_total{match_str}'},
                timeout=10
            )
            series_response.raise_for_status()
            series_data = series_response.json().get("data", [])

            for item in series_data:
                inst = item.get("instance")
                name = item.get("name")
                if inst and name:
                    container_names.setdefault(inst, set()).add(name)

            # Convert sets to sorted lists
            for inst in container_names:
                container_names[inst] = sorted(container_names[inst])

        except Exception as e:
            print(f"[Warning] Failed to fetch container names: {str(e)}")

    # --- Final Response ---
    return {
        "filters": {
            "org": org,
            "region": region,
            "job": job,
            "instance": instance,
            "env": env,
            "status_filter": status_filter
        },
        "query": promql,
        "matched_regions": sorted(list(region_set)),
        "jobs": result,
        "containers": container_names if job == "cadvisor" else {}
    }


@app.get("/metrics/list")
def list_metrics(
    org: str = Query(...),
    region: str = Query(...),
    job: Optional[str] = Query(None),
    instance: Optional[str] = Query(None)
):
    labels = [f'org="{org}"', f'region="{region}"']
    if job:
        labels.append(f'job="{job}"')
    if instance:
        labels.append(f'instance=~"{instance}"')

    match_param = f'{{{",".join(labels)}}}' if labels else "{}"

    try:
        response = requests.get(
            f"{VICTORIA_BASE_URL}/api/v1/series",
            params={"match[]": match_param},
            timeout=10
        )
        response.raise_for_status()
        data = response.json()

        metrics = sorted({item["__name__"] for item in data.get("data", []) if "__name__" in item})

        return {
            "filters": {
                "org": org,
                "region": region,
                "job": job,
                "instance": instance
            },
            "match_param": match_param,
            "metrics": metrics
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch metrics: {str(e)}")

#@app.websocket("/ws/metrics/query")
async def websocket_metrics_query(websocket: WebSocket):
    await websocket.accept()
    try:
        payload = await websocket.receive_json()
        metric = payload.get("metric")
        org = payload.get("org")
        region = payload.get("region")
        instance = payload.get("instance")
        name_regex = payload.get("name_regex")
        job = payload.get("job")
        use_rate = payload.get("use_rate", False)
        window = payload.get("window", "5m")
        agg = payload.get("agg", "sum")
        group_by = payload.get("group_by")
        multiply = payload.get("multiply")
        interval = int(payload.get("interval", 10))
        mode = payload.get("mode", "instant")

        promql = build_advanced_promql(
            metric, org, region, instance, name_regex, job, use_rate, window, agg, group_by, multiply
        )

        while True:
            data = query_prometheus(promql, mode)
            await websocket.send_text(json.dumps({
                "query": promql,
                "result": data.get("data", {})
            }))
            await asyncio.sleep(interval)

    except WebSocketDisconnect:
        print("WebSocket disconnected")
    except Exception as e:
        await websocket.send_json({"error": str(e)})
        await websocket.close()


class MetricQuery(BaseModel):
    panel_id: str
    metric: str
    org: str
    region: str
    job: Optional[str] = None
    instance: Optional[str] = None
    name_regex: Optional[str] = None
    use_rate: bool = False
    window: Optional[str] = "5m"
    agg: Optional[str] = "sum"
    group_by: Optional[str] = None
    multiply: Optional[int] = None
    mode: Optional[str] = "instant"
    start: Optional[str] = None
    end: Optional[str] = None
    step: Optional[str] = "60s"


#@app.post("/metrics/dashboard")
async def dashboard_metrics(queries: List[MetricQuery]):
    response = {}

    for q in queries:
        try:
            promql = build_advanced_promql(
                q.metric, q.org, q.region, q.instance, q.name_regex,
                q.job, q.use_rate, q.window, q.agg, q.group_by, q.multiply
            )
            data = query_prometheus(promql, q.mode, q.start, q.end, q.step)
            response.setdefault(q.panel_id, []).append({
                "query": promql,
                "result": data.get("data", {})
            })
        except Exception as e:
            response.setdefault(q.panel_id, []).append({"error": str(e)})

    return response

def query(promql: str) -> dict:
    url = f"{VICTORIA_BASE_URL}/api/v1/query?query={quote(promql)}"
    res = requests.get(url)
    return res.json()

@app.websocket("/ws/metrics/dashboard")
async def websocket_dashboard(websocket: WebSocket):
    await websocket.accept()
    config = {
        "dashboard_id": None,
        "interval": 10,
        "filters": {},
        "groups": []
    }

    try:
        init_data = await websocket.receive_json()
        config["dashboard_id"] = init_data.get("dashboard_id")
        config["interval"] = init_data.get("interval", 10)
        config["filters"] = init_data.get("filters", {})
        config["groups"] = init_data.get("groups", [])

        print(f"[WebSocket Connected] Dashboard: {config['dashboard_id']}")

        while True:
            try:
                incoming = await asyncio.wait_for(websocket.receive_json(), timeout=0.1)

                if incoming.get("type") == "update_filters":
                    new_filters = incoming.get("filters", {})
                    if isinstance(new_filters, dict):
                        config["filters"].update(new_filters)
                        await websocket.send_text(json.dumps({
                            "type": "filter_ack",
                            "message": "Filters updated",
                            "updated_filters": config["filters"],
                            "timestamp": int(time.time()),
                            "iso_timestamp": datetime.datetime.utcnow().isoformat() + "Z"
                        }))
                    else:
                        await websocket.send_text(json.dumps({
                            "type": "error",
                            "message": "Invalid filters format"
                        }))
                elif incoming.get("type") == "update_interval":
                    new_interval = incoming.get("interval")
                    if isinstance(new_interval, int) and new_interval > 0:
                        config["interval"] = new_interval
                        await websocket.send_text(json.dumps({
                            "type": "interval_ack",
                            "message": f"Interval updated to {new_interval} seconds",
                            "interval": new_interval,
                            "timestamp": int(time.time())
                        }))
                    else:
                        await websocket.send_text(json.dumps({
                            "type": "error",
                            "message": "Invalid interval value"
                        }))

            except asyncio.TimeoutError:
                pass
            except json.JSONDecodeError:
                await websocket.send_text(json.dumps({"error": "Invalid JSON format"}))
                continue

            now_ts = int(time.time())
            now_iso = datetime.datetime.utcfromtimestamp(now_ts).isoformat() + "Z"
            full_response = {
                "timestamp": now_ts,
                "timestamp_iso": now_iso,
                "dashboard_id": config["dashboard_id"],
                "results": []
            }

            for group in config["groups"]:
                group_name = group.get("group_name")
                panels = group.get("panels", [])
                group_result = {"group_name": group_name, "panels": []}

                for panel in panels:
                    panel_id = panel.get("panel_id")
                    queries = panel.get("queries", [])
                    panel_result = {"panel_id": panel_id, "queries": []}

                    for query_def in queries:
                        try:
                            promql = build_advanced_promql(
                                query_def.get("metric"),
                                config["filters"].get("org"),
                                config["filters"].get("region"),
                                config["filters"].get("instance"),
                                config["filters"].get("name_regex"),
                                config["filters"].get("job"),
                                query_def.get("use_rate", False),
                                query_def.get("window", "5m"),
                                query_def.get("agg", "sum"),
                                query_def.get("group_by"),
                                query_def.get("multiply"),
                                query_def.get("custom_expression"),
                                config["filters"]
                            )

                            result = query(promql)

                            for metric_result in result.get("data", {}).get("result", []):
                                value = metric_result.get("value")
                                if isinstance(value, list) and len(value) >= 1:
                                    try:
                                        ts = int(float(value[0]))
                                        iso_ts = datetime.datetime.utcfromtimestamp(ts).isoformat() + "Z"
                                        metric_result["iso_timestamp"] = iso_ts
                                    except Exception:
                                        metric_result["iso_timestamp"] = None

                            panel_result["queries"].append({
                                "promql": promql,
                                "data": result.get("data", {})
                            })

                        except Exception as e:
                            panel_result["queries"].append({
                                "promql": None,
                                "error": str(e)
                            })

                    group_result["panels"].append(panel_result)

                full_response["results"].append(group_result)

            await websocket.send_text(json.dumps(full_response))
            await asyncio.sleep(config["interval"])

    except WebSocketDisconnect:
        print(f"[WebSocket Disconnected] Dashboard: {config['dashboard_id']}")
    except Exception as e:
        await websocket.send_text(json.dumps({"type": "error", "message": str(e)}))
