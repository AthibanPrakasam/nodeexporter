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

def match_operator(value: str) -> str:
    return "=~" if ".*" in value else "="

def build_advanced_promql(
    metric: str,
    orgid: str,
    anchorid: str,
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
    labels = [
        f'orgid{match_operator(orgid)}"{orgid}"',
        f'anchorid{match_operator(anchorid)}"{anchorid}"'
    ]
    if instance:
        labels.append(f'instance{match_operator(instance)}"{instance}"')
    if name_regex:
        labels.append(f'name{match_operator(name_regex)}"{name_regex}"')
    if job:
        labels.append(f'job{match_operator(job)}"{job}"')

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
        custom_expression = custom_expression.replace("$__rate_interval", window)
        for k, v in (filters or {}).items():
            if k in ["anchorid", "orgid", "instance", "job", "name_regex"]:
                op = match_operator(str(v))
                custom_expression = custom_expression.replace(f'{k}="$' + f'{k}"', f'{k}{op}"{v}"')
                custom_expression = custom_expression.replace(f"${k}", str(v))
            else:
                custom_expression = custom_expression.replace(f"${k}", str(v))
        base = custom_expression.replace("$base", base)

        if "up{" in base and "or on(instance) vector(0)" not in base:
            base = f"({base}) or on(instance) vector(0)"
        elif "* on(instance) group_left up{" in base and "or on(instance) vector(0)" not in base:
            base = f"({base}) or on(instance) vector(0)"

    if multiply:
        base = f"{base} * {multiply}"

    if "[30s]" in base or "[5m]" in base:
        now = int(time.time())
        base += f" @{now}"

    return base

@app.get("/time")
def get_time():
    now_ts = int(time.time())
    now_utc = datetime.datetime.utcfromtimestamp(now_ts).isoformat() + "Z"
    now_ist = datetime.datetime.fromtimestamp(now_ts + 19800).strftime("%Y-%m-%d %H:%M:%S IST")
    return {
        "timestamp": now_ts,
        "utc": now_utc,
        "ist": now_ist
    }

def query(promql: str) -> dict:
    url = f"{VICTORIA_BASE_URL}/api/v1/query?query={quote(promql)}"
    res = requests.get(url)
    return res.json()

def query_prometheus(promql: str, mode: str, start: Optional[str] = None, end: Optional[str] = None, step: str = "60s") -> dict:
    if mode == "range":
        if not start or not end:
            raise HTTPException(status_code=422, detail="start and end time required")
        params = {"query": promql, "start": start, "end": end, "step": step}
        url = f"{VICTORIA_BASE_URL}/api/v1/query_range"
    else:
        params = {"query": promql}
        url = f"{VICTORIA_BASE_URL}/api/v1/query"
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()

    # Optional check for freshness (only for instant queries)
    if mode == "instant" and data.get("data", {}).get("result"):
        now = int(time.time())
        ts = int(float(data["data"]["result"][0]["value"][0]))
        if abs(now - ts) > 60:
            # Treat as stale
            data["data"]["result"] = []

    return data

# ------------------------------------------------
# ✅ Unified Metrics Query API
# ------------------------------------------------
@app.get("/metrics/query")
def dynamic_metrics_query(
    metric: str = Query(..., description="Prometheus metric name"),
    orgid: str = Query(..., description="orgid label (required)"),
    anchorid: str = Query(..., description="anchorid label (required)"),
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
            metric, orgid, anchorid, instance, name_regex, job,
            use_rate, window, agg, group_by, multiply
        )
        data = query_prometheus(promql, mode, start, end, step)

        return {
            "filters_used": {
                "metric": metric,
                "orgid": orgid,
                "anchorid": anchorid,
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
    orgid: str = Query(..., description="Organization label"),
    anchorid: Optional[str] = Query(None, description="anchorid label (optional, omit to fetch all anchorid)"),
    job: Optional[str] = Query(None),
    instance: Optional[str] = Query(None),
    env: Optional[str] = Query(None),
    status_filter: Optional[str] = Query("all", description="Filter by status: up, down, or all")
):
    # --- Label Filtering Logic ---
    labels = [f'orgid="{orgid}"']

    if anchorid:
        # Regex or literal
        if "*" in anchorid or "." in anchorid or anchorid.startswith("("):
            labels.append(f'anchorid=~"{anchorid}"')
        else:
            labels.append(f'anchorid="{anchorid}"')

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
    anchorid_set = set()

    for item in data.get("data", {}).get("result", []):
        metric = item.get("metric", {})
        job_label = metric.get("job", "unknown")
        instance_label = metric.get("instance", "unknown")
        anchorid_label = metric.get("anchorid", "unknown")
        status = item.get("value", [])[1]

        anchorid_set.add(anchorid_label)

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
        container_labels = [f'orgid="{orgid}"']
        if anchorid:
            if "*" in anchorid or "." in anchorid or anchorid.startswith("("):
                container_labels.append(f'anchorid=~"{anchorid}"')
            else:
                container_labels.append(f'anchorid="{anchorid}"')
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
            "orgid": orgid,
            "anchorid": anchorid,
            "job": job,
            "instance": instance,
            "env": env,
            "status_filter": status_filter
        },
        "query": promql,
        "matched_anchorid": sorted(list(anchorid_set)),
        "jobs": result,
        "containers": container_names if job == "cadvisor" else {}
    }


@app.get("/metrics/list")
def list_metrics(
    orgid: str = Query(...),
    anchorid: str = Query(...),
    job: Optional[str] = Query(None),
    instance: Optional[str] = Query(None)
):
    labels = [f'orgid="{orgid}"', f'anchorid="{anchorid}"']
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
                "orgid": orgid,
                "anchorid": anchorid,
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
        orgid = payload.get("orgid")
        anchorid = payload.get("anchorid")
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
            metric, orgid, anchorid, instance, name_regex, job, use_rate, window, agg, group_by, multiply
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
    orgid: str
    anchorid: str
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


@app.websocket("/ws/metrics/dashboard")
async def websocket_dashboard(websocket: WebSocket):
    await websocket.accept()
    config = {"dashboard_id": None, "interval": 10, "filters": {}, "groups": []}
    last_up_time = {}

    try:
        while True:
            try:
                message = await asyncio.wait_for(websocket.receive_json(), timeout=0.1)
                msg_type = message.get("type")
                if msg_type == "init":
                    config.update({
                        "dashboard_id": message.get("dashboard_id"),
                        "interval": message.get("interval", 10),
                        "filters": message.get("filters", {}),
                        "groups": message.get("groups", [])
                    })
                elif msg_type == "update_filters":
                    if "filters" in message:
                        config["filters"].update(message["filters"])
                    if "interval" in message:
                        config["interval"] = message["interval"]
            except asyncio.TimeoutError:
                pass  # no message, continue

            start_time = time.time()
            now_ts = int(start_time)
            now_iso = datetime.datetime.utcfromtimestamp(now_ts).isoformat() + "Z"

            full_response = {
                "timestamp": now_ts,
                "timestamp_iso": now_iso,
                "dashboard_id": config["dashboard_id"],
                "results": []
            }

            for group in config["groups"]:
                group_result = {"group_name": group.get("group_name"), "panels": []}
                for panel in group.get("panels", []):
                    panel_id = panel.get("panel_id")
                    panel_result = {"panel_id": panel_id, "queries": []}

                    for idx, q in enumerate(panel.get("queries", [])):
                        try:
                            promql = build_advanced_promql(
                                q.get("metric"),
                                config["filters"].get("orgid", ""),
                                config["filters"].get("anchorid", ""),
                                config["filters"].get("instance"),
                                config["filters"].get("name_regex"),
                                config["filters"].get("job"),
                                q.get("use_rate", False),
                                q.get("window", "5m"),
                                q.get("agg", "sum"),
                                q.get("group_by"),
                                q.get("multiply"),
                                q.get("custom_expression"),
                                config["filters"]
                            )

                            result = query(promql)
                            result_data = result.get("data", {})
                            result_list = result_data.get("result", [])

                            # ---- Staleness check: treat result as stale if timestamp is >60s old
                            if result_list:
                                try:
                                    ts = float(result_list[0]["value"][0])
                                    if now_ts - ts > 60:
                                        result_list = []
                                        result_data["result"] = []
                                except Exception as e:
                                    print(f"[Warning] Failed staleness check: {e}")

                            query_key = f"{config['dashboard_id']}::{panel_id}::{idx}"
                            if result_list:
                                status = "up"
                                last_up_time[query_key] = now_iso
                            else:
                                status = "down"

                            panel_result["queries"].append({
                                "promql": promql,
                                "data": result_data,
                                "status": status,
                                "last_seen_up": last_up_time.get(query_key)
                            })

                        except Exception as e:
                            panel_result["queries"].append({
                                "promql": None,
                                "error": str(e),
                                "status": "error"
                            })

                    group_result["panels"].append(panel_result)
                full_response["results"].append(group_result)

            await websocket.send_text(json.dumps(full_response))

            elapsed = time.time() - start_time
            delay = max(0, config["interval"] - elapsed)
            await asyncio.sleep(delay)

    except WebSocketDisconnect:
        print(f"Dashboard disconnected: {config['dashboard_id']}")
    except Exception as e:
        await websocket.send_text(json.dumps({
            "type": "error",
            "message": str(e)
        }))