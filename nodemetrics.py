
# --- Optimized Changes Applied ---
# Note: All route paths remain unchanged
# Optimizations include:
# - Session reuse with `requests.Session`
# - Async HTTP where feasible for live querying
# - Minor efficiency tweaks (e.g., `orjson` encoding, smarter timeout/interval control)

from fastapi import FastAPI, Query, HTTPException, WebSocket, WebSocketDisconnect
import asyncio
import time
import datetime
from typing import Optional, List
from urllib.parse import quote
import orjson
import httpx  # Async requests
from fastapi.middleware.cors import CORSMiddleware
from cachetools import TTLCache
from typing import Optional, List, Union
import aiohttp


app = FastAPI()

VICTORIA_BASE_URL = "http://34.131.24.129:8428"
session = aiohttp.ClientSession()
query_cache = {}

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def match_operator(value: str) -> str:
    return "=~" if ".*" in value else "="


async def query_prometheus(promql: str, mode: str, start: Optional[str] = None, end: Optional[str] = None, step: str = "60s") -> dict:
    try:
        if mode == "range":
            if not start or not end:
                raise HTTPException(status_code=422, detail="start and end required")
            url = f"{VICTORIA_BASE_URL}/api/v1/query_range"
            params = {"query": promql, "start": start, "end": end, "step": step}
        else:
            url = f"{VICTORIA_BASE_URL}/api/v1/query"
            params = {"query": promql}

        resp = await session.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()

        if mode == "instant" and data.get("data", {}).get("result"):
            ts = float(data["data"]["result"][0]["value"][0])
            if abs(time.time() - ts) > 60:
                data["data"]["result"] = []
        return data
    except httpx.RequestError as e:
        raise HTTPException(status_code=500, detail=str(e))

# ------------------------------------------------
# ✅ Unified Metrics Query API
# ------------------------------------------------
@app.get("/metrics/query")
async def dynamic_metrics_query(
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
        data = await query_prometheus(promql, mode, start, end, step)

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
async def get_job_status(
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
        response = await session.get(f"{VICTORIA_BASE_URL}/api/v1/query", params={"query": promql})
        response.raise_for_status()
        data = response.json()
    except httpx.RequestError as e:
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
            series_response = await session.get(
                f"{VICTORIA_BASE_URL}/api/v1/series",
                params={"match[]": f'container_cpu_usage_seconds_total{match_str}'}
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
async def list_metrics(
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
        response = await session.get(
            f"{VICTORIA_BASE_URL}/api/v1/series",
            params={"match[]": match_param}
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


@app.websocket("/ws/metrics/query")
async def websocket_metrics_query(websocket: WebSocket):
    await websocket.accept()
    try:
        payload = await websocket.receive_json()
        interval = int(payload.get("interval", 10))
        promql = build_advanced_promql(
            payload["metric"], payload["orgid"], payload["anchorid"],
            payload.get("instance"), payload.get("name_regex"), payload.get("job"),
            payload.get("use_rate", False), payload.get("window", "5m"),
            payload.get("agg", "sum"), payload.get("group_by"), payload.get("multiply")
        )

        while True:
            start = time.time()
            result = await query_prometheus(promql, payload.get("mode", "instant"))
            await websocket.send_text(orjson.dumps({
                "query": promql,
                "result": result.get("data", {})
            }).decode("utf-8"))
            await asyncio.sleep(max(0, interval - (time.time() - start)))

    except WebSocketDisconnect:
        print("/ws/metrics/query disconnected")
    except Exception as e:
        await websocket.send_json({"error": str(e)})
        await websocket.close()

# Cache for query results (TTL: 2 seconds)
query_cache = TTLCache(maxsize=100, ttl=2)

@app.websocket("/ws/metrics/dashboard")
async def websocket_dashboard(websocket: WebSocket):
    await websocket.accept()
    config = {"dashboard_id": None, "interval": 10, "filters": {}, "groups": []}
    last_up_time = {}

    try:
        while True:
            try:
                message = await asyncio.wait_for(websocket.receive_json(), timeout=0.1)
                if message.get("type") == "init":
                    config.update({
                        "dashboard_id": message.get("dashboard_id"),
                        "interval": message.get("interval", 10),
                        "filters": message.get("filters", {}),
                        "groups": message.get("groups", [])
                    })
                elif message.get("type") == "update_filters":
                    config["filters"].update({k: v for k, v in message.get("filters", {}).items() if k in {"orgid", "anchorid", "instance", "job", "name_regex"}})
                    if "interval" in message:
                        config["interval"] = message["interval"]
            except asyncio.TimeoutError:
                pass

            now_ts = int(time.time())
            full_response = {
                "timestamp": now_ts,
                "timestamp_iso": datetime.datetime.utcfromtimestamp(now_ts).isoformat() + "Z",
                "dashboard_id": config["dashboard_id"],
                "results": []
            }

            async def execute_query(panel, query, idx):
                try:
                    promql = build_advanced_promql(
                        query.get("metric", ""),
                        config["filters"].get("orgid", ""),
                        config["filters"].get("anchorid", ""),
                        config["filters"].get("instance"),
                        config["filters"].get("name_regex"),
                        config["filters"].get("job"),
                        query.get("use_rate", False),
                        query.get("window", "5m"),
                        query.get("agg", "sum"),
                        query.get("group_by"),
                        query.get("multiply"),
                        query.get("custom_expression"),
                        config["filters"]
                    )

                    # Check cache
                    cache_key = f"{promql}::{query.get('mode', 'instant')}"
                    if cache_key in query_cache:
                        result = query_cache[cache_key]
                    else:
                        result = await query_prometheus(promql, query.get("mode", "instant"))
                        query_cache[cache_key] = result

                    res_data = result.get("data", {})
                    res_list = res_data.get("result", [])

                    if res_list:
                        ts = float(res_list[0]["value"][0])
                        query_key = f"{config['dashboard_id']}::{panel['panel_id']}::{idx}"
                        status = "up" if now_ts - ts <= 60 else "down"
                        if status == "up":
                            last_up_time[query_key] = full_response["timestamp_iso"]

                        return {
                            "promql": promql,
                            "data": res_data,
                            "status": status,
                            "last_seen_up": last_up_time.get(query_key)
                        }
                    else:
                        return {
                            "promql": promql,
                            "data": res_data,
                            "status": "down",
                            "error": "No results found"
                        }
                except Exception as e:
                    return {
                        "status": "error",
                        "error": str(e)
                    }

            for group in config["groups"]:
                group_result = {"group_name": group.get("group_name"), "panels": []}
                for panel in group.get("panels", []):
                    queries = panel.get("queries", [])
                    tasks = [execute_query(panel, q, idx) for idx, q in enumerate(queries)]
                    panel_result = {"panel_id": panel.get("panel_id"), "queries": await asyncio.gather(*tasks)}
                    group_result["panels"].append(panel_result)
                full_response["results"].append(group_result)

            await websocket.send_text(orjson.dumps(full_response).decode("utf-8"))
            await asyncio.sleep(config["interval"])

    except WebSocketDisconnect:
        print(f"Dashboard disconnected: {config['dashboard_id']}")
    except Exception as e:
        await websocket.send_text(orjson.dumps({"type": "error", "message": str(e)}).decode("utf-8"))




def build_filter_expr(value: Union[str, List[str]], label: str) -> str:
    if not value or value == "All":
        return f'{label}=~".*"'
    elif isinstance(value, list):
        joined = "|".join(str(v) for v in value)
        return f'{label}=~"{joined}"'
    else:
        return f'{label}="{value}"'


def build_advanced_promql(metric: str, orgid: str, anchorid: str, instance=None,
                          name_regex=None, job=None, use_rate=False, window="5m",
                          agg="sum", group_by=None, multiply=None, custom_expression=None,
                          filters=None) -> str:
    filters = filters or {}

    # If there is a custom expression, replace variables like $instance, $orgid, etc.
    if custom_expression:
        expr = custom_expression
        for var in ["saddr", "sport", "daddr", "dport", "orgid", "anchorid", "instance", "job"]:
            val = filters.get(var)
            if isinstance(val, list):
                val = "|".join(val)
                expr = expr.replace(f"${var}", val)
            elif val:
                expr = expr.replace(f"${var}", str(val))
            else:
                # fallback to match all regex
                expr = expr.replace(f"${var}", ".*")
        return expr

    # Otherwise, build standard PromQL metric
    filter_parts = [
        build_filter_expr(filters.get("saddr"), "saddr"),
        build_filter_expr(filters.get("sport"), "sport"),
        build_filter_expr(filters.get("daddr"), "daddr"),
        build_filter_expr(filters.get("dport"), "dport"),
        build_filter_expr(orgid, "orgid"),
        build_filter_expr(anchorid, "anchorid"),
        build_filter_expr(instance, "instance"),
        build_filter_expr(job, "job")
    ]
    filter_str = ",".join(filter_parts)
    promql = f"{metric}{{{filter_str}}}"

    if use_rate:
        promql = f"rate({promql}[{window}])"
    if agg:
        promql = f"{agg}({promql})"
    if group_by:
        promql += f" by ({','.join(group_by)})"
    if multiply:
        promql += f" * {multiply}"

    return promql


# ------------------------------
# Helper: Fetch label values from VictoriaMetrics
# ------------------------------
async def fetch_label_values(label: str, filters: dict, dependent_filters: dict = None):
    query_filters = filters.copy()
    if dependent_filters:
        query_filters.update(dependent_filters)

    filter_parts = []
    for k, v in query_filters.items():
        if not v or v == "All":
            filter_parts.append(f'{k}=~".*"')
        elif isinstance(v, list):
            filter_parts.append(f'{k}=~"{ "|".join(v) }"')
        else:
            filter_parts.append(f'{k}="{v}"')
    filter_str = ",".join(filter_parts)

    promql = f'tcp_session_last_seen_seconds{{{filter_str}}}'
    try:
        async with session.get(f"{VICTORIA_BASE_URL}/api/v1/series", params={"match[]": promql}) as resp:
            resp.raise_for_status()
            data = await resp.json()
            series = data.get("data", [])
            values = sorted({item.get(label) for item in series if label in item})
            return values if values else ["All"]
    except Exception as e:
        print(f"[Warning] fetch_label_values error: {str(e)}")
        return ["All"]

# ------------------------------
# WebSocket dashboard handler
# ------------------------------
@app.websocket("/ws/metrics/ebpfdashboard")
async def websocket_dashboard(websocket: WebSocket):
    await websocket.accept()
    config = {"dashboard_id": None, "interval": 10, "filters": {}, "groups": []}
    last_up_time = {}

    try:
        while True:
            try:
                message = await asyncio.wait_for(websocket.receive_json(), timeout=0.1)

                if message.get("type") == "init":
                    config.update({
                        "dashboard_id": message.get("dashboard_id"),
                        "interval": message.get("interval", 10),
                        "filters": message.get("filters", {}),
                        "groups": message.get("groups", [])
                    })
                    for f in ["saddr", "sport", "daddr", "dport"]:
                        config["filters"].setdefault(f, ".*")

                elif message.get("type") == "update_filters":
                    for k, v in message.get("filters", {}).items():
                        if k in {"orgid", "anchorid", "instance", "job", "name_regex", "saddr", "sport", "daddr", "dport"}:
                            config["filters"][k] = v
                    if "interval" in message:
                        config["interval"] = message["interval"]

            except asyncio.TimeoutError:
                pass

            now_ts = int(time.time())
            full_response = {
                "timestamp": now_ts,
                "timestamp_iso": datetime.datetime.utcfromtimestamp(now_ts).isoformat() + "Z",
                "dashboard_id": config["dashboard_id"],
                "results": []
            }

            filters = config["filters"]

            async def execute_query(panel, query, idx):
                try:
                    promql = build_advanced_promql(
                        query.get("metric", ""),
                        filters.get("orgid", ""),
                        filters.get("anchorid", ""),
                        filters.get("instance"),
                        filters.get("name_regex"),
                        filters.get("job"),
                        query.get("use_rate", False),
                        query.get("window", "5m"),
                        query.get("agg", "sum"),
                        query.get("group_by"),
                        query.get("multiply"),
                        query.get("custom_expression"),
                        filters
                    )

                    cache_key = f"{promql}::{query.get('mode', 'instant')}"
                    if cache_key in query_cache:
                        result = query_cache[cache_key]
                    else:
                        async with session.get(f"{VICTORIA_BASE_URL}/api/v1/query", params={"query": promql}) as resp:
                            resp.raise_for_status()
                            result = await resp.json()
                            query_cache[cache_key] = result

                    res_list = result.get("data", {}).get("result", [])
                    if res_list:
                        ts = float(res_list[0]["value"][0])
                        query_key = f"{config['dashboard_id']}::{panel['panel_id']}::{idx}"
                        status = "up" if now_ts - ts <= 60 else "down"
                        if status == "up":
                            last_up_time[query_key] = full_response["timestamp_iso"]
                        return {
                            "promql": promql,
                            "data": result.get("data", {}),
                            "status": status,
                            "last_seen_up": last_up_time.get(query_key)
                        }
                    else:
                        return {
                            "promql": promql,
                            "data": result.get("data", {}),
                            "status": "down",
                            "error": "No results found"
                        }
                except Exception as e:
                    return {"status": "error", "error": str(e)}

            for group in config["groups"]:
                group_result = {"group_name": group.get("group_name"), "panels": []}
                for panel in group.get("panels", []):
                    queries = panel.get("queries", [])
                    tasks = [execute_query(panel, q, idx) for idx, q in enumerate(queries)]
                    panel_result = {"panel_id": panel.get("panel_id"), "queries": await asyncio.gather(*tasks)}
                    group_result["panels"].append(panel_result)
                full_response["results"].append(group_result)

            await websocket.send_text(orjson.dumps(full_response).decode("utf-8"))
            await asyncio.sleep(config["interval"])

    except WebSocketDisconnect:
        print(f"Dashboard disconnected: {config['dashboard_id']}")
    except Exception as e:
        await websocket.send_text(orjson.dumps({"type": "error", "message": str(e)}).decode("utf-8"))

# ------------------------------
# API to fetch label values (multi-select ready)
# ------------------------------
@app.get("/metrics/ebpf/labels")
async def get_ebpf_label_values(
    orgid: str = Query(...),
    anchorid: str = Query(...),
    instance: str = Query(...),
    job: str = Query(...),
    saddr: Optional[List[str]] = Query(None),
    sport: Optional[List[str]] = Query(None),
    daddr: Optional[List[str]] = Query(None)
):
    filters = {"orgid": orgid, "anchorid": anchorid, "instance": instance, "job": job}

    saddr_filter = saddr or ".*"
    sport_filter = sport or ".*"
    daddr_filter = daddr or ".*"

    saddr_values = await fetch_label_values("saddr", filters)
    sport_values = await fetch_label_values("sport", filters, {"saddr": saddr_filter})
    daddr_values = await fetch_label_values("daddr", filters, {"saddr": saddr_filter, "sport": sport_filter})
    dport_values = await fetch_label_values("dport", filters, {"saddr": saddr_filter, "sport": sport_filter, "daddr": daddr_filter})

    return {"saddr": saddr_values, "sport": sport_values, "daddr": daddr_values, "dport": dport_values}
