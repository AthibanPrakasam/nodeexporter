import os
import httpx
from dotenv import load_dotenv

load_dotenv()
VM_URL = os.getenv("VM_URL")

def query_victoriametrics(promql: str) -> dict:
    url = f"{VM_URL}/api/v1/query"
    params = {"query": promql}
    try:
        response = httpx.get(url, params=params, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        return {"error": str(e)}

def summarize_response(response: dict) -> str:
    if "data" not in response or "result" not in response["data"]:
        return "❌ No data found or error in response."

    results = response["data"]["result"]
    if not results:
        return "✅ Query returned no results (clean metrics)."

    summary_lines = []
    for r in results:
        metric = r.get("metric", {})
        value = r.get("value", [])
        line = f"{metric.get('instance', 'unknown')} ➤ {value[1]} at {value[0]}"
        summary_lines.append(line)
    return "\n".join(summary_lines)
