"""
Poorvika Meta Ads MCP Server - Minimal stable version
"""
import subprocess, sys

# Auto-install
for pkg in ["httpx", "mcp[cli]", "uvicorn[standard]", "starlette"]:
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", pkg, "-q", "--root-user-action=ignore"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )

import os, json
from datetime import datetime, timedelta
import httpx
import uvicorn
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route, Mount
from starlette.requests import Request
from mcp.server import Server
from mcp.server.sse import SseServerTransport
from mcp import types

# ── Config ────────────────────────────────────────────────────────────────────
META_BASE     = "https://graph.facebook.com/v19.0"
ACCESS_TOKEN  = os.environ.get("META_ACCESS_TOKEN", "")
AD_ACCOUNT_ID = os.environ.get("META_AD_ACCOUNT_ID", "")
PORT          = int(os.environ.get("PORT", 8000))

# ── MCP Server ────────────────────────────────────────────────────────────────
server = Server("poorvika-meta-ads")

# ── Helpers ───────────────────────────────────────────────────────────────────
def api_get(path, params=None):
    p = params or {}
    p["access_token"] = ACCESS_TOKEN
    r = httpx.get(f"{META_BASE}{path}", params=p, timeout=30)
    r.raise_for_status()
    return r.json()

def api_post(path, data=None):
    d = data or {}
    d["access_token"] = ACCESS_TOKEN
    r = httpx.post(f"{META_BASE}{path}", data=d, timeout=30)
    r.raise_for_status()
    return r.json()

def dr(days=7):
    t = datetime.utcnow().date()
    return {"since": str(t - timedelta(days=days)), "until": str(t)}

def parse_name(name):
    parts = [p.strip() for p in name.split("|")]
    keys  = ["main_category","category","sub_category","objective",
             "creative_type","creative_type_2","cta","location","date","staff_code"]
    return {keys[i]: parts[i] if i < len(parts) else "" for i in range(len(keys))}

# ── Tool Definitions ──────────────────────────────────────────────────────────
@server.list_tools()
async def list_tools():
    return [
        types.Tool(name="get_campaigns",         description="Get campaigns from Poorvika Meta Ads account. Use limit to control how many (default 25, max 50).", inputSchema={"type":"object","properties":{"status_filter":{"type":"string","default":"ACTIVE"},"limit":{"type":"integer","default":25}}}),
        types.Tool(name="get_account_insights",  description="Get account-level performance summary",            inputSchema={"type":"object","properties":{"days_back":{"type":"integer","default":7}}}),
        types.Tool(name="get_campaign_insights", description="Get insights for a specific campaign",             inputSchema={"type":"object","properties":{"campaign_id":{"type":"string"},"days_back":{"type":"integer","default":7}},"required":["campaign_id"]}),
        types.Tool(name="get_adsets",            description="List all ad sets under a campaign",                inputSchema={"type":"object","properties":{"campaign_id":{"type":"string"}},"required":["campaign_id"]}),
        types.Tool(name="get_daily_report",      description="Full daily performance report for Poorvika",       inputSchema={"type":"object","properties":{"days_back":{"type":"integer","default":7}}}),
        types.Tool(name="get_top_campaigns",     description="Top N campaigns by metric (ctr/cpc/spend)",        inputSchema={"type":"object","properties":{"days_back":{"type":"integer","default":7},"metric":{"type":"string","default":"ctr"},"top_n":{"type":"integer","default":5}}}),
        types.Tool(name="pause_campaign",        description="Pause a campaign by ID",                           inputSchema={"type":"object","properties":{"campaign_id":{"type":"string"}},"required":["campaign_id"]}),
        types.Tool(name="resume_campaign",       description="Resume a paused campaign by ID",                   inputSchema={"type":"object","properties":{"campaign_id":{"type":"string"}},"required":["campaign_id"]}),
        types.Tool(name="pause_adset",           description="Pause a specific ad set by ID",                    inputSchema={"type":"object","properties":{"adset_id":{"type":"string"}},"required":["adset_id"]}),
        types.Tool(name="resume_adset",          description="Resume a paused ad set by ID",                     inputSchema={"type":"object","properties":{"adset_id":{"type":"string"}},"required":["adset_id"]}),
        types.Tool(name="update_campaign_budget",description="Update campaign daily budget in INR",              inputSchema={"type":"object","properties":{"campaign_id":{"type":"string"},"new_daily_budget_inr":{"type":"number"}},"required":["campaign_id","new_daily_budget_inr"]}),
        types.Tool(name="update_adset_budget",   description="Update ad set daily budget in INR",                inputSchema={"type":"object","properties":{"adset_id":{"type":"string"},"new_daily_budget_inr":{"type":"number"}},"required":["adset_id","new_daily_budget_inr"]}),
        types.Tool(name="get_spend_by_objective",description="Spend breakdown by objective (REACH/TRAFFIC etc)", inputSchema={"type":"object","properties":{"days_back":{"type":"integer","default":30}}}),
        types.Tool(name="validate_campaign_name",description="Validate Poorvika pipe-delimited campaign name",   inputSchema={"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}),
        types.Tool(name="search_campaign_by_name", description="Search campaigns by name keyword, returns ID, name, status. Use this before pause/resume by name.", inputSchema={"type":"object","properties":{"name_query":{"type":"string"},"status_filter":{"type":"string","default":"ALL"}},"required":["name_query"]}),
    ]

# ── Tool Execution ─────────────────────────────────────────────────────────────
@server.call_tool()
async def call_tool(name, arguments):
    try:
        result = _execute(name, arguments)
    except Exception as e:
        result = json.dumps({"error": str(e)})
    return [types.TextContent(type="text", text=result)]

def _execute(name, args):
    if name == "get_campaigns":
        sf = args.get("status_filter", "ACTIVE")
        p  = {"fields": "id,name,status,objective,daily_budget,lifetime_budget", "limit": 100}
        if sf != "ALL":
            p["effective_status"] = json.dumps([sf])
        data = api_get(f"/{AD_ACCOUNT_ID}/campaigns", p)
        out  = []
        for c in data.get("data", []):
            out.append({"id": c["id"], "name": c.get("name"), "status": c.get("status"),
                        "objective": c.get("objective"),
                        "daily_budget_inr": int(c["daily_budget"])/100 if c.get("daily_budget") else None,
                        "parsed": parse_name(c.get("name",""))})
        return json.dumps({"total": len(out), "campaigns": out}, indent=2)

    elif name == "get_account_insights":
        days = args.get("days_back", 7)
        d    = api_get(f"/{AD_ACCOUNT_ID}/insights",
                       {"fields": "impressions,reach,clicks,spend,cpm,cpc,ctr,frequency",
                        "time_range": json.dumps(dr(days))})
        return json.dumps(d.get("data", [{}])[0], indent=2)

    elif name == "get_campaign_insights":
        d = api_get(f"/{args['campaign_id']}/insights",
                    {"fields": "impressions,reach,clicks,spend,cpm,cpc,ctr,frequency",
                     "time_range": json.dumps(dr(args.get("days_back", 7)))})
        return json.dumps(d.get("data", [{}])[0], indent=2)

    elif name == "get_adsets":
        d = api_get(f"/{args['campaign_id']}/adsets",
                    {"fields": "id,name,status,daily_budget,optimization_goal", "limit": 100})
        return json.dumps({"total": len(d.get("data",[])), "adsets": d.get("data",[])}, indent=2)

    elif name == "get_daily_report":
        days  = args.get("days_back", 7)
        tr    = json.dumps(dr(days))
        def sf(v):
            try: return float(v)
            except: return 0
        # Fetch account summary and top 20 campaigns in parallel-ish (sequential but lightweight)
        acct  = api_get(f"/{AD_ACCOUNT_ID}/insights",
                        {"fields":"impressions,reach,clicks,spend,cpm,cpc,ctr","time_range":tr}).get("data",[{}])[0]
        camps = api_get(f"/{AD_ACCOUNT_ID}/insights",
                        {"fields":"campaign_name,spend,clicks,ctr,cpc","time_range":tr,
                         "level":"campaign","limit":20}).get("data",[])
        by_spend = sorted(camps, key=lambda x: sf(x.get("spend",0)), reverse=True)
        return json.dumps({
            "date": str(datetime.utcnow().date()), "days": days,
            "summary": {"spend": f"INR {sf(acct.get('spend',0)):,.2f}",
                        "impressions": acct.get("impressions","0"),
                        "clicks": acct.get("clicks","0"),
                        "ctr": f"{sf(acct.get('ctr',0)):.2f}%",
                        "cpc": f"INR {sf(acct.get('cpc',0)):,.2f}"},
            "top_5": by_spend[:5], "bottom_5": by_spend[-5:]
        }, indent=2)

    elif name == "get_top_campaigns":
        days   = args.get("days_back", 7)
        metric = args.get("metric", "ctr")
        top_n  = args.get("top_n", 5)
        rows   = api_get(f"/{AD_ACCOUNT_ID}/insights",
                         {"fields":"campaign_name,spend,clicks,ctr,cpc,impressions",
                          "time_range":json.dumps(dr(days)),"level":"campaign","limit":30}).get("data",[])
        def sf(v):
            try: return float(v)
            except: return 0
        asc = metric in ["cpc","cpm"]
        return json.dumps(sorted(rows, key=lambda x: sf(x.get(metric,0)), reverse=not asc)[:top_n], indent=2)

    elif name == "pause_campaign":
        return json.dumps(api_post(f"/{args['campaign_id']}", {"status":"PAUSED"}))

    elif name == "resume_campaign":
        return json.dumps(api_post(f"/{args['campaign_id']}", {"status":"ACTIVE"}))

    elif name == "pause_adset":
        return json.dumps(api_post(f"/{args['adset_id']}", {"status":"PAUSED"}))

    elif name == "resume_adset":
        return json.dumps(api_post(f"/{args['adset_id']}", {"status":"ACTIVE"}))

    elif name == "update_campaign_budget":
        return json.dumps(api_post(f"/{args['campaign_id']}",
                                   {"daily_budget": int(args["new_daily_budget_inr"]*100)}))

    elif name == "update_adset_budget":
        return json.dumps(api_post(f"/{args['adset_id']}",
                                   {"daily_budget": int(args["new_daily_budget_inr"]*100)}))

    elif name == "get_spend_by_objective":
        rows = api_get(f"/{AD_ACCOUNT_ID}/insights",
                       {"fields":"objective,spend,impressions,clicks",
                        "time_range":json.dumps(dr(args.get("days_back",30))),
                        "level":"campaign","limit":100}).get("data",[])
        bd = {}
        for r in rows:
            o = r.get("objective","UNKNOWN")
            if o not in bd: bd[o] = {"spend":0,"impressions":0,"clicks":0,"count":0}
            bd[o]["spend"]       += float(r.get("spend",0))
            bd[o]["impressions"] += int(r.get("impressions",0))
            bd[o]["clicks"]      += int(r.get("clicks",0))
            bd[o]["count"]       += 1
        for o in bd: bd[o]["spend"] = f"INR {bd[o]['spend']:,.2f}"
        return json.dumps(bd, indent=2)

    elif name == "validate_campaign_name":
        name_val = args["name"]
        parts    = [p.strip() for p in name_val.split("|")]
        issues   = []
        if len(parts) != 10:
            issues.append(f"Expected 10 segments, got {len(parts)}")
        parsed = parse_name(name_val)
        if parsed.get("staff_code") not in {"AR","BS","SV","SK","UP","KP","RAJ","SD","DEE",""}:
            issues.append(f"Unknown staff code: {parsed['staff_code']}")
        if parsed.get("location") not in {"Chennai","Bangalore","Hyderabad","Coimbatore","Madurai","Puducherry",""}:
            issues.append(f"Unknown location: {parsed['location']}")
        return json.dumps({"is_valid": len(issues)==0, "issues": issues, "parsed": parsed}, indent=2)

    elif name == "search_campaign_by_name":
        query   = args["name_query"].lower()
        sf      = args.get("status_filter", "ALL")
        results = []
        params  = {"fields": "id,name,status,objective,daily_budget", "limit": 100}
        if sf != "ALL":
            params["effective_status"] = json.dumps([sf])
        data    = api_get(f"/{AD_ACCOUNT_ID}/campaigns", params)
        for c in data.get("data", []):
            if query in c.get("name", "").lower():
                results.append({
                    "id": c["id"], "name": c.get("name"),
                    "status": c.get("status"), "objective": c.get("objective"),
                    "daily_budget_inr": int(c["daily_budget"])/100 if c.get("daily_budget") else None
                })
        # paginate if more results
        while data.get("paging", {}).get("next"):
            data = api_get("", {"after": data["paging"]["cursors"]["after"],
                                "fields": "id,name,status,objective,daily_budget",
                                "limit": 100, "access_token": ACCESS_TOKEN})
            for c in data.get("data", []):
                if query in c.get("name", "").lower():
                    results.append({"id": c["id"], "name": c.get("name"), "status": c.get("status")})
        return json.dumps({"query": args["name_query"], "matches": len(results), "campaigns": results}, indent=2)

    return json.dumps({"error": f"Unknown tool: {name}"})

# ── SSE Transport ─────────────────────────────────────────────────────────────
sse = SseServerTransport("/messages/")

async def handle_sse(request: Request):
    async with sse.connect_sse(request.scope, request.receive, request._send) as streams:
        await server.run(streams[0], streams[1], server.create_initialization_options())

async def healthcheck(request):
    return JSONResponse({"status": "ok", "service": "poorvika-meta-ads-mcp", "port": PORT})

app = Starlette(routes=[
    Route("/",        healthcheck),
    Route("/health",  healthcheck),
    Route("/sse",     handle_sse),
    Mount("/messages/", app=sse.handle_post_message),
])

if __name__ == "__main__":
    print(f"✅ Poorvika Meta Ads MCP Server starting on port {PORT}")
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")
