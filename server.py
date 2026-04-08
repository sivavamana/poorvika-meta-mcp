"""
Poorvika Meta Ads MCP Server
Exposes Meta Marketing API as MCP tools for Claude to use autonomously.
"""

import os
import json
import httpx
from datetime import datetime, timedelta
from mcp.server.fastmcp import FastMCP

# ── Init ──────────────────────────────────────────────────────────────────────
mcp = FastMCP("poorvika-meta-ads")

META_API_BASE = "https://graph.facebook.com/v19.0"
ACCESS_TOKEN  = os.environ["META_ACCESS_TOKEN"]
AD_ACCOUNT_ID = os.environ["META_AD_ACCOUNT_ID"]   # format: act_XXXXXXXXXX


# ── Helpers ───────────────────────────────────────────────────────────────────

def api_get(path: str, params: dict = {}) -> dict:
    params["access_token"] = ACCESS_TOKEN
    r = httpx.get(f"{META_API_BASE}{path}", params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def api_post(path: str, data: dict = {}) -> dict:
    data["access_token"] = ACCESS_TOKEN
    r = httpx.post(f"{META_API_BASE}{path}", data=data, timeout=30)
    r.raise_for_status()
    return r.json()


def parse_campaign_name(name: str) -> dict:
    """
    Parses Poorvika pipe-delimited campaign naming convention.
    Format: Main Category | Category | Sub Category | Objective |
            Creative Type | Creative Type 2 | CTA | Location | Date | Staff Code
    """
    parts = [p.strip() for p in name.split("|")]
    keys  = [
        "main_category", "category", "sub_category", "objective",
        "creative_type", "creative_type_2", "cta", "location",
        "date", "staff_code"
    ]
    return {keys[i]: parts[i] if i < len(parts) else "" for i in range(len(keys))}


def date_range(days_back: int = 7) -> dict:
    today = datetime.utcnow().date()
    since = today - timedelta(days=days_back)
    return {"since": str(since), "until": str(today)}


# ── TOOLS ─────────────────────────────────────────────────────────────────────

# 1. Get All Campaigns ─────────────────────────────────────────────────────────
@mcp.tool()
def get_campaigns(status_filter: str = "ACTIVE") -> str:
    """
    Fetch all campaigns from the Poorvika Meta Ads account.
    status_filter: ACTIVE | PAUSED | ALL
    Returns campaign ID, name, status, objective, daily/lifetime budget.
    """
    params = {
        "fields": "id,name,status,objective,daily_budget,lifetime_budget,created_time",
        "limit": 100,
    }
    if status_filter != "ALL":
        params["effective_status"] = json.dumps([status_filter])

    data = api_get(f"/{AD_ACCOUNT_ID}/campaigns", params)
    campaigns = data.get("data", [])

    results = []
    for c in campaigns:
        parsed = parse_campaign_name(c.get("name", ""))
        results.append({
            "id":             c["id"],
            "name":           c.get("name"),
            "status":         c.get("status"),
            "objective":      c.get("objective"),
            "daily_budget":   int(c["daily_budget"]) / 100 if c.get("daily_budget") else None,
            "lifetime_budget":int(c["lifetime_budget"]) / 100 if c.get("lifetime_budget") else None,
            "created_time":   c.get("created_time"),
            "parsed_name":    parsed,
        })

    return json.dumps({"total": len(results), "campaigns": results}, indent=2)


# 2. Get Campaign Insights ─────────────────────────────────────────────────────
@mcp.tool()
def get_campaign_insights(campaign_id: str, days_back: int = 7) -> str:
    """
    Fetch performance insights for a specific campaign.
    Returns impressions, reach, clicks, spend, CPM, CPC, CTR, frequency.
    days_back: number of past days to pull data for (default 7).
    """
    params = {
        "fields": "impressions,reach,clicks,spend,cpm,cpc,ctr,frequency,actions,cost_per_action_type",
        "date_preset": "last_7d" if days_back == 7 else "last_30d",
        "time_range": json.dumps(date_range(days_back)),
    }
    data = api_get(f"/{campaign_id}/insights", params)
    insights = data.get("data", [{}])
    return json.dumps(insights[0] if insights else {"message": "No data found"}, indent=2)


# 3. Get All AdSets for a Campaign ─────────────────────────────────────────────
@mcp.tool()
def get_adsets(campaign_id: str) -> str:
    """
    List all ad sets under a given campaign with budget, targeting, and status.
    """
    params = {
        "fields": "id,name,status,daily_budget,lifetime_budget,targeting,optimization_goal,billing_event,bid_amount",
        "limit": 100,
    }
    data = api_get(f"/{campaign_id}/adsets", params)
    adsets = data.get("data", [])
    return json.dumps({"total": len(adsets), "adsets": adsets}, indent=2)


# 4. Get AdSet Insights ────────────────────────────────────────────────────────
@mcp.tool()
def get_adset_insights(adset_id: str, days_back: int = 7) -> str:
    """
    Fetch performance insights for a specific ad set.
    Returns impressions, reach, clicks, spend, CPM, CPC, CTR, frequency, results.
    """
    params = {
        "fields": "impressions,reach,clicks,spend,cpm,cpc,ctr,frequency,actions,cost_per_action_type",
        "time_range": json.dumps(date_range(days_back)),
    }
    data = api_get(f"/{adset_id}/insights", params)
    insights = data.get("data", [{}])
    return json.dumps(insights[0] if insights else {"message": "No data found"}, indent=2)


# 5. Get Account-Level Insights ───────────────────────────────────────────────
@mcp.tool()
def get_account_insights(days_back: int = 7) -> str:
    """
    Fetch overall account-level performance summary for Poorvika.
    Returns total spend, impressions, reach, clicks, CPM, CPC, CTR.
    """
    params = {
        "fields": "impressions,reach,clicks,spend,cpm,cpc,ctr,frequency",
        "time_range": json.dumps(date_range(days_back)),
    }
    data = api_get(f"/{AD_ACCOUNT_ID}/insights", params)
    insights = data.get("data", [{}])
    return json.dumps(insights[0] if insights else {"message": "No data"}, indent=2)


# 6. Pause a Campaign ─────────────────────────────────────────────────────────
@mcp.tool()
def pause_campaign(campaign_id: str) -> str:
    """
    Pause a campaign by its ID. Use when performance is below threshold.
    """
    result = api_post(f"/{campaign_id}", {"status": "PAUSED"})
    return json.dumps({"campaign_id": campaign_id, "action": "PAUSED", "result": result})


# 7. Resume a Campaign ────────────────────────────────────────────────────────
@mcp.tool()
def resume_campaign(campaign_id: str) -> str:
    """
    Resume (activate) a paused campaign by its ID.
    """
    result = api_post(f"/{campaign_id}", {"status": "ACTIVE"})
    return json.dumps({"campaign_id": campaign_id, "action": "ACTIVE", "result": result})


# 8. Pause an AdSet ───────────────────────────────────────────────────────────
@mcp.tool()
def pause_adset(adset_id: str) -> str:
    """
    Pause a specific ad set. Use for underperforming ad sets within a campaign.
    """
    result = api_post(f"/{adset_id}", {"status": "PAUSED"})
    return json.dumps({"adset_id": adset_id, "action": "PAUSED", "result": result})


# 9. Resume an AdSet ──────────────────────────────────────────────────────────
@mcp.tool()
def resume_adset(adset_id: str) -> str:
    """
    Resume a paused ad set.
    """
    result = api_post(f"/{adset_id}", {"status": "ACTIVE"})
    return json.dumps({"adset_id": adset_id, "action": "ACTIVE", "result": result})


# 10. Update Campaign Budget ──────────────────────────────────────────────────
@mcp.tool()
def update_campaign_budget(campaign_id: str, new_daily_budget_inr: float) -> str:
    """
    Update the daily budget of a campaign.
    new_daily_budget_inr: amount in INR (e.g. 1000 for ₹1,000/day).
    Meta API accepts budget in paise (1 INR = 100 paise).
    """
    budget_paise = int(new_daily_budget_inr * 100)
    result = api_post(f"/{campaign_id}", {"daily_budget": budget_paise})
    return json.dumps({
        "campaign_id": campaign_id,
        "new_daily_budget_inr": new_daily_budget_inr,
        "result": result
    })


# 11. Update AdSet Budget ─────────────────────────────────────────────────────
@mcp.tool()
def update_adset_budget(adset_id: str, new_daily_budget_inr: float) -> str:
    """
    Update the daily budget of a specific ad set.
    new_daily_budget_inr: amount in INR.
    """
    budget_paise = int(new_daily_budget_inr * 100)
    result = api_post(f"/{adset_id}", {"daily_budget": budget_paise})
    return json.dumps({
        "adset_id": adset_id,
        "new_daily_budget_inr": new_daily_budget_inr,
        "result": result
    })


# 12. Get All Ads in an AdSet ─────────────────────────────────────────────────
@mcp.tool()
def get_ads(adset_id: str) -> str:
    """
    List all individual ads within an ad set with creative details and status.
    """
    params = {
        "fields": "id,name,status,creative,effective_status",
        "limit": 50,
    }
    data = api_get(f"/{adset_id}/ads", params)
    return json.dumps(data.get("data", []), indent=2)


# 13. Get Ad Insights ─────────────────────────────────────────────────────────
@mcp.tool()
def get_ad_insights(ad_id: str, days_back: int = 7) -> str:
    """
    Fetch performance insights for a specific ad creative.
    Useful for creative fatigue detection.
    """
    params = {
        "fields": "impressions,reach,clicks,spend,cpm,cpc,ctr,frequency,actions",
        "time_range": json.dumps(date_range(days_back)),
    }
    data = api_get(f"/{ad_id}/insights", params)
    insights = data.get("data", [{}])
    return json.dumps(insights[0] if insights else {"message": "No data"}, indent=2)


# 14. Get Top Performing Campaigns ────────────────────────────────────────────
@mcp.tool()
def get_top_campaigns(days_back: int = 7, metric: str = "ctr", top_n: int = 5) -> str:
    """
    Return top N performing campaigns sorted by a given metric.
    metric: ctr | cpc | cpm | spend | impressions | clicks
    Useful for budget reallocation decisions.
    """
    params = {
        "fields": "campaign_id,campaign_name,impressions,reach,clicks,spend,cpm,cpc,ctr,frequency",
        "time_range": json.dumps(date_range(days_back)),
        "level": "campaign",
        "limit": 100,
    }
    data = api_get(f"/{AD_ACCOUNT_ID}/insights", params)
    rows = data.get("data", [])

    def safe_float(val):
        try:
            return float(val)
        except:
            return 0.0

    ascending = metric in ["cpc", "cpm"]
    sorted_rows = sorted(rows, key=lambda x: safe_float(x.get(metric, 0)), reverse=not ascending)
    top = sorted_rows[:top_n]

    return json.dumps({
        "metric": metric,
        "days_back": days_back,
        "top_campaigns": top
    }, indent=2)


# 15. Daily Performance Report ────────────────────────────────────────────────
@mcp.tool()
def get_daily_report(days_back: int = 7) -> str:
    """
    Generate a full daily performance report for Poorvika's Meta Ads account.
    Returns account summary + top/bottom campaigns + spend breakdown.
    Perfect for morning briefings.
    """
    # Account summary
    acct_params = {
        "fields": "impressions,reach,clicks,spend,cpm,cpc,ctr,frequency",
        "time_range": json.dumps(date_range(days_back)),
    }
    acct_data  = api_get(f"/{AD_ACCOUNT_ID}/insights", acct_params)
    acct       = acct_data.get("data", [{}])[0]

    # Campaign-level breakdown
    camp_params = {
        "fields": "campaign_id,campaign_name,impressions,clicks,spend,ctr,cpc",
        "time_range": json.dumps(date_range(days_back)),
        "level": "campaign",
        "limit": 100,
    }
    camp_data  = api_get(f"/{AD_ACCOUNT_ID}/insights", camp_params)
    campaigns  = camp_data.get("data", [])

    def safe_float(val):
        try: return float(val)
        except: return 0.0

    sorted_by_spend = sorted(campaigns, key=lambda x: safe_float(x.get("spend", 0)), reverse=True)

    report = {
        "report_date": str(datetime.utcnow().date()),
        "period_days": days_back,
        "account_summary": {
            "total_spend_inr":  f"₹{safe_float(acct.get('spend', 0)):,.2f}",
            "total_impressions": acct.get("impressions", "0"),
            "total_reach":       acct.get("reach", "0"),
            "total_clicks":      acct.get("clicks", "0"),
            "avg_cpm":           f"₹{safe_float(acct.get('cpm', 0)):,.2f}",
            "avg_cpc":           f"₹{safe_float(acct.get('cpc', 0)):,.2f}",
            "avg_ctr":           f"{safe_float(acct.get('ctr', 0)):.2f}%",
            "avg_frequency":     f"{safe_float(acct.get('frequency', 0)):.2f}",
        },
        "top_5_by_spend": sorted_by_spend[:5],
        "bottom_5_by_spend": sorted_by_spend[-5:],
        "total_active_campaigns": len(campaigns),
    }

    return json.dumps(report, indent=2)


# 16. Validate Campaign Name ──────────────────────────────────────────────────
@mcp.tool()
def validate_campaign_name(name: str) -> str:
    """
    Validate a campaign name against Poorvika's pipe-delimited naming convention.
    Format: Main Category | Category | Sub Category | Objective |
            Creative Type | Creative Type 2 | CTA | Location | Date | Staff Code
    Returns validation result and parsed components.
    """
    parts = [p.strip() for p in name.split("|")]
    expected = 10
    valid_staff   = {"AR", "BS", "SV", "SK", "UP", "KP", "RAJ", "SD", "DEE"}
    valid_locations = {"Chennai", "Bangalore", "Hyderabad", "Coimbatore", "Madurai", "Puducherry"}
    valid_categories = {"Mobile", "Laptop", "Appliance", "Accessory"}

    issues = []
    if len(parts) != expected:
        issues.append(f"Expected {expected} segments, found {len(parts)}")

    parsed = parse_campaign_name(name)

    if parsed.get("staff_code") and parsed["staff_code"] not in valid_staff:
        issues.append(f"Unknown staff code: '{parsed['staff_code']}'. Valid: {valid_staff}")

    if parsed.get("location") and parsed["location"] not in valid_locations:
        issues.append(f"Unknown location: '{parsed['location']}'. Valid: {valid_locations}")

    if parsed.get("main_category") and parsed["main_category"] not in valid_categories:
        issues.append(f"Unknown main category: '{parsed['main_category']}'. Valid: {valid_categories}")

    return json.dumps({
        "name": name,
        "is_valid": len(issues) == 0,
        "issues": issues,
        "parsed": parsed,
    }, indent=2)


# 17. Get Account Spend Summary by Objective ──────────────────────────────────
@mcp.tool()
def get_spend_by_objective(days_back: int = 30) -> str:
    """
    Break down total Meta Ads spend by campaign objective (REACH, TRAFFIC, etc.)
    Useful for understanding funnel-stage budget allocation.
    """
    params = {
        "fields": "campaign_name,objective,spend,impressions,clicks",
        "time_range": json.dumps(date_range(days_back)),
        "level": "campaign",
        "limit": 100,
    }
    data = api_get(f"/{AD_ACCOUNT_ID}/insights", params)
    rows = data.get("data", [])

    breakdown: dict = {}
    for row in rows:
        obj   = row.get("objective", "UNKNOWN")
        spend = float(row.get("spend", 0))
        if obj not in breakdown:
            breakdown[obj] = {"spend": 0, "impressions": 0, "clicks": 0, "campaign_count": 0}
        breakdown[obj]["spend"]          += spend
        breakdown[obj]["impressions"]    += int(row.get("impressions", 0))
        breakdown[obj]["clicks"]         += int(row.get("clicks", 0))
        breakdown[obj]["campaign_count"] += 1

    for obj in breakdown:
        breakdown[obj]["spend"] = f"₹{breakdown[obj]['spend']:,.2f}"

    return json.dumps({
        "period_days": days_back,
        "spend_by_objective": breakdown
    }, indent=2)


# ── Entry Point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    mcp.run(transport="sse")
