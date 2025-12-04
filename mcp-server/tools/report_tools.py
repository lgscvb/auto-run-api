"""
Hour Jungle CRM - Report Tools
報表生成相關工具
"""

import logging
import os
from datetime import datetime, date, timedelta
from typing import Dict, Any, Optional

import httpx

logger = logging.getLogger(__name__)

POSTGREST_URL = os.getenv("POSTGREST_URL", "http://postgrest:3000")


async def postgrest_get(endpoint: str, params: dict = None) -> Any:
    """PostgREST GET 請求"""
    url = f"{POSTGREST_URL}/{endpoint}"
    async with httpx.AsyncClient() as client:
        response = await client.get(url, params=params, timeout=30.0)
        response.raise_for_status()
        return response.json()


def get_period_dates(period: str) -> tuple:
    """
    根據期間字串取得開始和結束日期

    Args:
        period: this_month/last_month/this_year/last_year

    Returns:
        (start_date, end_date)
    """
    today = date.today()

    if period == "this_month":
        start_date = today.replace(day=1)
        if today.month == 12:
            end_date = today.replace(year=today.year + 1, month=1, day=1) - timedelta(days=1)
        else:
            end_date = today.replace(month=today.month + 1, day=1) - timedelta(days=1)

    elif period == "last_month":
        first_of_this_month = today.replace(day=1)
        end_date = first_of_this_month - timedelta(days=1)
        start_date = end_date.replace(day=1)

    elif period == "this_year":
        start_date = today.replace(month=1, day=1)
        end_date = today.replace(month=12, day=31)

    elif period == "last_year":
        start_date = today.replace(year=today.year - 1, month=1, day=1)
        end_date = today.replace(year=today.year - 1, month=12, day=31)

    else:
        # 預設本月
        start_date = today.replace(day=1)
        if today.month == 12:
            end_date = today.replace(year=today.year + 1, month=1, day=1) - timedelta(days=1)
        else:
            end_date = today.replace(month=today.month + 1, day=1) - timedelta(days=1)

    return start_date, end_date


# ============================================================================
# 報表工具
# ============================================================================

async def get_revenue_summary(
    branch_id: int = None,
    period: str = "this_month"
) -> Dict[str, Any]:
    """
    營收摘要報表

    Args:
        branch_id: 場館ID (可選)
        period: 期間 (this_month/last_month/this_year)

    Returns:
        營收摘要
    """
    start_date, end_date = get_period_dates(period)

    try:
        # 取得場館摘要
        summary = await postgrest_get("v_branch_revenue_summary", {
            **({"branch_id": f"eq.{branch_id}"} if branch_id else {})
        })

        # 取得期間內的付款記錄統計
        params = {
            "due_date": f"gte.{start_date}",
            "due_date": f"lte.{end_date}"
        }
        if branch_id:
            params["branch_id"] = f"eq.{branch_id}"

        payments = await postgrest_get("payments", params)

        # 計算統計
        total_revenue = sum(
            p["amount"] for p in payments
            if p.get("payment_status") == "paid"
        )
        total_pending = sum(
            p["amount"] for p in payments
            if p.get("payment_status") == "pending"
        )
        total_overdue = sum(
            p["amount"] for p in payments
            if p.get("payment_status") == "overdue"
        )

        paid_count = len([p for p in payments if p.get("payment_status") == "paid"])
        pending_count = len([p for p in payments if p.get("payment_status") == "pending"])
        overdue_count = len([p for p in payments if p.get("payment_status") == "overdue"])

        collection_rate = 0
        if paid_count + pending_count + overdue_count > 0:
            collection_rate = round(
                paid_count / (paid_count + pending_count + overdue_count) * 100, 2
            )

        return {
            "period": period,
            "start_date": str(start_date),
            "end_date": str(end_date),
            "branch_id": branch_id,
            "summary": {
                "total_revenue": total_revenue,
                "total_pending": total_pending,
                "total_overdue": total_overdue,
                "paid_count": paid_count,
                "pending_count": pending_count,
                "overdue_count": overdue_count,
                "collection_rate": collection_rate
            },
            "branch_summary": summary
        }

    except Exception as e:
        logger.error(f"get_revenue_summary error: {e}")
        raise Exception(f"取得營收摘要失敗: {e}")


async def get_overdue_list(
    branch_id: int = None,
    min_days: int = 0
) -> Dict[str, Any]:
    """
    逾期款項報表

    Args:
        branch_id: 場館ID (可選)
        min_days: 最少逾期天數

    Returns:
        逾期款項列表
    """
    try:
        params = {
            "order": "days_overdue.desc"
        }
        if branch_id:
            params["branch_id"] = f"eq.{branch_id}"
        if min_days > 0:
            params["days_overdue"] = f"gte.{min_days}"

        overdue = await postgrest_get("v_overdue_details", params)

        # 計算統計
        total_amount = sum(p.get("total_due", 0) for p in overdue)
        customer_count = len(set(p["customer_id"] for p in overdue))

        # 按逾期等級分類
        by_level = {
            "severe": [],  # >60天
            "high": [],    # 31-60天
            "medium": [],  # 15-30天
            "low": []      # <15天
        }

        for p in overdue:
            level = p.get("overdue_level", "low")
            by_level[level].append(p)

        return {
            "branch_id": branch_id,
            "min_days": min_days,
            "statistics": {
                "total_count": len(overdue),
                "total_amount": total_amount,
                "customer_count": customer_count,
                "by_level": {
                    "severe": len(by_level["severe"]),
                    "high": len(by_level["high"]),
                    "medium": len(by_level["medium"]),
                    "low": len(by_level["low"])
                }
            },
            "overdue_list": overdue,
            "severe_cases": by_level["severe"][:10],  # 嚴重個案前10
            "high_priority": by_level["high"][:10]    # 高優先前10
        }

    except Exception as e:
        logger.error(f"get_overdue_list error: {e}")
        raise Exception(f"取得逾期報表失敗: {e}")


async def get_commission_due(
    status: str = "eligible"
) -> Dict[str, Any]:
    """
    應付佣金報表

    Args:
        status: 狀態 (pending/eligible/all)

    Returns:
        應付佣金列表
    """
    try:
        params = {"order": "eligible_date.asc"}

        if status == "eligible":
            params["commission_status"] = "eq.eligible"
        elif status == "pending":
            params["commission_status"] = "eq.pending"
        # all 則不加篩選

        commissions = await postgrest_get("v_commission_tracker", params)

        # 按會計事務所分組
        by_firm = {}
        for c in commissions:
            firm_name = c.get("firm_name") or "無介紹所"
            if firm_name not in by_firm:
                by_firm[firm_name] = {
                    "firm_name": firm_name,
                    "firm_contact": c.get("firm_contact"),
                    "firm_phone": c.get("firm_phone"),
                    "commissions": [],
                    "total_amount": 0,
                    "count": 0
                }
            by_firm[firm_name]["commissions"].append(c)
            by_firm[firm_name]["total_amount"] += c.get("commission_amount", 0)
            by_firm[firm_name]["count"] += 1

        # 計算總計
        total_amount = sum(c.get("commission_amount", 0) for c in commissions)
        eligible_now = [c for c in commissions if c.get("is_eligible_now")]
        eligible_amount = sum(c.get("commission_amount", 0) for c in eligible_now)

        return {
            "status_filter": status,
            "statistics": {
                "total_count": len(commissions),
                "total_amount": total_amount,
                "eligible_now_count": len(eligible_now),
                "eligible_now_amount": eligible_amount,
                "firm_count": len(by_firm)
            },
            "by_firm": list(by_firm.values()),
            "all_commissions": commissions
        }

    except Exception as e:
        logger.error(f"get_commission_due error: {e}")
        raise Exception(f"取得佣金報表失敗: {e}")


async def get_daily_summary(
    branch_id: int = None
) -> Dict[str, Any]:
    """
    每日摘要報表

    Args:
        branch_id: 場館ID (可選)

    Returns:
        每日摘要
    """
    try:
        params = {}
        if branch_id:
            params["branch_id"] = f"eq.{branch_id}"

        # 今日待辦
        tasks = await postgrest_get("v_today_tasks", params)

        # 應收款
        payments_due = await postgrest_get("v_payments_due", params)

        # 續約提醒
        renewals = await postgrest_get("v_renewal_reminders", {
            **params,
            "days_remaining": "lte.7"  # 7天內到期
        })

        # 分類任務
        payment_tasks = [t for t in tasks if t["task_type"] == "payment_due"]
        contract_tasks = [t for t in tasks if t["task_type"] == "contract_expiring"]
        commission_tasks = [t for t in tasks if t["task_type"] == "commission_due"]

        # 逾期統計
        overdue_payments = [p for p in payments_due if p.get("payment_status") == "overdue"]

        return {
            "date": str(date.today()),
            "branch_id": branch_id,
            "today_tasks": {
                "total": len(tasks),
                "payment_due": len(payment_tasks),
                "contract_expiring": len(contract_tasks),
                "commission_due": len(commission_tasks)
            },
            "overdue_summary": {
                "count": len(overdue_payments),
                "amount": sum(p.get("total_due", 0) for p in overdue_payments),
                "critical": len([p for p in overdue_payments if p.get("urgency") == "critical"])
            },
            "renewals_urgent": {
                "count": len(renewals),
                "list": [
                    {
                        "customer": r["customer_name"],
                        "days_remaining": r["days_remaining"],
                        "end_date": r["end_date"]
                    }
                    for r in renewals[:5]
                ]
            },
            "tasks_detail": tasks[:20]  # 前20項任務
        }

    except Exception as e:
        logger.error(f"get_daily_summary error: {e}")
        raise Exception(f"取得每日摘要失敗: {e}")


async def get_customer_analytics(
    branch_id: int = None
) -> Dict[str, Any]:
    """
    客戶分析報表

    Args:
        branch_id: 場館ID (可選)

    Returns:
        客戶分析
    """
    try:
        params = {}
        if branch_id:
            params["branch_id"] = f"eq.{branch_id}"

        customers = await postgrest_get("v_customer_summary", params)

        # 狀態分布
        status_dist = {}
        for c in customers:
            status = c.get("status", "unknown")
            status_dist[status] = status_dist.get(status, 0) + 1

        # 來源分布
        source_dist = {}
        for c in customers:
            source = c.get("source_channel") or "others"
            source_dist[source] = source_dist.get(source, 0) + 1

        # 風險分布
        risk_dist = {}
        for c in customers:
            risk = c.get("risk_level", "low")
            risk_dist[risk] = risk_dist.get(risk, 0) + 1

        # LINE 綁定率
        with_line = len([c for c in customers if c.get("line_user_id")])
        line_binding_rate = round(with_line / len(customers) * 100, 2) if customers else 0

        # 活躍客戶 (有活躍合約)
        active_with_contracts = len([c for c in customers if c.get("active_contracts", 0) > 0])

        return {
            "branch_id": branch_id,
            "total_customers": len(customers),
            "statistics": {
                "active_with_contracts": active_with_contracts,
                "line_binding_rate": line_binding_rate,
                "with_line_count": with_line,
                "high_risk_count": risk_dist.get("high", 0)
            },
            "distribution": {
                "by_status": status_dist,
                "by_source": source_dist,
                "by_risk": risk_dist
            },
            "top_overdue": sorted(
                [c for c in customers if c.get("overdue_count", 0) > 0],
                key=lambda x: x.get("overdue_amount", 0),
                reverse=True
            )[:10]
        }

    except Exception as e:
        logger.error(f"get_customer_analytics error: {e}")
        raise Exception(f"取得客戶分析失敗: {e}")
