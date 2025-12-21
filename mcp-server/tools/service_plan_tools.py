"""
服務方案價格管理工具
提供 service_plans 表的 CRUD 操作，以及同步到 Brain RAG 的功能
"""

import os
import logging
import httpx
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)

# PostgREST 設定
POSTGREST_URL = os.getenv("POSTGREST_URL", "http://localhost:3000")
BRAIN_API_URL = os.getenv("BRAIN_API_URL", "https://brain.yourspce.org")


async def postgrest_get(endpoint: str, params: dict = None) -> Any:
    """查詢 PostgREST"""
    url = f"{POSTGREST_URL}/{endpoint}"
    async with httpx.AsyncClient() as client:
        response = await client.get(url, params=params, timeout=30.0)
        response.raise_for_status()
        return response.json()


async def postgrest_post(endpoint: str, data: dict) -> Any:
    """建立資料"""
    url = f"{POSTGREST_URL}/{endpoint}"
    headers = {
        "Content-Type": "application/json",
        "Prefer": "return=representation"
    }
    async with httpx.AsyncClient() as client:
        response = await client.post(url, json=data, headers=headers, timeout=30.0)
        response.raise_for_status()
        return response.json()


async def postgrest_patch(endpoint: str, params: dict, data: dict) -> Any:
    """更新資料"""
    url = f"{POSTGREST_URL}/{endpoint}"
    headers = {
        "Content-Type": "application/json",
        "Prefer": "return=representation"
    }
    async with httpx.AsyncClient() as client:
        response = await client.patch(url, params=params, json=data, headers=headers, timeout=30.0)
        response.raise_for_status()
        return response.json()


async def postgrest_delete(endpoint: str, params: dict) -> bool:
    """刪除資料"""
    url = f"{POSTGREST_URL}/{endpoint}"
    async with httpx.AsyncClient() as client:
        response = await client.delete(url, params=params, timeout=30.0)
        response.raise_for_status()
        return True


# ============================================================================
# 服務方案 CRUD
# ============================================================================

async def list_service_plans(
    category: str = None,
    is_active: bool = None
) -> Dict[str, Any]:
    """
    列出服務方案

    Args:
        category: 分類篩選（空間服務/登記服務/代辦服務）
        is_active: 是否只顯示啟用的方案

    Returns:
        按分類分組的服務方案列表
    """
    params = {"order": "sort_order.asc,id.asc"}

    if category:
        params["category"] = f"eq.{category}"
    if is_active is not None:
        params["is_active"] = f"eq.{is_active}"

    try:
        plans = await postgrest_get("service_plans", params)

        # 按分類分組
        grouped = {}
        for plan in plans:
            cat = plan.get("category", "其他")
            if cat not in grouped:
                grouped[cat] = []
            grouped[cat].append(plan)

        return {
            "success": True,
            "count": len(plans),
            "plans": plans,
            "grouped": grouped
        }
    except Exception as e:
        logger.error(f"list_service_plans error: {e}")
        return {"success": False, "error": str(e)}


async def get_service_plan(plan_id: int) -> Dict[str, Any]:
    """取得單一服務方案"""
    try:
        plans = await postgrest_get("service_plans", {"id": f"eq.{plan_id}"})
        if not plans:
            return {"success": False, "error": "找不到服務方案"}
        return {"success": True, "plan": plans[0]}
    except Exception as e:
        logger.error(f"get_service_plan error: {e}")
        return {"success": False, "error": str(e)}


async def create_service_plan(
    category: str,
    name: str,
    code: str,
    unit_price: float,
    unit: str,
    billing_cycle: str = None,
    deposit: float = 0,
    original_price: float = None,
    min_duration: str = None,
    revenue_type: str = "own",
    annual_months: int = None,
    notes: str = None,
    sort_order: int = 0
) -> Dict[str, Any]:
    """
    建立服務方案

    Args:
        category: 分類（空間服務/登記服務/代辦服務/加值服務）
        name: 服務名稱
        code: 服務代碼（唯一）
        unit_price: 單價
        unit: 計價單位（月/小時/次/天）
        billing_cycle: 繳費週期（monthly/quarterly/semi_annual/annual/one_time）
        deposit: 押金
        original_price: 原價（有優惠時）
        min_duration: 最低租期
        revenue_type: 營收類型（own=自己收款/referral=轉介）
        annual_months: 年度月數（會計服務收14個月）
        notes: 備註
        sort_order: 排序
    """
    data = {
        "category": category,
        "name": name,
        "code": code,
        "unit_price": unit_price,
        "unit": unit,
        "billing_cycle": billing_cycle,
        "deposit": deposit or 0,
        "original_price": original_price,
        "min_duration": min_duration,
        "revenue_type": revenue_type or "own",
        "annual_months": annual_months,
        "notes": notes,
        "sort_order": sort_order or 0,
        "is_active": True
    }

    # 移除 None 值
    data = {k: v for k, v in data.items() if v is not None}

    try:
        result = await postgrest_post("service_plans", data)
        plan = result[0] if isinstance(result, list) else result
        return {
            "success": True,
            "message": f"服務方案「{name}」建立成功",
            "plan": plan
        }
    except Exception as e:
        error_msg = str(e)
        if "duplicate key" in error_msg.lower():
            return {"success": False, "error": f"服務代碼「{code}」已存在"}
        logger.error(f"create_service_plan error: {e}")
        return {"success": False, "error": str(e)}


async def update_service_plan(
    plan_id: int,
    updates: Dict[str, Any]
) -> Dict[str, Any]:
    """
    更新服務方案

    Args:
        plan_id: 方案 ID
        updates: 要更新的欄位
    """
    allowed_fields = [
        "category", "name", "code", "unit_price", "unit",
        "billing_cycle", "deposit", "original_price", "min_duration",
        "revenue_type", "annual_months", "notes", "sort_order", "is_active"
    ]

    filtered_updates = {k: v for k, v in updates.items() if k in allowed_fields}

    if not filtered_updates:
        return {"success": False, "error": "沒有有效的更新欄位"}

    try:
        result = await postgrest_patch(
            "service_plans",
            {"id": f"eq.{plan_id}"},
            filtered_updates
        )
        plan = result[0] if isinstance(result, list) and result else None
        return {
            "success": True,
            "message": "服務方案已更新",
            "plan": plan
        }
    except Exception as e:
        logger.error(f"update_service_plan error: {e}")
        return {"success": False, "error": str(e)}


async def delete_service_plan(plan_id: int) -> Dict[str, Any]:
    """刪除服務方案"""
    try:
        # 先檢查是否存在
        plans = await postgrest_get("service_plans", {"id": f"eq.{plan_id}"})
        if not plans:
            return {"success": False, "error": "找不到服務方案"}

        plan = plans[0]
        await postgrest_delete("service_plans", {"id": f"eq.{plan_id}"})

        return {
            "success": True,
            "message": f"服務方案「{plan['name']}」已刪除"
        }
    except Exception as e:
        logger.error(f"delete_service_plan error: {e}")
        return {"success": False, "error": str(e)}


async def reorder_service_plans(orders: List[Dict[str, int]]) -> Dict[str, Any]:
    """
    批量更新排序

    Args:
        orders: [{"id": 1, "sort_order": 10}, {"id": 2, "sort_order": 20}, ...]
    """
    try:
        updated = 0
        for order in orders:
            plan_id = order.get("id")
            sort_order = order.get("sort_order")
            if plan_id is not None and sort_order is not None:
                await postgrest_patch(
                    "service_plans",
                    {"id": f"eq.{plan_id}"},
                    {"sort_order": sort_order}
                )
                updated += 1

        return {
            "success": True,
            "message": f"已更新 {updated} 個方案的排序"
        }
    except Exception as e:
        logger.error(f"reorder_service_plans error: {e}")
        return {"success": False, "error": str(e)}


# ============================================================================
# 同步到 Brain RAG
# ============================================================================

def format_billing_cycle(cycle: str) -> str:
    """格式化繳費週期"""
    mapping = {
        "monthly": "月繳",
        "quarterly": "季繳",
        "semi_annual": "半年繳",
        "annual": "年繳",
        "biennial": "兩年繳",
        "one_time": "一次性"
    }
    return mapping.get(cycle, cycle or "")


def format_plan_to_knowledge(plan: dict) -> dict:
    """
    將服務方案轉換為 Brain 知識條目格式
    """
    name = plan.get("name", "")
    unit_price = plan.get("unit_price", 0)
    unit = plan.get("unit", "")
    billing_cycle = plan.get("billing_cycle")
    deposit = plan.get("deposit", 0)
    original_price = plan.get("original_price")
    min_duration = plan.get("min_duration")
    notes = plan.get("notes", "")
    revenue_type = plan.get("revenue_type", "own")
    annual_months = plan.get("annual_months")

    # 組建知識內容
    content_parts = [f"{name}："]
    content_parts.append(f"- 費用：${unit_price:,.0f}/{unit}")

    if billing_cycle and billing_cycle != "one_time":
        cycle_label = format_billing_cycle(billing_cycle)
        content_parts.append(f"- 繳費方式：{cycle_label}")

        # 計算每期金額
        cycle_months = {
            "monthly": 1,
            "quarterly": 3,
            "semi_annual": 6,
            "annual": 12,
            "biennial": 24
        }
        months = cycle_months.get(billing_cycle, 1)
        period_amount = unit_price * months
        content_parts.append(f"- 每期金額：${period_amount:,.0f}（{months}個月）")

    if deposit and deposit > 0:
        content_parts.append(f"- 押金：${deposit:,.0f}")

        # 計算首次簽約應付（押金 + 首期）
        if billing_cycle and billing_cycle != "one_time":
            cycle_months = {"monthly": 1, "quarterly": 3, "semi_annual": 6, "annual": 12, "biennial": 24}
            months = cycle_months.get(billing_cycle, 1)
            first_payment = deposit + (unit_price * months)
            content_parts.append(f"- 首次簽約應付：${first_payment:,.0f}")

    if original_price and original_price > unit_price:
        content_parts.append(f"- 原價：${original_price:,.0f}/{unit}（優惠中）")

    if min_duration:
        content_parts.append(f"- 最低租期：{min_duration}")

    if annual_months:
        content_parts.append(f"- 年度計算：{annual_months}個月")

    if revenue_type == "referral":
        content_parts.append("- 備註：此為代辦服務，費用於服務完成後向合作事務所繳納")

    if notes:
        content_parts.append(f"- 說明：{notes}")

    content = "\n".join(content_parts)

    return {
        "content": content,
        "category": "service_info",
        "sub_category": "pricing",
        "service_type": plan.get("category", "")
    }


async def sync_prices_to_brain() -> Dict[str, Any]:
    """
    同步價格資訊到 Brain RAG 知識庫

    1. 查詢所有啟用的服務方案
    2. 格式化為知識條目
    3. 呼叫 Brain API 批量匯入
    """
    try:
        # 1. 查詢啟用的服務方案
        plans = await postgrest_get("service_plans", {
            "is_active": "eq.true",
            "order": "category.asc,sort_order.asc"
        })

        if not plans:
            return {"success": False, "error": "沒有啟用的服務方案"}

        # 2. 格式化為知識條目
        knowledge_items = []
        for plan in plans:
            item = format_plan_to_knowledge(plan)
            knowledge_items.append(item)

        # 3. 呼叫 Brain API
        brain_url = f"{BRAIN_API_URL}/api/knowledge/bulk-import"

        async with httpx.AsyncClient() as client:
            response = await client.post(
                brain_url,
                json=knowledge_items,
                headers={"Content-Type": "application/json"},
                timeout=60.0
            )
            response.raise_for_status()
            result = response.json()

        imported = result.get("imported", 0)
        errors = result.get("errors", [])

        return {
            "success": True,
            "message": f"已同步 {imported} 個價格資訊到 AI 知識庫",
            "total_plans": len(plans),
            "imported": imported,
            "errors": errors if errors else None
        }

    except httpx.HTTPError as e:
        logger.error(f"sync_prices_to_brain HTTP error: {e}")
        return {"success": False, "error": f"無法連接 Brain API: {e}"}
    except Exception as e:
        logger.error(f"sync_prices_to_brain error: {e}")
        return {"success": False, "error": str(e)}
