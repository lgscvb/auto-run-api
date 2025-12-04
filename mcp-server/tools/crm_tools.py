"""
Hour Jungle CRM - CRM Tools
客戶、合約、付款相關工具
"""

import logging
from datetime import datetime, date
from typing import Optional, List, Dict, Any

import httpx
import psycopg2
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)

# 取得設定（從 main.py 導入）
import os

POSTGREST_URL = os.getenv("POSTGREST_URL", "http://postgrest:3000")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "hourjungle")
POSTGRES_USER = os.getenv("POSTGRES_USER", "hjadmin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")


def get_db_connection():
    """取得資料庫連接"""
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        cursor_factory=RealDictCursor
    )


async def postgrest_get(endpoint: str, params: dict = None) -> Any:
    """PostgREST GET 請求"""
    url = f"{POSTGREST_URL}/{endpoint}"
    async with httpx.AsyncClient() as client:
        response = await client.get(url, params=params, timeout=30.0)
        response.raise_for_status()
        return response.json()


async def postgrest_post(endpoint: str, data: dict) -> Any:
    """PostgREST POST 請求"""
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
    """PostgREST PATCH 請求"""
    url = f"{POSTGREST_URL}/{endpoint}"
    headers = {
        "Content-Type": "application/json",
        "Prefer": "return=representation"
    }
    async with httpx.AsyncClient() as client:
        response = await client.patch(url, params=params, json=data, headers=headers, timeout=30.0)
        response.raise_for_status()
        return response.json()


# ============================================================================
# 查詢工具
# ============================================================================

async def search_customers(
    query: str = None,
    branch_id: int = None,
    status: str = None,
    limit: int = 20
) -> Dict[str, Any]:
    """
    搜尋客戶

    Args:
        query: 搜尋關鍵字 (姓名/電話/公司名)
        branch_id: 場館ID (1=大忠, 2=環瑞)
        status: 客戶狀態 (active/prospect/churned)
        limit: 回傳筆數

    Returns:
        客戶列表
    """
    params = {"limit": limit}

    if branch_id:
        params["branch_id"] = f"eq.{branch_id}"
    if status:
        params["status"] = f"eq.{status}"
    if query:
        # 模糊搜尋姓名、電話、公司名
        params["or"] = f"(name.ilike.*{query}*,phone.ilike.*{query}*,company_name.ilike.*{query}*)"

    try:
        customers = await postgrest_get("v_customer_summary", params)
        return {
            "count": len(customers),
            "customers": customers
        }
    except Exception as e:
        logger.error(f"search_customers error: {e}")
        raise Exception(f"搜尋客戶失敗: {e}")


async def get_customer_detail(
    customer_id: int = None,
    line_user_id: str = None
) -> Dict[str, Any]:
    """
    取得客戶詳細資料

    Args:
        customer_id: 客戶ID
        line_user_id: LINE User ID

    Returns:
        客戶詳細資料
    """
    if not customer_id and not line_user_id:
        raise ValueError("必須提供 customer_id 或 line_user_id")

    params = {"limit": 1}
    if customer_id:
        params["id"] = f"eq.{customer_id}"
    elif line_user_id:
        params["line_user_id"] = f"eq.{line_user_id}"

    try:
        customers = await postgrest_get("v_customer_summary", params)
        if not customers:
            return {"found": False, "message": "找不到客戶"}

        customer = customers[0]

        # 取得合約資料
        contracts = await postgrest_get("contracts", {
            "customer_id": f"eq.{customer['id']}",
            "order": "start_date.desc"
        })

        # 取得付款記錄
        payments = await postgrest_get("payments", {
            "customer_id": f"eq.{customer['id']}",
            "order": "due_date.desc",
            "limit": 10
        })

        return {
            "found": True,
            "customer": customer,
            "contracts": contracts,
            "recent_payments": payments
        }
    except Exception as e:
        logger.error(f"get_customer_detail error: {e}")
        raise Exception(f"取得客戶資料失敗: {e}")


async def list_payments_due(
    branch_id: int = None,
    urgency: str = None,
    limit: int = 20
) -> Dict[str, Any]:
    """
    列出應收款項

    Args:
        branch_id: 場館ID
        urgency: 緊急度 (critical/high/medium/upcoming/all)
        limit: 回傳筆數

    Returns:
        應收款列表
    """
    params = {"limit": limit}

    if branch_id:
        params["branch_id"] = f"eq.{branch_id}"
    if urgency and urgency != "all":
        params["urgency"] = f"eq.{urgency}"

    try:
        payments = await postgrest_get("v_payments_due", params)

        # 計算統計
        total_amount = sum(p.get("total_due", 0) for p in payments)
        overdue_count = sum(1 for p in payments if p.get("payment_status") == "overdue")
        overdue_amount = sum(p.get("total_due", 0) for p in payments if p.get("payment_status") == "overdue")

        return {
            "count": len(payments),
            "total_amount": total_amount,
            "overdue_count": overdue_count,
            "overdue_amount": overdue_amount,
            "payments": payments
        }
    except Exception as e:
        logger.error(f"list_payments_due error: {e}")
        raise Exception(f"取得應收款失敗: {e}")


async def list_renewals_due(
    branch_id: int = None,
    days_ahead: int = 30
) -> Dict[str, Any]:
    """
    列出即將到期的合約

    Args:
        branch_id: 場館ID
        days_ahead: 未來幾天內到期

    Returns:
        即將到期合約列表
    """
    params = {
        "days_remaining": f"lte.{days_ahead}",
        "order": "days_remaining.asc"
    }

    if branch_id:
        params["branch_id"] = f"eq.{branch_id}"

    try:
        renewals = await postgrest_get("v_renewal_reminders", params)

        # 分類統計
        urgent = [r for r in renewals if r.get("priority") == "urgent"]
        high = [r for r in renewals if r.get("priority") == "high"]

        return {
            "count": len(renewals),
            "urgent_count": len(urgent),
            "high_priority_count": len(high),
            "renewals": renewals
        }
    except Exception as e:
        logger.error(f"list_renewals_due error: {e}")
        raise Exception(f"取得續約提醒失敗: {e}")


# ============================================================================
# 操作工具
# ============================================================================

async def create_customer(
    name: str,
    branch_id: int,
    phone: str = None,
    email: str = None,
    company_name: str = None,
    customer_type: str = "individual",
    source_channel: str = "others",
    line_user_id: str = None,
    notes: str = None
) -> Dict[str, Any]:
    """
    建立新客戶

    Args:
        name: 客戶姓名
        branch_id: 場館ID
        phone: 電話
        email: Email
        company_name: 公司名稱
        customer_type: 客戶類型
        source_channel: 來源管道
        line_user_id: LINE User ID
        notes: 備註

    Returns:
        新建客戶資料
    """
    data = {
        "name": name,
        "branch_id": branch_id,
        "customer_type": customer_type,
        "source_channel": source_channel,
        "status": "prospect"
    }

    if phone:
        data["phone"] = phone
    if email:
        data["email"] = email
    if company_name:
        data["company_name"] = company_name
    if line_user_id:
        data["line_user_id"] = line_user_id
    if notes:
        data["notes"] = notes

    try:
        result = await postgrest_post("customers", data)
        customer = result[0] if isinstance(result, list) else result

        return {
            "success": True,
            "message": f"客戶 {name} 建立成功",
            "customer": customer
        }
    except Exception as e:
        logger.error(f"create_customer error: {e}")
        raise Exception(f"建立客戶失敗: {e}")


async def update_customer(
    customer_id: int,
    updates: Dict[str, Any]
) -> Dict[str, Any]:
    """
    更新客戶資料

    Args:
        customer_id: 客戶ID
        updates: 要更新的欄位

    Returns:
        更新後的客戶資料
    """
    # 允許更新的欄位
    allowed_fields = [
        "name", "phone", "email", "company_name", "company_tax_id",
        "address", "line_user_id", "line_display_name",
        "invoice_title", "invoice_tax_id", "invoice_delivery", "invoice_carrier",
        "status", "risk_level", "risk_notes", "notes", "metadata"
    ]

    # 過濾非允許的欄位
    filtered_updates = {k: v for k, v in updates.items() if k in allowed_fields}

    if not filtered_updates:
        raise ValueError("沒有有效的更新欄位")

    try:
        result = await postgrest_patch(
            "customers",
            {"id": f"eq.{customer_id}"},
            filtered_updates
        )

        if not result:
            return {"success": False, "message": "找不到客戶"}

        customer = result[0] if isinstance(result, list) else result

        return {
            "success": True,
            "message": "客戶資料更新成功",
            "updated_fields": list(filtered_updates.keys()),
            "customer": customer
        }
    except Exception as e:
        logger.error(f"update_customer error: {e}")
        raise Exception(f"更新客戶失敗: {e}")


async def record_payment(
    payment_id: int,
    payment_method: str,
    notes: str = None
) -> Dict[str, Any]:
    """
    記錄繳費

    Args:
        payment_id: 付款ID
        payment_method: 付款方式 (cash/transfer/credit_card/line_pay)
        notes: 備註

    Returns:
        更新後的付款記錄
    """
    valid_methods = ["cash", "transfer", "credit_card", "line_pay"]
    if payment_method not in valid_methods:
        raise ValueError(f"無效的付款方式，允許: {', '.join(valid_methods)}")

    update_data = {
        "payment_status": "paid",
        "payment_method": payment_method,
        "paid_at": datetime.now().isoformat()
    }

    if notes:
        update_data["notes"] = notes

    try:
        result = await postgrest_patch(
            "payments",
            {"id": f"eq.{payment_id}"},
            update_data
        )

        if not result:
            return {"success": False, "message": "找不到付款記錄"}

        payment = result[0] if isinstance(result, list) else result

        return {
            "success": True,
            "message": f"付款 #{payment_id} 已標記為已付款",
            "payment": payment
        }
    except Exception as e:
        logger.error(f"record_payment error: {e}")
        raise Exception(f"記錄繳費失敗: {e}")


async def create_contract(
    customer_id: int,
    branch_id: int,
    start_date: str,
    end_date: str,
    monthly_rent: float,
    contract_type: str = "virtual_office",
    deposit: float = 0,
    payment_cycle: str = "monthly",
    payment_day: int = 5,
    plan_name: str = None,
    broker_name: str = None,
    broker_firm_id: int = None,
    notes: str = None
) -> Dict[str, Any]:
    """
    建立新合約

    Args:
        customer_id: 客戶ID
        branch_id: 場館ID
        start_date: 開始日期 (YYYY-MM-DD)
        end_date: 結束日期 (YYYY-MM-DD)
        monthly_rent: 月租金
        contract_type: 合約類型
        deposit: 押金
        payment_cycle: 繳費週期
        payment_day: 繳費日
        plan_name: 方案名稱
        broker_name: 介紹人
        broker_firm_id: 介紹會計事務所ID
        notes: 備註

    Returns:
        新建合約資料
    """
    data = {
        "customer_id": customer_id,
        "branch_id": branch_id,
        "start_date": start_date,
        "end_date": end_date,
        "monthly_rent": monthly_rent,
        "contract_type": contract_type,
        "deposit": deposit,
        "payment_cycle": payment_cycle,
        "payment_day": payment_day,
        "status": "draft"
    }

    if plan_name:
        data["plan_name"] = plan_name
    if broker_name:
        data["broker_name"] = broker_name
    if broker_firm_id:
        data["broker_firm_id"] = broker_firm_id
        data["commission_eligible"] = True
    if notes:
        data["notes"] = notes

    try:
        result = await postgrest_post("contracts", data)
        contract = result[0] if isinstance(result, list) else result

        return {
            "success": True,
            "message": f"合約 {contract.get('contract_number', contract['id'])} 建立成功",
            "contract": contract
        }
    except Exception as e:
        logger.error(f"create_contract error: {e}")
        raise Exception(f"建立合約失敗: {e}")
