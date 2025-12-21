"""
Hour Jungle CRM - CRM Tools
客戶、合約、付款相關工具
"""

import logging
import calendar
import re
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
    from urllib.parse import urlencode, quote

    url = f"{POSTGREST_URL}/{endpoint}"

    # 手動編碼參數，保留 PostgREST 特殊字符 (*, ., 括號等)
    if params:
        # 自訂編碼：只編碼中文等特殊字符，保留 PostgREST 語法字符
        def encode_value(v):
            # 保留 PostgREST 運算符和語法字符
            return quote(str(v), safe='*.,()=')

        query_parts = [f"{k}={encode_value(v)}" for k, v in params.items()]
        url = f"{url}?{'&'.join(query_parts)}"

    async with httpx.AsyncClient() as client:
        response = await client.get(url, timeout=30.0)
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
        客戶列表（含活動合約詳情）
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

        # 為每個客戶查詢活動合約詳情
        customer_ids = [c["id"] for c in customers if c.get("active_contracts", 0) > 0]
        if customer_ids:
            contracts_params = {
                "customer_id": f"in.({','.join(map(str, customer_ids))})",
                "status": "eq.active",
                "select": "id,customer_id,contract_number,contract_type,branch_id,monthly_rent,position_number"
            }
            contracts = await postgrest_get("contracts", contracts_params)

            # 按 customer_id 分組
            contracts_by_customer = {}
            for c in contracts:
                cid = c["customer_id"]
                if cid not in contracts_by_customer:
                    contracts_by_customer[cid] = []
                contracts_by_customer[cid].append(c)

            # 附加到客戶資料
            for customer in customers:
                customer["active_contracts"] = contracts_by_customer.get(customer["id"], [])
        else:
            for customer in customers:
                customer["active_contracts"] = []

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


async def commission_pay(
    commission_id: int,
    payment_method: str,
    reference: str = None,
    notes: str = None
) -> Dict[str, Any]:
    """
    執行佣金付款

    Args:
        commission_id: 佣金ID
        payment_method: 付款方式 (transfer/check/cash)
        reference: 參考資訊（轉帳後五碼或支票號碼）
        notes: 備註

    Returns:
        更新後的佣金資料
    """
    valid_methods = ["transfer", "check", "cash"]
    if payment_method not in valid_methods:
        raise ValueError(f"無效的付款方式，允許: {', '.join(valid_methods)}")

    # 先檢查佣金狀態
    try:
        commissions = await postgrest_get("commissions", {"id": f"eq.{commission_id}"})
        if not commissions:
            return {"success": False, "message": "找不到佣金記錄"}

        commission = commissions[0]

        # 驗證狀態必須是 eligible 才能付款
        if commission.get("status") != "eligible":
            current_status = commission.get("status", "unknown")
            return {
                "success": False,
                "message": f"佣金狀態為 {current_status}，只有 eligible 狀態才能付款"
            }

        # 更新佣金為已付款
        update_data = {
            "status": "paid",
            "payment_method": payment_method,
            "paid_at": datetime.now().isoformat()
        }

        if reference:
            update_data["payment_reference"] = reference
        if notes:
            update_data["notes"] = notes

        result = await postgrest_patch(
            "commissions",
            {"id": f"eq.{commission_id}"},
            update_data
        )

        if not result:
            return {"success": False, "message": "更新失敗"}

        updated_commission = result[0] if isinstance(result, list) else result

        return {
            "success": True,
            "message": f"佣金 #{commission_id} 已標記為已付款",
            "commission": updated_commission
        }
    except Exception as e:
        logger.error(f"commission_pay error: {e}")
        raise Exception(f"佣金付款失敗: {e}")


async def payment_undo(
    payment_id: int,
    reason: str
) -> Dict[str, Any]:
    """
    撤銷繳費記錄（將已付款狀態改回待付款）

    Args:
        payment_id: 付款ID
        reason: 撤銷原因（必填）

    Returns:
        更新後的付款記錄
    """
    if not reason or not reason.strip():
        raise ValueError("必須提供撤銷原因")

    try:
        # 先檢查付款狀態
        payments = await postgrest_get("payments", {"id": f"eq.{payment_id}"})
        if not payments:
            return {"success": False, "message": "找不到付款記錄"}

        payment = payments[0]

        # 驗證狀態必須是 paid 才能撤銷
        if payment.get("payment_status") != "paid":
            current_status = payment.get("payment_status", "unknown")
            return {
                "success": False,
                "message": f"付款狀態為 {current_status}，只有 paid 狀態才能撤銷"
            }

        # 記錄原本的付款資訊（用於審計追蹤）
        original_info = {
            "paid_at": payment.get("paid_at"),
            "payment_method": payment.get("payment_method"),
            "reference": payment.get("reference"),
            "undone_at": datetime.now().isoformat(),
            "undo_reason": reason.strip()
        }

        # 取得現有的 notes，附加撤銷記錄
        existing_notes = payment.get("notes") or ""
        undo_note = f"\n[撤銷記錄] {datetime.now().strftime('%Y-%m-%d %H:%M')} - 原付款方式: {payment.get('payment_method')}, 原付款日: {payment.get('paid_at')}, 撤銷原因: {reason.strip()}"
        new_notes = existing_notes + undo_note

        # 更新付款狀態為 pending，清除付款資訊
        update_data = {
            "payment_status": "pending",
            "payment_method": None,
            "paid_at": None,
            "reference": None,
            "notes": new_notes.strip()
        }

        result = await postgrest_patch(
            "payments",
            {"id": f"eq.{payment_id}"},
            update_data
        )

        if not result:
            return {"success": False, "message": "更新失敗"}

        updated_payment = result[0] if isinstance(result, list) else result

        return {
            "success": True,
            "message": f"付款 #{payment_id} 已撤銷",
            "payment": updated_payment,
            "original_info": original_info
        }
    except Exception as e:
        logger.error(f"payment_undo error: {e}")
        raise Exception(f"撤銷繳費失敗: {e}")


# ============================================================================
# 合約編號產生器
# ============================================================================

async def generate_contract_number(branch_id: int) -> str:
    """
    根據分館產生下一個合約編號

    Args:
        branch_id: 分館 ID
            - 1: 大忠館 → DZ-XXX（3位數）
            - 2: 環瑞館 → HR-VXX（2位數）

    Returns:
        新的合約編號
    """
    if branch_id == 1:
        # 大忠館：DZ-XXX 格式
        contracts = await postgrest_get(
            "contracts",
            {
                "select": "contract_number",
                "contract_number": "like.DZ-%",
                "limit": "2000"
            }
        )

        max_num = 0
        for c in contracts:
            cn = c.get("contract_number", "")
            # 匹配 DZ-XXX 格式（可能有 -E 後綴表示已結束）
            match = re.match(r"DZ-E?(\d+)", cn)
            if match:
                num = int(match.group(1))
                if num > max_num:
                    max_num = num

        next_num = max_num + 1
        return f"DZ-{next_num:03d}"

    elif branch_id == 2:
        # 環瑞館：HR-VXX 格式
        contracts = await postgrest_get(
            "contracts",
            {
                "select": "contract_number",
                "contract_number": "like.HR-V%",
                "order": "contract_number.desc",
                "limit": "50"
            }
        )

        max_num = 0
        for c in contracts:
            cn = c.get("contract_number", "")
            match = re.match(r"HR-V(\d+)", cn)
            if match:
                num = int(match.group(1))
                if num > max_num:
                    max_num = num

        next_num = max_num + 1
        return f"HR-V{next_num:02d}"

    else:
        # 未知分館，使用通用格式
        return f"HJ-{datetime.now().strftime('%Y%m%d')}-{branch_id}"


async def create_contract(
    branch_id: int,
    start_date: str,
    end_date: str,
    monthly_rent: float,
    # 承租人資訊（新架構：合約為主體）
    company_name: str = None,
    representative_name: str = None,
    representative_address: str = None,
    id_number: str = None,
    company_tax_id: str = None,
    phone: str = None,
    email: str = None,
    # 合約資訊
    customer_id: int = None,  # 可選，觸發器會自動查找/建立
    contract_type: str = "virtual_office",
    deposit_amount: float = 0,
    original_price: float = None,
    payment_cycle: str = "monthly",
    payment_day: int = 5,
    plan_name: str = None,
    broker_name: str = None,
    broker_firm_id: int = None,
    notes: str = None
) -> Dict[str, Any]:
    """
    建立新合約（以合約為主體的架構）

    觸發器會根據 company_tax_id 或 phone 自動查找或建立客戶

    Args:
        branch_id: 場館ID
        start_date: 開始日期 (YYYY-MM-DD)
        end_date: 結束日期 (YYYY-MM-DD)
        monthly_rent: 月租金
        company_name: 公司名稱
        representative_name: 負責人姓名
        representative_address: 負責人地址
        id_number: 身分證/居留證號碼
        company_tax_id: 公司統編（可為空，新設立公司）
        phone: 聯絡電話
        email: 電子郵件
        customer_id: 客戶ID（可選，如不提供則由觸發器處理）
        contract_type: 合約類型
        deposit_amount: 押金
        original_price: 定價（原價）
        payment_cycle: 繳費週期
        payment_day: 繳費日
        plan_name: 方案名稱
        broker_name: 介紹人
        broker_firm_id: 介紹會計事務所ID
        notes: 備註

    Returns:
        新建合約資料
    """
    # 生成合約編號（根據分館使用正確格式）
    contract_number = await generate_contract_number(branch_id)
    logger.info(f"Generated contract number: {contract_number} for branch {branch_id}")

    data = {
        "contract_number": contract_number,
        "branch_id": branch_id,
        "start_date": start_date,
        "end_date": end_date,
        "monthly_rent": monthly_rent,
        "contract_type": contract_type,
        "deposit": deposit_amount,  # 資料庫欄位是 deposit 不是 deposit_amount
        "payment_cycle": payment_cycle,
        "payment_day": payment_day,
        "status": "active"  # 直接建立的合約狀態為 active
    }

    # 承租人資訊
    if company_name:
        data["company_name"] = company_name
    if representative_name:
        data["representative_name"] = representative_name
    if representative_address:
        data["representative_address"] = representative_address
    if id_number:
        data["id_number"] = id_number
    if company_tax_id:
        data["company_tax_id"] = company_tax_id
    if phone:
        data["phone"] = phone
    if email:
        data["email"] = email
    if original_price:
        data["original_price"] = original_price

    # 如果有明確指定 customer_id，使用它（跳過觸發器的自動查找）
    if customer_id:
        data["customer_id"] = customer_id

    # 其他欄位
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

        # 方案 B：營業登記合約建立後自動分配空位
        position_assigned = None
        if contract_type == "virtual_office" and not contract.get("position_number"):
            try:
                # 查詢該場館的空位（沒有被活動合約佔用的位置）
                vacant_positions = await postgrest_get("v_floor_positions", {
                    "branch_id": f"eq.{branch_id}",
                    "contract_id": "is.null",
                    "order": "position_number.asc",
                    "limit": 1
                })

                if vacant_positions and len(vacant_positions) > 0:
                    position_number = vacant_positions[0]["position_number"]
                    # 更新合約的位置編號
                    await postgrest_patch(
                        f"contracts?id=eq.{contract['id']}",
                        {"position_number": position_number}
                    )
                    contract["position_number"] = position_number
                    position_assigned = position_number
                    logger.info(f"合約 {contract['id']} 自動分配位置 {position_number}")
            except Exception as pos_err:
                logger.warning(f"自動分配位置失敗（不影響合約建立）: {pos_err}")

        # 建立繳費記錄
        payments_created = []
        try:
            # 計算第一期繳費日期和期間
            start_dt = datetime.fromisoformat(start_date)
            first_payment_period = start_dt.strftime("%Y-%m")

            # 計算 due_date：使用 payment_day，若超過該月最後一天則用月底
            last_day_of_month = calendar.monthrange(start_dt.year, start_dt.month)[1]
            actual_payment_day = min(payment_day, last_day_of_month)
            first_due_date = date(start_dt.year, start_dt.month, actual_payment_day).isoformat()

            # 押金記錄（如果有押金）
            if deposit_amount and deposit_amount > 0:
                deposit_payment = {
                    "contract_id": contract["id"],
                    "customer_id": contract.get("customer_id"),
                    "branch_id": branch_id,
                    "payment_type": "deposit",
                    "payment_period": first_payment_period,
                    "amount": deposit_amount,
                    "due_date": first_due_date,
                    "payment_status": "pending"
                }
                await postgrest_post("payments", deposit_payment)
                payments_created.append("押金")
                logger.info(f"Created deposit payment for contract {contract['id']}")

            # 第一期租金記錄
            if monthly_rent and monthly_rent > 0:
                # 根據繳費週期計算當期金額
                cycle_multiplier = {
                    'monthly': 1,
                    'quarterly': 3,
                    'semi_annual': 6,
                    'annual': 12,
                    'biennial': 24,
                    'triennial': 36
                }
                multiplier = cycle_multiplier.get(payment_cycle, 1)
                period_amount = monthly_rent * multiplier

                first_rent_payment = {
                    "contract_id": contract["id"],
                    "customer_id": contract.get("customer_id"),
                    "branch_id": branch_id,
                    "payment_type": "rent",
                    "payment_period": first_payment_period,
                    "amount": period_amount,
                    "due_date": first_due_date,
                    "payment_status": "pending"
                }
                await postgrest_post("payments", first_rent_payment)
                payments_created.append(f"第一期租金 ${period_amount:,.0f}")
                logger.info(f"Created first rent payment for contract {contract['id']}, amount: {period_amount} ({payment_cycle})")

        except Exception as pay_err:
            logger.warning(f"繳費記錄建立失敗（不影響合約建立）: {pay_err}")

        # 組合訊息
        message = f"合約 {contract.get('contract_number')} 建立成功"
        if position_assigned:
            message += f"，已自動分配位置 {position_assigned}"
        if payments_created:
            message += f"，已建立繳費記錄：{', '.join(payments_created)}"

        return {
            "success": True,
            "message": message,
            "contract": contract,
            "contract_id": contract["id"],
            "contract_number": contract.get("contract_number"),
            "position_assigned": position_assigned,
            "payments_created": payments_created
        }
    except Exception as e:
        logger.error(f"create_contract error: {e}")
        raise Exception(f"建立合約失敗: {e}")


async def contract_renew(
    contract_id: int,
    new_start_date: str,
    new_end_date: str,
    new_monthly_rent: float = None,
    new_deposit_amount: float = None,
    notes: str = None
) -> Dict[str, Any]:
    """
    續約：將舊合約標記為「已續約」，建立新合約

    Args:
        contract_id: 舊合約ID
        new_start_date: 新合約開始日期
        new_end_date: 新合約結束日期
        new_monthly_rent: 新月租金（不填則沿用）
        new_deposit_amount: 新押金（不填則沿用）
        notes: 備註

    Returns:
        續約結果，包含新舊合約資訊
    """
    try:
        # 1. 取得舊合約
        contracts = await postgrest_get("contracts", {"id": f"eq.{contract_id}"})
        if not contracts:
            return {"success": False, "message": "找不到合約"}

        old_contract = contracts[0]

        # 2. 檢查狀態（只有 active 的合約才能續約）
        if old_contract.get("status") not in ["active", "expired"]:
            return {
                "success": False,
                "message": f"只有生效中或已到期的合約才能續約，目前狀態為 {old_contract.get('status')}"
            }

        # 3. 建立新合約（複製舊合約的客戶資訊）
        new_contract_data = {
            "branch_id": old_contract.get("branch_id"),
            "customer_id": old_contract.get("customer_id"),
            "contract_type": old_contract.get("contract_type"),
            "plan_name": old_contract.get("plan_name"),
            "start_date": new_start_date,
            "end_date": new_end_date,
            "monthly_rent": new_monthly_rent or old_contract.get("monthly_rent"),
            "original_price": old_contract.get("original_price"),
            "deposit_amount": new_deposit_amount if new_deposit_amount is not None else old_contract.get("deposit_amount", 0),
            "payment_cycle": old_contract.get("payment_cycle", "monthly"),
            "payment_day": old_contract.get("payment_day", 5),
            "status": "pending",
            "renewed_from_id": contract_id,
            # 複製承租人資訊
            "company_name": old_contract.get("company_name"),
            "representative_name": old_contract.get("representative_name"),
            "representative_address": old_contract.get("representative_address"),
            "id_number": old_contract.get("id_number"),
            "company_tax_id": old_contract.get("company_tax_id"),
            "phone": old_contract.get("phone"),
            "email": old_contract.get("email"),
        }

        if notes:
            new_contract_data["notes"] = notes
        else:
            new_contract_data["notes"] = f"續約自合約 #{contract_id}"

        # 複製介紹人資訊
        if old_contract.get("broker_name"):
            new_contract_data["broker_name"] = old_contract.get("broker_name")
        if old_contract.get("broker_firm_id"):
            new_contract_data["broker_firm_id"] = old_contract.get("broker_firm_id")
            new_contract_data["commission_eligible"] = True

        new_result = await postgrest_post("contracts", new_contract_data)
        new_contract = new_result[0] if isinstance(new_result, list) else new_result

        # 4. 更新舊合約狀態為「已續約」
        await postgrest_patch(
            "contracts",
            {"id": f"eq.{contract_id}"},
            {"status": "renewed"}
        )

        return {
            "success": True,
            "message": f"續約成功",
            "old_contract": {
                "id": contract_id,
                "contract_number": old_contract.get("contract_number"),
                "status": "renewed"
            },
            "new_contract": {
                "id": new_contract["id"],
                "contract_number": new_contract.get("contract_number"),
                "start_date": new_start_date,
                "end_date": new_end_date,
                "monthly_rent": new_contract.get("monthly_rent"),
                "status": "pending"
            }
        }

    except Exception as e:
        logger.error(f"contract_renew error: {e}")
        raise Exception(f"續約失敗: {e}")


async def contract_update_tax_id(
    contract_id: int,
    company_tax_id: str,
    update_customer: bool = True
) -> Dict[str, Any]:
    """
    補上公司統編（新設立公司後續補上）

    Args:
        contract_id: 合約ID
        company_tax_id: 公司統編
        update_customer: 是否同時更新客戶表（預設是）

    Returns:
        更新結果
    """
    if not company_tax_id or len(company_tax_id) != 8:
        return {"success": False, "message": "統編格式錯誤，應為8碼"}

    try:
        # 1. 取得合約
        contracts = await postgrest_get("contracts", {"id": f"eq.{contract_id}"})
        if not contracts:
            return {"success": False, "message": "找不到合約"}

        contract = contracts[0]

        # 2. 更新合約的統編
        await postgrest_patch(
            "contracts",
            {"id": f"eq.{contract_id}"},
            {"company_tax_id": company_tax_id}
        )

        # 3. 同時更新客戶表
        customer_updated = False
        if update_customer and contract.get("customer_id"):
            await postgrest_patch(
                "customers",
                {"id": f"eq.{contract['customer_id']}"},
                {"company_tax_id": company_tax_id}
            )
            customer_updated = True

        return {
            "success": True,
            "message": f"統編已更新為 {company_tax_id}",
            "contract_id": contract_id,
            "customer_updated": customer_updated,
            "note": "請重新產生合約 PDF 以包含新統編"
        }

    except Exception as e:
        logger.error(f"contract_update_tax_id error: {e}")
        raise Exception(f"更新統編失敗: {e}")
