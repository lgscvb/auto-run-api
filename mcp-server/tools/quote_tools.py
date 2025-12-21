"""
Hour Jungle CRM - Quote Tools
報價單相關工具
"""

import logging
import json
import calendar
from datetime import datetime, date, timedelta
from typing import Optional, List, Dict, Any

import httpx
import google.auth
from google.auth.transport.requests import Request
from google.oauth2 import id_token

logger = logging.getLogger(__name__)

# PostgREST URL (從環境變數)
import os
POSTGREST_URL = os.getenv("POSTGREST_URL", "http://postgrest:3000")

# Cloud Run PDF Generator URL
PDF_GENERATOR_URL = os.getenv(
    "PDF_GENERATOR_URL",
    "https://pdf-generator-743652001579.asia-east1.run.app"
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
        if response.status_code >= 400:
            logger.error(f"PostgREST POST error: {response.status_code} - {response.text}")
            logger.error(f"Request data: {data}")
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
        if response.status_code >= 400:
            logger.error(f"PostgREST PATCH error: {response.status_code} - {response.text}")
            logger.error(f"Request params: {params}, data: {data}")
        response.raise_for_status()
        return response.json()


async def postgrest_delete(endpoint: str, params: dict) -> bool:
    """PostgREST DELETE 請求"""
    url = f"{POSTGREST_URL}/{endpoint}"
    async with httpx.AsyncClient() as client:
        response = await client.delete(url, params=params, timeout=30.0)
        response.raise_for_status()
        return True


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
    import re

    if branch_id == 1:
        # 大忠館：DZ-XXX 格式
        prefix = "DZ-"
        # 查詢現有最大編號（取全部，因為字串排序無法正確取得最大數字編號）
        contracts = await postgrest_get(
            "contracts",
            {
                "select": "contract_number",
                "contract_number": "like.DZ-%",
                "limit": "2000"  # 取全部後在程式碼中找最大值
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
        prefix = "HR-V"
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
            # 匹配 HR-VXX 格式
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


# ============================================================================
# 報價單工具
# ============================================================================

# ============================================================================
# 服務價格表工具
# ============================================================================

async def list_service_plans(
    category: str = None,
    is_active: bool = True
) -> Dict[str, Any]:
    """
    取得服務價格列表

    Args:
        category: 分類篩選 (空間服務/登記服務/代辦服務)
        is_active: 是否只顯示啟用的服務

    Returns:
        服務列表，按分類和排序
    """
    params = {"order": "sort_order.asc"}

    if category:
        params["category"] = f"eq.{category}"
    if is_active is not None:
        params["is_active"] = f"eq.{is_active}"

    try:
        plans = await postgrest_get("service_plans", params)

        # 按分類分組
        by_category = {}
        for plan in plans:
            cat = plan.get("category", "其他")
            if cat not in by_category:
                by_category[cat] = []
            by_category[cat].append(plan)

        return {
            "count": len(plans),
            "by_category": by_category,
            "plans": plans
        }
    except Exception as e:
        logger.error(f"list_service_plans error: {e}")
        raise Exception(f"取得服務價格列表失敗: {e}")


async def get_service_plan(code: str) -> Dict[str, Any]:
    """
    根據代碼取得服務方案

    Args:
        code: 服務代碼 (如 virtual_office_2year)

    Returns:
        服務方案詳情
    """
    try:
        plans = await postgrest_get("service_plans", {"code": f"eq.{code}"})
        if not plans:
            return {"found": False, "message": f"找不到服務代碼: {code}"}

        return {
            "found": True,
            "plan": plans[0]
        }
    except Exception as e:
        logger.error(f"get_service_plan error: {e}")
        raise Exception(f"取得服務方案失敗: {e}")


async def create_quote_from_service_plans(
    branch_id: int,
    service_codes: List[str],
    customer_name: str = None,
    customer_phone: str = None,
    customer_email: str = None,
    company_name: str = None,
    contract_months: int = None,
    discount_amount: float = 0,
    discount_note: str = None,
    valid_days: int = 30,
    internal_notes: str = None,
    customer_notes: str = None,
    line_user_id: str = None
) -> Dict[str, Any]:
    """
    根據服務代碼建立報價單

    自動從 service_plans 表取得價格資訊，建立報價單項目

    Args:
        branch_id: 場館ID
        service_codes: 服務代碼列表 (如 ['virtual_office_2year', 'company_setup'])
        customer_name: 客戶姓名
        customer_phone: 客戶電話
        customer_email: 客戶Email
        company_name: 公司名稱
        contract_months: 合約月數（覆蓋預設值）
        discount_amount: 折扣金額
        discount_note: 折扣說明
        valid_days: 有效天數
        internal_notes: 內部備註
        customer_notes: 給客戶的備註
        line_user_id: LINE User ID

    Returns:
        新建報價單
    """
    try:
        # 1. 取得所有服務方案
        items = []
        total_deposit = 0
        primary_contract_type = "virtual_office"
        primary_plan_name = None
        default_contract_months = 12

        for code in service_codes:
            plan_result = await get_service_plan(code)
            if not plan_result.get("found"):
                logger.warning(f"找不到服務代碼: {code}，跳過")
                continue

            plan = plan_result["plan"]

            # 計算金額
            unit_price = float(plan.get("unit_price", 0))
            quantity = 1

            # 根據計費方式計算
            unit = plan.get("unit", "月")
            billing_cycle = plan.get("billing_cycle", "monthly")

            # 如果是月租，根據合約月數計算
            if unit == "月" and billing_cycle in ["monthly", "semi_annual", "annual"]:
                if contract_months:
                    quantity = contract_months
                elif plan.get("min_duration"):
                    # 從最低租期推算
                    if "2年" in plan.get("min_duration", ""):
                        quantity = 24
                        default_contract_months = 24
                    elif "1年" in plan.get("min_duration", ""):
                        quantity = 12
                        default_contract_months = 12

            item = {
                "name": plan.get("name"),
                "code": code,
                "quantity": quantity,
                "unit": unit,
                "unit_price": unit_price,
                "amount": unit_price * quantity,
                "revenue_type": plan.get("revenue_type", "own"),  # own=自己收款, referral=代辦服務
                "billing_cycle": billing_cycle  # one_time=一次性, monthly=月繳
            }
            items.append(item)

            # 累計押金
            deposit = float(plan.get("deposit", 0))
            if deposit > 0:
                total_deposit += deposit

            # 設定主要方案名稱（取第一個）
            if not primary_plan_name:
                primary_plan_name = plan.get("name")
                # 推斷合約類型
                if "借址" in plan.get("name", "") or "登記" in plan.get("name", ""):
                    primary_contract_type = "virtual_office"
                elif "辦公室" in plan.get("name", ""):
                    primary_contract_type = "private_office"
                elif "會議室" in plan.get("name", ""):
                    primary_contract_type = "meeting_room"
                elif "共享" in plan.get("name", ""):
                    primary_contract_type = "coworking"

        if not items:
            return {
                "success": False,
                "message": "沒有有效的服務項目"
            }

        # 2. 使用現有的 create_quote 函數建立報價單
        result = await create_quote(
            branch_id=branch_id,
            customer_name=customer_name,
            customer_phone=customer_phone,
            customer_email=customer_email,
            company_name=company_name,
            contract_type=primary_contract_type,
            plan_name=primary_plan_name,
            contract_months=contract_months or default_contract_months,
            items=items,
            discount_amount=discount_amount,
            discount_note=discount_note,
            deposit_amount=total_deposit,
            valid_days=valid_days,
            internal_notes=internal_notes,
            customer_notes=customer_notes,
            line_user_id=line_user_id
        )

        if result.get("success"):
            result["service_plans_used"] = service_codes
            result["message"] = f"報價單建立成功，包含 {len(items)} 項服務"

        return result

    except Exception as e:
        logger.error(f"create_quote_from_service_plans error: {e}")
        raise Exception(f"建立報價單失敗: {e}")


async def list_quotes(
    branch_id: int = None,
    status: str = None,
    customer_id: int = None,
    limit: int = 50
) -> Dict[str, Any]:
    """
    列出報價單

    Args:
        branch_id: 場館ID
        status: 狀態篩選 (draft/sent/viewed/accepted/rejected/expired/converted)
        customer_id: 客戶ID
        limit: 回傳筆數

    Returns:
        報價單列表
    """
    params = {
        "limit": limit,
        "order": "created_at.desc"
    }

    if branch_id:
        params["branch_id"] = f"eq.{branch_id}"
    if status:
        params["status"] = f"eq.{status}"
    if customer_id:
        params["customer_id"] = f"eq.{customer_id}"

    try:
        quotes = await postgrest_get("v_quotes", params)

        # 統計
        stats = {
            "draft": 0,
            "sent": 0,
            "accepted": 0,
            "expired": 0
        }
        for q in quotes:
            if q.get("status") in stats:
                stats[q["status"]] += 1
            if q.get("is_expired") and q.get("status") not in ["accepted", "converted", "rejected"]:
                stats["expired"] += 1

        return {
            "count": len(quotes),
            "stats": stats,
            "quotes": quotes
        }
    except Exception as e:
        logger.error(f"list_quotes error: {e}")
        raise Exception(f"取得報價單列表失敗: {e}")


async def get_quote(quote_id: int) -> Dict[str, Any]:
    """
    取得報價單詳情

    Args:
        quote_id: 報價單ID

    Returns:
        報價單詳情
    """
    try:
        quotes = await postgrest_get("v_quotes", {"id": f"eq.{quote_id}"})
        if not quotes:
            return {"found": False, "message": "找不到報價單"}

        return {
            "found": True,
            "quote": quotes[0]
        }
    except Exception as e:
        logger.error(f"get_quote error: {e}")
        raise Exception(f"取得報價單失敗: {e}")


async def create_quote(
    branch_id: int,
    customer_id: int = None,
    customer_name: str = None,
    customer_phone: str = None,
    customer_email: str = None,
    company_name: str = None,
    contract_type: str = "virtual_office",
    plan_name: str = None,
    contract_months: int = 12,
    proposed_start_date: str = None,
    original_price: float = None,
    items: List[Dict] = None,
    discount_amount: float = 0,
    discount_note: str = None,
    deposit_amount: float = 0,
    valid_days: int = 30,
    internal_notes: str = None,
    customer_notes: str = None,
    created_by: str = None,
    line_user_id: str = None
) -> Dict[str, Any]:
    """
    建立報價單

    Args:
        branch_id: 場館ID
        customer_id: 客戶ID (可選)
        customer_name: 客戶姓名 (未建立客戶時)
        customer_phone: 客戶電話
        customer_email: 客戶Email
        company_name: 公司名稱
        contract_type: 合約類型
        plan_name: 方案名稱
        contract_months: 合約月數
        proposed_start_date: 預計開始日期
        original_price: 服務原價（用於合約，例如營業登記原價 3000）
        items: 費用項目 [{name, quantity, unit_price, amount}]
        discount_amount: 折扣金額
        discount_note: 折扣說明
        deposit_amount: 押金
        valid_days: 有效天數
        internal_notes: 內部備註
        customer_notes: 給客戶的備註
        created_by: 建立者

    Returns:
        新建報價單
    """
    # 計算金額
    items = items or []
    subtotal = sum(item.get("amount", 0) for item in items)
    total_amount = subtotal - (discount_amount or 0)

    data = {
        "branch_id": branch_id,
        "contract_type": contract_type,
        "contract_months": contract_months,
        "items": items,  # PostgREST JSONB 欄位直接傳 list，httpx 會自動序列化
        "subtotal": subtotal,
        "discount_amount": discount_amount or 0,
        "total_amount": total_amount,
        "deposit_amount": deposit_amount or 0,
        "valid_from": date.today().isoformat(),
        "valid_until": (date.today() + timedelta(days=valid_days)).isoformat(),
        "status": "draft"
    }

    if customer_id:
        data["customer_id"] = customer_id
    if customer_name:
        data["customer_name"] = customer_name
    if customer_phone:
        data["customer_phone"] = customer_phone
    if customer_email:
        data["customer_email"] = customer_email
    if company_name:
        data["company_name"] = company_name
    if plan_name:
        data["plan_name"] = plan_name
    if proposed_start_date:
        data["proposed_start_date"] = proposed_start_date
    if original_price:
        data["original_price"] = original_price
    if discount_note:
        data["discount_note"] = discount_note
    if internal_notes:
        data["internal_notes"] = internal_notes
    if customer_notes:
        data["customer_notes"] = customer_notes
    if created_by:
        data["created_by"] = created_by
    if line_user_id:
        data["line_user_id"] = line_user_id

    try:
        result = await postgrest_post("quotes", data)
        quote = result[0] if isinstance(result, list) else result

        return {
            "success": True,
            "message": f"報價單 {quote.get('quote_number', quote['id'])} 建立成功",
            "quote": quote
        }
    except Exception as e:
        logger.error(f"create_quote error: {e}")
        raise Exception(f"建立報價單失敗: {e}")


async def update_quote(
    quote_id: int,
    updates: Dict[str, Any]
) -> Dict[str, Any]:
    """
    更新報價單

    Args:
        quote_id: 報價單ID
        updates: 要更新的欄位

    Returns:
        更新後的報價單
    """
    # 允許更新的欄位
    allowed_fields = [
        "customer_id", "customer_name", "customer_phone", "customer_email",
        "company_name", "contract_type", "plan_name", "contract_months",
        "proposed_start_date", "items", "subtotal", "discount_amount",
        "discount_note", "tax_amount", "total_amount", "deposit_amount",
        "valid_from", "valid_until", "status", "internal_notes", "customer_notes"
    ]

    # 過濾非允許的欄位
    filtered_updates = {k: v for k, v in updates.items() if k in allowed_fields}

    if not filtered_updates:
        raise ValueError("沒有有效的更新欄位")

    # 如果更新 items，重新計算金額
    if "items" in filtered_updates:
        items = filtered_updates["items"]
        if isinstance(items, list):
            subtotal = sum(item.get("amount", 0) for item in items)
            filtered_updates["subtotal"] = subtotal
            discount = filtered_updates.get("discount_amount", 0)
            filtered_updates["total_amount"] = subtotal - discount
            filtered_updates["items"] = json.dumps(items)

    try:
        result = await postgrest_patch(
            "quotes",
            {"id": f"eq.{quote_id}"},
            filtered_updates
        )

        if not result:
            return {"success": False, "message": "找不到報價單"}

        quote = result[0] if isinstance(result, list) else result

        return {
            "success": True,
            "message": "報價單更新成功",
            "updated_fields": list(filtered_updates.keys()),
            "quote": quote
        }
    except Exception as e:
        logger.error(f"update_quote error: {e}")
        raise Exception(f"更新報價單失敗: {e}")


async def update_quote_status(
    quote_id: int,
    status: str,
    notes: str = None
) -> Dict[str, Any]:
    """
    更新報價單狀態

    Args:
        quote_id: 報價單ID
        status: 新狀態 (draft/sent/viewed/accepted/rejected/expired/converted)
        notes: 備註

    Returns:
        更新後的報價單
    """
    valid_statuses = ["draft", "sent", "viewed", "accepted", "rejected", "expired", "converted"]
    if status not in valid_statuses:
        raise ValueError(f"無效的狀態，允許: {', '.join(valid_statuses)}")

    update_data = {"status": status}

    # 根據狀態設置時間戳
    if status == "sent":
        update_data["sent_at"] = datetime.now().isoformat()
    elif status == "viewed":
        update_data["viewed_at"] = datetime.now().isoformat()
    elif status in ["accepted", "rejected"]:
        update_data["responded_at"] = datetime.now().isoformat()

    if notes:
        update_data["internal_notes"] = notes

    try:
        result = await postgrest_patch(
            "quotes",
            {"id": f"eq.{quote_id}"},
            update_data
        )

        if not result:
            return {"success": False, "message": "找不到報價單"}

        quote = result[0] if isinstance(result, list) else result

        return {
            "success": True,
            "message": f"報價單狀態已更新為 {status}",
            "quote": quote
        }
    except Exception as e:
        logger.error(f"update_quote_status error: {e}")
        raise Exception(f"更新報價單狀態失敗: {e}")


async def delete_quote(quote_id: int) -> Dict[str, Any]:
    """
    刪除報價單（僅限草稿狀態）

    Args:
        quote_id: 報價單ID

    Returns:
        刪除結果
    """
    try:
        # 先檢查狀態
        quotes = await postgrest_get("quotes", {"id": f"eq.{quote_id}"})
        if not quotes:
            return {"success": False, "message": "找不到報價單"}

        quote = quotes[0]
        # 只禁止刪除已轉換為合約的報價單
        if quote.get("status") == "converted":
            return {
                "success": False,
                "message": "無法刪除已轉換為合約的報價單"
            }

        await postgrest_delete("quotes", {"id": f"eq.{quote_id}"})

        return {
            "success": True,
            "message": f"報價單 {quote.get('quote_number')} 已刪除"
        }
    except Exception as e:
        logger.error(f"delete_quote error: {e}")
        raise Exception(f"刪除報價單失敗: {e}")


async def convert_quote_to_contract(
    quote_id: int,
    # 合約基本資訊
    start_date: str = None,
    end_date: str = None,
    payment_cycle: str = "monthly",
    payment_day: int = 5,
    # 承租人資訊（前端重新填寫）
    company_name: str = None,
    representative_name: str = None,
    representative_address: str = None,
    id_number: str = None,
    company_tax_id: str = None,
    phone: str = None,
    email: str = None,
    # 金額資訊
    original_price: float = None,
    monthly_rent: float = None,
    deposit_amount: float = None,
    # 其他
    notes: str = None
) -> Dict[str, Any]:
    """
    將已接受的報價單轉換為合約草稿

    業務流程：報價單 → 合約時，前端重新填寫完整客戶資訊
    因為報價階段客戶通常不會提供完整資訊

    Args:
        quote_id: 報價單ID
        start_date: 合約開始日期
        end_date: 合約結束日期
        payment_cycle: 繳費週期 (monthly/quarterly/semi_annual/annual)
        payment_day: 每期繳費日（1-28）
        company_name: 公司名稱
        representative_name: 負責人姓名
        representative_address: 負責人地址
        id_number: 身分證/居留證號碼
        company_tax_id: 公司統編（可為空，新設立公司）
        phone: 聯絡電話
        email: 電子郵件
        original_price: 定價（原價，用於違約金計算）
        monthly_rent: 折扣後月租金
        deposit_amount: 押金
        notes: 備註

    Returns:
        轉換結果，包含新建的合約資訊
    """
    try:
        # 1. 取得報價單
        quotes = await postgrest_get("v_quotes", {"id": f"eq.{quote_id}"})
        if not quotes:
            return {"success": False, "message": "找不到報價單"}

        quote = quotes[0]

        # 2. 檢查狀態
        if quote.get("status") != "accepted":
            return {
                "success": False,
                "message": f"只有已接受的報價單才能轉換為合約，目前狀態為 {quote.get('status')}"
            }

        # 3. 檢查是否已轉換過
        if quote.get("converted_contract_id"):
            return {
                "success": False,
                "message": f"此報價單已轉換過，合約 ID: {quote.get('converted_contract_id')}"
            }

        # 4. 計算合約日期
        contract_start = start_date or quote.get("proposed_start_date") or date.today().isoformat()
        contract_months = quote.get("contract_months", 12)

        # 計算結束日期（如果沒有提供）
        if not end_date:
            start_dt = datetime.fromisoformat(contract_start)
            end_dt = start_dt + timedelta(days=contract_months * 30)
            contract_end = end_dt.strftime("%Y-%m-%d")
        else:
            contract_end = end_date

        # 5. 計算金額（使用提供的值或從報價單 items 正確推算）
        if monthly_rent is None:
            # 從 items 中找出月租類型的 own 項目
            items_raw = quote.get("items", [])
            if isinstance(items_raw, str):
                items_raw = json.loads(items_raw)

            # 所有 own 項目（非 referral）
            own_items = [
                item for item in items_raw
                if item.get("revenue_type") != "referral"
            ]

            # 方式 1: 找有 billing_cycle 設定且為月租的項目
            monthly_own_items = [
                item for item in own_items
                if item.get("billing_cycle") not in [None, "one_time"]
            ]

            if monthly_own_items:
                # 月租金 = 所有月租項目的 unit_price 總和
                monthly_rent = sum(float(item.get("unit_price", 0)) for item in monthly_own_items)
                logger.info(f"Calculated monthly_rent from billing_cycle items: {monthly_rent}")
            elif own_items:
                # 方式 2: 如果沒有 billing_cycle 設定，使用 own 項目的 unit_price 總和
                # （假設所有 own 項目都是月租項目）
                monthly_rent = sum(float(item.get("unit_price", 0)) for item in own_items)
                logger.info(f"Calculated monthly_rent from all own items unit_price: {monthly_rent}")

                # 如果 unit_price 總和為 0，fallback 到 amount / months
                if monthly_rent == 0:
                    own_total = sum(float(item.get("amount", 0)) for item in own_items)
                    monthly_rent = round(own_total / contract_months) if contract_months > 0 else own_total
                    logger.info(f"Fallback: monthly_rent from amount/months: {monthly_rent}")
            else:
                monthly_rent = 0
                logger.info("No own items found, monthly_rent = 0")

        if deposit_amount is None:
            deposit_amount = quote.get("deposit_amount", 0)

        # 6. 建立合約（草稿狀態）
        # 注意：不傳 customer_id，讓觸發器根據統編/電話自動查找或建立
        branch_id = quote.get("branch_id")
        contract_number = await generate_contract_number(branch_id)
        logger.info(f"Generated contract number: {contract_number} for branch {branch_id}")

        contract_data = {
            "contract_number": contract_number,
            "branch_id": branch_id,
            "contract_type": quote.get("contract_type", "virtual_office"),
            "plan_name": quote.get("plan_name"),
            "start_date": contract_start,
            "end_date": contract_end,
            "monthly_rent": monthly_rent,
            "payment_cycle": payment_cycle,
            "payment_day": payment_day,
            "deposit": deposit_amount,  # 資料庫欄位是 deposit
            "status": "pending_sign",  # 待簽約
            # 承租人資訊（存入合約表，觸發器會自動建立/關聯客戶）
            "company_name": company_name or quote.get("company_name"),
            "representative_name": representative_name or quote.get("customer_name"),
            "phone": phone or quote.get("customer_phone"),
            "email": email or quote.get("customer_email"),
        }

        # 可選欄位
        if representative_address:
            contract_data["representative_address"] = representative_address
        if id_number:
            contract_data["id_number"] = id_number
        if company_tax_id:
            contract_data["company_tax_id"] = company_tax_id
        if original_price:
            contract_data["original_price"] = original_price
        if notes:
            contract_data["notes"] = notes
        else:
            contract_data["notes"] = f"從報價單 {quote.get('quote_number')} 轉換"

        contract_result = await postgrest_post("contracts", contract_data)
        contract = contract_result[0] if isinstance(contract_result, list) else contract_result

        # 6.5 方案 B：營業登記合約建立後自動分配空位
        position_assigned = None
        contract_type = quote.get("contract_type", "virtual_office")
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
                        "contracts",
                        {"id": f"eq.{contract['id']}"},
                        {"position_number": position_number}
                    )
                    contract["position_number"] = position_number
                    position_assigned = position_number
                    logger.info(f"合約 {contract['id']} 自動分配位置 {position_number}")
            except Exception as pos_err:
                logger.warning(f"自動分配位置失敗（不影響合約建立）: {pos_err}")

        # 7. 創建繳費記錄
        # 合約建立後自動產生繳費記錄，方便財務追蹤
        payments_created = []

        # 計算第一期繳費日期和期間
        start_dt = datetime.fromisoformat(contract_start)
        first_payment_period = start_dt.strftime("%Y-%m")  # 格式：2025-12

        # 計算 due_date：使用 payment_day，若超過該月最後一天則用月底
        last_day_of_month = calendar.monthrange(start_dt.year, start_dt.month)[1]
        actual_payment_day = min(payment_day, last_day_of_month)
        first_due_date = date(start_dt.year, start_dt.month, actual_payment_day).isoformat()

        # 7.1 押金記錄（如果有押金）
        if deposit_amount and deposit_amount > 0:
            deposit_payment = {
                "contract_id": contract["id"],
                "customer_id": contract.get("customer_id"),
                "branch_id": contract.get("branch_id") or quote.get("branch_id"),
                "payment_type": "deposit",
                "payment_period": first_payment_period,  # 使用年月格式，與腳本一致
                "amount": deposit_amount,
                "due_date": first_due_date,  # 押金在第一期繳費日到期
                "payment_status": "pending"
            }
            await postgrest_post("payments", deposit_payment)
            payments_created.append("押金")
            logger.info(f"Created deposit payment for contract {contract['id']}, period: {first_payment_period}")

        # 7.2 第一期租金記錄
        if monthly_rent and monthly_rent > 0:
            first_rent_payment = {
                "contract_id": contract["id"],
                "customer_id": contract.get("customer_id"),
                "branch_id": contract.get("branch_id") or quote.get("branch_id"),
                "payment_type": "rent",
                "payment_period": first_payment_period,  # 使用年月格式，與腳本一致
                "amount": monthly_rent,
                "due_date": first_due_date,  # 第一期在繳費日到期
                "payment_status": "pending"
            }
            await postgrest_post("payments", first_rent_payment)
            payments_created.append("第一期租金")
            logger.info(f"Created first rent payment for contract {contract['id']}, period: {first_payment_period}")

        # 8. 更新報價單狀態為已轉換
        await postgrest_patch(
            "quotes",
            {"id": f"eq.{quote_id}"},
            {
                "status": "converted",
                "converted_contract_id": contract["id"]
            }
        )

        # 組合回傳訊息
        payments_msg = f"，已建立繳費記錄：{', '.join(payments_created)}" if payments_created else ""
        position_msg = f"，已自動分配位置 {position_assigned}" if position_assigned else ""

        return {
            "success": True,
            "message": f"報價單已成功轉換為合約{payments_msg}{position_msg}",
            "contract": {
                "id": contract["id"],
                "contract_number": contract.get("contract_number"),
                "customer_id": contract.get("customer_id"),  # 觸發器自動填入
                "company_name": contract.get("company_name"),
                "representative_name": contract.get("representative_name"),
                "start_date": contract_start,
                "end_date": contract_end,
                "monthly_rent": monthly_rent,
                "deposit": deposit_amount,
                "status": "pending_sign",
                "position_number": position_assigned
            },
            "quote_number": quote.get("quote_number"),
            "payments_created": payments_created,
            "position_assigned": position_assigned
        }

    except Exception as e:
        logger.error(f"convert_quote_to_contract error: {e}")
        raise Exception(f"報價單轉換失敗: {e}")


def get_id_token_for_cloud_run(target_url: str) -> str:
    """取得 Cloud Run 的 ID Token"""
    try:
        credentials, project = google.auth.default()
        auth_req = Request()
        token = id_token.fetch_id_token(auth_req, target_url)
        return token
    except Exception as e:
        logger.warning(f"無法取得 ID Token: {e}，嘗試不帶認證呼叫")
        return None


# 分館銀行帳戶設定
BRANCH_BANK_INFO = {
    1: {  # 大忠館
        "bank_account_name": "你的空間有限公司",
        "bank_name": "永豐商業銀行(南台中分行)",
        "bank_code": "807",
        "bank_account_number": "03801800183399",
        "contact_email": "wtxg@hourjungle.com",
        "contact_phone": "04-23760282"
    },
    2: {  # 環瑞館
        "bank_account_name": "你的空間有限公司",
        "bank_name": "永豐商業銀行(南台中分行)",
        "bank_code": "807",
        "bank_account_number": "03801800183399",
        "contact_email": "wtxg@hourjungle.com",
        "contact_phone": "04-23760282"
    }
}


async def quote_generate_pdf(quote_id: int) -> Dict[str, Any]:
    """
    生成報價單 PDF（呼叫 Cloud Run 服務）

    Args:
        quote_id: 報價單ID

    Returns:
        包含 GCS Signed URL 的結果
    """
    try:
        # 1. 取得報價單資料
        quotes = await postgrest_get("v_quotes", {"id": f"eq.{quote_id}"})
        if not quotes:
            return {"success": False, "message": "找不到報價單"}

        quote = quotes[0]

        # 2. 取得分館資訊
        branch_id = quote.get("branch_id", 1)
        branches = await postgrest_get("branches", {"id": f"eq.{branch_id}"})
        branch = branches[0] if branches else {}

        # 3. 解析項目
        items_raw = quote.get("items", [])
        if isinstance(items_raw, str):
            items_raw = json.loads(items_raw)

        items = []
        for item in items_raw:
            items.append({
                "name": item.get("name", ""),
                "quantity": item.get("quantity", 1),
                "unit_price": float(item.get("unit_price", 0)),
                "amount": float(item.get("amount", 0))
            })

        # 4. 取得銀行資訊
        bank_info = BRANCH_BANK_INFO.get(branch_id, BRANCH_BANK_INFO[1])

        # 5. 準備報價單資料
        quote_data = {
            "quote_id": quote_id,
            "quote_number": quote.get("quote_number", f"Q-{quote_id}"),
            "quote_date": quote.get("valid_from", date.today().isoformat()),
            "valid_until": quote.get("valid_until", ""),
            "branch_name": branch.get("name", "台中館"),
            "section_title": f"{quote.get('plan_name', '')}（依合約內指定付款時間點）" if quote.get('plan_name') else "",
            "items": items,
            "deposit_amount": float(quote.get("deposit_amount", 0)),
            "total_amount": float(quote.get("total_amount", 0)) + float(quote.get("deposit_amount", 0)),
            **bank_info
        }

        # 6. 呼叫 Cloud Run 服務
        token = get_id_token_for_cloud_run(PDF_GENERATOR_URL)

        headers = {"Content-Type": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"

        request_data = {
            "quote_data": quote_data,
            "template": "quote"
        }

        logger.info(f"呼叫 Cloud Run PDF 服務生成報價單: {quote_id}")

        async with httpx.AsyncClient(timeout=120.0) as client:
            response = await client.post(
                f"{PDF_GENERATOR_URL}/generate",
                json=request_data,
                headers=headers
            )

            if response.status_code == 401:
                return {
                    "success": False,
                    "message": "Cloud Run 認證失敗，請確認服務帳號權限"
                }

            response.raise_for_status()
            result = response.json()

        if result.get("success"):
            logger.info(f"報價單 PDF 生成成功: {result.get('pdf_path')}")
            return {
                "success": True,
                "message": result.get("message", "報價單 PDF 生成成功"),
                "quote_number": quote_data["quote_number"],
                "pdf_url": result.get("pdf_url"),
                "pdf_path": result.get("pdf_path"),
                "expires_at": result.get("expires_at")
            }
        else:
            return {
                "success": False,
                "message": result.get("message", "PDF 生成失敗")
            }

    except httpx.HTTPStatusError as e:
        logger.error(f"Cloud Run HTTP 錯誤: {e}")
        return {
            "success": False,
            "message": f"PDF 服務錯誤: {e.response.status_code}"
        }
    except Exception as e:
        logger.error(f"報價單 PDF 生成失敗: {e}")
        return {
            "success": False,
            "message": f"PDF 生成失敗: {e}"
        }


async def send_quote_to_line(
    quote_id: int,
    line_user_id: str
) -> Dict[str, Any]:
    """
    發送報價單給 LINE 用戶

    透過 LINE Messaging API 發送報價單摘要和 PDF 下載連結給潛在客戶

    Args:
        quote_id: 報價單ID
        line_user_id: LINE User ID

    Returns:
        發送結果
    """
    from tools.line_tools import send_line_push

    try:
        # 1. 取得報價單資料
        quotes = await postgrest_get("v_quotes", {"id": f"eq.{quote_id}"})
        if not quotes:
            return {"success": False, "message": "找不到報價單"}

        quote = quotes[0]

        # 2. 取得報價單資訊
        quote_number = quote.get("quote_number", f"Q-{quote_id}")
        plan_name = quote.get("plan_name", "報價方案")
        deposit_amount = quote.get("deposit_amount", 0)
        valid_until = quote.get("valid_until", "")
        branch_name = quote.get("branch_name", "Hour Jungle")
        customer_name = quote.get("customer_name", "貴賓")
        items = quote.get("items", []) or []

        # 3. 分離簽約費用與代辦服務
        own_items = [item for item in items if item.get("revenue_type") != "referral"]
        referral_items = [item for item in items if item.get("revenue_type") == "referral"]

        # 計算簽約應付金額（自己收款的項目）
        own_total = sum(float(item.get("amount", 0)) for item in own_items)
        sign_total = own_total + float(deposit_amount)

        # 計算代辦服務金額
        referral_total = sum(float(item.get("amount", 0)) for item in referral_items)

        # 4. 建構 Flex Message body 內容
        body_contents = [
            # 報價單號
            {
                "type": "box",
                "layout": "horizontal",
                "contents": [
                    {"type": "text", "text": "報價單號", "size": "sm", "color": "#888888", "flex": 1},
                    {"type": "text", "text": quote_number, "size": "sm", "color": "#333333", "flex": 2, "align": "end"}
                ]
            },
            # 方案
            {
                "type": "box",
                "layout": "horizontal",
                "contents": [
                    {"type": "text", "text": "方案", "size": "sm", "color": "#888888", "flex": 1},
                    {"type": "text", "text": plan_name[:20] + ("..." if len(plan_name) > 20 else ""), "size": "sm", "color": "#333333", "flex": 2, "align": "end"}
                ]
            },
        ]

        # 簽約應付款項區塊
        if own_items or deposit_amount > 0:
            body_contents.append({"type": "separator", "margin": "lg"})
            body_contents.append({
                "type": "text",
                "text": "【簽約應付款項】",
                "size": "sm",
                "color": "#2d5a27",
                "weight": "bold",
                "margin": "lg"
            })

            # 列出自己收款的項目
            for item in own_items:
                item_name = item.get("name", "")
                item_amount = float(item.get("amount", 0))
                body_contents.append({
                    "type": "box",
                    "layout": "horizontal",
                    "margin": "sm",
                    "contents": [
                        {"type": "text", "text": item_name[:15] + ("..." if len(item_name) > 15 else ""), "size": "xs", "color": "#666666", "flex": 2},
                        {"type": "text", "text": f"${item_amount:,.0f}", "size": "xs", "color": "#333333", "flex": 1, "align": "end"}
                    ]
                })

            # 押金
            if deposit_amount > 0:
                body_contents.append({
                    "type": "box",
                    "layout": "horizontal",
                    "margin": "sm",
                    "contents": [
                        {"type": "text", "text": "押金", "size": "xs", "color": "#666666", "flex": 2},
                        {"type": "text", "text": f"${deposit_amount:,.0f}", "size": "xs", "color": "#333333", "flex": 1, "align": "end"}
                    ]
                })

            # 簽約應付合計
            body_contents.append({
                "type": "box",
                "layout": "horizontal",
                "margin": "md",
                "contents": [
                    {"type": "text", "text": "簽約應付合計", "size": "md", "color": "#2d5a27", "weight": "bold", "flex": 2},
                    {"type": "text", "text": f"${sign_total:,.0f}", "size": "lg", "color": "#2d5a27", "weight": "bold", "flex": 1, "align": "end"}
                ]
            })

        # 代辦服務區塊（如有）
        if referral_items:
            body_contents.append({"type": "separator", "margin": "lg"})
            body_contents.append({
                "type": "text",
                "text": "【代辦服務】",
                "size": "sm",
                "color": "#666666",
                "weight": "bold",
                "margin": "lg"
            })
            body_contents.append({
                "type": "text",
                "text": "費用於服務完成後收取",
                "size": "xxs",
                "color": "#999999",
                "margin": "xs"
            })

            for item in referral_items:
                item_name = item.get("name", "")
                billing_cycle = item.get("billing_cycle", "one_time")
                unit_price = float(item.get("unit_price", 0))
                item_amount = float(item.get("amount", 0))

                # 月繳服務顯示「每月金額」，一次性顯示「總金額」
                if billing_cycle != "one_time" and unit_price > 0:
                    display_amount = f"${unit_price:,.0f}/月"
                else:
                    display_amount = f"${item_amount:,.0f}"

                body_contents.append({
                    "type": "box",
                    "layout": "horizontal",
                    "margin": "sm",
                    "contents": [
                        {"type": "text", "text": item_name[:15] + ("..." if len(item_name) > 15 else ""), "size": "xs", "color": "#666666", "flex": 2},
                        {"type": "text", "text": display_amount, "size": "xs", "color": "#666666", "flex": 1, "align": "end"}
                    ]
                })

        # 有效期限
        body_contents.append({
            "type": "text",
            "text": f"報價有效期限：{valid_until}",
            "size": "xs",
            "color": "#999999",
            "margin": "lg",
            "align": "center"
        })

        # 5. 組裝 Flex Message
        flex_message = {
            "type": "flex",
            "altText": f"Hour Jungle 報價單 {quote_number}",
            "contents": {
                "type": "bubble",
                "size": "mega",
                "header": {
                    "type": "box",
                    "layout": "vertical",
                    "backgroundColor": "#2d5a27",
                    "paddingAll": "15px",
                    "contents": [
                        {
                            "type": "text",
                            "text": "HOUR JUNGLE",
                            "color": "#ffffff",
                            "size": "xs",
                            "weight": "bold"
                        },
                        {
                            "type": "text",
                            "text": f"{branch_name}報價單",
                            "color": "#ffffff",
                            "size": "lg",
                            "weight": "bold",
                            "margin": "sm"
                        }
                    ]
                },
                "body": {
                    "type": "box",
                    "layout": "vertical",
                    "paddingAll": "15px",
                    "spacing": "md",
                    "contents": body_contents
                },
                "footer": {
                    "type": "box",
                    "layout": "vertical",
                    "paddingAll": "15px",
                    "spacing": "sm",
                    "contents": [
                        {
                            "type": "button",
                            "action": {
                                "type": "uri",
                                "label": "查看完整報價單",
                                "uri": f"https://hj.yourspce.org/quote/{quote_number}"
                            },
                            "style": "primary",
                            "color": "#2d5a27"
                        },
                        {
                            "type": "text",
                            "text": "如有任何問題，歡迎隨時詢問！",
                            "size": "xs",
                            "color": "#888888",
                            "align": "center",
                            "margin": "md"
                        }
                    ]
                }
            }
        }

        # 5. 發送 LINE 訊息
        messages = [flex_message]
        result = await send_line_push(
            line_user_id=line_user_id,
            messages=messages,
            sender_name=customer_name,
            log_to_brain_enabled=True
        )

        if result.get("success"):
            # 6. 更新報價單狀態為已發送
            await postgrest_patch(
                "quotes",
                {"id": f"eq.{quote_id}"},
                {
                    "status": "sent",
                    "sent_at": datetime.now().isoformat(),
                    "line_user_id": line_user_id  # 記錄發送對象
                }
            )

            return {
                "success": True,
                "message": f"報價單 {quote_number} 已發送給 {customer_name}",
                "quote_id": quote_id,
                "quote_number": quote_number,
                "line_user_id": line_user_id
            }
        else:
            return {
                "success": False,
                "message": f"LINE 發送失敗: {result.get('error', '未知錯誤')}"
            }

    except Exception as e:
        logger.error(f"send_quote_to_line error: {e}")
        return {
            "success": False,
            "message": f"發送報價單失敗: {e}"
        }
