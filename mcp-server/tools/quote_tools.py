"""
Hour Jungle CRM - Quote Tools
報價單相關工具
"""

import logging
import json
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
# 報價單工具
# ============================================================================

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
    items: List[Dict] = None,
    discount_amount: float = 0,
    discount_note: str = None,
    deposit_amount: float = 0,
    valid_days: int = 30,
    internal_notes: str = None,
    customer_notes: str = None,
    created_by: str = None
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
        "items": json.dumps(items),
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
    if discount_note:
        data["discount_note"] = discount_note
    if internal_notes:
        data["internal_notes"] = internal_notes
    if customer_notes:
        data["customer_notes"] = customer_notes
    if created_by:
        data["created_by"] = created_by

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
        if quote.get("status") != "draft":
            return {
                "success": False,
                "message": f"只能刪除草稿狀態的報價單，目前狀態為 {quote.get('status')}"
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

        # 5. 計算金額（使用提供的值或從報價單推算）
        if monthly_rent is None:
            total_amount = quote.get("total_amount", 0)
            monthly_rent = round(total_amount / contract_months) if contract_months > 0 else total_amount

        if deposit_amount is None:
            deposit_amount = quote.get("deposit_amount", 0)

        # 6. 建立合約（草稿狀態）
        # 注意：不傳 customer_id，讓觸發器根據統編/電話自動查找或建立
        contract_data = {
            "branch_id": quote.get("branch_id"),
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

        # 7. 更新報價單狀態為已轉換
        await postgrest_patch(
            "quotes",
            {"id": f"eq.{quote_id}"},
            {
                "status": "converted",
                "converted_contract_id": contract["id"],
                "converted_at": datetime.now().isoformat()
            }
        )

        return {
            "success": True,
            "message": f"報價單已成功轉換為合約",
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
                "status": "pending_sign"
            },
            "quote_number": quote.get("quote_number")
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
