"""
Hour Jungle CRM - Invoice Tools
電子發票工具（光貿 Amego API）

API 文件：https://invoice.amego.tw/api_doc/
注意：環瑞和大忠館是兩家不同公司，使用不同的 API 憑證
"""

import logging
import json
import hashlib
import hmac
import time
from datetime import datetime
from typing import Optional, Dict, Any

import httpx

logger = logging.getLogger(__name__)

# 環境變數設定
import os

# 光貿 API 設定（兩組憑證）
AMEGO_API_BASE = os.getenv("AMEGO_API_BASE", "https://invoice.amego.tw/api")

# 環瑞 API 憑證 (branch_id = 1, 2)
AMEGO_API_KEY_HUANRUI = os.getenv("AMEGO_API_KEY_HUANRUI", "")
AMEGO_API_SECRET_HUANRUI = os.getenv("AMEGO_API_SECRET_HUANRUI", "")

# 大忠館 API 憑證 (branch_id = 3)
AMEGO_API_KEY_DAZHONG = os.getenv("AMEGO_API_KEY_DAZHONG", "")
AMEGO_API_SECRET_DAZHONG = os.getenv("AMEGO_API_SECRET_DAZHONG", "")

# PostgREST URL
POSTGREST_URL = os.getenv("POSTGREST_URL", "http://postgrest:3000")


def get_amego_credentials(branch_id: int) -> tuple[str, str]:
    """根據 branch_id 取得對應的 API 憑證"""
    if branch_id == 3:  # 大忠館
        return AMEGO_API_KEY_DAZHONG, AMEGO_API_SECRET_DAZHONG
    else:  # 環瑞（1號店、2號店）
        return AMEGO_API_KEY_HUANRUI, AMEGO_API_SECRET_HUANRUI


def generate_amego_signature(api_secret: str, params: dict) -> str:
    """生成光貿 API 簽名"""
    # 按 key 排序後串接
    sorted_params = sorted(params.items())
    param_str = "&".join([f"{k}={v}" for k, v in sorted_params])

    # HMAC-SHA256 簽名
    signature = hmac.new(
        api_secret.encode('utf-8'),
        param_str.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()

    return signature


async def postgrest_get(endpoint: str, params: dict = None) -> Any:
    """PostgREST GET 請求"""
    url = f"{POSTGREST_URL}/{endpoint}"
    async with httpx.AsyncClient() as client:
        response = await client.get(url, params=params, timeout=30.0)
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
# 發票工具
# ============================================================================

async def invoice_create(
    payment_id: int,
    invoice_type: str = "personal",
    buyer_name: str = None,
    buyer_tax_id: str = None,
    carrier_type: str = None,
    carrier_number: str = None,
    donate_code: str = None,
    print_flag: bool = False
) -> Dict[str, Any]:
    """
    開立電子發票

    Args:
        payment_id: 繳費記錄 ID
        invoice_type: 發票類型 (personal=個人, business=公司)
        buyer_name: 買受人名稱（公司發票必填）
        buyer_tax_id: 統一編號（公司發票必填）
        carrier_type: 載具類型 (mobile=手機條碼, natural_person=自然人憑證, donate=捐贈)
        carrier_number: 載具號碼
        donate_code: 愛心碼（捐贈時使用）
        print_flag: 是否列印

    Returns:
        發票開立結果
    """
    # 1. 取得繳費記錄
    try:
        payments = await postgrest_get("payments", {"id": f"eq.{payment_id}"})
        if not payments:
            return {"success": False, "message": "找不到繳費記錄"}

        payment = payments[0]

        # 檢查是否已開發票
        if payment.get("invoice_number"):
            return {
                "success": False,
                "message": f"此繳費已開立發票：{payment['invoice_number']}"
            }

        # 檢查是否已付款
        if payment.get("payment_status") != "paid":
            return {
                "success": False,
                "message": "繳費尚未完成，無法開立發票"
            }

        branch_id = payment.get("branch_id")
        amount = payment.get("amount", 0)

    except Exception as e:
        logger.error(f"取得繳費記錄失敗: {e}")
        raise Exception(f"取得繳費記錄失敗: {e}")

    # 2. 取得 API 憑證
    api_key, api_secret = get_amego_credentials(branch_id)

    if not api_key or not api_secret:
        return {
            "success": False,
            "message": f"分館 {branch_id} 的發票 API 憑證未設定",
            "note": "請在環境變數設定 AMEGO_API_KEY 和 AMEGO_API_SECRET"
        }

    # 3. 準備 API 參數
    timestamp = int(time.time())

    params = {
        "api_key": api_key,
        "timestamp": str(timestamp),
        "amount": str(int(amount)),  # 發票金額（整數）
        "tax_type": "1",  # 應稅
        "invoice_type": "07" if invoice_type == "personal" else "08",  # 07=一般, 08=特種
    }

    # 買受人資訊
    if invoice_type == "business":
        if not buyer_tax_id:
            return {"success": False, "message": "公司發票需要統一編號"}
        params["buyer_identifier"] = buyer_tax_id
        params["buyer_name"] = buyer_name or ""

    # 載具設定
    if carrier_type == "mobile" and carrier_number:
        params["carrier_type"] = "3J0002"  # 手機條碼
        params["carrier_id1"] = carrier_number
    elif carrier_type == "natural_person" and carrier_number:
        params["carrier_type"] = "CQ0001"  # 自然人憑證
        params["carrier_id1"] = carrier_number
    elif carrier_type == "donate" and donate_code:
        params["donate_mark"] = "1"
        params["love_code"] = donate_code

    # 是否列印
    params["print_mark"] = "Y" if print_flag else "N"

    # 生成簽名
    params["signature"] = generate_amego_signature(api_secret, params)

    # 4. 呼叫 API
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{AMEGO_API_BASE}/invoice/issue",
                data=params,
                timeout=30.0
            )
            result = response.json()

            if result.get("status") == "success" or result.get("code") == "0":
                invoice_number = result.get("invoice_number") or result.get("data", {}).get("invoice_number")
                invoice_date = result.get("invoice_date") or datetime.now().strftime("%Y-%m-%d")

                # 更新繳費記錄
                await postgrest_patch(
                    "payments",
                    {"id": f"eq.{payment_id}"},
                    {
                        "invoice_number": invoice_number,
                        "invoice_date": invoice_date,
                        "invoice_status": "issued"
                    }
                )

                return {
                    "success": True,
                    "message": f"發票開立成功",
                    "invoice_number": invoice_number,
                    "invoice_date": invoice_date,
                    "amount": amount
                }
            else:
                error_msg = result.get("message") or result.get("msg") or "未知錯誤"
                return {
                    "success": False,
                    "message": f"發票開立失敗: {error_msg}",
                    "api_response": result
                }

    except httpx.HTTPError as e:
        logger.error(f"發票 API 呼叫失敗: {e}")
        return {
            "success": False,
            "message": f"發票 API 呼叫失敗: {str(e)}"
        }


async def invoice_void(
    payment_id: int,
    reason: str
) -> Dict[str, Any]:
    """
    作廢發票

    Args:
        payment_id: 繳費記錄 ID
        reason: 作廢原因

    Returns:
        作廢結果
    """
    if not reason or not reason.strip():
        return {"success": False, "message": "必須提供作廢原因"}

    # 1. 取得繳費記錄
    try:
        payments = await postgrest_get("payments", {"id": f"eq.{payment_id}"})
        if not payments:
            return {"success": False, "message": "找不到繳費記錄"}

        payment = payments[0]
        invoice_number = payment.get("invoice_number")

        if not invoice_number:
            return {"success": False, "message": "此繳費尚未開立發票"}

        if payment.get("invoice_status") == "voided":
            return {"success": False, "message": "發票已作廢"}

        branch_id = payment.get("branch_id")

    except Exception as e:
        logger.error(f"取得繳費記錄失敗: {e}")
        raise Exception(f"取得繳費記錄失敗: {e}")

    # 2. 取得 API 憑證
    api_key, api_secret = get_amego_credentials(branch_id)

    if not api_key or not api_secret:
        return {
            "success": False,
            "message": f"分館 {branch_id} 的發票 API 憑證未設定"
        }

    # 3. 準備 API 參數
    timestamp = int(time.time())

    params = {
        "api_key": api_key,
        "timestamp": str(timestamp),
        "invoice_number": invoice_number,
        "void_reason": reason
    }

    params["signature"] = generate_amego_signature(api_secret, params)

    # 4. 呼叫 API
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{AMEGO_API_BASE}/invoice/void",
                data=params,
                timeout=30.0
            )
            result = response.json()

            if result.get("status") == "success" or result.get("code") == "0":
                # 更新繳費記錄
                await postgrest_patch(
                    "payments",
                    {"id": f"eq.{payment_id}"},
                    {
                        "invoice_status": "voided",
                        "notes": f"{payment.get('notes', '')}\n[發票作廢] {reason}"
                    }
                )

                return {
                    "success": True,
                    "message": f"發票 {invoice_number} 已作廢",
                    "invoice_number": invoice_number,
                    "reason": reason
                }
            else:
                error_msg = result.get("message") or result.get("msg") or "未知錯誤"
                return {
                    "success": False,
                    "message": f"發票作廢失敗: {error_msg}",
                    "api_response": result
                }

    except httpx.HTTPError as e:
        logger.error(f"發票 API 呼叫失敗: {e}")
        return {
            "success": False,
            "message": f"發票 API 呼叫失敗: {str(e)}"
        }


async def invoice_query(
    invoice_number: str = None,
    payment_id: int = None,
    branch_id: int = None,
    start_date: str = None,
    end_date: str = None
) -> Dict[str, Any]:
    """
    查詢發票

    Args:
        invoice_number: 發票號碼
        payment_id: 繳費記錄 ID
        branch_id: 分館 ID
        start_date: 開始日期 (YYYY-MM-DD)
        end_date: 結束日期 (YYYY-MM-DD)

    Returns:
        發票查詢結果
    """
    # 如果提供 payment_id，直接從資料庫查
    if payment_id:
        try:
            payments = await postgrest_get("payments", {"id": f"eq.{payment_id}"})
            if not payments:
                return {"success": False, "message": "找不到繳費記錄"}

            payment = payments[0]
            if not payment.get("invoice_number"):
                return {"found": False, "message": "此繳費尚未開立發票"}

            return {
                "found": True,
                "invoice": {
                    "invoice_number": payment.get("invoice_number"),
                    "invoice_date": payment.get("invoice_date"),
                    "invoice_status": payment.get("invoice_status"),
                    "amount": payment.get("amount"),
                    "payment_id": payment_id
                }
            }
        except Exception as e:
            logger.error(f"查詢發票失敗: {e}")
            raise Exception(f"查詢發票失敗: {e}")

    # 查詢多筆發票
    params = {"order": "paid_at.desc", "limit": 50}

    if invoice_number:
        params["invoice_number"] = f"eq.{invoice_number}"
    if branch_id:
        params["branch_id"] = f"eq.{branch_id}"
    if start_date:
        params["invoice_date"] = f"gte.{start_date}"
    if end_date:
        if "invoice_date" in params:
            params["invoice_date"] = f"gte.{start_date}&invoice_date=lte.{end_date}"
        else:
            params["invoice_date"] = f"lte.{end_date}"

    # 只查有發票的
    params["invoice_number"] = params.get("invoice_number", "not.is.null")

    try:
        payments = await postgrest_get("payments", params)

        invoices = [{
            "invoice_number": p.get("invoice_number"),
            "invoice_date": p.get("invoice_date"),
            "invoice_status": p.get("invoice_status"),
            "amount": p.get("amount"),
            "payment_id": p.get("id"),
            "customer_id": p.get("customer_id"),
            "branch_id": p.get("branch_id")
        } for p in payments if p.get("invoice_number")]

        return {
            "count": len(invoices),
            "invoices": invoices
        }

    except Exception as e:
        logger.error(f"查詢發票失敗: {e}")
        raise Exception(f"查詢發票失敗: {e}")


async def invoice_allowance(
    payment_id: int,
    allowance_amount: float,
    reason: str
) -> Dict[str, Any]:
    """
    開立折讓單

    Args:
        payment_id: 繳費記錄 ID
        allowance_amount: 折讓金額
        reason: 折讓原因

    Returns:
        折讓結果
    """
    if allowance_amount <= 0:
        return {"success": False, "message": "折讓金額必須大於 0"}

    if not reason or not reason.strip():
        return {"success": False, "message": "必須提供折讓原因"}

    # 1. 取得繳費記錄
    try:
        payments = await postgrest_get("payments", {"id": f"eq.{payment_id}"})
        if not payments:
            return {"success": False, "message": "找不到繳費記錄"}

        payment = payments[0]
        invoice_number = payment.get("invoice_number")

        if not invoice_number:
            return {"success": False, "message": "此繳費尚未開立發票"}

        if payment.get("invoice_status") == "voided":
            return {"success": False, "message": "發票已作廢，無法折讓"}

        if allowance_amount > payment.get("amount", 0):
            return {"success": False, "message": "折讓金額不可大於發票金額"}

        branch_id = payment.get("branch_id")

    except Exception as e:
        logger.error(f"取得繳費記錄失敗: {e}")
        raise Exception(f"取得繳費記錄失敗: {e}")

    # 2. 取得 API 憑證
    api_key, api_secret = get_amego_credentials(branch_id)

    if not api_key or not api_secret:
        return {
            "success": False,
            "message": f"分館 {branch_id} 的發票 API 憑證未設定"
        }

    # 3. 準備 API 參數
    timestamp = int(time.time())

    params = {
        "api_key": api_key,
        "timestamp": str(timestamp),
        "invoice_number": invoice_number,
        "allowance_amount": str(int(allowance_amount)),
        "allowance_reason": reason
    }

    params["signature"] = generate_amego_signature(api_secret, params)

    # 4. 呼叫 API
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{AMEGO_API_BASE}/invoice/allowance",
                data=params,
                timeout=30.0
            )
            result = response.json()

            if result.get("status") == "success" or result.get("code") == "0":
                allowance_number = result.get("allowance_number") or result.get("data", {}).get("allowance_number")

                # 更新繳費記錄備註
                await postgrest_patch(
                    "payments",
                    {"id": f"eq.{payment_id}"},
                    {
                        "notes": f"{payment.get('notes', '')}\n[折讓] {allowance_number} 金額:{allowance_amount} 原因:{reason}"
                    }
                )

                return {
                    "success": True,
                    "message": f"折讓單開立成功",
                    "allowance_number": allowance_number,
                    "invoice_number": invoice_number,
                    "allowance_amount": allowance_amount
                }
            else:
                error_msg = result.get("message") or result.get("msg") or "未知錯誤"
                return {
                    "success": False,
                    "message": f"折讓單開立失敗: {error_msg}",
                    "api_response": result
                }

    except httpx.HTTPError as e:
        logger.error(f"發票 API 呼叫失敗: {e}")
        return {
            "success": False,
            "message": f"發票 API 呼叫失敗: {str(e)}"
        }
