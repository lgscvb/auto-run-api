"""
Hour Jungle CRM - LINE Tools
LINE 訊息發送相關工具
"""

import logging
import os
from typing import Dict, Any, Optional

import httpx

logger = logging.getLogger(__name__)

LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "")
LINE_API_URL = "https://api.line.me/v2/bot/message/push"

POSTGREST_URL = os.getenv("POSTGREST_URL", "http://postgrest:3000")


async def postgrest_get(endpoint: str, params: dict = None) -> Any:
    """PostgREST GET 請求"""
    url = f"{POSTGREST_URL}/{endpoint}"
    async with httpx.AsyncClient() as client:
        response = await client.get(url, params=params, timeout=30.0)
        response.raise_for_status()
        return response.json()


async def send_line_push(line_user_id: str, messages: list) -> Dict[str, Any]:
    """
    發送 LINE Push Message

    Args:
        line_user_id: LINE User ID
        messages: 訊息內容列表

    Returns:
        發送結果
    """
    if not LINE_CHANNEL_ACCESS_TOKEN:
        logger.warning("LINE_CHANNEL_ACCESS_TOKEN not configured")
        return {
            "success": False,
            "error": "LINE Bot 未設定"
        }

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}"
    }

    payload = {
        "to": line_user_id,
        "messages": messages
    }

    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                LINE_API_URL,
                json=payload,
                headers=headers,
                timeout=30.0
            )

            if response.status_code == 200:
                return {"success": True}
            else:
                logger.error(f"LINE API error: {response.status_code} - {response.text}")
                return {
                    "success": False,
                    "error": f"LINE API 錯誤: {response.status_code}"
                }
    except Exception as e:
        logger.error(f"LINE send error: {e}")
        return {
            "success": False,
            "error": str(e)
        }


# ============================================================================
# LINE 工具
# ============================================================================

async def send_line_message(
    customer_id: int,
    message: str
) -> Dict[str, Any]:
    """
    發送 LINE 訊息給客戶

    Args:
        customer_id: 客戶ID
        message: 訊息內容

    Returns:
        發送結果
    """
    # 取得客戶 LINE ID
    try:
        customers = await postgrest_get("customers", {
            "id": f"eq.{customer_id}",
            "select": "id,name,line_user_id"
        })

        if not customers:
            return {
                "success": False,
                "error": "找不到客戶"
            }

        customer = customers[0]

        if not customer.get("line_user_id"):
            return {
                "success": False,
                "error": f"客戶 {customer['name']} 沒有綁定 LINE"
            }

        # 發送訊息
        messages = [{"type": "text", "text": message}]
        result = await send_line_push(customer["line_user_id"], messages)

        if result["success"]:
            return {
                "success": True,
                "message": f"已發送訊息給 {customer['name']}"
            }
        else:
            return result

    except Exception as e:
        logger.error(f"send_line_message error: {e}")
        raise Exception(f"發送訊息失敗: {e}")


async def send_payment_reminder(
    payment_id: int,
    reminder_type: str = "upcoming"
) -> Dict[str, Any]:
    """
    發送繳費提醒

    Args:
        payment_id: 付款ID
        reminder_type: 提醒類型 (upcoming/due/overdue)

    Returns:
        發送結果
    """
    # 取得付款資訊
    try:
        payments = await postgrest_get("v_payments_due", {
            "id": f"eq.{payment_id}"
        })

        if not payments:
            return {
                "success": False,
                "error": "找不到付款記錄"
            }

        payment = payments[0]

        if not payment.get("line_user_id"):
            return {
                "success": False,
                "error": f"客戶 {payment['customer_name']} 沒有綁定 LINE"
            }

        # 根據提醒類型產生訊息
        customer_name = payment["customer_name"]
        period = payment.get("payment_period", "")
        amount = payment["total_due"]
        due_date = payment["due_date"]

        if reminder_type == "upcoming":
            message = (
                f"親愛的 {customer_name} 您好 🙋‍♀️\n\n"
                f"提醒您 {period} 的租金 ${amount:,.0f} 將於 {due_date} 到期，"
                f"請記得繳費喔！\n\n"
                f"如有任何問題歡迎聯繫我們 💼"
            )
        elif reminder_type == "due":
            message = (
                f"親愛的 {customer_name} 您好 📢\n\n"
                f"您 {period} 的租金 ${amount:,.0f} 今天到期囉！\n"
                f"請儘快完成繳費，謝謝您的配合 🙏"
            )
        elif reminder_type == "overdue":
            overdue_days = payment.get("overdue_days", 0)
            message = (
                f"親愛的 {customer_name} 您好 ⚠️\n\n"
                f"您 {period} 的租金 ${amount:,.0f} 已逾期 {overdue_days} 天，"
                f"請儘速處理。\n\n"
                f"如有任何困難請聯繫我們協助處理 📞"
            )
        else:
            message = f"親愛的 {customer_name}，您有一筆 ${amount:,.0f} 的款項需要處理。"

        # 發送訊息
        messages = [{"type": "text", "text": message}]
        result = await send_line_push(payment["line_user_id"], messages)

        if result["success"]:
            return {
                "success": True,
                "message": f"已發送{reminder_type}提醒給 {customer_name}",
                "payment_id": payment_id,
                "reminder_type": reminder_type
            }
        else:
            return result

    except Exception as e:
        logger.error(f"send_payment_reminder error: {e}")
        raise Exception(f"發送繳費提醒失敗: {e}")


async def send_renewal_reminder(
    contract_id: int
) -> Dict[str, Any]:
    """
    發送續約提醒

    Args:
        contract_id: 合約ID

    Returns:
        發送結果
    """
    # 取得合約資訊
    try:
        renewals = await postgrest_get("v_renewal_reminders", {
            "contract_id": f"eq.{contract_id}"
        })

        if not renewals:
            return {
                "success": False,
                "error": "找不到合約或合約不在續約提醒範圍內"
            }

        renewal = renewals[0]

        if not renewal.get("line_user_id"):
            return {
                "success": False,
                "error": f"客戶 {renewal['customer_name']} 沒有綁定 LINE"
            }

        # 產生續約提醒訊息
        customer_name = renewal["customer_name"]
        end_date = renewal["end_date"]
        days_remaining = renewal["days_remaining"]
        branch_name = renewal["branch_name"]

        if days_remaining <= 7:
            urgency = "⚠️ 緊急"
        elif days_remaining <= 30:
            urgency = "📢 重要"
        else:
            urgency = "📋 提醒"

        message = (
            f"{urgency} 續約通知\n\n"
            f"親愛的 {customer_name} 您好，\n\n"
            f"您在 {branch_name} 的合約將於 {end_date} 到期，"
            f"距今還有 {days_remaining} 天。\n\n"
            f"如需續約或有任何問題，歡迎隨時聯繫我們！\n\n"
            f"感謝您對 Hour Jungle 的支持 🙏"
        )

        # 發送訊息
        messages = [{"type": "text", "text": message}]
        result = await send_line_push(renewal["line_user_id"], messages)

        if result["success"]:
            return {
                "success": True,
                "message": f"已發送續約提醒給 {customer_name}",
                "contract_id": contract_id,
                "days_remaining": days_remaining
            }
        else:
            return result

    except Exception as e:
        logger.error(f"send_renewal_reminder error: {e}")
        raise Exception(f"發送續約提醒失敗: {e}")


async def send_bulk_payment_reminders(
    branch_id: int = None,
    urgency: str = "overdue",
    dry_run: bool = True
) -> Dict[str, Any]:
    """
    批次發送繳費提醒

    Args:
        branch_id: 場館ID (可選)
        urgency: 緊急度篩選
        dry_run: 是否為測試模式 (True=只計算不發送)

    Returns:
        發送結果統計
    """
    params = {}
    if branch_id:
        params["branch_id"] = f"eq.{branch_id}"
    if urgency != "all":
        params["urgency"] = f"eq.{urgency}"

    try:
        payments = await postgrest_get("v_payments_due", params)

        # 過濾有 LINE ID 的
        payments_with_line = [p for p in payments if p.get("line_user_id")]

        if dry_run:
            return {
                "dry_run": True,
                "total_payments": len(payments),
                "with_line_id": len(payments_with_line),
                "would_send": len(payments_with_line),
                "total_amount": sum(p.get("total_due", 0) for p in payments_with_line)
            }

        # 實際發送
        sent_count = 0
        failed_count = 0
        errors = []

        for payment in payments_with_line:
            reminder_type = "overdue" if payment.get("payment_status") == "overdue" else "upcoming"
            result = await send_payment_reminder(payment["id"], reminder_type)

            if result.get("success"):
                sent_count += 1
            else:
                failed_count += 1
                errors.append({
                    "payment_id": payment["id"],
                    "customer": payment["customer_name"],
                    "error": result.get("error")
                })

        return {
            "dry_run": False,
            "sent_count": sent_count,
            "failed_count": failed_count,
            "errors": errors if errors else None
        }

    except Exception as e:
        logger.error(f"send_bulk_payment_reminders error: {e}")
        raise Exception(f"批次發送失敗: {e}")
