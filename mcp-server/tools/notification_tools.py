"""
Hour Jungle CRM - 通知記錄工具
管理自動通知和通知記錄
"""

import logging
from datetime import datetime
from typing import Dict, Any, Optional, List

logger = logging.getLogger(__name__)

# 導入資料庫連接（將在 main.py 中設置）
postgrest_request = None


def set_postgrest_request(func):
    """設置 postgrest_request 函數"""
    global postgrest_request
    postgrest_request = func


# ============================================================================
# 通知記錄
# ============================================================================

async def log_notification(
    notification_type: str,
    customer_id: int = None,
    contract_id: int = None,
    payment_id: int = None,
    recipient_name: str = None,
    recipient_line_id: str = None,
    message_content: str = None,
    status: str = "sent",
    error_message: str = None,
    triggered_by: str = "manual"
) -> Dict[str, Any]:
    """
    記錄通知到 notification_logs 表

    Args:
        notification_type: 通知類型 (payment_reminder/renewal_reminder)
        customer_id: 客戶 ID
        contract_id: 合約 ID
        payment_id: 繳費 ID
        recipient_name: 收件人姓名
        recipient_line_id: LINE User ID
        message_content: 訊息內容
        status: 狀態 (sent/failed/pending)
        error_message: 錯誤訊息
        triggered_by: 觸發來源 (manual/scheduler/system)

    Returns:
        記錄結果
    """
    try:
        data = {
            "notification_type": notification_type,
            "customer_id": customer_id,
            "contract_id": contract_id,
            "payment_id": payment_id,
            "recipient_name": recipient_name,
            "recipient_line_id": recipient_line_id,
            "message_content": message_content,
            "status": status,
            "error_message": error_message,
            "triggered_by": triggered_by
        }

        # 移除 None 值
        data = {k: v for k, v in data.items() if v is not None}

        result = await postgrest_request(
            "POST",
            "notification_logs",
            data=data,
            headers={"Prefer": "return=representation"}
        )

        return {
            "success": True,
            "log_id": result[0]["id"] if result else None
        }
    except Exception as e:
        logger.error(f"記錄通知失敗: {e}")
        return {"success": False, "error": str(e)}


async def get_notification_logs(
    notification_type: str = None,
    customer_id: int = None,
    limit: int = 50,
    offset: int = 0
) -> Dict[str, Any]:
    """
    取得通知記錄

    Args:
        notification_type: 通知類型篩選
        customer_id: 客戶 ID 篩選
        limit: 回傳筆數
        offset: 偏移量

    Returns:
        通知記錄列表
    """
    try:
        params = {
            "order": "created_at.desc",
            "limit": limit,
            "offset": offset
        }

        if notification_type:
            params["notification_type"] = f"eq.{notification_type}"
        if customer_id:
            params["customer_id"] = f"eq.{customer_id}"

        result = await postgrest_request("GET", "v_notification_logs", params=params)

        return {
            "success": True,
            "data": result or [],
            "count": len(result) if result else 0
        }
    except Exception as e:
        logger.error(f"取得通知記錄失敗: {e}")
        return {"success": False, "error": str(e), "data": []}


async def get_today_notifications() -> Dict[str, Any]:
    """
    取得今日通知統計

    Returns:
        今日通知統計
    """
    try:
        result = await postgrest_request("GET", "v_today_notifications")
        return {
            "success": True,
            "data": result or []
        }
    except Exception as e:
        logger.error(f"取得今日通知統計失敗: {e}")
        return {"success": False, "error": str(e), "data": []}


# ============================================================================
# 系統設定
# ============================================================================

async def get_notification_settings() -> Dict[str, Any]:
    """
    取得通知相關設定

    Returns:
        通知設定
    """
    try:
        params = {
            "setting_key": "in.(auto_payment_reminder,auto_renewal_reminder,reminder_time,overdue_reminder_days)"
        }
        result = await postgrest_request("GET", "system_settings", params=params)

        settings = {}
        for item in (result or []):
            key = item.get("setting_key")
            value = item.get("setting_value")
            # 轉換布林值
            if value in ("true", "false"):
                value = value == "true"
            settings[key] = value

        return {
            "success": True,
            "settings": settings
        }
    except Exception as e:
        logger.error(f"取得通知設定失敗: {e}")
        return {"success": False, "error": str(e), "settings": {}}


async def update_notification_setting(
    key: str,
    value: str
) -> Dict[str, Any]:
    """
    更新通知設定

    Args:
        key: 設定鍵值
        value: 設定值

    Returns:
        更新結果
    """
    valid_keys = [
        "auto_payment_reminder",
        "auto_renewal_reminder",
        "reminder_time",
        "overdue_reminder_days"
    ]

    if key not in valid_keys:
        return {
            "success": False,
            "error": f"無效的設定鍵值: {key}"
        }

    try:
        await postgrest_request(
            "PATCH",
            f"system_settings?setting_key=eq.{key}",
            data={"setting_value": value, "updated_at": datetime.now().isoformat()},
            headers={"Prefer": "return=representation"}
        )

        return {
            "success": True,
            "key": key,
            "value": value
        }
    except Exception as e:
        logger.error(f"更新通知設定失敗: {e}")
        return {"success": False, "error": str(e)}


# ============================================================================
# 排程觸發
# ============================================================================

async def trigger_daily_reminders(
    dry_run: bool = True
) -> Dict[str, Any]:
    """
    觸發每日自動提醒（由 Cloud Scheduler 呼叫）

    Args:
        dry_run: 是否為測試模式

    Returns:
        執行結果
    """
    from tools.line_tools import send_payment_reminder, send_renewal_reminder

    results = {
        "payment_reminders": {"sent": 0, "failed": 0, "skipped": 0},
        "renewal_reminders": {"sent": 0, "failed": 0, "skipped": 0},
        "dry_run": dry_run
    }

    try:
        # 取得設定
        settings_result = await get_notification_settings()
        settings = settings_result.get("settings", {})

        auto_payment = settings.get("auto_payment_reminder", False)
        auto_renewal = settings.get("auto_renewal_reminder", False)

        if not auto_payment and not auto_renewal:
            return {
                "success": True,
                "message": "自動通知已關閉",
                "results": results
            }

        # 處理繳費提醒
        if auto_payment:
            overdue_days_str = settings.get("overdue_reminder_days", "3,7,14,30")
            overdue_days = [int(d.strip()) for d in overdue_days_str.split(",")]

            # 取得逾期記錄
            overdue_result = await postgrest_request(
                "GET",
                "v_overdue_details",
                params={"order": "days_overdue.desc", "limit": 100}
            )

            for item in (overdue_result or []):
                days = item.get("days_overdue", 0)
                payment_id = item.get("payment_id")
                line_user_id = item.get("line_user_id")

                # 只有符合天數且有 LINE ID 才發送
                if days in overdue_days and line_user_id:
                    if dry_run:
                        results["payment_reminders"]["skipped"] += 1
                    else:
                        try:
                            await send_payment_reminder(payment_id, "overdue")
                            # 記錄通知
                            await log_notification(
                                notification_type="payment_reminder",
                                customer_id=item.get("customer_id"),
                                payment_id=payment_id,
                                recipient_name=item.get("customer_name"),
                                recipient_line_id=line_user_id,
                                message_content=f"逾期 {days} 天催繳提醒",
                                triggered_by="scheduler"
                            )
                            results["payment_reminders"]["sent"] += 1
                        except Exception as e:
                            logger.error(f"發送繳費提醒失敗: {e}")
                            results["payment_reminders"]["failed"] += 1

        # 處理續約提醒
        if auto_renewal:
            renewal_result = await postgrest_request(
                "GET",
                "v_renewal_reminders",
                params={"days_remaining": "lte.30", "limit": 100}
            )

            for item in (renewal_result or []):
                contract_id = item.get("contract_id")
                line_user_id = item.get("line_user_id")
                days_remaining = item.get("days_remaining", 0)

                # 只有有 LINE ID 且尚未通知的才發送
                if line_user_id and not item.get("renewal_notified_at"):
                    if dry_run:
                        results["renewal_reminders"]["skipped"] += 1
                    else:
                        try:
                            await send_renewal_reminder(contract_id)
                            await log_notification(
                                notification_type="renewal_reminder",
                                customer_id=item.get("customer_id"),
                                contract_id=contract_id,
                                recipient_name=item.get("customer_name"),
                                recipient_line_id=line_user_id,
                                message_content=f"合約 {days_remaining} 天後到期",
                                triggered_by="scheduler"
                            )
                            results["renewal_reminders"]["sent"] += 1
                        except Exception as e:
                            logger.error(f"發送續約提醒失敗: {e}")
                            results["renewal_reminders"]["failed"] += 1

        return {
            "success": True,
            "message": "排程執行完成",
            "results": results
        }

    except Exception as e:
        logger.error(f"每日提醒排程失敗: {e}")
        return {
            "success": False,
            "error": str(e),
            "results": results
        }


async def get_monthly_reminders_summary() -> Dict[str, Any]:
    """
    取得當月應催繳/續約統計

    Returns:
        統計資料
    """
    try:
        result = await postgrest_request("GET", "v_monthly_reminders_summary")
        return {
            "success": True,
            "data": result or []
        }
    except Exception as e:
        logger.error(f"取得當月統計失敗: {e}")
        return {"success": False, "error": str(e), "data": []}
