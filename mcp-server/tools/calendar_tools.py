"""
Hour Jungle CRM - Calendar MCP Tools
簽約行程 Google Calendar 整合
"""

import logging
from datetime import datetime, timedelta
from typing import Optional

from tools.google_calendar import get_calendar_service

logger = logging.getLogger(__name__)

# Hour Jungle 簽約行事曆 ID（需要在環境變數設定或此處設定）
import os
SIGNING_CALENDAR_ID = os.getenv(
    "SIGNING_CALENDAR_ID",
    "primary"  # 預設使用主行事曆，正式環境應設定專屬行事曆 ID
)


# =============================================================================
# MCP Tool Definitions
# =============================================================================

calendar_create_signing_appointment_schema = {
    "name": "calendar_create_signing_appointment",
    "description": """建立簽約行程到 Google Calendar。
當客戶確認簽約時間時使用此工具。
會自動建立行事曆事件，包含客戶資訊、地點、需攜帶文件提醒。

使用時機：
- 客戶說「我下週一下午3點可以」
- 確認了簽約時間後

不適用：
- 客戶還在詢問時間
- 尚未確定日期時間""",
    "inputSchema": {
        "type": "object",
        "properties": {
            "customer_name": {
                "type": "string",
                "description": "客戶姓名"
            },
            "company_name": {
                "type": "string",
                "description": "公司名稱（選填）"
            },
            "appointment_datetime": {
                "type": "string",
                "description": "簽約日期時間，格式：YYYY-MM-DD HH:MM（如 2024-12-23 15:30）"
            },
            "duration_minutes": {
                "type": "integer",
                "description": "預計簽約時長（分鐘），預設 60",
                "default": 60
            },
            "plan_name": {
                "type": "string",
                "description": "簽約方案名稱（如：借址登記一年約）"
            },
            "customer_phone": {
                "type": "string",
                "description": "客戶電話（選填）"
            },
            "customer_email": {
                "type": "string",
                "description": "客戶 Email（選填，會收到行事曆邀請）"
            },
            "notes": {
                "type": "string",
                "description": "備註（選填）"
            },
            "branch": {
                "type": "string",
                "description": "場館名稱",
                "enum": ["大忠館", "台中館"],
                "default": "大忠館"
            }
        },
        "required": ["customer_name", "appointment_datetime"]
    }
}


async def calendar_create_signing_appointment(
    customer_name: str,
    appointment_datetime: str,
    company_name: str = None,
    duration_minutes: int = 60,
    plan_name: str = None,
    customer_phone: str = None,
    customer_email: str = None,
    notes: str = None,
    branch: str = "大忠館"
) -> dict:
    """
    建立簽約行程到 Google Calendar

    Args:
        customer_name: 客戶姓名
        appointment_datetime: 簽約日期時間 (YYYY-MM-DD HH:MM)
        company_name: 公司名稱
        duration_minutes: 預計時長（分鐘）
        plan_name: 簽約方案
        customer_phone: 客戶電話
        customer_email: 客戶 Email
        notes: 備註
        branch: 場館

    Returns:
        建立結果
    """
    try:
        # 解析日期時間
        try:
            start_dt = datetime.strptime(appointment_datetime, "%Y-%m-%d %H:%M")
        except ValueError:
            # 嘗試其他格式
            try:
                start_dt = datetime.strptime(appointment_datetime, "%Y/%m/%d %H:%M")
            except ValueError:
                return {
                    "success": False,
                    "message": f"日期時間格式錯誤，請使用 YYYY-MM-DD HH:MM 格式"
                }

        end_dt = start_dt + timedelta(minutes=duration_minutes)

        # 建立事件標題
        title_parts = ["簽約"]
        if company_name:
            title_parts.append(f"- {company_name}")
        title_parts.append(f"({customer_name})")
        title = " ".join(title_parts)

        # 地點
        locations = {
            "大忠館": "台中市西區大忠南街55號7F-5",
            "台中館": "台中市西區大忠南街55號7F-5"
        }
        location = locations.get(branch, locations["大忠館"])

        # 描述
        description_parts = [
            f"客戶：{customer_name}",
        ]
        if company_name:
            description_parts.append(f"公司：{company_name}")
        if plan_name:
            description_parts.append(f"方案：{plan_name}")
        if customer_phone:
            description_parts.append(f"電話：{customer_phone}")
        if customer_email:
            description_parts.append(f"Email：{customer_email}")

        description_parts.append("")
        description_parts.append("【請客戶攜帶】")
        description_parts.append("- 身分證正反面影本")
        description_parts.append("- 公司大小章（如已設立）")
        description_parts.append("- 公司登記相關文件")

        if notes:
            description_parts.append("")
            description_parts.append(f"備註：{notes}")

        description = "\n".join(description_parts)

        # 建立行事曆事件
        calendar_service = get_calendar_service()

        attendees = []
        if customer_email:
            attendees.append(customer_email)

        result = calendar_service.create_event(
            calendar_id=SIGNING_CALENDAR_ID,
            title=title,
            start_datetime=start_dt,
            end_datetime=end_dt,
            description=description,
            location=location,
            attendees=attendees if attendees else None
        )

        if result.get("success"):
            return {
                "success": True,
                "message": f"已建立簽約行程：{start_dt.strftime('%Y/%m/%d %H:%M')} @ {branch}",
                "event_id": result.get("event_id"),
                "event_link": result.get("html_link"),
                "appointment": {
                    "customer_name": customer_name,
                    "company_name": company_name,
                    "datetime": start_dt.isoformat(),
                    "location": location,
                    "plan_name": plan_name
                }
            }
        else:
            logger.error(f"Failed to create calendar event: {result.get('error')}")
            return {
                "success": False,
                "message": f"建立行事曆事件失敗：{result.get('error')}"
            }

    except Exception as e:
        logger.error(f"calendar_create_signing_appointment error: {e}")
        return {
            "success": False,
            "message": f"建立簽約行程失敗：{str(e)}"
        }


calendar_list_signing_appointments_schema = {
    "name": "calendar_list_signing_appointments",
    "description": "列出即將到來的簽約行程",
    "inputSchema": {
        "type": "object",
        "properties": {
            "days_ahead": {
                "type": "integer",
                "description": "查詢未來幾天的行程，預設 7",
                "default": 7
            }
        }
    }
}


async def calendar_list_signing_appointments(days_ahead: int = 7) -> dict:
    """列出即將到來的簽約行程"""
    try:
        from datetime import date

        calendar_service = get_calendar_service()
        today = date.today()
        end_date = today + timedelta(days=days_ahead)

        result = calendar_service.list_events(
            calendar_id=SIGNING_CALENDAR_ID,
            date_from=today,
            date_to=end_date,
            max_results=20
        )

        if result.get("success"):
            # 過濾出簽約相關事件
            signing_events = [
                e for e in result.get("events", [])
                if "簽約" in e.get("summary", "")
            ]

            return {
                "success": True,
                "appointments": signing_events,
                "count": len(signing_events),
                "period": f"{today.isoformat()} ~ {end_date.isoformat()}"
            }
        else:
            return result

    except Exception as e:
        logger.error(f"calendar_list_signing_appointments error: {e}")
        return {
            "success": False,
            "message": f"查詢簽約行程失敗：{str(e)}"
        }


# =============================================================================
# Tool Registration
# =============================================================================

CALENDAR_TOOLS = [
    {
        "schema": calendar_create_signing_appointment_schema,
        "handler": calendar_create_signing_appointment
    },
    {
        "schema": calendar_list_signing_appointments_schema,
        "handler": calendar_list_signing_appointments
    }
]


def get_calendar_tools():
    """取得所有 Calendar 工具定義"""
    return CALENDAR_TOOLS
