"""
Hour Jungle CRM - MCP Server
FastAPI + MCP Protocol for AI Agent Integration
"""

import os
import json
import logging
from contextlib import asynccontextmanager
from typing import Any, List, Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse
from openai import OpenAI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from pydantic_settings import BaseSettings

# 設定日誌
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============================================================================
# 設定
# ============================================================================

class Settings(BaseSettings):
    """應用設定"""
    # PostgREST
    postgrest_url: str = "http://postgrest:3000"

    # PostgreSQL (直連用)
    postgres_host: str = "postgres"
    postgres_port: int = 5432
    postgres_db: str = "hourjungle"
    postgres_user: str = "hjadmin"
    postgres_password: str = ""

    # JWT
    jwt_secret: str = ""

    # LINE Bot
    line_channel_access_token: str = ""
    line_channel_secret: str = ""

    # AI - OpenRouter API
    openrouter_api_key: str = ""

    class Config:
        env_file = ".env"


settings = Settings()


# ============================================================================
# 資料庫連接
# ============================================================================

import httpx
import psycopg2
from psycopg2.extras import RealDictCursor


def get_db_connection():
    """取得 PostgreSQL 直連"""
    return psycopg2.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        dbname=settings.postgres_db,
        user=settings.postgres_user,
        password=settings.postgres_password,
        cursor_factory=RealDictCursor
    )


async def postgrest_request(
    method: str,
    endpoint: str,
    params: dict = None,
    data: dict = None,
    headers: dict = None
) -> Any:
    """PostgREST API 請求"""
    url = f"{settings.postgrest_url}/{endpoint}"

    default_headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    if headers:
        default_headers.update(headers)

    async with httpx.AsyncClient() as client:
        response = await client.request(
            method=method,
            url=url,
            params=params,
            json=data,
            headers=default_headers,
            timeout=30.0
        )

        if response.status_code >= 400:
            logger.error(f"PostgREST error: {response.status_code} - {response.text}")
            raise HTTPException(
                status_code=response.status_code,
                detail=response.text
            )

        if response.status_code == 204:
            return None

        return response.json()


# ============================================================================
# MCP Tools - CRM 查詢工具
# ============================================================================

from tools.crm_tools import (
    search_customers,
    get_customer_detail,
    list_payments_due,
    list_renewals_due,
    create_customer,
    update_customer,
    record_payment,
    create_contract,
    contract_renew,
    contract_update_tax_id,
    commission_pay,
    payment_undo
)

from tools.line_tools import (
    send_line_message,
    send_payment_reminder,
    send_renewal_reminder
)

from tools.report_tools import (
    get_revenue_summary,
    get_overdue_list,
    get_commission_due
)

from tools.renewal_tools import (
    update_renewal_status,
    update_invoice_status,
    get_renewal_status_summary,
    batch_update_renewal_status,
    renewal_set_flag,
    set_postgrest_request as set_renewal_postgrest
)

from tools.quote_tools import (
    list_quotes,
    get_quote,
    create_quote,
    update_quote,
    update_quote_status,
    delete_quote,
    convert_quote_to_contract,
    quote_generate_pdf,
    send_quote_to_line,
    list_service_plans,
    get_service_plan,
    create_quote_from_service_plans
)

from tools.invoice_tools import (
    invoice_create,
    invoice_void,
    invoice_query,
    invoice_allowance
)

from tools.contract_tools import (
    contract_generate_pdf,
    contract_preview
)

from tools.settings_tools import (
    settings_get,
    settings_update,
    settings_get_all
)

from tools.legal_letter_tools import (
    legal_record_reminder,
    legal_list_candidates,
    legal_generate_content,
    legal_create_letter,
    legal_generate_pdf,
    legal_notify_staff,
    legal_list_pending,
    legal_update_status
)

from tools.booking_tools import (
    booking_list_rooms,
    booking_check_availability,
    booking_create,
    booking_cancel,
    booking_update,
    booking_list,
    booking_get,
    booking_send_reminder,
    set_postgrest_request as set_booking_postgrest
)

from tools.floor_plan_tools import (
    floor_plan_get_positions,
    floor_plan_update_position,
    floor_plan_generate,
    floor_plan_preview_html
)

from tools.line_webhook import (
    handle_line_event,
    verify_signature
)

from tools.notification_tools import (
    log_notification,
    get_notification_logs,
    get_today_notifications,
    get_notification_settings,
    update_notification_setting,
    trigger_daily_reminders,
    get_monthly_reminders_summary,
    set_postgrest_request as set_notification_postgrest
)

from tools.brain_tools import (
    brain_save_knowledge,
    brain_search_knowledge,
    brain_list_categories,
    brain_save_customer_traits
)

from tools.calendar_tools import (
    calendar_create,
    calendar_share,
    calendar_create_signing_appointment,
    calendar_list_signing_appointments
)

from tools.service_plan_tools import (
    list_service_plans,
    get_service_plan,
    create_service_plan,
    update_service_plan,
    delete_service_plan,
    reorder_service_plans,
    sync_prices_to_brain
)

from tools.feedback_tools import (
    feedback_submit,
    feedback_list,
    set_postgrest_request as set_feedback_postgrest
)


# ============================================================================
# MCP Tool 定義
# ============================================================================

MCP_TOOLS = {
    # 查詢工具
    "crm_search_customers": {
        "description": "搜尋客戶資料",
        "parameters": {
            "query": {"type": "string", "description": "搜尋關鍵字 (姓名/電話/公司名)"},
            "branch_id": {"type": "integer", "description": "場館ID (1=大忠, 2=環瑞)", "optional": True},
            "status": {"type": "string", "description": "客戶狀態 (active/prospect/churned)", "optional": True}
        },
        "handler": search_customers
    },
    "crm_get_customer_detail": {
        "description": "取得客戶詳細資料",
        "parameters": {
            "customer_id": {"type": "integer", "description": "客戶ID", "optional": True},
            "line_user_id": {"type": "string", "description": "LINE User ID", "optional": True}
        },
        "handler": get_customer_detail
    },
    "crm_list_payments_due": {
        "description": "列出應收款項",
        "parameters": {
            "branch_id": {"type": "integer", "description": "場館ID", "optional": True},
            "urgency": {"type": "string", "description": "緊急度 (critical/high/medium/upcoming/all)", "optional": True},
            "limit": {"type": "integer", "description": "回傳筆數", "default": 20}
        },
        "handler": list_payments_due
    },
    "crm_list_renewals_due": {
        "description": "列出即將到期的合約",
        "parameters": {
            "branch_id": {"type": "integer", "description": "場館ID", "optional": True},
            "days_ahead": {"type": "integer", "description": "未來幾天內到期", "default": 30}
        },
        "handler": list_renewals_due
    },

    # 操作工具
    "crm_create_customer": {
        "description": "建立新客戶",
        "parameters": {
            "name": {"type": "string", "description": "客戶姓名", "required": True},
            "branch_id": {"type": "integer", "description": "場館ID", "required": True},
            "phone": {"type": "string", "description": "電話", "optional": True},
            "email": {"type": "string", "description": "Email", "optional": True},
            "company_name": {"type": "string", "description": "公司名稱", "optional": True},
            "source_channel": {"type": "string", "description": "來源管道", "optional": True}
        },
        "handler": create_customer
    },
    "crm_update_customer": {
        "description": "更新客戶資料",
        "parameters": {
            "customer_id": {"type": "integer", "description": "客戶ID", "required": True},
            "updates": {"type": "object", "description": "要更新的欄位", "required": True}
        },
        "handler": update_customer
    },
    "crm_record_payment": {
        "description": "記錄繳費",
        "parameters": {
            "payment_id": {"type": "integer", "description": "付款ID", "required": True},
            "payment_method": {"type": "string", "description": "付款方式 (cash/transfer/credit_card/line_pay)", "required": True},
            "notes": {"type": "string", "description": "備註", "optional": True}
        },
        "handler": record_payment
    },
    "crm_payment_undo": {
        "description": "撤銷繳費記錄（將已付款狀態改回待付款）",
        "parameters": {
            "payment_id": {"type": "integer", "description": "付款ID", "required": True},
            "reason": {"type": "string", "description": "撤銷原因（必填）", "required": True}
        },
        "handler": payment_undo
    },
    "crm_create_contract": {
        "description": "建立新合約（以合約為主體架構）。觸發器會根據統編或電話自動查找/建立客戶",
        "parameters": {
            "branch_id": {"type": "integer", "description": "場館ID", "required": True},
            "start_date": {"type": "string", "description": "開始日期 (YYYY-MM-DD)", "required": True},
            "end_date": {"type": "string", "description": "結束日期 (YYYY-MM-DD)", "required": True},
            "monthly_rent": {"type": "number", "description": "月租金", "required": True},
            "company_name": {"type": "string", "description": "公司名稱", "optional": True},
            "representative_name": {"type": "string", "description": "負責人姓名", "optional": True},
            "representative_address": {"type": "string", "description": "負責人地址", "optional": True},
            "id_number": {"type": "string", "description": "身分證/居留證號碼", "optional": True},
            "company_tax_id": {"type": "string", "description": "公司統編（可為空）", "optional": True},
            "phone": {"type": "string", "description": "聯絡電話", "optional": True},
            "email": {"type": "string", "description": "電子郵件", "optional": True},
            "customer_id": {"type": "integer", "description": "客戶ID（可選，不填則自動處理）", "optional": True},
            "contract_type": {"type": "string", "description": "合約類型", "default": "virtual_office"},
            "deposit_amount": {"type": "number", "description": "押金", "default": 0},
            "original_price": {"type": "number", "description": "定價（原價）", "optional": True}
        },
        "handler": create_contract
    },
    "contract_renew": {
        "description": "續約：將舊合約標記為「已續約」，建立新合約",
        "parameters": {
            "contract_id": {"type": "integer", "description": "舊合約ID", "required": True},
            "new_start_date": {"type": "string", "description": "新合約開始日期 (YYYY-MM-DD)", "required": True},
            "new_end_date": {"type": "string", "description": "新合約結束日期 (YYYY-MM-DD)", "required": True},
            "new_monthly_rent": {"type": "number", "description": "新月租金（不填則沿用）", "optional": True},
            "new_deposit_amount": {"type": "number", "description": "新押金（不填則沿用）", "optional": True},
            "notes": {"type": "string", "description": "備註", "optional": True}
        },
        "handler": contract_renew
    },
    "contract_update_tax_id": {
        "description": "補上公司統編（新設立公司後續補上）",
        "parameters": {
            "contract_id": {"type": "integer", "description": "合約ID", "required": True},
            "company_tax_id": {"type": "string", "description": "公司統編（8碼）", "required": True},
            "update_customer": {"type": "boolean", "description": "是否同時更新客戶表", "default": True}
        },
        "handler": contract_update_tax_id
    },

    # LINE 通知工具
    "line_send_message": {
        "description": "發送 LINE 訊息給客戶",
        "parameters": {
            "customer_id": {"type": "integer", "description": "客戶ID", "required": True},
            "message": {"type": "string", "description": "訊息內容", "required": True}
        },
        "handler": send_line_message
    },
    "line_send_payment_reminder": {
        "description": "發送繳費提醒",
        "parameters": {
            "payment_id": {"type": "integer", "description": "付款ID", "required": True},
            "reminder_type": {"type": "string", "description": "提醒類型 (upcoming/due/overdue)", "default": "upcoming"}
        },
        "handler": send_payment_reminder
    },
    "line_send_renewal_reminder": {
        "description": "發送續約提醒",
        "parameters": {
            "contract_id": {"type": "integer", "description": "合約ID", "required": True}
        },
        "handler": send_renewal_reminder
    },

    # 報表工具
    "report_revenue_summary": {
        "description": "營收摘要報表",
        "parameters": {
            "branch_id": {"type": "integer", "description": "場館ID", "optional": True},
            "period": {"type": "string", "description": "期間 (this_month/last_month/this_year)", "default": "this_month"}
        },
        "handler": get_revenue_summary
    },
    "report_overdue_list": {
        "description": "逾期款項報表",
        "parameters": {
            "branch_id": {"type": "integer", "description": "場館ID", "optional": True},
            "min_days": {"type": "integer", "description": "最少逾期天數", "default": 0}
        },
        "handler": get_overdue_list
    },
    "report_commission_due": {
        "description": "應付佣金報表",
        "parameters": {
            "status": {"type": "string", "description": "狀態 (pending/eligible/all)", "default": "eligible"}
        },
        "handler": get_commission_due
    },

    # 佣金操作工具
    "commission_pay": {
        "description": "執行佣金付款（將狀態從 eligible 更新為 paid）",
        "parameters": {
            "commission_id": {"type": "integer", "description": "佣金ID", "required": True},
            "payment_method": {"type": "string", "description": "付款方式 (transfer/check/cash)", "required": True},
            "reference": {"type": "string", "description": "參考資訊（轉帳後五碼或支票號碼）", "optional": True},
            "notes": {"type": "string", "description": "備註", "optional": True}
        },
        "handler": commission_pay
    },

    # 續約流程管理工具
    "renewal_update_status": {
        "description": "更新合約的續約狀態",
        "parameters": {
            "contract_id": {"type": "integer", "description": "合約ID", "required": True},
            "renewal_status": {"type": "string", "description": "續約狀態 (notified/confirmed/paid/invoiced/signed/completed)", "required": True},
            "notes": {"type": "string", "description": "備註", "optional": True}
        },
        "handler": update_renewal_status
    },
    "renewal_update_invoice_status": {
        "description": "更新合約的發票狀態",
        "parameters": {
            "contract_id": {"type": "integer", "description": "合約ID", "required": True},
            "invoice_status": {"type": "string", "description": "發票狀態 (pending_tax_id/issued_personal/issued_business)", "required": True},
            "notes": {"type": "string", "description": "備註", "optional": True}
        },
        "handler": update_invoice_status
    },
    "renewal_get_summary": {
        "description": "取得續約狀態統計",
        "parameters": {
            "branch_id": {"type": "integer", "description": "場館ID", "optional": True}
        },
        "handler": get_renewal_status_summary
    },
    "renewal_batch_update": {
        "description": "批次更新多個合約的續約狀態",
        "parameters": {
            "contract_ids": {"type": "array", "description": "合約ID列表", "required": True},
            "renewal_status": {"type": "string", "description": "續約狀態", "required": True},
            "notes": {"type": "string", "description": "備註", "optional": True}
        },
        "handler": batch_update_renewal_status
    },
    "renewal_set_flag": {
        "description": "設定或清除續約 Checklist 的 flag（使用時間戳作為事實來源）。設定 paid/signed 會自動補上 confirmed（Cascade Logic）",
        "parameters": {
            "contract_id": {"type": "integer", "description": "合約ID", "required": True},
            "flag": {"type": "string", "description": "flag 名稱 (notified/confirmed/paid/signed)", "required": True},
            "value": {"type": "boolean", "description": "True = 設定, False = 清除", "required": True},
            "notes": {"type": "string", "description": "備註", "optional": True}
        },
        "handler": renewal_set_flag
    },

    # 報價單工具
    "quote_list": {
        "description": "列出報價單",
        "parameters": {
            "branch_id": {"type": "integer", "description": "場館ID", "optional": True},
            "status": {"type": "string", "description": "狀態 (draft/sent/viewed/accepted/rejected/expired/converted)", "optional": True},
            "customer_id": {"type": "integer", "description": "客戶ID", "optional": True},
            "limit": {"type": "integer", "description": "回傳筆數", "default": 50}
        },
        "handler": list_quotes
    },
    "quote_get": {
        "description": "取得報價單詳情",
        "parameters": {
            "quote_id": {"type": "integer", "description": "報價單ID", "required": True}
        },
        "handler": get_quote
    },
    "quote_create": {
        "description": "建立報價單",
        "parameters": {
            "branch_id": {"type": "integer", "description": "場館ID", "required": True},
            "customer_id": {"type": "integer", "description": "客戶ID", "optional": True},
            "customer_name": {"type": "string", "description": "客戶姓名（未建立客戶時）", "optional": True},
            "customer_phone": {"type": "string", "description": "客戶電話", "optional": True},
            "customer_email": {"type": "string", "description": "客戶Email", "optional": True},
            "company_name": {"type": "string", "description": "公司名稱", "optional": True},
            "contract_type": {"type": "string", "description": "合約類型", "default": "virtual_office"},
            "plan_name": {"type": "string", "description": "方案名稱", "optional": True},
            "contract_months": {"type": "integer", "description": "合約月數", "default": 12},
            "items": {"type": "array", "description": "費用項目", "optional": True},
            "discount_amount": {"type": "number", "description": "折扣金額", "default": 0},
            "discount_note": {"type": "string", "description": "折扣說明", "optional": True},
            "deposit_amount": {"type": "number", "description": "押金", "default": 0},
            "valid_days": {"type": "integer", "description": "有效天數", "default": 30},
            "internal_notes": {"type": "string", "description": "內部備註", "optional": True},
            "customer_notes": {"type": "string", "description": "給客戶的備註", "optional": True},
            "line_user_id": {"type": "string", "description": "LINE User ID（來自 Brain 詢問）", "optional": True}
        },
        "handler": create_quote
    },
    "quote_update": {
        "description": "更新報價單",
        "parameters": {
            "quote_id": {"type": "integer", "description": "報價單ID", "required": True},
            "updates": {"type": "object", "description": "要更新的欄位", "required": True}
        },
        "handler": update_quote
    },
    "quote_update_status": {
        "description": "更新報價單狀態",
        "parameters": {
            "quote_id": {"type": "integer", "description": "報價單ID", "required": True},
            "status": {"type": "string", "description": "新狀態 (draft/sent/viewed/accepted/rejected/expired/converted)", "required": True},
            "notes": {"type": "string", "description": "備註", "optional": True}
        },
        "handler": update_quote_status
    },
    "quote_delete": {
        "description": "刪除報價單（僅限草稿）",
        "parameters": {
            "quote_id": {"type": "integer", "description": "報價單ID", "required": True}
        },
        "handler": delete_quote
    },
    "quote_convert_to_contract": {
        "description": "將已接受的報價單轉換為合約草稿。前端需重新填寫完整客戶資訊",
        "parameters": {
            "quote_id": {"type": "integer", "description": "報價單ID", "required": True},
            "start_date": {"type": "string", "description": "合約開始日期 (YYYY-MM-DD)", "optional": True},
            "end_date": {"type": "string", "description": "合約結束日期 (YYYY-MM-DD)", "optional": True},
            "payment_cycle": {"type": "string", "description": "繳費週期 (monthly/quarterly/semi_annual/annual)", "optional": True},
            "payment_day": {"type": "integer", "description": "每期繳費日（1-28）", "default": 5},
            "company_name": {"type": "string", "description": "公司名稱", "optional": True},
            "representative_name": {"type": "string", "description": "負責人姓名", "optional": True},
            "representative_address": {"type": "string", "description": "負責人地址", "optional": True},
            "id_number": {"type": "string", "description": "身分證/居留證號碼", "optional": True},
            "company_tax_id": {"type": "string", "description": "公司統編（可為空）", "optional": True},
            "phone": {"type": "string", "description": "聯絡電話", "optional": True},
            "email": {"type": "string", "description": "電子郵件", "optional": True},
            "original_price": {"type": "number", "description": "定價（原價）", "optional": True},
            "monthly_rent": {"type": "number", "description": "折扣後月租金", "optional": True},
            "deposit_amount": {"type": "number", "description": "押金", "optional": True},
            "notes": {"type": "string", "description": "備註", "optional": True}
        },
        "handler": convert_quote_to_contract
    },
    "quote_generate_pdf": {
        "description": "生成報價單 PDF",
        "parameters": {
            "quote_id": {"type": "integer", "description": "報價單ID", "required": True}
        },
        "handler": quote_generate_pdf
    },
    "quote_send_to_line": {
        "description": "發送報價單給 LINE 用戶（透過 LINE Messaging API 發送報價單摘要和 PDF 下載連結）",
        "parameters": {
            "quote_id": {"type": "integer", "description": "報價單ID", "required": True},
            "line_user_id": {"type": "string", "description": "LINE User ID", "required": True}
        },
        "handler": send_quote_to_line
    },

    # 服務價格表工具
    "service_plan_list": {
        "description": "取得服務價格列表（共享空間、獨立辦公室、會議室、借址登記、代辦服務等）",
        "parameters": {
            "category": {"type": "string", "description": "分類篩選 (空間服務/登記服務/代辦服務)", "optional": True},
            "is_active": {"type": "boolean", "description": "是否只顯示啟用的服務", "default": True}
        },
        "handler": list_service_plans
    },
    "service_plan_get": {
        "description": "根據代碼取得服務方案詳情",
        "parameters": {
            "code": {"type": "string", "description": "服務代碼 (如 virtual_office_2year, company_setup)", "required": True}
        },
        "handler": get_service_plan
    },
    "quote_create_from_service_plans": {
        "description": "根據服務代碼建立報價單，自動從價格表取得價格資訊",
        "parameters": {
            "branch_id": {"type": "integer", "description": "場館ID", "required": True},
            "service_codes": {"type": "array", "description": "服務代碼列表 (如 ['virtual_office_2year', 'company_setup'])", "required": True},
            "customer_name": {"type": "string", "description": "客戶姓名", "optional": True},
            "customer_phone": {"type": "string", "description": "客戶電話", "optional": True},
            "customer_email": {"type": "string", "description": "客戶Email", "optional": True},
            "company_name": {"type": "string", "description": "公司名稱", "optional": True},
            "contract_months": {"type": "integer", "description": "合約月數（覆蓋預設值）", "optional": True},
            "discount_amount": {"type": "number", "description": "折扣金額", "default": 0},
            "discount_note": {"type": "string", "description": "折扣說明", "optional": True},
            "valid_days": {"type": "integer", "description": "有效天數", "default": 30},
            "internal_notes": {"type": "string", "description": "內部備註", "optional": True},
            "customer_notes": {"type": "string", "description": "給客戶的備註", "optional": True},
            "line_user_id": {"type": "string", "description": "LINE User ID（來自 Brain 詢問）", "optional": True}
        },
        "handler": create_quote_from_service_plans
    },

    # 發票工具
    "invoice_create": {
        "description": "開立電子發票（光貿 API）",
        "parameters": {
            "payment_id": {"type": "integer", "description": "繳費記錄ID", "required": True},
            "invoice_type": {"type": "string", "description": "發票類型 (personal=個人, business=公司)", "optional": True},
            "buyer_name": {"type": "string", "description": "買受人名稱（公司發票必填）", "optional": True},
            "buyer_tax_id": {"type": "string", "description": "統一編號（公司發票必填）", "optional": True},
            "carrier_type": {"type": "string", "description": "載具類型 (mobile=手機條碼, natural_person=自然人憑證, donate=捐贈)", "optional": True},
            "carrier_number": {"type": "string", "description": "載具號碼", "optional": True},
            "donate_code": {"type": "string", "description": "愛心碼", "optional": True},
            "print_flag": {"type": "boolean", "description": "是否列印", "optional": True}
        },
        "handler": invoice_create
    },
    "invoice_void": {
        "description": "作廢電子發票",
        "parameters": {
            "payment_id": {"type": "integer", "description": "繳費記錄ID", "required": True},
            "reason": {"type": "string", "description": "作廢原因", "required": True}
        },
        "handler": invoice_void
    },
    "invoice_query": {
        "description": "查詢電子發票",
        "parameters": {
            "invoice_number": {"type": "string", "description": "發票號碼", "optional": True},
            "payment_id": {"type": "integer", "description": "繳費記錄ID", "optional": True},
            "branch_id": {"type": "integer", "description": "分館ID", "optional": True},
            "start_date": {"type": "string", "description": "開始日期 (YYYY-MM-DD)", "optional": True},
            "end_date": {"type": "string", "description": "結束日期 (YYYY-MM-DD)", "optional": True}
        },
        "handler": invoice_query
    },
    "invoice_allowance": {
        "description": "開立發票折讓單",
        "parameters": {
            "payment_id": {"type": "integer", "description": "繳費記錄ID", "required": True},
            "allowance_amount": {"type": "number", "description": "折讓金額", "required": True},
            "reason": {"type": "string", "description": "折讓原因", "required": True}
        },
        "handler": invoice_allowance
    },

    # 合約生成工具
    "contract_generate_pdf": {
        "description": "生成合約 PDF",
        "parameters": {
            "contract_id": {"type": "integer", "description": "合約ID", "required": True},
            "template": {"type": "string", "description": "模板名稱 (standard, virtual_office, shared_office)", "optional": True}
        },
        "handler": contract_generate_pdf
    },
    "contract_preview": {
        "description": "預覽合約內容（不生成 PDF）",
        "parameters": {
            "contract_id": {"type": "integer", "description": "合約ID", "required": True}
        },
        "handler": contract_preview
    },

    # 系統設定工具
    "settings_get": {
        "description": "取得系統設定",
        "parameters": {
            "key": {"type": "string", "description": "設定鍵值（可選）", "optional": True},
            "category": {"type": "string", "description": "分類篩選（可選）", "optional": True}
        },
        "handler": settings_get
    },
    "settings_update": {
        "description": "更新系統設定",
        "parameters": {
            "key": {"type": "string", "description": "設定鍵值", "required": True},
            "value": {"type": "object", "description": "新的設定值", "required": True}
        },
        "handler": settings_update
    },
    "settings_get_all": {
        "description": "取得所有系統設定",
        "parameters": {},
        "handler": settings_get_all
    },

    # 存證信函工具
    "legal_record_reminder": {
        "description": "記錄催繳，更新付款記錄的催繳次數",
        "parameters": {
            "payment_id": {"type": "integer", "description": "付款ID", "required": True},
            "notes": {"type": "string", "description": "催繳備註", "optional": True}
        },
        "handler": legal_record_reminder
    },
    "legal_list_candidates": {
        "description": "列出存證信函候選客戶（逾期>14天且催繳>=5次）",
        "parameters": {
            "branch_id": {"type": "integer", "description": "場館ID", "optional": True},
            "limit": {"type": "integer", "description": "回傳筆數", "default": 50}
        },
        "handler": legal_list_candidates
    },
    "legal_generate_content": {
        "description": "使用 AI 生成存證信函內容（支援從逾期付款或合約建立）",
        "parameters": {
            "payment_id": {"type": "integer", "description": "付款ID（模式1：逾期付款）", "optional": True},
            "contract_id": {"type": "integer", "description": "合約ID（模式2：手動建立）", "optional": True},
            "customer_name": {"type": "string", "description": "客戶姓名", "optional": True},
            "company_name": {"type": "string", "description": "公司名稱", "optional": True},
            "address": {"type": "string", "description": "地址", "optional": True},
            "overdue_amount": {"type": "number", "description": "逾期金額", "default": 0},
            "overdue_days": {"type": "integer", "description": "逾期天數", "default": 0},
            "contract_number": {"type": "string", "description": "合約編號", "optional": True},
            "reminder_count": {"type": "integer", "description": "催繳次數", "default": 0},
            "branch_name": {"type": "string", "description": "場館名稱", "optional": True},
            "service_items": {"type": "string", "description": "服務項目（手動建立時使用）", "optional": True},
            "monthly_rent": {"type": "number", "description": "月租金（手動建立時使用）", "default": 0}
        },
        "handler": legal_generate_content
    },
    "legal_create_letter": {
        "description": "建立存證信函記錄（支援從逾期付款或合約建立）",
        "parameters": {
            "content": {"type": "string", "description": "存證信函內容", "required": True},
            "payment_id": {"type": "integer", "description": "付款ID（模式1）", "optional": True},
            "contract_id": {"type": "integer", "description": "合約ID（模式2）", "optional": True},
            "recipient_name": {"type": "string", "description": "收件人姓名", "optional": True},
            "recipient_address": {"type": "string", "description": "收件人地址", "optional": True}
        },
        "handler": legal_create_letter
    },
    "legal_generate_pdf": {
        "description": "生成存證信函 PDF",
        "parameters": {
            "letter_id": {"type": "integer", "description": "存證信函ID", "required": True}
        },
        "handler": legal_generate_pdf
    },
    "legal_notify_staff": {
        "description": "發送 LINE 通知給業務（存證信函待處理）",
        "parameters": {
            "letter_id": {"type": "integer", "description": "存證信函ID", "required": True},
            "staff_line_id": {"type": "string", "description": "業務的 LINE User ID", "optional": True},
            "message": {"type": "string", "description": "自訂訊息", "optional": True}
        },
        "handler": legal_notify_staff
    },
    "legal_list_pending": {
        "description": "列出待處理存證信函",
        "parameters": {
            "branch_id": {"type": "integer", "description": "場館ID", "optional": True},
            "status": {"type": "string", "description": "狀態篩選 (draft/approved/sent)", "optional": True},
            "limit": {"type": "integer", "description": "回傳筆數", "default": 50}
        },
        "handler": legal_list_pending
    },
    "legal_update_status": {
        "description": "更新存證信函狀態",
        "parameters": {
            "letter_id": {"type": "integer", "description": "存證信函ID", "required": True},
            "status": {"type": "string", "description": "新狀態 (draft/approved/sent/cancelled)", "required": True},
            "approved_by": {"type": "string", "description": "審核人", "optional": True},
            "tracking_number": {"type": "string", "description": "郵局掛號號碼", "optional": True},
            "notes": {"type": "string", "description": "備註", "optional": True}
        },
        "handler": legal_update_status
    },

    # 會議室預約工具
    "booking_list_rooms": {
        "description": "列出會議室",
        "parameters": {
            "branch_id": {"type": "integer", "description": "場館ID", "optional": True}
        },
        "handler": booking_list_rooms
    },
    "booking_check_availability": {
        "description": "查詢會議室可用時段",
        "parameters": {
            "room_id": {"type": "integer", "description": "會議室ID", "required": True},
            "date_str": {"type": "string", "description": "日期 (YYYY-MM-DD)", "required": True},
            "start_time": {"type": "string", "description": "開始時間 (HH:MM)", "optional": True},
            "end_time": {"type": "string", "description": "結束時間 (HH:MM)", "optional": True}
        },
        "handler": booking_check_availability
    },
    "booking_create": {
        "description": "建立會議室預約",
        "parameters": {
            "room_id": {"type": "integer", "description": "會議室ID", "required": True},
            "customer_id": {"type": "integer", "description": "客戶ID", "required": True},
            "date_str": {"type": "string", "description": "日期 (YYYY-MM-DD)", "required": True},
            "start_time": {"type": "string", "description": "開始時間 (HH:MM)", "required": True},
            "end_time": {"type": "string", "description": "結束時間 (HH:MM)", "required": True},
            "purpose": {"type": "string", "description": "會議目的", "optional": True},
            "attendees_count": {"type": "integer", "description": "預計人數", "optional": True},
            "notes": {"type": "string", "description": "備註", "optional": True}
        },
        "handler": booking_create
    },
    "booking_cancel": {
        "description": "取消會議室預約",
        "parameters": {
            "booking_id": {"type": "integer", "description": "預約ID", "required": True},
            "reason": {"type": "string", "description": "取消原因", "optional": True}
        },
        "handler": booking_cancel
    },
    "booking_update": {
        "description": "修改會議室預約",
        "parameters": {
            "booking_id": {"type": "integer", "description": "預約ID", "required": True},
            "date_str": {"type": "string", "description": "新日期 (YYYY-MM-DD)", "optional": True},
            "start_time": {"type": "string", "description": "新開始時間 (HH:MM)", "optional": True},
            "end_time": {"type": "string", "description": "新結束時間 (HH:MM)", "optional": True},
            "purpose": {"type": "string", "description": "會議目的", "optional": True},
            "attendees_count": {"type": "integer", "description": "預計人數", "optional": True},
            "notes": {"type": "string", "description": "備註", "optional": True}
        },
        "handler": booking_update
    },
    "booking_list": {
        "description": "列出會議室預約",
        "parameters": {
            "customer_id": {"type": "integer", "description": "客戶ID", "optional": True},
            "date_str": {"type": "string", "description": "特定日期", "optional": True},
            "date_from": {"type": "string", "description": "開始日期", "optional": True},
            "date_to": {"type": "string", "description": "結束日期", "optional": True},
            "branch_id": {"type": "integer", "description": "場館ID", "optional": True},
            "status": {"type": "string", "description": "狀態", "optional": True},
            "limit": {"type": "integer", "description": "回傳筆數", "default": 50}
        },
        "handler": booking_list
    },
    "booking_get": {
        "description": "取得預約詳情",
        "parameters": {
            "booking_id": {"type": "integer", "description": "預約ID", "required": True}
        },
        "handler": booking_get
    },
    "booking_send_reminder": {
        "description": "發送預約提醒",
        "parameters": {
            "booking_id": {"type": "integer", "description": "預約ID", "required": True}
        },
        "handler": booking_send_reminder
    },

    # 平面圖工具
    "floor_plan_get_positions": {
        "description": "取得場館所有位置的當前租戶狀態",
        "parameters": {
            "branch_id": {"type": "integer", "description": "場館ID（1=大忠本館）", "default": 1}
        },
        "handler": floor_plan_get_positions
    },
    "floor_plan_update_position": {
        "description": "更新位置的租戶關聯（設定或清空）",
        "parameters": {
            "position_number": {"type": "integer", "description": "位置編號", "required": True},
            "contract_id": {"type": "integer", "description": "合約ID（null=清空位置）", "optional": True},
            "branch_id": {"type": "integer", "description": "場館ID", "default": 1}
        },
        "handler": floor_plan_update_position
    },
    "floor_plan_generate": {
        "description": "生成場館平面圖 PDF，顯示位置與租戶對照",
        "parameters": {
            "branch_id": {"type": "integer", "description": "場館ID（1=大忠本館）", "default": 1},
            "output_date": {"type": "string", "description": "輸出日期 YYYYMMDD（預設今天）", "optional": True},
            "include_table": {"type": "boolean", "description": "是否包含右側租戶表格", "default": True}
        },
        "handler": floor_plan_generate
    },
    "floor_plan_preview_html": {
        "description": "預覽平面圖 HTML（不生成 PDF）",
        "parameters": {
            "branch_id": {"type": "integer", "description": "場館ID", "default": 1}
        },
        "handler": floor_plan_preview_html
    },

    # Brain 知識庫工具
    "brain_save_knowledge": {
        "description": "儲存知識到 Brain RAG 知識庫。當對話中發現有價值的資訊（如法規規定、流程說明、價格資訊、常見問題答案）時使用此工具。",
        "parameters": {
            "content": {"type": "string", "description": "知識內容（至少 10 字）", "required": True},
            "category": {
                "type": "string",
                "description": "分類：faq=常見問題, service_info=服務資訊, process=流程說明, regulation=法規規定, objection=異議處理, value_prop=價值主張, tactics=銷售技巧, customer_info=客戶資訊",
                "default": "faq"
            },
            "sub_category": {"type": "string", "description": "子分類（如：公司登記、稅務、銀行開戶）", "optional": True},
            "service_type": {"type": "string", "description": "適用服務：address_service, coworking, private_office, meeting_room", "optional": True}
        },
        "handler": brain_save_knowledge
    },
    "brain_search_knowledge": {
        "description": "搜尋 Brain 知識庫，使用向量語意搜尋找出相關知識",
        "parameters": {
            "query": {"type": "string", "description": "搜尋關鍵字或問題", "required": True},
            "top_k": {"type": "integer", "description": "回傳結果數量", "default": 5},
            "category": {"type": "string", "description": "限定分類", "optional": True}
        },
        "handler": brain_search_knowledge
    },
    "brain_list_categories": {
        "description": "列出 Brain 知識庫的所有分類及說明",
        "parameters": {},
        "handler": brain_list_categories
    },
    "brain_save_customer_traits": {
        "description": "儲存客戶特性到 Brain RAG 知識庫，讓 AI 客服在對話時能參考客戶的特點進行個性化應對。",
        "parameters": {
            "customer_name": {"type": "string", "description": "客戶姓名", "required": True},
            "company_name": {"type": "string", "description": "公司名稱", "optional": True},
            "line_user_id": {"type": "string", "description": "LINE User ID（用於 Brain 識別客戶）", "optional": True},
            "tags": {
                "type": "array",
                "description": "特性標籤。預設值：payment_risk, far_location, cooperative, strict, cautious, vip, referral。也支援自訂標籤如「喜歡用現金」「需要收據」「習慣遲到」等任意文字",
                "optional": True
            },
            "notes": {"type": "string", "description": "額外備註說明", "optional": True}
        },
        "handler": brain_save_customer_traits
    },

    # 回報問題/建議工具
    "feedback_submit": {
        "description": "提交問題回報或功能建議。當用戶反映系統問題、提出改進建議或新功能需求時使用。",
        "parameters": {
            "feedback_type": {
                "type": "string",
                "description": "類型：bug=系統錯誤, feature=新功能需求, improvement=改進建議, question=使用問題, other=其他",
                "required": True
            },
            "title": {"type": "string", "description": "標題（簡短描述）", "required": True},
            "description": {"type": "string", "description": "詳細說明", "optional": True},
            "priority": {"type": "string", "description": "優先級：low/medium/high/critical", "default": "medium"},
            "related_feature": {"type": "string", "description": "相關功能（如：繳費管理、報表）", "optional": True}
        },
        "handler": feedback_submit
    },
    "feedback_list": {
        "description": "列出已提交的問題回報（供管理員查看）",
        "parameters": {
            "status": {"type": "string", "description": "篩選狀態：open/reviewing/in_progress/resolved", "optional": True},
            "feedback_type": {"type": "string", "description": "篩選類型", "optional": True},
            "limit": {"type": "integer", "description": "回傳數量上限", "default": 20}
        },
        "handler": feedback_list
    },

    # Calendar 工具
    "calendar_create": {
        "description": "建立新的 Google Calendar（用於建立專屬簽約行事曆）",
        "parameters": {
            "name": {"type": "string", "description": "行事曆名稱", "required": True},
            "description": {"type": "string", "description": "行事曆描述", "optional": True}
        },
        "handler": calendar_create
    },
    "calendar_share": {
        "description": "分享行事曆給指定的 Email 使用者",
        "parameters": {
            "emails": {"type": "array", "description": "要分享的 Email 列表", "required": True},
            "calendar_id": {"type": "string", "description": "行事曆 ID（預設使用簽約行事曆）", "optional": True},
            "role": {"type": "string", "description": "權限角色 (reader/writer)", "default": "writer"}
        },
        "handler": calendar_share
    },
    "calendar_create_signing_appointment": {
        "description": "建立簽約行程到 Google Calendar。當客戶確認簽約時間時使用。",
        "parameters": {
            "customer_name": {"type": "string", "description": "客戶姓名", "required": True},
            "appointment_datetime": {"type": "string", "description": "簽約日期時間 (YYYY-MM-DD HH:MM)", "required": True},
            "company_name": {"type": "string", "description": "公司名稱", "optional": True},
            "duration_minutes": {"type": "integer", "description": "預計時長（分鐘）", "default": 60},
            "plan_name": {"type": "string", "description": "簽約方案", "optional": True},
            "customer_phone": {"type": "string", "description": "客戶電話", "optional": True},
            "customer_email": {"type": "string", "description": "客戶 Email", "optional": True},
            "notes": {"type": "string", "description": "備註", "optional": True},
            "branch": {"type": "string", "description": "場館名稱", "default": "大忠館"}
        },
        "handler": calendar_create_signing_appointment
    },
    "calendar_list_signing_appointments": {
        "description": "列出即將到來的簽約行程",
        "parameters": {
            "days_ahead": {"type": "integer", "description": "查詢未來幾天的行程", "default": 7}
        },
        "handler": calendar_list_signing_appointments
    },

    # 服務方案價格管理
    "service_plan_list": {
        "description": "列出服務方案價格表",
        "parameters": {
            "category": {"type": "string", "description": "分類篩選（登記服務/空間服務/代辦服務）", "optional": True},
            "is_active": {"type": "boolean", "description": "是否只顯示啟用的方案", "optional": True}
        },
        "handler": list_service_plans
    },
    "service_plan_get": {
        "description": "取得單一服務方案詳情",
        "parameters": {
            "plan_id": {"type": "integer", "description": "方案ID", "required": True}
        },
        "handler": get_service_plan
    },
    "service_plan_create": {
        "description": "建立服務方案",
        "parameters": {
            "category": {"type": "string", "description": "分類", "required": True},
            "name": {"type": "string", "description": "服務名稱", "required": True},
            "code": {"type": "string", "description": "服務代碼（唯一）", "required": True},
            "unit_price": {"type": "number", "description": "單價", "required": True},
            "unit": {"type": "string", "description": "計價單位（月/小時/次）", "required": True},
            "billing_cycle": {"type": "string", "description": "繳費週期", "optional": True},
            "deposit": {"type": "number", "description": "押金", "optional": True},
            "original_price": {"type": "number", "description": "原價", "optional": True},
            "min_duration": {"type": "string", "description": "最低租期", "optional": True},
            "revenue_type": {"type": "string", "description": "營收類型（own/referral）", "optional": True},
            "notes": {"type": "string", "description": "備註", "optional": True},
            "sort_order": {"type": "integer", "description": "排序", "optional": True}
        },
        "handler": create_service_plan
    },
    "service_plan_update": {
        "description": "更新服務方案",
        "parameters": {
            "plan_id": {"type": "integer", "description": "方案ID", "required": True},
            "updates": {"type": "object", "description": "要更新的欄位", "required": True}
        },
        "handler": update_service_plan
    },
    "service_plan_delete": {
        "description": "刪除服務方案",
        "parameters": {
            "plan_id": {"type": "integer", "description": "方案ID", "required": True}
        },
        "handler": delete_service_plan
    },
    "service_plan_reorder": {
        "description": "批量更新服務方案排序",
        "parameters": {
            "orders": {"type": "array", "description": "[{id, sort_order}, ...]", "required": True}
        },
        "handler": reorder_service_plans
    },
    "sync_prices_to_brain": {
        "description": "同步價格資訊到 AI 知識庫",
        "parameters": {},
        "handler": sync_prices_to_brain
    }
}


# ============================================================================
# FastAPI App
# ============================================================================

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from datetime import datetime, timedelta

# 排程器
scheduler = AsyncIOScheduler()


async def send_booking_reminders():
    """每 10 分鐘檢查並發送預約提醒（1 小時前）"""
    try:
        now = datetime.now()
        one_hour_later = now + timedelta(hours=1)

        # 查詢需要提醒的預約
        params = {
            "booking_date": f"eq.{now.strftime('%Y-%m-%d')}",
            "status": "eq.confirmed",
            "reminder_sent": "eq.false",
            "select": "id,start_time"
        }

        bookings = await postgrest_request("GET", "meeting_room_bookings", params=params)

        for booking in bookings:
            # 解析開始時間
            start_time = booking["start_time"][:5]  # HH:MM
            start_hour, start_min = map(int, start_time.split(":"))
            booking_datetime = now.replace(hour=start_hour, minute=start_min, second=0, microsecond=0)

            # 如果預約時間在 now+50min ~ now+70min 之間（約 1 小時前），發送提醒
            time_diff = (booking_datetime - now).total_seconds() / 60
            if 50 <= time_diff <= 70:
                try:
                    await booking_send_reminder(booking["id"])
                    logger.info(f"Sent reminder for booking {booking['id']}")
                except Exception as e:
                    logger.error(f"Failed to send reminder for booking {booking['id']}: {e}")

    except Exception as e:
        logger.error(f"send_booking_reminders error: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """應用生命週期"""
    logger.info("MCP Server starting...")

    # 設置續約工具的 postgrest_request
    set_renewal_postgrest(postgrest_request)
    logger.info("Renewal tools initialized")

    # 設置預約工具的 postgrest_request
    set_booking_postgrest(postgrest_request)
    logger.info("Booking tools initialized")

    # 設置通知工具的 postgrest_request
    set_notification_postgrest(postgrest_request)
    logger.info("Notification tools initialized")

    # 設置回報工具的 postgrest_request
    set_feedback_postgrest(postgrest_request)
    logger.info("Feedback tools initialized")

    # 啟動排程器
    scheduler.add_job(send_booking_reminders, 'interval', minutes=10)
    scheduler.start()
    logger.info("Scheduler started (booking reminders every 10 min)")

    # 測試資料庫連接
    try:
        conn = get_db_connection()
        conn.close()
        logger.info("Database connection OK")
    except Exception as e:
        logger.error(f"Database connection failed: {e}")

    yield

    # 關閉排程器
    scheduler.shutdown()
    logger.info("MCP Server shutting down...")


app = FastAPI(
    title="Hour Jungle CRM - MCP Server",
    description="AI Agent 整合介面",
    version="1.0.0",
    lifespan=lifespan
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================================
# API Endpoints
# ============================================================================

@app.get("/health")
async def health_check():
    """健康檢查"""
    return {
        "status": "healthy",
        "service": "mcp-server",
        "version": "1.0.0"
    }


@app.get("/tools")
async def list_tools():
    """列出所有可用工具"""
    tools = []
    for name, tool in MCP_TOOLS.items():
        tools.append({
            "name": name,
            "description": tool["description"],
            "parameters": tool["parameters"]
        })
    return {"tools": tools}


class ToolCallRequest(BaseModel):
    """工具調用請求"""
    tool: str
    parameters: dict = {}


@app.post("/tools/call")
async def call_tool(request: ToolCallRequest):
    """調用工具"""
    tool_name = request.tool
    params = request.parameters

    if tool_name not in MCP_TOOLS:
        raise HTTPException(
            status_code=404,
            detail=f"Tool '{tool_name}' not found"
        )

    tool = MCP_TOOLS[tool_name]
    handler = tool["handler"]

    try:
        result = await handler(**params)
        return {
            "success": True,
            "tool": tool_name,
            "result": result
        }
    except Exception as e:
        logger.error(f"Tool '{tool_name}' error: {e}")
        return {
            "success": False,
            "tool": tool_name,
            "error": str(e)
        }


# ============================================================================
# MCP Protocol Endpoints (for Claude Desktop)
# ============================================================================

@app.post("/mcp/initialize")
async def mcp_initialize():
    """MCP 初始化"""
    return {
        "protocolVersion": "2024-11-05",
        "serverInfo": {
            "name": "hourjungle-crm",
            "version": "1.0.0"
        },
        "capabilities": {
            "tools": {}
        }
    }


@app.post("/mcp/tools/list")
async def mcp_list_tools():
    """MCP 工具列表"""
    tools = []
    for name, tool in MCP_TOOLS.items():
        input_schema = {
            "type": "object",
            "properties": {},
            "required": []
        }

        for param_name, param_info in tool["parameters"].items():
            input_schema["properties"][param_name] = {
                "type": param_info["type"],
                "description": param_info.get("description", "")
            }
            if param_info.get("required"):
                input_schema["required"].append(param_name)

        tools.append({
            "name": name,
            "description": tool["description"],
            "inputSchema": input_schema
        })

    return {"tools": tools}


class MCPToolCall(BaseModel):
    """MCP 工具調用"""
    name: str
    arguments: dict = {}


@app.post("/mcp/tools/call")
async def mcp_call_tool(request: MCPToolCall):
    """MCP 工具調用"""
    tool_name = request.name
    args = request.arguments

    if tool_name not in MCP_TOOLS:
        return {
            "content": [{
                "type": "text",
                "text": f"Error: Tool '{tool_name}' not found"
            }],
            "isError": True
        }

    tool = MCP_TOOLS[tool_name]
    handler = tool["handler"]

    try:
        result = await handler(**args)
        return {
            "content": [{
                "type": "text",
                "text": str(result)
            }],
            "isError": False
        }
    except Exception as e:
        logger.error(f"MCP Tool '{tool_name}' error: {e}")
        return {
            "content": [{
                "type": "text",
                "text": f"Error: {str(e)}"
            }],
            "isError": True
        }


# ============================================================================
# 直接 API Endpoints (給 WebUI 使用)
# ============================================================================

@app.get("/api/customers")
async def api_list_customers(
    branch_id: int = None,
    status: str = None,
    limit: int = 50,
    offset: int = 0
):
    """客戶列表 API"""
    params = {"limit": limit, "offset": offset}

    if branch_id:
        params["branch_id"] = f"eq.{branch_id}"
    if status:
        params["status"] = f"eq.{status}"

    return await postgrest_request("GET", "v_customer_summary", params=params)


@app.get("/api/payments/due")
async def api_payments_due(
    branch_id: int = None,
    urgency: str = None
):
    """應收款 API"""
    params = {}

    if branch_id:
        params["branch_id"] = f"eq.{branch_id}"
    if urgency and urgency != "all":
        params["urgency"] = f"eq.{urgency}"

    return await postgrest_request("GET", "v_payments_due", params=params)


@app.get("/api/today-tasks")
async def api_today_tasks(branch_id: int = None):
    """今日待辦 API"""
    params = {}
    if branch_id:
        params["branch_id"] = f"eq.{branch_id}"

    return await postgrest_request("GET", "v_today_tasks", params=params)


# ============================================================================
# 會議室預約 API Endpoints
# ============================================================================

@app.get("/api/meeting-rooms")
async def api_list_meeting_rooms(branch_id: int = None):
    """會議室列表 API"""
    result = await booking_list_rooms(branch_id)
    return result


@app.get("/api/bookings")
async def api_list_bookings(
    customer_id: int = None,
    date: str = None,
    date_from: str = None,
    date_to: str = None,
    branch_id: int = None,
    status: str = None,
    limit: int = 50
):
    """預約列表 API"""
    result = await booking_list(
        customer_id=customer_id,
        date_str=date,
        date_from=date_from,
        date_to=date_to,
        branch_id=branch_id,
        status=status,
        limit=limit
    )
    return result


@app.get("/api/bookings/{booking_id}")
async def api_get_booking(booking_id: int):
    """預約詳情 API"""
    result = await booking_get(booking_id)
    return result


@app.get("/api/bookings/availability/{room_id}")
async def api_check_availability(
    room_id: int,
    date: str,
    start_time: str = None,
    end_time: str = None
):
    """查詢可用時段 API"""
    result = await booking_check_availability(room_id, date, start_time, end_time)
    return result


class BookingCreateRequest(BaseModel):
    """建立預約請求"""
    room_id: int
    customer_id: int
    date: str
    start_time: str
    end_time: str
    purpose: str = None
    attendees_count: int = None
    notes: str = None


@app.post("/api/bookings")
async def api_create_booking(request: BookingCreateRequest):
    """建立預約 API"""
    result = await booking_create(
        room_id=request.room_id,
        customer_id=request.customer_id,
        date_str=request.date,
        start_time=request.start_time,
        end_time=request.end_time,
        purpose=request.purpose,
        attendees_count=request.attendees_count,
        notes=request.notes,
        created_by="admin"
    )
    return result


class BookingUpdateRequest(BaseModel):
    """更新預約請求"""
    date: str = None
    start_time: str = None
    end_time: str = None
    purpose: str = None
    attendees_count: int = None
    notes: str = None


@app.patch("/api/bookings/{booking_id}")
async def api_update_booking(booking_id: int, request: BookingUpdateRequest):
    """更新預約 API"""
    result = await booking_update(
        booking_id=booking_id,
        date_str=request.date,
        start_time=request.start_time,
        end_time=request.end_time,
        purpose=request.purpose,
        attendees_count=request.attendees_count,
        notes=request.notes
    )
    return result


class BookingCancelRequest(BaseModel):
    """取消預約請求"""
    reason: str = None


@app.post("/api/bookings/{booking_id}/cancel")
async def api_cancel_booking(booking_id: int, request: BookingCancelRequest = None):
    """取消預約 API"""
    reason = request.reason if request else None
    result = await booking_cancel(booking_id, reason)
    return result


# ============================================================================
# LINE Webhook Endpoint
# ============================================================================

@app.post("/line/webhook")
async def line_webhook(request: Request):
    """LINE Bot Webhook 端點"""
    signature = request.headers.get("X-Line-Signature", "")
    body = await request.body()

    # 驗證簽名
    if not verify_signature(body, signature):
        logger.warning("Invalid LINE webhook signature")
        raise HTTPException(status_code=400, detail="Invalid signature")

    # 解析事件
    try:
        body_json = json.loads(body)
        events = body_json.get("events", [])

        for event in events:
            try:
                await handle_line_event(event)
            except Exception as e:
                logger.error(f"Error handling LINE event: {e}")

        return {"status": "ok"}
    except Exception as e:
        logger.error(f"LINE webhook error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


class LineForwardRequest(BaseModel):
    """LINE 事件轉發請求（來自 Brain）"""
    user_id: str
    message_text: str
    event_type: str = "message"  # message, postback
    postback_data: str = None    # postback 時使用


# ============================================================================
# 排程 API Endpoints
# ============================================================================

class SchedulerTriggerRequest(BaseModel):
    """排程觸發請求"""
    dry_run: bool = True


@app.post("/api/scheduler/daily-reminders")
async def api_trigger_daily_reminders(request: SchedulerTriggerRequest = None):
    """
    觸發每日自動提醒
    由 Cloud Scheduler 呼叫，或手動測試
    """
    dry_run = request.dry_run if request else True
    result = await trigger_daily_reminders(dry_run=dry_run)
    return result


@app.get("/api/notifications/settings")
async def api_get_notification_settings():
    """取得通知設定"""
    result = await get_notification_settings()
    return result


class NotificationSettingRequest(BaseModel):
    """更新通知設定請求"""
    key: str
    value: str


@app.patch("/api/notifications/settings")
async def api_update_notification_setting(request: NotificationSettingRequest):
    """更新通知設定"""
    result = await update_notification_setting(request.key, request.value)
    return result


@app.get("/api/notifications/logs")
async def api_get_notification_logs(
    notification_type: str = None,
    customer_id: int = None,
    limit: int = 50
):
    """取得通知記錄"""
    result = await get_notification_logs(
        notification_type=notification_type,
        customer_id=customer_id,
        limit=limit
    )
    return result


@app.get("/api/notifications/today")
async def api_get_today_notifications():
    """取得今日通知統計"""
    result = await get_today_notifications()
    return result


@app.get("/api/notifications/monthly-summary")
async def api_get_monthly_summary():
    """取得當月催繳/續約統計"""
    result = await get_monthly_reminders_summary()
    return result


# ============================================================================
# LLM 意圖分類器
# ============================================================================

INTENT_CLASSIFIER_PROMPT = """你是一個意圖分類器。根據用戶訊息，判斷屬於哪個意圖類別。

意圖類別：
1. booking_start - 用戶想要預約會議室（例如：「預約」「預約會議室」「我要訂會議室」「book room」）
2. booking_query - 用戶想查詢自己的預約（例如：「我的預約」「查詢預約」「我有什麼預約」）
3. booking_cancel - 用戶想取消預約（例如：「取消預約」「取消」「不要了」）
4. booking_help - 用戶詢問預約相關幫助（例如：「怎麼預約」「會議室怎麼訂」）
5. booking_flow - 用戶正在預約流程中的回應（這個很難判斷，通常是 other）
6. other - 其他一般對話、問候、問題等（不是預約相關）

⚠️ 重要規則：
- 只有明確提到「預約」「會議室」「book」「訂」等關鍵字才算 booking 意圖
- 一般問候（你好、嗨、早安）→ other
- 業務問題（稅務、報價、合約）→ other
- 不確定時 → other

只回覆意圖類別名稱，不要其他內容。

用戶訊息：{message}"""


async def classify_intent(message_text: str) -> str:
    """
    使用 LLM 分類用戶意圖
    返回: booking_start | booking_query | booking_cancel | booking_help | other
    """
    if not settings.openrouter_api_key:
        logger.warning("OpenRouter API key not configured, defaulting to 'other'")
        return "other"

    try:
        client = OpenAI(
            base_url="https://openrouter.ai/api/v1",
            api_key=settings.openrouter_api_key
        )

        # 使用快速模型進行意圖分類（成本低、速度快）
        response = client.chat.completions.create(
            model="google/gemini-2.0-flash-001",  # 快速便宜的模型
            max_tokens=20,
            messages=[
                {"role": "user", "content": INTENT_CLASSIFIER_PROMPT.format(message=message_text)}
            ],
            extra_headers={
                "HTTP-Referer": "https://hj.yourspce.org",
                "X-Title": "Hour Jungle CRM - Intent Classifier"
            }
        )

        intent = response.choices[0].message.content.strip().lower()

        # 正規化意圖
        if "booking_start" in intent or intent == "booking_start":
            return "booking_start"
        elif "booking_query" in intent or intent == "booking_query":
            return "booking_query"
        elif "booking_cancel" in intent or intent == "booking_cancel":
            return "booking_cancel"
        elif "booking_help" in intent or intent == "booking_help":
            return "booking_help"
        elif "booking_flow" in intent or intent == "booking_flow":
            return "booking_flow"
        else:
            return "other"

    except Exception as e:
        logger.error(f"Intent classification error: {e}")
        return "other"


@app.post("/api/line/forward")
async def line_forward(request: LineForwardRequest):
    """
    接收 Brain 轉發的 LINE 事件（會議室預約）

    注意：Brain 已經用 LLM 判斷意圖是「預約會議室」才會轉發到這裡
    這裡只需處理預約流程，不需再做意圖分類
    """
    try:
        # Postback 事件（預約流程中的選擇）
        if request.event_type == "postback":
            event = {
                "type": "postback",
                "source": {"userId": request.user_id},
                "postback": {"data": request.postback_data}
            }
            result = await handle_line_event(event)
            return {
                "success": True,
                "handled": result.get("handled", False),
                "intent": "booking_flow",
                "reply_text": result.get("reply_text", "")
            }

        # 文字訊息：直接進入預約流程
        event = {
            "type": "message",
            "source": {"userId": request.user_id},
            "message": {"type": "text", "text": request.message_text}
        }

        logger.info(f"Processing booking request: user={request.user_id}, text='{request.message_text}'")
        result = await handle_line_event(event)

        return {
            "success": True,
            "handled": result.get("handled", False),
            "intent": "booking",
            "reply_text": result.get("reply_text", "")
        }

    except Exception as e:
        logger.error(f"LINE forward error: {e}")
        return {"success": False, "error": str(e), "intent": "error"}


# ============================================================================
# AI Chat Endpoint (內部 AI 助手) - 使用 OpenRouter
# ============================================================================

# 可用模型列表
AVAILABLE_MODELS = {
    "claude-sonnet-4.5": {
        "id": "anthropic/claude-sonnet-4.5",
        "name": "Claude Sonnet 4.5",
        "description": "最新最強，適合複雜任務"
    },
    "claude-sonnet-4": {
        "id": "anthropic/claude-sonnet-4",
        "name": "Claude Sonnet 4",
        "description": "平衡性能與成本"
    },
    "claude-3.5-sonnet": {
        "id": "anthropic/claude-3.5-sonnet",
        "name": "Claude 3.5 Sonnet",
        "description": "快速經濟實惠"
    },
    "gpt-4o": {
        "id": "openai/gpt-4o",
        "name": "GPT-4o",
        "description": "OpenAI 多模態模型"
    },
    "gemini-2.0-flash": {
        "id": "google/gemini-2.0-flash-001",
        "name": "Gemini 2.0 Flash",
        "description": "Google 快速模型"
    }
}

DEFAULT_MODEL = "claude-sonnet-4"


def get_openrouter_client():
    """取得 OpenRouter 客戶端"""
    if not settings.openrouter_api_key:
        raise HTTPException(
            status_code=500,
            detail="OPENROUTER_API_KEY not configured"
        )
    return OpenAI(
        base_url="https://openrouter.ai/api/v1",
        api_key=settings.openrouter_api_key
    )


def convert_tools_for_openai():
    """將 MCP_TOOLS 轉換為 OpenAI 格式"""
    tools = []
    for name, tool in MCP_TOOLS.items():
        properties = {}
        required = []

        for param_name, param_info in tool["parameters"].items():
            param_type = param_info["type"]
            if param_type == "integer":
                param_type = "integer"
            elif param_type == "number":
                param_type = "number"
            elif param_type == "object":
                param_type = "object"
            else:
                param_type = "string"

            properties[param_name] = {
                "type": param_type,
                "description": param_info.get("description", "")
            }

            if param_info.get("required"):
                required.append(param_name)

        tools.append({
            "type": "function",
            "function": {
                "name": name,
                "description": tool["description"],
                "parameters": {
                    "type": "object",
                    "properties": properties,
                    "required": required
                }
            }
        })

    return tools


class ChatMessage(BaseModel):
    """聊天訊息"""
    role: str  # 'user' or 'assistant'
    content: str


class AIChatRequest(BaseModel):
    """AI 聊天請求"""
    messages: List[ChatMessage]
    model: str = DEFAULT_MODEL
    stream: bool = False


class AIChatResponse(BaseModel):
    """AI 聊天回應"""
    success: bool
    message: str
    model_used: str = ""
    tool_calls: List[dict] = []


# ============================================================================
# RAG 知識搜尋
# ============================================================================

BRAIN_API_URL = os.getenv("BRAIN_API_URL", "https://brain.yourspce.org")

async def search_brain_knowledge(query: str, top_k: int = 3) -> str:
    """
    搜尋 Brain 知識庫，回傳相關知識作為 context
    """
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{BRAIN_API_URL}/api/knowledge/search",
                json={"query": query, "top_k": top_k},
                timeout=10.0
            )

            if response.status_code == 200:
                results = response.json()
                if results:
                    knowledge_text = "\n".join([
                        f"- {r.get('content', '')}"
                        for r in results
                        if r.get('similarity', 0) > 0.5
                    ])
                    if knowledge_text:
                        return f"\n\n## 相關知識參考\n{knowledge_text}"
            return ""
    except Exception as e:
        logger.warning(f"Brain knowledge search failed: {e}")
        return ""


# CRM 系統使用教學（內建知識）
CRM_USAGE_GUIDE = """
## CRM 系統使用教學

### 頁面功能說明
1. **儀表板** - 總覽今日待辦、逾期款項、續約提醒
2. **客戶管理** - 搜尋、新增、編輯客戶資料
3. **合約管理** - 查看所有合約、建立新合約、續約處理
4. **繳費管理** - 記錄繳費、撤銷繳費、檢視繳費歷史
5. **發票管理** - 開立電子發票、作廢、折讓（整合光貿 API）
6. **報表中心** - 營收報表、佣金報表、客戶統計
7. **會議室預約** - 管理會議室預約
8. **平面圖** - 查看/編輯座位配置
9. **AI 助手** - 就是我！用自然語言操作 CRM

### 常見操作
- **記錄繳費**：在繳費管理頁面，找到對應的待繳記錄，點擊「記錄繳費」
- **發送催繳**：在儀表板的逾期款項區塊，點擊「催繳」按鈕
- **開立發票**：在發票管理頁面，找到已付款但未開發票的記錄，點擊「開立」
- **續約提醒**：在續約管理頁面，可批次發送 LINE 續約提醒

### 注意事項
- 發票開立後無法修改，只能作廢重開
- 繳費記錄撤銷需要填寫原因
- LINE 訊息發送需要客戶已綁定 LINE
"""

# 系統提示詞
CRM_SYSTEM_PROMPT = """你是 Hour Jungle CRM 的智能助手，專門協助員工管理客戶、合約、繳費等事務。

你的能力：
1. 查詢客戶資料（姓名、電話、公司名、LINE 綁定狀態）
2. 查詢應收款項、逾期款項
3. 查詢即將到期的合約、需續約提醒
4. 查詢營收報表、佣金報表
5. 發送 LINE 訊息給客戶（繳費提醒、續約提醒）
6. 建立新客戶、更新客戶資料、記錄繳費
7. 佣金付款、撤銷繳費記錄
8. 儲存有價值的知識到 AI 知識庫

📚 知識儲存功能
當對話中發現以下有價值的資訊時，你應該主動詢問用戶是否要儲存到知識庫：
- 法規規定（如：資本額 25 萬以下免資本證明）
- 流程說明（如：公司登記需要哪些文件）
- 價格資訊（如：某服務的收費標準）
- 常見問題的答案
- 異議處理方式（如：如何應對「太貴」的反應）
- 客戶特性資訊（使用 brain_save_customer_traits 工具）
  預設標籤：payment_risk, far_location, cooperative, strict, cautious, vip, referral
  也可使用自訂標籤如「喜歡用現金」「需要收據」「習慣遲到」等任意文字

詢問格式：
💡 我注意到這個資訊可能對未來的客服對話有幫助：
「[資訊摘要]」
是否要將這個知識儲存到 AI 知識庫？

如果用戶同意，使用 brain_save_knowledge 工具儲存，並根據內容選擇適當的分類：
- faq: 常見問題
- service_info: 服務資訊（價格、地址、營業時間）
- process: 流程說明
- regulation: 法規規定
- objection: 異議處理
- value_prop: 價值主張
- tactics: 銷售技巧
- customer_info: 客戶資訊

🐛 回報問題/建議功能
當用戶反映系統問題或提出建議時，使用 feedback_submit 工具記錄：
- 系統 Bug：「某功能壞了」「資料顯示錯誤」→ feedback_type: bug
- 新功能需求：「希望可以...」「能不能新增...」→ feedback_type: feature
- 改進建議：「這個功能用起來不方便」→ feedback_type: improvement
- 使用問題：「這個怎麼用」「找不到某功能」→ feedback_type: question

回報後告知用戶：「已收到您的回報，開發團隊會盡快處理！」

使用說明：
- 當用戶詢問客戶資料時，使用 crm_search_customers 或 crm_get_customer_detail
- 當用戶詢問逾期時，使用 report_overdue_list
- 當用戶詢問到期合約時，使用 crm_list_renewals_due
- 當用戶詢問營收時，使用 report_revenue_summary
- 當用戶詢問待繳款項時，使用 crm_list_payments_due

⚠️ 重要：執行前確認機制
以下操作屬於「寫入操作」，執行前必須先向用戶確認：
- crm_create_customer（建立客戶）
- crm_update_customer（更新客戶）
- crm_record_payment（記錄繳費）
- crm_payment_undo（撤銷繳費）
- crm_create_contract（建立合約）
- commission_pay（佣金付款）
- renewal_update_status（更新續約狀態）
- renewal_batch_update（批次更新續約）
- line_send_message（發送 LINE 訊息）
- line_send_payment_reminder（發送繳費提醒）
- line_send_renewal_reminder（發送續約提醒）
- invoice_create（開立發票）
- invoice_void（作廢發票）
- invoice_allowance（開立折讓單）
- contract_generate_pdf（生成合約 PDF）

執行寫入操作前，你必須：
1. 先說明你將要執行的操作內容（工具名稱、關鍵參數）
2. 明確詢問「是否確認執行？」
3. 只有在用戶回覆「是」「確認」「好」「執行」等肯定回答後才能執行
4. 如果用戶回覆「否」「取消」「不要」等，則不執行並詢問是否需要其他協助

範例：
用戶：幫我記錄王小明的繳費
你：我將執行以下操作：
📝 記錄繳費 (crm_record_payment)
- 客戶：王小明
- 付款ID: 123
- 金額：$5,000

是否確認執行此操作？

回覆時請使用繁體中文，保持簡潔專業。"""


@app.get("/ai/models")
async def list_ai_models():
    """列出可用的 AI 模型"""
    return {
        "models": [
            {"key": k, **v} for k, v in AVAILABLE_MODELS.items()
        ],
        "default": DEFAULT_MODEL
    }


@app.post("/ai/chat")
async def ai_chat(request: AIChatRequest):
    """AI 聊天端點 - 使用 OpenRouter + RAG"""
    try:
        client = get_openrouter_client()
        tools = convert_tools_for_openai()

        # 取得模型 ID
        model_key = request.model if request.model in AVAILABLE_MODELS else DEFAULT_MODEL
        model_id = AVAILABLE_MODELS[model_key]["id"]

        # 取得用戶最後一條訊息，用於 RAG 搜尋
        last_user_message = ""
        for m in reversed(request.messages):
            if m.role == "user":
                last_user_message = m.content
                break

        # 搜尋 Brain 知識庫（RAG）
        rag_context = ""
        if last_user_message:
            rag_context = await search_brain_knowledge(last_user_message, top_k=3)

        # 組合 system prompt（包含 RAG 知識 + CRM 使用指南）
        enhanced_prompt = CRM_SYSTEM_PROMPT + CRM_USAGE_GUIDE + rag_context

        # 轉換訊息格式
        messages = [
            {"role": "system", "content": enhanced_prompt}
        ]
        for m in request.messages:
            messages.append({"role": m.role, "content": m.content})

        # 呼叫 OpenRouter API
        response = client.chat.completions.create(
            model=model_id,
            max_tokens=4096,
            tools=tools,
            messages=messages,
            extra_headers={
                "HTTP-Referer": "https://hj.yourspce.org",
                "X-Title": "Hour Jungle CRM"
            }
        )

        # 處理工具調用
        tool_calls_made = []
        assistant_message = response.choices[0].message

        while assistant_message.tool_calls:
            # 收集工具調用結果
            tool_messages = []

            for tool_call in assistant_message.tool_calls:
                tool_name = tool_call.function.name
                # 防止 arguments 為 None 導致 json.loads 錯誤
                tool_args = json.loads(tool_call.function.arguments) if tool_call.function.arguments else {}
                tool_id = tool_call.id

                logger.info(f"AI calling tool: {tool_name} with {tool_args}")

                # 執行工具
                if tool_name in MCP_TOOLS:
                    handler = MCP_TOOLS[tool_name]["handler"]
                    try:
                        result = await handler(**tool_args)
                        tool_result = json.dumps(result, ensure_ascii=False, default=str)
                        tool_calls_made.append({
                            "tool": tool_name,
                            "input": tool_args,
                            "result": result
                        })
                    except Exception as e:
                        logger.error(f"Tool {tool_name} error: {e}")
                        tool_result = f"Error: {str(e)}"
                else:
                    tool_result = f"Tool '{tool_name}' not found"

                tool_messages.append({
                    "role": "tool",
                    "tool_call_id": tool_id,
                    "content": tool_result
                })

            # 繼續對話，包含工具結果
            messages.append({
                "role": "assistant",
                "content": assistant_message.content,
                "tool_calls": [
                    {
                        "id": tc.id,
                        "type": "function",
                        "function": {
                            "name": tc.function.name,
                            "arguments": tc.function.arguments or "{}"
                        }
                    } for tc in assistant_message.tool_calls
                ]
            })
            messages.extend(tool_messages)

            # 再次呼叫 API
            response = client.chat.completions.create(
                model=model_id,
                max_tokens=4096,
                tools=tools,
                messages=messages,
                extra_headers={
                    "HTTP-Referer": "https://hj.yourspce.org",
                    "X-Title": "Hour Jungle CRM"
                }
            )
            assistant_message = response.choices[0].message

        # 提取最終文字回應
        final_text = assistant_message.content or ""

        return AIChatResponse(
            success=True,
            message=final_text,
            model_used=model_key,
            tool_calls=tool_calls_made
        )

    except Exception as e:
        logger.error(f"AI chat error: {e}")
        return AIChatResponse(
            success=False,
            message=f"AI 服務錯誤：{str(e)}",
            model_used=request.model,
            tool_calls=[]
        )


# ============================================================================
# AI Chat Streaming Endpoint
# ============================================================================

@app.post("/ai/chat/stream")
async def ai_chat_stream(request: AIChatRequest):
    """AI 聊天串流端點 - 使用 Server-Sent Events + RAG"""

    # 取得用戶最後一條訊息，用於 RAG 搜尋（在 generate 外面執行）
    last_user_message = ""
    for m in reversed(request.messages):
        if m.role == "user":
            last_user_message = m.content
            break

    # 搜尋 Brain 知識庫（RAG）
    rag_context = ""
    if last_user_message:
        rag_context = await search_brain_knowledge(last_user_message, top_k=3)

    # 組合 system prompt
    enhanced_prompt = CRM_SYSTEM_PROMPT + CRM_USAGE_GUIDE + rag_context

    async def generate():
        try:
            client = get_openrouter_client()
            tools = convert_tools_for_openai()

            model_key = request.model if request.model in AVAILABLE_MODELS else DEFAULT_MODEL
            model_id = AVAILABLE_MODELS[model_key]["id"]

            messages = [{"role": "system", "content": enhanced_prompt}]
            for m in request.messages:
                messages.append({"role": m.role, "content": m.content})

            # 第一次調用（可能有工具調用）
            response = client.chat.completions.create(
                model=model_id,
                max_tokens=4096,
                tools=tools,
                messages=messages,
                extra_headers={
                    "HTTP-Referer": "https://hj.yourspce.org",
                    "X-Title": "Hour Jungle CRM"
                }
            )

            assistant_message = response.choices[0].message
            tool_calls_made = []

            # 處理工具調用（非串流）
            while assistant_message.tool_calls:
                # 發送工具調用狀態
                for tool_call in assistant_message.tool_calls:
                    tool_name = tool_call.function.name
                    yield f"data: {json.dumps({'type': 'tool', 'name': tool_name}, ensure_ascii=False)}\n\n"

                tool_messages = []
                for tool_call in assistant_message.tool_calls:
                    tool_name = tool_call.function.name
                    tool_args = json.loads(tool_call.function.arguments) if tool_call.function.arguments else {}
                    tool_id = tool_call.id

                    if tool_name in MCP_TOOLS:
                        handler = MCP_TOOLS[tool_name]["handler"]
                        try:
                            result = await handler(**tool_args)
                            tool_result = json.dumps(result, ensure_ascii=False, default=str)
                            tool_calls_made.append({"tool": tool_name, "input": tool_args})
                        except Exception as e:
                            tool_result = f"Error: {str(e)}"
                    else:
                        tool_result = f"Tool '{tool_name}' not found"

                    tool_messages.append({
                        "role": "tool",
                        "tool_call_id": tool_id,
                        "content": tool_result
                    })

                messages.append({
                    "role": "assistant",
                    "content": assistant_message.content,
                    "tool_calls": [
                        {
                            "id": tc.id,
                            "type": "function",
                            "function": {
                                "name": tc.function.name,
                                "arguments": tc.function.arguments or "{}"
                            }
                        } for tc in assistant_message.tool_calls
                    ]
                })
                messages.extend(tool_messages)

                response = client.chat.completions.create(
                    model=model_id,
                    max_tokens=4096,
                    tools=tools,
                    messages=messages,
                    extra_headers={
                        "HTTP-Referer": "https://hj.yourspce.org",
                        "X-Title": "Hour Jungle CRM"
                    }
                )
                assistant_message = response.choices[0].message

            # 最終回應使用串流
            stream = client.chat.completions.create(
                model=model_id,
                max_tokens=4096,
                messages=messages + [{"role": "assistant", "content": ""}] if not assistant_message.content else messages,
                stream=True,
                extra_headers={
                    "HTTP-Referer": "https://hj.yourspce.org",
                    "X-Title": "Hour Jungle CRM"
                }
            )

            # 如果已有最終內容，直接輸出
            if assistant_message.content:
                yield f"data: {json.dumps({'type': 'content', 'text': assistant_message.content}, ensure_ascii=False)}\n\n"
            else:
                # 串流輸出
                for chunk in stream:
                    if chunk.choices[0].delta.content:
                        yield f"data: {json.dumps({'type': 'content', 'text': chunk.choices[0].delta.content}, ensure_ascii=False)}\n\n"

            yield f"data: {json.dumps({'type': 'done'}, ensure_ascii=False)}\n\n"

        except Exception as e:
            logger.error(f"AI chat stream error: {e}")
            yield f"data: {json.dumps({'type': 'error', 'message': str(e)}, ensure_ascii=False)}\n\n"

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )


# ============================================================================
# 啟動
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
