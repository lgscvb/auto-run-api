"""
Hour Jungle CRM - MCP Server
FastAPI + MCP Protocol for AI Agent Integration
"""

import os
import logging
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Request
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
    create_contract
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
    "crm_create_contract": {
        "description": "建立新合約",
        "parameters": {
            "customer_id": {"type": "integer", "description": "客戶ID", "required": True},
            "branch_id": {"type": "integer", "description": "場館ID", "required": True},
            "start_date": {"type": "string", "description": "開始日期 (YYYY-MM-DD)", "required": True},
            "end_date": {"type": "string", "description": "結束日期 (YYYY-MM-DD)", "required": True},
            "monthly_rent": {"type": "number", "description": "月租金", "required": True},
            "contract_type": {"type": "string", "description": "合約類型", "default": "virtual_office"}
        },
        "handler": create_contract
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
    }
}


# ============================================================================
# FastAPI App
# ============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """應用生命週期"""
    logger.info("MCP Server starting...")

    # 測試資料庫連接
    try:
        conn = get_db_connection()
        conn.close()
        logger.info("Database connection OK")
    except Exception as e:
        logger.error(f"Database connection failed: {e}")

    yield

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
# 啟動
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
