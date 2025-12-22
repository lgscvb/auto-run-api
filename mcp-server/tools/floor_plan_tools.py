"""
Hour Jungle CRM - Floor Plan Tools
平面圖生成工具

用於生成場館平面圖 PDF，顯示位置與租戶對照
"""

import logging
import os
import json
from datetime import datetime
from typing import Dict, Any, List, Optional

import httpx
import google.auth
from google.auth.transport.requests import Request
from google.oauth2 import id_token

logger = logging.getLogger(__name__)

# PostgREST URL
POSTGREST_URL = os.getenv("POSTGREST_URL", "http://postgrest:3000")

# Cloud Run PDF Generator URL
PDF_GENERATOR_URL = os.getenv(
    "PDF_GENERATOR_URL",
    "https://pdf-generator-743652001579.asia-east1.run.app"
)

# GCS 底圖 URL
GCS_BUCKET = os.getenv("GCS_BUCKET", "hourjungle-contracts")
FLOOR_PLAN_IMAGE_BASE = f"https://storage.googleapis.com/{GCS_BUCKET}"


async def postgrest_get(endpoint: str, params: dict = None) -> Any:
    """PostgREST GET 請求"""
    url = f"{POSTGREST_URL}/{endpoint}"
    async with httpx.AsyncClient() as client:
        response = await client.get(url, params=params, timeout=30.0)
        response.raise_for_status()
        return response.json()


def get_id_token_for_cloud_run(target_url: str) -> Optional[str]:
    """取得 Cloud Run 的 ID Token"""
    try:
        credentials, project = google.auth.default()
        auth_req = Request()
        token = id_token.fetch_id_token(auth_req, target_url)
        return token
    except Exception as e:
        logger.warning(f"無法取得 ID Token: {e}，嘗試不帶認證呼叫")
        return None


# ============================================================================
# 平面圖工具
# ============================================================================

async def floor_plan_get_positions(
    branch_id: int = 1
) -> Dict[str, Any]:
    """
    取得場館所有位置的當前租戶狀態

    Args:
        branch_id: 場館ID（預設 1 = 大忠本館）

    Returns:
        位置列表，包含坐標和租戶資訊
    """
    try:
        # 取得平面圖資訊
        plans = await postgrest_get("floor_plans", {"branch_id": f"eq.{branch_id}"})
        if not plans:
            return {"success": False, "message": f"找不到場館 {branch_id} 的平面圖"}

        plan = plans[0]

        # 取得所有位置和租戶資訊
        positions = await postgrest_get(
            "v_floor_positions",
            {"branch_id": f"eq.{branch_id}", "order": "position_number"}
        )

        # 統計
        total = len(positions)
        occupied = sum(1 for p in positions if p.get("contract_id"))
        vacant = total - occupied

        return {
            "success": True,
            "floor_plan": {
                "id": plan["id"],
                "name": plan["name"],
                "image_url": plan.get("image_url") or (f"{FLOOR_PLAN_IMAGE_BASE}/{plan['image_filename']}" if plan.get("image_filename") else None),
                "width": plan["width"],
                "height": plan["height"]
            },
            "statistics": {
                "total_positions": total,
                "occupied": occupied,
                "vacant": vacant,
                "occupancy_rate": f"{(occupied / total * 100):.1f}%" if total > 0 else "0%"
            },
            "positions": positions
        }

    except Exception as e:
        logger.error(f"取得平面圖位置失敗: {e}")
        return {"success": False, "message": f"取得失敗: {e}"}


async def floor_plan_update_position(
    position_number: int,
    contract_id: Optional[int] = None,
    branch_id: int = 1
) -> Dict[str, Any]:
    """
    更新位置的租戶關聯（透過合約 position_number 欄位）

    Args:
        position_number: 位置編號
        contract_id: 合約ID（None = 清空位置）
        branch_id: 場館ID

    Returns:
        更新結果
    """
    try:
        if contract_id:
            # 設定合約的 position_number
            async with httpx.AsyncClient() as client:
                # 先清除該位置的其他合約
                await client.patch(
                    f"{POSTGREST_URL}/contracts",
                    params={
                        "position_number": f"eq.{position_number}",
                        "branch_id": f"eq.{branch_id}"
                    },
                    json={"position_number": None},
                    headers={"Content-Type": "application/json", "Prefer": "return=minimal"},
                    timeout=30.0
                )

                # 設定新合約的位置
                response = await client.patch(
                    f"{POSTGREST_URL}/contracts",
                    params={"id": f"eq.{contract_id}"},
                    json={"position_number": position_number},
                    headers={"Content-Type": "application/json", "Prefer": "return=representation"},
                    timeout=30.0
                )
                response.raise_for_status()

            return {
                "success": True,
                "message": f"位置 {position_number} 已設定為合約 {contract_id}",
                "position_number": position_number,
                "contract_id": contract_id
            }
        else:
            # 清空位置
            async with httpx.AsyncClient() as client:
                response = await client.patch(
                    f"{POSTGREST_URL}/contracts",
                    params={
                        "position_number": f"eq.{position_number}",
                        "branch_id": f"eq.{branch_id}"
                    },
                    json={"position_number": None},
                    headers={"Content-Type": "application/json", "Prefer": "return=minimal"},
                    timeout=30.0
                )
                response.raise_for_status()

            return {
                "success": True,
                "message": f"位置 {position_number} 已清空",
                "position_number": position_number,
                "contract_id": None
            }

    except Exception as e:
        logger.error(f"更新位置失敗: {e}")
        return {"success": False, "message": f"更新失敗: {e}"}


async def floor_plan_generate(
    branch_id: int = 1,
    output_date: Optional[str] = None,
    include_table: bool = True
) -> Dict[str, Any]:
    """
    生成場館平面圖 PDF

    Args:
        branch_id: 場館ID（預設 1 = 大忠本館）
        output_date: 輸出日期 YYYYMMDD（預設今天）
        include_table: 是否包含右側租戶表格

    Returns:
        包含 PDF URL 的結果
    """
    try:
        # 取得位置資料
        result = await floor_plan_get_positions(branch_id)
        if not result.get("success"):
            return result

        floor_plan = result["floor_plan"]
        positions = result["positions"]
        statistics = result["statistics"]

        # 準備輸出日期
        if not output_date:
            output_date = datetime.now().strftime("%Y%m%d")

        # 準備 PDF Generator 請求資料
        pdf_data = {
            "template": "floor_plan",
            "floor_plan": floor_plan,
            "positions": positions,
            "statistics": statistics,
            "output_date": output_date,
            "include_table": include_table,
            "generated_at": datetime.now().isoformat()
        }

        # 呼叫 Cloud Run PDF 服務
        token = get_id_token_for_cloud_run(PDF_GENERATOR_URL)

        headers = {"Content-Type": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"

        logger.info(f"呼叫 Cloud Run PDF 服務生成平面圖: {floor_plan['name']}")

        async with httpx.AsyncClient(timeout=120.0) as client:
            response = await client.post(
                f"{PDF_GENERATOR_URL}/generate-floor-plan",
                json=pdf_data,
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
            logger.info(f"平面圖 PDF 生成成功: {result.get('pdf_path')}")
            return {
                "success": True,
                "message": f"{floor_plan['name']} 平面圖 PDF 生成成功",
                "floor_plan_name": floor_plan["name"],
                "output_date": output_date,
                "statistics": statistics,
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
        logger.error(f"生成平面圖 PDF 失敗: {e}")
        return {"success": False, "message": f"生成失敗: {e}"}


async def floor_plan_preview_html(
    branch_id: int = 1
) -> Dict[str, Any]:
    """
    預覽平面圖 HTML（不生成 PDF）

    Args:
        branch_id: 場館ID

    Returns:
        HTML 預覽內容
    """
    try:
        result = await floor_plan_get_positions(branch_id)
        if not result.get("success"):
            return result

        floor_plan = result["floor_plan"]
        positions = result["positions"]

        # 生成 HTML
        html = generate_floor_plan_html(floor_plan, positions)

        return {
            "success": True,
            "floor_plan_name": floor_plan["name"],
            "html": html,
            "statistics": result["statistics"]
        }

    except Exception as e:
        logger.error(f"預覽平面圖失敗: {e}")
        return {"success": False, "message": f"預覽失敗: {e}"}


def generate_floor_plan_html(floor_plan: Dict, positions: List[Dict]) -> str:
    """生成平面圖 HTML"""

    # 位置文字框
    position_boxes = ""
    for pos in positions:
        company = pos.get("company_name") or ""
        # 處理長公司名（截斷）
        if len(company) > 8:
            company = company[:7] + "…"

        bg_color = "#fff" if pos.get("contract_id") else "#f0f0f0"
        text_color = "#333" if pos.get("contract_id") else "#999"

        position_boxes += f"""
        <div class="position-box" style="
            left: {pos['x']}px;
            top: {pos['y']}px;
            width: {pos['width']}px;
            height: {pos['height']}px;
            background: {bg_color};
            color: {text_color};
        ">
            <span class="pos-num">{pos['position_number']}</span>
            <span class="company">{company}</span>
        </div>
        """

    # 租戶表格
    table_rows = ""
    for pos in positions:
        if pos.get("contract_id"):
            table_rows += f"""
            <tr>
                <td>{pos['position_number']}</td>
                <td>{pos.get('customer_name', '')}</td>
                <td>{pos.get('company_name', '')}</td>
            </tr>
            """

    image_url = floor_plan.get("image_url") or ""

    html = f"""
<!DOCTYPE html>
<html lang="zh-TW">
<head>
    <meta charset="UTF-8">
    <title>{floor_plan['name']} 平面圖</title>
    <style>
        @page {{
            size: 14.22in 10.67in;
            margin: 0.5cm;
        }}
        body {{
            margin: 0;
            font-family: 'Noto Sans TC', 'Microsoft JhengHei', sans-serif;
            font-size: 9px;
        }}
        .container {{
            display: flex;
            width: 1365px;
        }}
        .floor-plan {{
            position: relative;
            width: {floor_plan['width']}px;
            height: {floor_plan['height']}px;
            background-image: url('{image_url}');
            background-size: contain;
            background-repeat: no-repeat;
            background-color: #f9f9f9;
        }}
        .position-box {{
            position: absolute;
            border: 1px solid #333;
            font-size: 8px;
            padding: 1px 2px;
            text-align: center;
            overflow: hidden;
            line-height: 1.1;
            box-sizing: border-box;
        }}
        .pos-num {{
            color: #0066cc;
            font-weight: bold;
            margin-right: 2px;
        }}
        .company {{
            font-size: 7px;
        }}
        .tenant-table {{
            width: 480px;
            margin-left: 20px;
            font-size: 9px;
        }}
        .tenant-table h3 {{
            margin: 0 0 10px 0;
            color: #2c5530;
            border-bottom: 2px solid #2c5530;
            padding-bottom: 5px;
        }}
        .tenant-table table {{
            width: 100%;
            border-collapse: collapse;
        }}
        .tenant-table th, .tenant-table td {{
            padding: 3px 5px;
            border-bottom: 1px solid #eee;
            text-align: left;
        }}
        .tenant-table th {{
            background: #f5f5f5;
            font-weight: bold;
        }}
        .header {{
            text-align: center;
            margin-bottom: 10px;
        }}
        .header h1 {{
            margin: 0;
            color: #2c5530;
            font-size: 18px;
        }}
        .header .date {{
            color: #666;
            font-size: 10px;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>{floor_plan['name']} 租戶配置圖</h1>
        <div class="date">製表日期：{datetime.now().strftime('%Y年%m月%d日')}</div>
    </div>
    <div class="container">
        <div class="floor-plan">
            {position_boxes}
        </div>
        <div class="tenant-table">
            <h3>租戶名冊</h3>
            <table>
                <thead>
                    <tr>
                        <th>位置</th>
                        <th>聯絡人</th>
                        <th>公司名稱</th>
                    </tr>
                </thead>
                <tbody>
                    {table_rows}
                </tbody>
            </table>
        </div>
    </div>
</body>
</html>
"""
    return html
