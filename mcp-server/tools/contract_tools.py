"""
Hour Jungle CRM - Contract Tools
合約生成工具

使用 Cloud Run 服務生成 PDF，儲存到 GCS
"""

import logging
import os
from datetime import datetime
from typing import Dict, Any, Optional

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


async def postgrest_get(endpoint: str, params: dict = None) -> Any:
    """PostgREST GET 請求"""
    url = f"{POSTGREST_URL}/{endpoint}"
    async with httpx.AsyncClient() as client:
        response = await client.get(url, params=params, timeout=30.0)
        response.raise_for_status()
        return response.json()


def format_date(date_str: str, fmt: str = "%Y年%m月%d日") -> str:
    """格式化日期"""
    if not date_str:
        return ""
    try:
        if isinstance(date_str, str):
            # 處理 ISO 格式
            dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        else:
            dt = date_str
        return dt.strftime(fmt)
    except Exception:
        return str(date_str)


def format_currency(amount: float) -> str:
    """格式化金額"""
    if amount is None:
        return "0"
    return f"{int(amount):,}"


# ============================================================================
# 合約生成工具
# ============================================================================

def get_id_token_for_cloud_run(target_url: str) -> str:
    """取得 Cloud Run 的 ID Token"""
    try:
        # 在 GCP 環境（VM/Cloud Run）會自動使用服務帳號
        credentials, project = google.auth.default()
        auth_req = Request()

        # 取得 ID Token
        token = id_token.fetch_id_token(auth_req, target_url)
        return token
    except Exception as e:
        logger.warning(f"無法取得 ID Token: {e}，嘗試不帶認證呼叫")
        return None


async def contract_generate_pdf(
    contract_id: int,
    template: str = None
) -> Dict[str, Any]:
    """
    生成合約 PDF（呼叫 Cloud Run 服務）

    Args:
        contract_id: 合約ID
        template: 模板名稱（可選，會根據合約類型自動選擇）

    Returns:
        包含 GCS Signed URL 的結果
    """
    # 1. 取得合約資料
    try:
        contracts = await postgrest_get("contracts", {"id": f"eq.{contract_id}"})
        if not contracts:
            return {"success": False, "message": "找不到合約"}

        contract = contracts[0]

        # 取得客戶資料（作為 fallback）
        customer_id = contract.get("customer_id")
        customers = await postgrest_get("customers", {"id": f"eq.{customer_id}"})
        customer = customers[0] if customers else {}

        # 取得場館資料
        branch_id = contract.get("branch_id", 1)
        branches = await postgrest_get("branches", {"id": f"eq.{branch_id}"})
        branch = branches[0] if branches else {}

    except Exception as e:
        logger.error(f"取得合約資料失敗: {e}")
        return {"success": False, "message": f"取得合約資料失敗: {e}"}

    # 2. 準備 Cloud Run 請求資料
    contract_type = contract.get("contract_type", "virtual_office")

    # 自動選擇模板（如果未指定）
    if not template:
        template = get_template_for_contract_type(contract_type)

    # 分館法人資訊
    BRANCH_INFO = {
        1: {
            "company_name": "你的空間有限公司",
            "tax_id": "83772050",
            "representative": "戴豪廷",
            "address": "台中市西區大忠南街55號7F-5",
            "court": "台南地方法院"
        },
        2: {
            "company_name": "樞紐前沿股份有限公司",
            "tax_id": "60710368",
            "representative": "戴豪廷",
            "address": "臺中市西區台灣大道二段181號4樓之1",
            "court": "台中地方法院"
        }
    }
    branch_info = BRANCH_INFO.get(branch_id, BRANCH_INFO[1])

    # 計算合約月數
    periods = 12
    try:
        if contract.get("start_date") and contract.get("end_date"):
            from datetime import datetime
            start = datetime.fromisoformat(str(contract["start_date"]))
            end = datetime.fromisoformat(str(contract["end_date"]))
            periods = (end.year - start.year) * 12 + (end.month - start.month) + 1
            if periods < 1:
                periods = 12
    except Exception:
        pass

    contract_data = {
        "contract_id": contract_id,
        "contract_number": contract.get("contract_number") or f"HJ-{contract_id}",
        "contract_type": contract_type,
        "contract_type_name": get_contract_type_name(contract_type),
        "start_date": contract.get("start_date") or "",
        "end_date": contract.get("end_date") or "",
        "monthly_rent": float(contract.get("monthly_rent") or 0),
        "deposit": float(contract.get("deposit") or 0),
        "original_price": float(contract.get("original_price") or contract.get("monthly_rent") or 0),
        "payment_day": contract.get("payment_day") or 8,
        "periods": periods,
        "room_number": contract.get("room_number") or "",
        "notes": contract.get("notes") or "",

        # 甲方（出租人）資訊 - 從分館帶入
        "branch_id": branch_id,
        "branch_name": branch.get("name") or "Hour Jungle",
        "branch_company_name": branch_info["company_name"],
        "branch_tax_id": branch_info["tax_id"],
        "branch_representative": branch_info["representative"],
        "branch_address": branch_info["address"],
        "branch_court": branch_info["court"],

        # 乙方（承租人）資訊 - 優先使用合約表欄位，fallback 到客戶表
        "company_name": contract.get("company_name") or customer.get("company_name") or "",
        "representative_name": contract.get("representative_name") or customer.get("name") or "",
        "representative_address": contract.get("representative_address") or customer.get("address") or "",
        "id_number": contract.get("id_number") or customer.get("id_number") or "",
        "company_tax_id": contract.get("company_tax_id") or customer.get("company_tax_id") or "",
        "phone": contract.get("phone") or customer.get("phone") or "",
        "email": contract.get("email") or customer.get("email") or "",

        # 電子用印
        "show_stamp": True
    }

    # 3. 呼叫 Cloud Run 服務
    try:
        # 取得認證 Token
        token = get_id_token_for_cloud_run(PDF_GENERATOR_URL)

        headers = {"Content-Type": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"

        request_data = {
            "contract_data": contract_data,
            "template": template
        }

        logger.info(f"呼叫 Cloud Run PDF 服務: {PDF_GENERATOR_URL}/generate")

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
            logger.info(f"合約 PDF 生成成功: {result.get('pdf_path')}")
            return {
                "success": True,
                "message": result.get("message", "合約 PDF 生成成功"),
                "contract_number": contract_data["contract_number"],
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
        logger.error(f"呼叫 Cloud Run 失敗: {e}")
        return {
            "success": False,
            "message": f"PDF 生成失敗: {e}"
        }


def get_contract_type_name(contract_type: str) -> str:
    """取得合約類型名稱"""
    type_names = {
        "virtual_office": "營業登記",
        "office": "辦公室租賃",
        "flex_seat": "自由座",
        "coworking_fixed": "固定座位",
        "coworking_flexible": "彈性座位",
        "meeting_room": "會議室租用",
        "mailbox": "郵件代收"
    }
    return type_names.get(contract_type, contract_type)


def get_template_for_contract_type(contract_type: str) -> str:
    """根據合約類型取得對應模板"""
    templates = {
        "virtual_office": "contract_virtual_office",
        "office": "contract_office",
        "flex_seat": "contract_flex_seat",
        "coworking_fixed": "contract_coworking",
        "coworking_flexible": "contract_coworking"
    }
    return templates.get(contract_type, "contract_virtual_office")


def generate_default_contract_html(data: Dict[str, Any]) -> str:
    """生成預設合約 HTML"""
    return f"""
<!DOCTYPE html>
<html lang="zh-TW">
<head>
    <meta charset="UTF-8">
    <title>服務合約 - {data['contract_number']}</title>
    <style>
        @page {{
            size: A4;
            margin: 2cm;
        }}
        body {{
            font-family: "Microsoft JhengHei", "微軟正黑體", sans-serif;
            font-size: 12pt;
            line-height: 1.8;
            color: #333;
        }}
        .header {{
            text-align: center;
            margin-bottom: 30px;
            border-bottom: 2px solid #2c5530;
            padding-bottom: 20px;
        }}
        .header h1 {{
            color: #2c5530;
            font-size: 24pt;
            margin: 0;
        }}
        .header .subtitle {{
            color: #666;
            font-size: 14pt;
            margin-top: 10px;
        }}
        .contract-number {{
            text-align: right;
            color: #666;
            font-size: 10pt;
            margin-bottom: 20px;
        }}
        .section {{
            margin-bottom: 25px;
        }}
        .section-title {{
            font-weight: bold;
            color: #2c5530;
            font-size: 14pt;
            margin-bottom: 10px;
            border-left: 4px solid #2c5530;
            padding-left: 10px;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin: 15px 0;
        }}
        table th, table td {{
            border: 1px solid #ddd;
            padding: 10px;
            text-align: left;
        }}
        table th {{
            background: #f5f5f5;
            width: 30%;
        }}
        .signature-area {{
            margin-top: 50px;
            display: flex;
            justify-content: space-between;
        }}
        .signature-box {{
            width: 45%;
        }}
        .signature-box h3 {{
            color: #2c5530;
            border-bottom: 1px solid #2c5530;
            padding-bottom: 5px;
        }}
        .signature-line {{
            border-bottom: 1px solid #333;
            height: 40px;
            margin: 20px 0;
        }}
        .footer {{
            margin-top: 30px;
            text-align: center;
            font-size: 10pt;
            color: #666;
        }}
        .terms {{
            font-size: 10pt;
            color: #666;
        }}
        .terms ol {{
            padding-left: 20px;
        }}
        .terms li {{
            margin-bottom: 8px;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>Hour Jungle</h1>
        <div class="subtitle">{data['contract_type_name']}服務合約</div>
    </div>

    <div class="contract-number">
        合約編號：{data['contract_number']}<br>
        製表日期：{data['today']}
    </div>

    <div class="section">
        <div class="section-title">甲方（服務提供者）</div>
        <table>
            <tr>
                <th>公司名稱</th>
                <td>{data['branch_name']}</td>
            </tr>
            <tr>
                <th>地址</th>
                <td>{data['branch_address']}</td>
            </tr>
            <tr>
                <th>聯絡電話</th>
                <td>{data['branch_phone']}</td>
            </tr>
        </table>
    </div>

    <div class="section">
        <div class="section-title">乙方（承租人）</div>
        <table>
            <tr>
                <th>姓名</th>
                <td>{data['customer_name']}</td>
            </tr>
            <tr>
                <th>公司名稱</th>
                <td>{data['company_name']}</td>
            </tr>
            <tr>
                <th>公司地址</th>
                <td>{data['company_address']}</td>
            </tr>
            <tr>
                <th>統一編號</th>
                <td>{data['tax_id']}</td>
            </tr>
            <tr>
                <th>聯絡電話</th>
                <td>{data['contact_phone']}</td>
            </tr>
            <tr>
                <th>電子郵件</th>
                <td>{data['contact_email']}</td>
            </tr>
        </table>
    </div>

    <div class="section">
        <div class="section-title">合約內容</div>
        <table>
            <tr>
                <th>服務類型</th>
                <td>{data['contract_type_name']}</td>
            </tr>
            <tr>
                <th>合約期間</th>
                <td>{data['start_date']} 至 {data['end_date']}</td>
            </tr>
            <tr>
                <th>月租金額</th>
                <td>NT$ {data['monthly_fee']} 元整</td>
            </tr>
            <tr>
                <th>押金</th>
                <td>NT$ {data['deposit']} 元整</td>
            </tr>
            <tr>
                <th>備註</th>
                <td>{data['notes']}</td>
            </tr>
        </table>
    </div>

    <div class="section terms">
        <div class="section-title">服務條款</div>
        <ol>
            <li>乙方應於每月5日前繳納當月租金，逾期未繳納者，甲方得收取滯納金。</li>
            <li>乙方如需終止合約，應於一個月前書面通知甲方。</li>
            <li>乙方應遵守甲方之相關管理規則，維護公共區域整潔。</li>
            <li>未經甲方書面同意，乙方不得將本服務轉讓或分租他人。</li>
            <li>合約期滿如欲續約，應於到期前一個月提出申請。</li>
            <li>本合約未盡事宜，依中華民國相關法令規定辦理。</li>
        </ol>
    </div>

    <div class="signature-area">
        <div class="signature-box">
            <h3>甲方</h3>
            <p>公司章戳：</p>
            <div class="signature-line"></div>
            <p>負責人簽章：</p>
            <div class="signature-line"></div>
        </div>
        <div class="signature-box">
            <h3>乙方</h3>
            <p>公司章戳：</p>
            <div class="signature-line"></div>
            <p>負責人簽章：</p>
            <div class="signature-line"></div>
        </div>
    </div>

    <div class="footer">
        <p>中 華 民 國 ＿＿＿ 年 ＿＿＿ 月 ＿＿＿ 日</p>
        <p>本合約一式兩份，甲乙雙方各執一份為憑。</p>
    </div>
</body>
</html>
"""


async def contract_preview(
    contract_id: int
) -> Dict[str, Any]:
    """
    預覽合約內容（不生成 PDF）

    Args:
        contract_id: 合約ID

    Returns:
        合約內容摘要
    """
    try:
        contracts = await postgrest_get("v_contracts", {"id": f"eq.{contract_id}"})
        if not contracts:
            return {"found": False, "message": "找不到合約"}

        contract = contracts[0]

        # 取得客戶資料
        customer_id = contract.get("customer_id")
        customers = await postgrest_get("customers", {"id": f"eq.{customer_id}"})
        customer = customers[0] if customers else {}

        return {
            "found": True,
            "contract": {
                "id": contract.get("id"),
                "contract_number": contract.get("contract_number"),
                "contract_type": contract.get("contract_type"),
                "contract_type_name": get_contract_type_name(contract.get("contract_type", "")),
                "start_date": contract.get("start_date"),
                "end_date": contract.get("end_date"),
                "monthly_fee": contract.get("monthly_fee"),
                "deposit": contract.get("deposit"),
                "status": contract.get("status")
            },
            "customer": {
                "name": customer.get("name"),
                "company_name": customer.get("company_name"),
                "phone": customer.get("phone"),
                "email": customer.get("email")
            }
        }

    except Exception as e:
        logger.error(f"預覽合約失敗: {e}")
        raise Exception(f"預覽合約失敗: {e}")
