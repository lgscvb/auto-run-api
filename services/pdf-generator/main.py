"""
Hour Jungle CRM - PDF Generator Service
Cloud Run 微服務：HTML → PDF → GCS
"""

import os
import logging
from datetime import datetime, timedelta
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from weasyprint import HTML
from google.cloud import storage
from google.auth import default
from google.auth.transport import requests
from jinja2 import Environment, FileSystemLoader

# 設定 logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="PDF Generator", version="1.0.0")

# CORS 設定（Cloud Run 內部通訊）
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# GCS 設定
GCS_BUCKET = os.getenv("GCS_BUCKET", "hourjungle-contracts")
SIGNED_URL_EXPIRATION = 3600  # 1小時

# 取得服務帳戶認證（用於 IAM signing）
def get_service_account_email():
    """取得當前服務帳戶的 email"""
    credentials, project = default()

    # 先嘗試從 credentials 取得
    if hasattr(credentials, 'service_account_email') and credentials.service_account_email:
        email = credentials.service_account_email
        if email and email != 'default' and '@' in email:
            logger.info(f"Got service account from credentials: {email}")
            return email

    # Cloud Run 環境下從 metadata server 取得
    try:
        import urllib.request
        req = urllib.request.Request(
            'http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/email',
            headers={'Metadata-Flavor': 'Google'}
        )
        email = urllib.request.urlopen(req, timeout=5).read().decode().strip()
        if email and '@' in email:
            logger.info(f"Got service account from metadata: {email}")
            return email
    except Exception as e:
        logger.warning(f"Failed to get service account from metadata: {e}")

    # 嘗試從專案號碼推導 (Cloud Run 預設使用 compute engine default service account)
    project_number = os.getenv("GOOGLE_CLOUD_PROJECT_NUMBER", "743652001579")
    fallback_email = f"{project_number}-compute@developer.gserviceaccount.com"
    logger.info(f"Using fallback service account: {fallback_email}")
    return fallback_email

def generate_signed_url_with_iam(blob, expiration_seconds: int = SIGNED_URL_EXPIRATION):
    """
    使用 IAM signing 生成 signed URL
    這可以在沒有私鑰的情況下運作（如 Cloud Run 預設認證）
    """
    from google.auth import compute_engine
    from datetime import timedelta

    # 取得當前認證
    credentials, project = default()

    # 如果是 Compute Engine 認證，需要使用 IAM signing
    if isinstance(credentials, compute_engine.Credentials):
        # 取得 signing credentials
        service_account_email = get_service_account_email()
        if not service_account_email:
            raise Exception("無法取得服務帳戶 email")

        # 重要：refresh credentials 以取得 access token
        auth_request = requests.Request()
        credentials.refresh(auth_request)

        # 使用 service_account_email 和 access_token 參數進行 IAM signing
        return blob.generate_signed_url(
            version="v4",
            expiration=timedelta(seconds=expiration_seconds),
            method="GET",
            service_account_email=service_account_email,
            access_token=credentials.token
        )
    else:
        # 如果有私鑰的認證（如本機開發），直接使用
        return blob.generate_signed_url(
            version="v4",
            expiration=timedelta(seconds=expiration_seconds),
            method="GET"
        )

# Jinja2 模板
TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "templates")
jinja_env = Environment(loader=FileSystemLoader(TEMPLATE_DIR))


class ContractData(BaseModel):
    """合約資料"""
    contract_id: int
    contract_number: str
    contract_type: str
    contract_type_name: str
    start_date: str
    end_date: str
    monthly_fee: float
    deposit: float = 0
    notes: str = ""

    # 客戶資訊
    customer_name: str
    company_name: str = ""
    company_address: str = ""
    tax_id: str = ""
    id_number: str = ""
    contact_phone: str = ""
    contact_email: str = ""

    # 場館資訊
    branch_name: str
    branch_address: str = ""
    branch_phone: str = ""

    # 合約細節
    list_price: Optional[float] = None  # 定價
    payment_day: int = 5  # 繳款日
    periods: int = 12  # 期數


class QuoteItem(BaseModel):
    """報價單項目"""
    name: str
    quantity: int = 1
    unit_price: float = 0
    amount: float


class QuoteData(BaseModel):
    """報價單資料"""
    quote_id: int
    quote_number: str
    quote_date: str
    valid_until: str

    # 場館資訊
    branch_name: str

    # 服務項目
    section_title: str = ""  # 例如：營業登記一年合約（依合約內指定付款時間點）
    items: list[QuoteItem]
    deposit_amount: float = 0
    total_amount: float

    # 銀行資訊
    bank_account_name: str = "你的空間有限公司"
    bank_name: str = "永豐商業銀行(南台中分行)"
    bank_code: str = "807"
    bank_account_number: str = "03801800183399"

    # 聯絡資訊
    contact_email: str = "wtxg@hourjungle.com"
    contact_phone: str = "04-23760282"


class LegalLetterData(BaseModel):
    """存證信函資料"""
    letter_id: int
    letter_number: str
    recipient_name: str
    recipient_address: str
    content: str
    overdue_amount: float
    overdue_days: int
    contract_number: str = ""
    branch_name: str = ""
    created_at: str = ""
    sender_name: str = "你的空間有限公司"
    sender_address: str = "台中市西區大忠南街 118 號 8 樓"


class FloorPlanInfo(BaseModel):
    """平面圖資訊"""
    id: int
    name: str
    image_url: Optional[str] = None
    width: int = 853
    height: int = 959


class FloorPositionInfo(BaseModel):
    """位置資訊"""
    position_number: int
    x: int
    y: int
    width: int = 68
    height: int = 21
    contract_id: Optional[int] = None
    customer_name: Optional[str] = None
    company_name: Optional[str] = None


class FloorPlanStatistics(BaseModel):
    """統計資訊"""
    total_positions: int
    occupied: int
    vacant: int
    occupancy_rate: str


class FloorPlanRequest(BaseModel):
    """平面圖 PDF 生成請求"""
    template: str = "floor_plan"
    floor_plan: FloorPlanInfo
    positions: list[dict]  # 使用 dict 而非 FloorPositionInfo 以保持彈性
    statistics: FloorPlanStatistics
    output_date: str
    include_table: bool = True
    generated_at: Optional[str] = None


class GenerateRequest(BaseModel):
    """PDF 生成請求"""
    contract_data: Optional[ContractData] = None
    quote_data: Optional[QuoteData] = None
    template: str = "contract_coworking"


class LegalLetterRequest(BaseModel):
    """存證信函 PDF 生成請求"""
    legal_letter_data: LegalLetterData
    template: str = "legal_letter"


class GenerateResponse(BaseModel):
    """PDF 生成回應"""
    success: bool
    message: str
    pdf_url: Optional[str] = None
    pdf_path: Optional[str] = None
    expires_at: Optional[str] = None


def format_currency(amount: float) -> str:
    """格式化金額"""
    if amount is None:
        return "0"
    return f"{int(amount):,}"


def format_date_chinese(date_str: str) -> str:
    """格式化日期為中文格式"""
    if not date_str:
        return ""
    try:
        dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        return dt.strftime("%Y年%m月%d日")
    except Exception:
        return date_str


def get_roc_year(date_str: str) -> str:
    """取得民國年"""
    if not date_str:
        return ""
    try:
        dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        roc_year = dt.year - 1911
        return str(roc_year)
    except Exception:
        return ""


@app.get("/health")
async def health_check():
    """健康檢查"""
    return {"status": "healthy", "service": "pdf-generator"}


@app.post("/generate", response_model=GenerateResponse)
async def generate_pdf(request: GenerateRequest):
    """
    生成合約或報價單 PDF 並上傳到 GCS

    1. 使用 Jinja2 渲染 HTML 模板
    2. 使用 WeasyPrint 轉換為 PDF
    3. 上傳到 GCS
    4. 生成 Signed URL
    """
    try:
        template_name = f"{request.template}.html"

        # 根據類型準備資料
        if request.quote_data:
            # 報價單
            data = request.quote_data
            template_data = {
                **data.model_dump(),
                "items": [item.model_dump() for item in data.items],
            }
            doc_type = "quote"
            doc_id = data.quote_id
            doc_number = data.quote_number
            gcs_folder = "quotes"
            template_name = "quote.html"
            success_msg = "報價單 PDF 生成成功"

        elif request.contract_data:
            # 合約
            data = request.contract_data
            template_data = {
                **data.model_dump(),
                "monthly_fee_formatted": format_currency(data.monthly_fee),
                "deposit_formatted": format_currency(data.deposit),
                "list_price_formatted": format_currency(data.list_price) if data.list_price else format_currency(data.monthly_fee),
                "start_date_formatted": format_date_chinese(data.start_date),
                "end_date_formatted": format_date_chinese(data.end_date),
                "today": datetime.now().strftime("%Y年%m月%d日"),
                "today_roc_year": str(datetime.now().year - 1911),
                "today_month": str(datetime.now().month),
                "today_day": str(datetime.now().day),
            }
            doc_type = "contract"
            doc_id = data.contract_id
            doc_number = data.contract_number
            gcs_folder = "contracts"
            success_msg = "合約 PDF 生成成功"

        else:
            raise HTTPException(status_code=400, detail="需要提供 contract_data 或 quote_data")

        template_data["generated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # 2. 渲染 HTML
        try:
            template = jinja_env.get_template(template_name)
        except Exception:
            # 如果指定模板不存在，使用預設模板
            logger.warning(f"Template {template_name} not found, using default")
            template = jinja_env.get_template("contract_coworking.html")

        html_content = template.render(**template_data)

        # 3. 生成 PDF
        logger.info(f"Generating PDF for {doc_type} {doc_number}")
        pdf_bytes = HTML(string=html_content).write_pdf()

        # 4. 上傳到 GCS
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        blob_path = f"{gcs_folder}/{doc_id}/{timestamp}.pdf"

        storage_client = storage.Client()
        bucket = storage_client.bucket(GCS_BUCKET)
        blob = bucket.blob(blob_path)

        blob.upload_from_string(pdf_bytes, content_type="application/pdf")
        logger.info(f"Uploaded PDF to gs://{GCS_BUCKET}/{blob_path}")

        # 5. 生成 Signed URL (使用 IAM signing)
        signed_url = generate_signed_url_with_iam(blob)
        expires_at = (datetime.now() + timedelta(seconds=SIGNED_URL_EXPIRATION)).isoformat()

        return GenerateResponse(
            success=True,
            message=success_msg,
            pdf_url=signed_url,
            pdf_path=f"gs://{GCS_BUCKET}/{blob_path}",
            expires_at=expires_at
        )

    except Exception as e:
        logger.error(f"PDF generation failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/generate-legal-letter", response_model=GenerateResponse)
async def generate_legal_letter(request: LegalLetterRequest):
    """
    生成存證信函 PDF 並上傳到 GCS

    1. 使用 Jinja2 渲染 HTML 模板
    2. 使用 WeasyPrint 轉換為 PDF
    3. 上傳到 GCS
    4. 生成 Signed URL
    """
    try:
        data = request.legal_letter_data

        # 準備模板資料
        template_data = {
            **data.model_dump(),
            "overdue_amount_formatted": format_currency(data.overdue_amount),
            "today": datetime.now().strftime("%Y年%m月%d日"),
            "today_roc_year": str(datetime.now().year - 1911),
            "today_month": str(datetime.now().month),
            "today_day": str(datetime.now().day),
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

        # 渲染 HTML
        try:
            template = jinja_env.get_template("legal_letter.html")
        except Exception as e:
            logger.error(f"Template not found: {e}")
            raise HTTPException(status_code=500, detail="存證信函模板不存在")

        html_content = template.render(**template_data)

        # 生成 PDF
        logger.info(f"Generating legal letter PDF for {data.letter_number}")
        pdf_bytes = HTML(string=html_content).write_pdf()

        # 上傳到 GCS
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        blob_path = f"legal_letters/{data.letter_id}/{timestamp}.pdf"

        storage_client = storage.Client()
        bucket = storage_client.bucket(GCS_BUCKET)
        blob = bucket.blob(blob_path)

        blob.upload_from_string(pdf_bytes, content_type="application/pdf")
        logger.info(f"Uploaded legal letter PDF to gs://{GCS_BUCKET}/{blob_path}")

        # 生成 Signed URL (使用 IAM signing)
        signed_url = generate_signed_url_with_iam(blob)
        expires_at = (datetime.now() + timedelta(seconds=SIGNED_URL_EXPIRATION)).isoformat()

        return GenerateResponse(
            success=True,
            message="存證信函 PDF 生成成功",
            pdf_url=signed_url,
            pdf_path=f"gs://{GCS_BUCKET}/{blob_path}",
            expires_at=expires_at
        )

    except Exception as e:
        logger.error(f"Legal letter PDF generation failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/generate-floor-plan", response_model=GenerateResponse)
async def generate_floor_plan(request: FloorPlanRequest):
    """
    生成平面圖 PDF 並上傳到 GCS

    1. 使用 Jinja2 渲染 HTML 模板
    2. 使用 WeasyPrint 轉換為 PDF
    3. 上傳到 GCS
    4. 生成 Signed URL
    """
    try:
        # 準備模板資料
        template_data = {
            "floor_plan": request.floor_plan.model_dump(),
            "positions": request.positions,
            "statistics": request.statistics.model_dump(),
            "output_date": request.output_date,
            "include_table": request.include_table,
            "generated_at": request.generated_at or datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

        # 渲染 HTML
        try:
            template = jinja_env.get_template("floor_plan.html")
        except Exception as e:
            logger.error(f"Template not found: {e}")
            raise HTTPException(status_code=500, detail="平面圖模板不存在")

        html_content = template.render(**template_data)

        # 生成 PDF
        logger.info(f"Generating floor plan PDF for {request.floor_plan.name}")
        pdf_bytes = HTML(string=html_content).write_pdf()

        # 上傳到 GCS
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        blob_path = f"floor_plans/{request.floor_plan.id}/{request.output_date}_{timestamp}.pdf"

        storage_client = storage.Client()
        bucket = storage_client.bucket(GCS_BUCKET)
        blob = bucket.blob(blob_path)

        blob.upload_from_string(pdf_bytes, content_type="application/pdf")
        logger.info(f"Uploaded floor plan PDF to gs://{GCS_BUCKET}/{blob_path}")

        # 生成 Signed URL (使用 IAM signing)
        signed_url = generate_signed_url_with_iam(blob)
        expires_at = (datetime.now() + timedelta(seconds=SIGNED_URL_EXPIRATION)).isoformat()

        return GenerateResponse(
            success=True,
            message=f"{request.floor_plan.name} 平面圖 PDF 生成成功",
            pdf_url=signed_url,
            pdf_path=f"gs://{GCS_BUCKET}/{blob_path}",
            expires_at=expires_at
        )

    except Exception as e:
        logger.error(f"Floor plan PDF generation failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/regenerate-url")
async def regenerate_signed_url(pdf_path: str):
    """
    重新生成 Signed URL（當連結過期時使用）
    """
    try:
        # 解析 GCS 路徑
        if pdf_path.startswith("gs://"):
            path = pdf_path.replace(f"gs://{GCS_BUCKET}/", "")
        else:
            path = pdf_path

        storage_client = storage.Client()
        bucket = storage_client.bucket(GCS_BUCKET)
        blob = bucket.blob(path)

        if not blob.exists():
            raise HTTPException(status_code=404, detail="PDF not found")

        # 使用 IAM signing
        signed_url = generate_signed_url_with_iam(blob)

        return {
            "success": True,
            "pdf_url": signed_url,
            "expires_at": (datetime.now() + timedelta(seconds=SIGNED_URL_EXPIRATION)).isoformat()
        }

    except Exception as e:
        logger.error(f"Failed to regenerate URL: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
