"""
Hour Jungle CRM - Legal Letter Tools
存證信函自動生成相關工具
"""

import logging
import os
import json
from datetime import datetime, date
from typing import Dict, Any, Optional, List

import httpx
import google.auth
from google.auth.transport.requests import Request
from google.oauth2 import id_token

logger = logging.getLogger(__name__)

# 環境變數
POSTGREST_URL = os.getenv("POSTGREST_URL", "http://postgrest:3000")
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")
LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "")
PDF_GENERATOR_URL = os.getenv(
    "PDF_GENERATOR_URL",
    "https://pdf-generator-743652001579.asia-east1.run.app"
)

# LINE API
LINE_API_URL = "https://api.line.me/v2/bot/message/push"

# OpenRouter API
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"


# ============================================================================
# 基礎 HTTP 請求
# ============================================================================

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


def get_id_token_for_cloud_run(target_url: str) -> str:
    """取得 Cloud Run 的 ID Token"""
    try:
        credentials, project = google.auth.default()
        auth_req = Request()
        token = id_token.fetch_id_token(auth_req, target_url)
        return token
    except Exception as e:
        logger.warning(f"無法取得 ID Token: {e}")
        return None


# ============================================================================
# 存證信函工具
# ============================================================================

async def legal_record_reminder(
    payment_id: int,
    notes: str = None
) -> Dict[str, Any]:
    """
    記錄催繳，更新 payments 表的 reminder_count

    Args:
        payment_id: 付款ID
        notes: 催繳備註

    Returns:
        更新結果
    """
    try:
        # 取得目前的 reminder_count
        payments = await postgrest_get("payments", {
            "id": f"eq.{payment_id}",
            "select": "id,customer_id,amount,due_date,reminder_count,payment_status"
        })

        if not payments:
            return {"success": False, "message": "找不到付款記錄"}

        payment = payments[0]

        # 檢查是否已付款
        if payment.get("payment_status") == "paid":
            return {"success": False, "message": "此款項已付款，無需催繳"}

        # 更新 reminder_count
        current_count = payment.get("reminder_count") or 0
        new_count = current_count + 1

        update_data = {
            "reminder_count": new_count,
            "last_reminder_at": datetime.now().isoformat()
        }

        if notes:
            # 追加到 metadata
            metadata = payment.get("metadata") or {}
            if not isinstance(metadata, dict):
                metadata = {}
            reminders = metadata.get("reminders", [])
            reminders.append({
                "time": datetime.now().isoformat(),
                "notes": notes
            })
            metadata["reminders"] = reminders
            update_data["metadata"] = json.dumps(metadata)

        result = await postgrest_patch(
            "payments",
            {"id": f"eq.{payment_id}"},
            update_data
        )

        return {
            "success": True,
            "message": f"催繳記錄已更新，目前催繳次數: {new_count}",
            "payment_id": payment_id,
            "reminder_count": new_count,
            "last_reminder_at": update_data["last_reminder_at"]
        }

    except Exception as e:
        logger.error(f"legal_record_reminder error: {e}")
        raise Exception(f"記錄催繳失敗: {e}")


async def legal_list_candidates(
    branch_id: int = None,
    limit: int = 50
) -> Dict[str, Any]:
    """
    列出存證信函候選客戶（逾期>14天且催繳>=5次）

    Args:
        branch_id: 場館ID（可選）
        limit: 回傳筆數

    Returns:
        候選客戶列表
    """
    try:
        params = {
            "limit": limit,
            "order": "days_overdue.desc,overdue_amount.desc"
        }

        if branch_id:
            params["branch_id"] = f"eq.{branch_id}"

        candidates = await postgrest_get("v_legal_letter_candidates", params)

        # 統計
        stats = {
            "total": len(candidates),
            "critical": sum(1 for c in candidates if c.get("urgency_level") == "critical"),
            "high": sum(1 for c in candidates if c.get("urgency_level") == "high"),
            "medium": sum(1 for c in candidates if c.get("urgency_level") == "medium"),
            "total_amount": sum(c.get("overdue_amount", 0) for c in candidates)
        }

        return {
            "count": len(candidates),
            "stats": stats,
            "candidates": candidates
        }

    except Exception as e:
        logger.error(f"legal_list_candidates error: {e}")
        raise Exception(f"取得候選客戶列表失敗: {e}")


async def legal_generate_content(
    payment_id: int = None,
    contract_id: int = None,
    customer_name: str = None,
    company_name: str = None,
    address: str = None,
    overdue_amount: float = 0,
    overdue_days: int = 0,
    contract_number: str = None,
    reminder_count: int = 0,
    branch_name: str = None,
    service_items: str = None,
    monthly_rent: float = 0
) -> Dict[str, Any]:
    """
    使用 LLM 生成存證信函內容

    支援兩種模式：
    1. 從逾期付款生成（提供 payment_id）
    2. 從合約手動生成（提供 contract_id）

    Args:
        payment_id: 付款ID（模式1）
        contract_id: 合約ID（模式2，手動建立時使用）
        customer_name: 客戶姓名
        company_name: 公司名稱（可選）
        address: 地址
        overdue_amount: 逾期金額（模式1）
        overdue_days: 逾期天數（模式1）
        contract_number: 合約編號
        reminder_count: 催繳次數（模式1）
        branch_name: 場館名稱
        service_items: 服務項目（模式2）
        monthly_rent: 月租金（模式2）

    Returns:
        生成的存證信函內容
    """
    if not OPENROUTER_API_KEY:
        return {
            "success": False,
            "message": "OpenRouter API 未設定"
        }

    # 組合收件人名稱
    recipient = company_name if company_name else customer_name

    # 根據模式生成不同的 prompt
    if payment_id and overdue_amount > 0:
        # 模式 1：逾期付款催繳
        prompt = f"""你是一位專業的台灣法律文書撰寫專家。請根據以下資訊撰寫一封正式的存證信函：

收件人資訊：
- 姓名/公司：{recipient}
- 地址：{address or '（待補）'}

欠款資訊：
- 合約編號：{contract_number or '（未提供）'}
- 逾期金額：新台幣 {overdue_amount:,.0f} 元整
- 逾期天數：{overdue_days} 天
- 催繳次數：{reminder_count} 次

寄件人：你的空間有限公司（Hour Jungle {branch_name or ''}）

請撰寫存證信函，包含：
1. 正式開頭稱謂
2. 說明債務事實（租金逾期未繳）
3. 說明已多次催繳未果
4. 要求限期（7日內）清償全部欠款
5. 說明逾期未繳將採取法律行動（包括但不限於終止合約、請求損害賠償、聲請支付命令等）
6. 正式結尾

請使用正式法律文書用語，但保持清晰易懂。不需要加入日期和郵局格式資訊，只需要信函主體內容。"""
    else:
        # 模式 2：手動從合約建立（非逾期情況，可能是違約或其他原因）
        prompt = f"""你是一位專業的台灣法律文書撰寫專家。請根據以下資訊撰寫一封正式的存證信函：

收件人資訊：
- 姓名/公司：{recipient}
- 地址：{address or '（待補）'}

合約資訊：
- 合約編號：{contract_number or '（未提供）'}
- 服務項目：{service_items or '營業登記服務'}
- 月租金：新台幣 {monthly_rent:,.0f} 元整

寄件人：你的空間有限公司（Hour Jungle {branch_name or ''}）

請撰寫存證信函，包含：
1. 正式開頭稱謂
2. 說明租賃合約事實
3. 說明違約情事（請保留空白讓使用者填寫具體違約事項）
4. 要求限期改善或清償
5. 說明未依限改善將採取法律行動（包括但不限於終止合約、請求損害賠償等）
6. 正式結尾

請使用正式法律文書用語，但保持清晰易懂。不需要加入日期和郵局格式資訊，只需要信函主體內容。
在違約事項的部分，請用「【請填寫具體違約事項】」作為佔位符，讓使用者可以自行編輯。"""

    try:
        headers = {
            "Authorization": f"Bearer {OPENROUTER_API_KEY}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://hourjungle.com",
            "X-Title": "Hour Jungle CRM"
        }

        payload = {
            "model": "anthropic/claude-3.5-sonnet",
            "messages": [
                {"role": "user", "content": prompt}
            ],
            "temperature": 0.3,
            "max_tokens": 2000
        }

        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(
                OPENROUTER_URL,
                json=payload,
                headers=headers
            )
            response.raise_for_status()
            result = response.json()

        content = result.get("choices", [{}])[0].get("message", {}).get("content", "")

        if not content:
            return {
                "success": False,
                "message": "LLM 未返回內容"
            }

        return {
            "success": True,
            "message": "存證信函內容生成成功",
            "content": content,
            "recipient": recipient,
            "address": address,
            "overdue_amount": overdue_amount
        }

    except Exception as e:
        logger.error(f"legal_generate_content error: {e}")
        return {
            "success": False,
            "message": f"生成存證信函內容失敗: {e}"
        }


async def legal_create_letter(
    content: str,
    payment_id: int = None,
    contract_id: int = None,
    recipient_name: str = None,
    recipient_address: str = None
) -> Dict[str, Any]:
    """
    建立存證信函記錄

    支援兩種模式：
    1. 從逾期付款建立（提供 payment_id）
    2. 從合約手動建立（提供 contract_id）

    Args:
        content: 存證信函內容
        payment_id: 付款ID（模式1）
        contract_id: 合約ID（模式2）
        recipient_name: 收件人姓名（可選）
        recipient_address: 收件人地址（可選）

    Returns:
        新建的存證信函記錄
    """
    try:
        # 生成編號
        today_str = date.today().strftime("%Y%m%d")
        letters = await postgrest_get("legal_letters", {
            "letter_number": f"like.LL{today_str}-%",
            "order": "letter_number.desc",
            "limit": 1
        })

        if letters:
            last_num = int(letters[0]["letter_number"].split("-")[-1])
            seq_num = last_num + 1
        else:
            seq_num = 1

        letter_number = f"LL{today_str}-{str(seq_num).zfill(3)}"

        if payment_id:
            # 模式 1：從逾期付款建立
            candidates = await postgrest_get("v_legal_letter_candidates", {
                "payment_id": f"eq.{payment_id}"
            })

            if not candidates:
                return {"success": False, "message": "找不到符合條件的付款記錄"}

            candidate = candidates[0]

            data = {
                "payment_id": payment_id,
                "customer_id": candidate["customer_id"],
                "contract_id": candidate["contract_id"],
                "branch_id": candidate["branch_id"],
                "letter_number": letter_number,
                "recipient_name": recipient_name or candidate.get("company_name") or candidate.get("customer_name"),
                "recipient_address": recipient_address or candidate.get("legal_address") or "",
                "content": content,
                "overdue_amount": candidate.get("overdue_amount", 0),
                "overdue_days": candidate.get("days_overdue", 0),
                "reminder_count": candidate.get("reminder_count", 0),
                "status": "draft"
            }
        elif contract_id:
            # 模式 2：從合約手動建立
            contracts = await postgrest_get("contracts", {
                "id": f"eq.{contract_id}",
                "select": "*,customers(id,name,company_name),branches(id,name)"
            })

            if not contracts:
                return {"success": False, "message": "找不到合約"}

            contract = contracts[0]
            customer = contract.get("customers", {})

            data = {
                "contract_id": contract_id,
                "customer_id": customer.get("id"),
                "branch_id": contract.get("branch_id"),
                "letter_number": letter_number,
                "recipient_name": recipient_name or customer.get("company_name") or customer.get("name"),
                "recipient_address": recipient_address or contract.get("registered_address") or "",
                "content": content,
                "overdue_amount": 0,
                "overdue_days": 0,
                "reminder_count": 0,
                "status": "draft",
                "notes": "手動從合約建立"
            }
        else:
            return {"success": False, "message": "請提供 payment_id 或 contract_id"}

        result = await postgrest_post("legal_letters", data)
        letter = result[0] if isinstance(result, list) else result

        return {
            "success": True,
            "message": f"存證信函 {letter_number} 建立成功",
            "letter": letter
        }

    except Exception as e:
        logger.error(f"legal_create_letter error: {e}")
        raise Exception(f"建立存證信函失敗: {e}")


async def legal_generate_pdf(letter_id: int) -> Dict[str, Any]:
    """
    生成存證信函 PDF（呼叫 Cloud Run 服務）

    Args:
        letter_id: 存證信函ID

    Returns:
        包含 GCS Signed URL 的結果
    """
    try:
        # 取得存證信函資料
        letters = await postgrest_get("v_pending_legal_letters", {
            "id": f"eq.{letter_id}"
        })

        if not letters:
            return {"success": False, "message": "找不到存證信函"}

        letter = letters[0]

        # 準備資料
        letter_data = {
            "letter_id": letter_id,
            "letter_number": letter.get("letter_number"),
            "recipient_name": letter.get("recipient_name"),
            "recipient_address": letter.get("recipient_address"),
            "content": letter.get("content", ""),
            "overdue_amount": float(letter.get("overdue_amount", 0)),
            "overdue_days": letter.get("overdue_days", 0),
            "contract_number": letter.get("contract_number"),
            "branch_name": letter.get("branch_name"),
            "created_at": letter.get("created_at"),
            "sender_name": "你的空間有限公司",
            "sender_address": "台中市西區大忠南街 118 號 8 樓"
        }

        # 呼叫 Cloud Run 服務
        token = get_id_token_for_cloud_run(PDF_GENERATOR_URL)

        headers = {"Content-Type": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"

        request_data = {
            "legal_letter_data": letter_data,
            "template": "legal_letter"
        }

        logger.info(f"呼叫 Cloud Run PDF 服務生成存證信函: {letter_id}")

        async with httpx.AsyncClient(timeout=120.0) as client:
            response = await client.post(
                f"{PDF_GENERATOR_URL}/generate-legal-letter",
                json=request_data,
                headers=headers
            )

            if response.status_code == 401:
                return {
                    "success": False,
                    "message": "Cloud Run 認證失敗"
                }

            response.raise_for_status()
            result = response.json()

        if result.get("success"):
            # 更新 PDF 路徑
            await postgrest_patch(
                "legal_letters",
                {"id": f"eq.{letter_id}"},
                {
                    "pdf_path": result.get("pdf_path"),
                    "pdf_generated_at": datetime.now().isoformat()
                }
            )

            return {
                "success": True,
                "message": "存證信函 PDF 生成成功",
                "letter_number": letter_data["letter_number"],
                "pdf_url": result.get("pdf_url"),
                "pdf_path": result.get("pdf_path"),
                "expires_at": result.get("expires_at")
            }
        else:
            return {
                "success": False,
                "message": result.get("message", "PDF 生成失敗")
            }

    except Exception as e:
        logger.error(f"legal_generate_pdf error: {e}")
        return {
            "success": False,
            "message": f"PDF 生成失敗: {e}"
        }


async def legal_notify_staff(
    letter_id: int,
    staff_line_id: str = None,
    message: str = None
) -> Dict[str, Any]:
    """
    發送 LINE 通知給業務

    Args:
        letter_id: 存證信函ID
        staff_line_id: 業務的 LINE User ID（可選）
        message: 自訂訊息（可選）

    Returns:
        發送結果
    """
    if not LINE_CHANNEL_ACCESS_TOKEN:
        return {
            "success": False,
            "message": "LINE Bot 未設定"
        }

    try:
        # 取得存證信函資料
        letters = await postgrest_get("v_pending_legal_letters", {
            "id": f"eq.{letter_id}"
        })

        if not letters:
            return {"success": False, "message": "找不到存證信函"}

        letter = letters[0]

        # 預設通知訊息
        if not message:
            message = f"""【存證信函待處理通知】

編號：{letter.get('letter_number')}
客戶：{letter.get('customer_name')}
公司：{letter.get('company_name') or '無'}
逾期金額：NT$ {letter.get('overdue_amount', 0):,.0f}
逾期天數：{letter.get('overdue_days', 0)} 天

請至 CRM 系統下載 PDF 並寄出。

狀態：{letter.get('status_label')}"""

        # TODO: 這裡需要從系統設定取得業務的 LINE ID
        # 目前先回傳成功但標記未發送
        if not staff_line_id:
            # 更新通知時間但不實際發送
            await postgrest_patch(
                "legal_letters",
                {"id": f"eq.{letter_id}"},
                {
                    "notified_at": datetime.now().isoformat(),
                    "notes": "通知已建立，待設定業務 LINE ID 後發送"
                }
            )

            return {
                "success": True,
                "message": "通知已記錄（未設定業務 LINE ID）",
                "letter_number": letter.get("letter_number")
            }

        # 發送 LINE 訊息
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}"
        }

        payload = {
            "to": staff_line_id,
            "messages": [{"type": "text", "text": message}]
        }

        async with httpx.AsyncClient() as client:
            response = await client.post(
                LINE_API_URL,
                json=payload,
                headers=headers,
                timeout=30.0
            )

            if response.status_code == 200:
                # 更新通知狀態
                await postgrest_patch(
                    "legal_letters",
                    {"id": f"eq.{letter_id}"},
                    {
                        "notified_at": datetime.now().isoformat(),
                        "notified_to": staff_line_id
                    }
                )

                return {
                    "success": True,
                    "message": "已發送 LINE 通知給業務",
                    "letter_number": letter.get("letter_number")
                }
            else:
                logger.error(f"LINE API error: {response.status_code}")
                return {
                    "success": False,
                    "message": f"LINE 發送失敗: {response.status_code}"
                }

    except Exception as e:
        logger.error(f"legal_notify_staff error: {e}")
        return {
            "success": False,
            "message": f"發送通知失敗: {e}"
        }


async def legal_list_pending(
    branch_id: int = None,
    status: str = None,
    limit: int = 50
) -> Dict[str, Any]:
    """
    列出待處理存證信函

    Args:
        branch_id: 場館ID（可選）
        status: 狀態篩選 (draft/approved/sent)
        limit: 回傳筆數

    Returns:
        存證信函列表
    """
    try:
        params = {
            "limit": limit,
            "order": "created_at.desc"
        }

        if branch_id:
            params["branch_id"] = f"eq.{branch_id}"
        if status:
            params["status"] = f"eq.{status}"

        letters = await postgrest_get("v_pending_legal_letters", params)

        # 統計
        stats = {
            "draft": sum(1 for l in letters if l.get("status") == "draft"),
            "approved": sum(1 for l in letters if l.get("status") == "approved"),
            "sent": sum(1 for l in letters if l.get("status") == "sent"),
            "total_amount": sum(l.get("overdue_amount", 0) for l in letters)
        }

        return {
            "count": len(letters),
            "stats": stats,
            "letters": letters
        }

    except Exception as e:
        logger.error(f"legal_list_pending error: {e}")
        raise Exception(f"取得存證信函列表失敗: {e}")


async def legal_update_status(
    letter_id: int,
    status: str,
    approved_by: str = None,
    tracking_number: str = None,
    notes: str = None
) -> Dict[str, Any]:
    """
    更新存證信函狀態

    Args:
        letter_id: 存證信函ID
        status: 新狀態 (draft/approved/sent/cancelled)
        approved_by: 審核人（當狀態為 approved 時）
        tracking_number: 郵局掛號號碼（當狀態為 sent 時）
        notes: 備註

    Returns:
        更新結果
    """
    valid_statuses = ["draft", "approved", "sent", "cancelled"]
    if status not in valid_statuses:
        raise ValueError(f"無效的狀態，允許: {', '.join(valid_statuses)}")

    try:
        # 取得目前狀態
        letters = await postgrest_get("legal_letters", {
            "id": f"eq.{letter_id}"
        })

        if not letters:
            return {"success": False, "message": "找不到存證信函"}

        letter = letters[0]
        current_status = letter.get("status")

        # 狀態轉換驗證
        valid_transitions = {
            "draft": ["approved", "cancelled"],
            "approved": ["sent", "cancelled"],
            "sent": [],
            "cancelled": []
        }

        if status not in valid_transitions.get(current_status, []):
            return {
                "success": False,
                "message": f"無法從 {current_status} 轉換到 {status}"
            }

        # 準備更新資料
        update_data = {"status": status}

        if status == "approved":
            update_data["approved_at"] = datetime.now().isoformat()
            if approved_by:
                update_data["approved_by"] = approved_by

        if status == "sent":
            update_data["sent_at"] = date.today().isoformat()
            if tracking_number:
                update_data["tracking_number"] = tracking_number

        if notes:
            update_data["notes"] = notes

        result = await postgrest_patch(
            "legal_letters",
            {"id": f"eq.{letter_id}"},
            update_data
        )

        if not result:
            return {"success": False, "message": "更新失敗"}

        letter = result[0] if isinstance(result, list) else result

        status_labels = {
            "draft": "草稿",
            "approved": "已審核",
            "sent": "已寄送",
            "cancelled": "已取消"
        }

        return {
            "success": True,
            "message": f"存證信函狀態已更新為「{status_labels.get(status, status)}」",
            "letter": letter
        }

    except Exception as e:
        logger.error(f"legal_update_status error: {e}")
        raise Exception(f"更新狀態失敗: {e}")
