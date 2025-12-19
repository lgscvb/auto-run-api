"""
Hour Jungle CRM - Brain Tools
Brain AI 知識庫整合工具

用途：讓 LLM 能自動將對話中的知識儲存到 Brain 的 RAG 知識庫
"""

import logging
import os
from typing import Dict, Any, Optional, List

import httpx

logger = logging.getLogger(__name__)

# Brain API URL
BRAIN_API_URL = os.getenv("BRAIN_API_URL", "https://brain.yourspce.org")

# 知識分類對照表
KNOWLEDGE_CATEGORIES = {
    "faq": "常見問題",
    "service_info": "服務資訊",
    "process": "流程說明",
    "regulation": "法規規定",
    "objection": "異議處理",
    "value_prop": "價值主張",
    "spin_question": "SPIN 問題庫",
    "tactics": "銷售技巧",
    "scenario": "情境範例",
    "example_response": "對話範例",
    "customer_info": "客戶資訊"
}


async def brain_save_knowledge(
    content: str,
    category: str = "faq",
    sub_category: Optional[str] = None,
    service_type: Optional[str] = None,
    source: str = "mcp_tool"
) -> Dict[str, Any]:
    """
    儲存知識到 Brain RAG 知識庫

    當對話中發現有價值的資訊時，可以使用此工具儲存到知識庫，
    讓 AI 客服在未來的對話中能夠參考這些知識。

    Args:
        content: 知識內容（必填）
                 範例：「資本額 25 萬以下的行號免辦資本證明」
        category: 知識分類（預設 faq）
                 可選值：
                 - faq: 常見問題
                 - service_info: 服務資訊（價格、地址、營業時間等）
                 - process: 流程說明（公司登記流程、預約流程等）
                 - regulation: 法規規定（公司法、稅法相關規定）
                 - objection: 異議處理（價格太貴、需要考慮等）
                 - value_prop: 價值主張（我們的優勢）
                 - tactics: 銷售技巧
                 - customer_info: 客戶資訊（特定客戶的備註）
        sub_category: 子分類（可選）
                 範例：「公司登記」「稅務」「銀行開戶」
        service_type: 適用服務類型（可選）
                 可選值：address_service, coworking, private_office, meeting_room
        source: 知識來源標記（預設 mcp_tool）

    Returns:
        執行結果，包含新建的知識 ID
    """
    # 驗證分類
    if category not in KNOWLEDGE_CATEGORIES:
        return {
            "success": False,
            "message": f"無效的分類: {category}",
            "valid_categories": list(KNOWLEDGE_CATEGORIES.keys())
        }

    # 驗證內容
    if not content or len(content.strip()) < 10:
        return {
            "success": False,
            "message": "知識內容太短，請提供更詳細的內容（至少 10 個字）"
        }

    # 準備 API 請求
    payload = {
        "content": content.strip(),
        "category": category,
        "metadata": {
            "source": source
        }
    }

    if sub_category:
        payload["sub_category"] = sub_category
    if service_type:
        payload["service_type"] = service_type

    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{BRAIN_API_URL}/api/knowledge",
                json=payload,
                timeout=30.0
            )

            if response.status_code == 200:
                result = response.json()
                return {
                    "success": True,
                    "message": f"知識已儲存到 {KNOWLEDGE_CATEGORIES.get(category, category)} 分類",
                    "knowledge_id": result.get("id"),
                    "content": content[:100] + "..." if len(content) > 100 else content
                }
            else:
                error_detail = response.text
                logger.error(f"Brain API error: {response.status_code} - {error_detail}")
                return {
                    "success": False,
                    "message": f"Brain API 錯誤: {response.status_code}",
                    "detail": error_detail[:200]
                }

    except httpx.TimeoutException:
        return {
            "success": False,
            "message": "Brain API 連線超時，請稍後再試"
        }
    except Exception as e:
        logger.error(f"brain_save_knowledge error: {e}")
        return {
            "success": False,
            "message": f"儲存失敗: {str(e)}"
        }


async def brain_search_knowledge(
    query: str,
    top_k: int = 5,
    category: Optional[str] = None
) -> Dict[str, Any]:
    """
    搜尋 Brain RAG 知識庫

    使用向量語意搜尋找出相關知識。

    Args:
        query: 搜尋關鍵字或問題
        top_k: 回傳結果數量（預設 5）
        category: 限定分類搜尋（可選）

    Returns:
        搜尋結果列表
    """
    payload = {
        "query": query,
        "top_k": top_k
    }

    if category:
        payload["category"] = category

    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{BRAIN_API_URL}/api/knowledge/search",
                json=payload,
                timeout=30.0
            )

            if response.status_code == 200:
                results = response.json()
                return {
                    "success": True,
                    "count": len(results),
                    "results": [
                        {
                            "id": r.get("id"),
                            "content": r.get("content"),
                            "category": r.get("category"),
                            "similarity": round(r.get("similarity", 0), 3)
                        }
                        for r in results
                    ]
                }
            else:
                return {
                    "success": False,
                    "message": f"搜尋失敗: {response.status_code}"
                }

    except Exception as e:
        logger.error(f"brain_search_knowledge error: {e}")
        return {
            "success": False,
            "message": f"搜尋失敗: {str(e)}"
        }


async def brain_list_categories() -> Dict[str, Any]:
    """
    列出所有知識分類

    Returns:
        分類列表及說明
    """
    return {
        "success": True,
        "categories": [
            {"id": k, "name": v}
            for k, v in KNOWLEDGE_CATEGORIES.items()
        ]
    }
