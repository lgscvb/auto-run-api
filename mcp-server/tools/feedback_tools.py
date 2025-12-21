"""
Hour Jungle CRM - Feedback Tools
使用者問題回報與建議工具
"""

import logging
from typing import Dict, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

# PostgREST 請求函數（由 main.py 注入）
_postgrest_request = None


def set_postgrest_request(func):
    """注入 PostgREST 請求函數"""
    global _postgrest_request
    _postgrest_request = func


async def feedback_submit(
    feedback_type: str,
    title: str,
    description: Optional[str] = None,
    priority: str = "medium",
    page_url: Optional[str] = None,
    related_feature: Optional[str] = None,
    submitted_by: Optional[str] = None
) -> Dict[str, Any]:
    """
    提交問題回報或功能建議

    讓 CRM 使用者可以透過 AI 助手回報系統問題或提出改進建議。

    Args:
        feedback_type: 回報類型
                      - bug: 系統錯誤/Bug
                      - feature: 新功能需求
                      - improvement: 現有功能改進
                      - question: 使用問題
                      - other: 其他
        title: 標題（簡短描述問題）
        description: 詳細說明（可選）
        priority: 優先級 (low/medium/high/critical)，預設 medium
        page_url: 問題發生的頁面 URL（可選）
        related_feature: 相關功能名稱，如「繳費管理」「報表」（可選）
        submitted_by: 提交者名稱（可選）

    Returns:
        執行結果，包含新建的回報 ID
    """
    # 驗證 feedback_type
    valid_types = ['bug', 'feature', 'improvement', 'question', 'other']
    if feedback_type not in valid_types:
        return {
            "success": False,
            "message": f"無效的回報類型: {feedback_type}",
            "valid_types": valid_types
        }

    # 驗證 priority
    valid_priorities = ['low', 'medium', 'high', 'critical']
    if priority not in valid_priorities:
        priority = 'medium'

    # 驗證標題
    if not title or len(title.strip()) < 5:
        return {
            "success": False,
            "message": "請提供更詳細的標題（至少 5 個字）"
        }

    # 準備資料
    data = {
        "feedback_type": feedback_type,
        "title": title.strip(),
        "priority": priority,
        "status": "open",
        "submitted_via": "ai_assistant"
    }

    if description:
        data["description"] = description.strip()
    if page_url:
        data["page_url"] = page_url
    if related_feature:
        data["related_feature"] = related_feature
    if submitted_by:
        data["submitted_by"] = submitted_by

    try:
        result = await _postgrest_request(
            "POST",
            "/feedback",
            json=data
        )

        # 類型對照表
        type_labels = {
            'bug': '錯誤回報',
            'feature': '新功能需求',
            'improvement': '改進建議',
            'question': '使用問題',
            'other': '其他'
        }

        return {
            "success": True,
            "message": f"已收到您的{type_labels.get(feedback_type, '回報')}，我們會盡快處理！",
            "feedback_id": result.get("id") if isinstance(result, dict) else None,
            "title": title,
            "type": feedback_type,
            "priority": priority
        }

    except Exception as e:
        logger.error(f"feedback_submit error: {e}")
        return {
            "success": False,
            "message": f"回報提交失敗: {str(e)}"
        }


async def feedback_list(
    status: Optional[str] = None,
    feedback_type: Optional[str] = None,
    limit: int = 20
) -> Dict[str, Any]:
    """
    列出回報記錄

    Args:
        status: 篩選狀態（open/reviewing/in_progress/resolved）
        feedback_type: 篩選類型
        limit: 回傳數量上限

    Returns:
        回報列表
    """
    try:
        # 建構查詢參數
        params = {
            "order": "created_at.desc",
            "limit": limit
        }
        if status:
            params["status"] = f"eq.{status}"
        if feedback_type:
            params["feedback_type"] = f"eq.{feedback_type}"

        result = await _postgrest_request(
            "GET",
            "/feedback",
            params=params
        )

        return {
            "success": True,
            "count": len(result) if isinstance(result, list) else 0,
            "feedbacks": result if isinstance(result, list) else []
        }

    except Exception as e:
        logger.error(f"feedback_list error: {e}")
        return {
            "success": False,
            "message": f"查詢失敗: {str(e)}"
        }
