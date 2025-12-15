"""
Hour Jungle CRM - Settings Tools
系統設定工具
"""

import logging
import json
from typing import Dict, Any, Optional

import httpx

logger = logging.getLogger(__name__)

# PostgREST URL
import os
POSTGREST_URL = os.getenv("POSTGREST_URL", "http://postgrest:3000")


async def postgrest_get(endpoint: str, params: dict = None) -> Any:
    """PostgREST GET 請求"""
    url = f"{POSTGREST_URL}/{endpoint}"
    async with httpx.AsyncClient() as client:
        response = await client.get(url, params=params, timeout=30.0)
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


# ============================================================================
# 設定工具
# ============================================================================

async def settings_get(
    key: str = None,
    category: str = None
) -> Dict[str, Any]:
    """
    取得系統設定

    Args:
        key: 設定鍵值（可選）
        category: 分類篩選（可選）

    Returns:
        設定值
    """
    params = {}

    if key:
        params["key"] = f"eq.{key}"
    if category:
        params["category"] = f"eq.{category}"

    try:
        settings = await postgrest_get("system_settings", params)

        if key:
            # 單一設定
            if not settings:
                return {"found": False, "message": f"找不到設定 {key}"}

            setting = settings[0]
            return {
                "found": True,
                "key": setting["key"],
                "value": setting["value"],
                "category": setting.get("category"),
                "updated_at": setting.get("updated_at")
            }
        else:
            # 多個設定
            result = {}
            for s in settings:
                result[s["key"]] = s["value"]

            return {
                "count": len(settings),
                "settings": result
            }

    except Exception as e:
        logger.error(f"settings_get error: {e}")
        raise Exception(f"取得設定失敗: {e}")


async def settings_update(
    key: str,
    value: Dict[str, Any]
) -> Dict[str, Any]:
    """
    更新系統設定

    Args:
        key: 設定鍵值
        value: 新的設定值（完整替換或部分更新）

    Returns:
        更新結果
    """
    if not key:
        raise ValueError("必須指定設定 key")

    try:
        # 先取得現有設定
        existing = await postgrest_get("system_settings", {"key": f"eq.{key}"})

        if existing:
            # 更新現有設定
            current_value = existing[0].get("value", {})

            # 合併更新（部分更新）
            if isinstance(current_value, dict) and isinstance(value, dict):
                merged_value = {**current_value, **value}
            else:
                merged_value = value

            result = await postgrest_patch(
                "system_settings",
                {"key": f"eq.{key}"},
                {"value": merged_value}
            )

            setting = result[0] if result else None

            return {
                "success": True,
                "message": f"設定 {key} 已更新",
                "key": key,
                "value": merged_value
            }
        else:
            # 建立新設定
            result = await postgrest_post(
                "system_settings",
                {
                    "key": key,
                    "value": value,
                    "category": "custom"
                }
            )

            setting = result[0] if isinstance(result, list) else result

            return {
                "success": True,
                "message": f"設定 {key} 已建立",
                "key": key,
                "value": value
            }

    except Exception as e:
        logger.error(f"settings_update error: {e}")
        raise Exception(f"更新設定失敗: {e}")


async def settings_get_all() -> Dict[str, Any]:
    """
    取得所有系統設定

    Returns:
        所有設定值
    """
    try:
        settings = await postgrest_get("system_settings", {"order": "category,key"})

        # 按分類整理
        by_category = {}
        flat = {}

        for s in settings:
            key = s["key"]
            value = s["value"]
            category = s.get("category", "other")

            if category not in by_category:
                by_category[category] = {}

            by_category[category][key] = value
            flat[key] = value

        return {
            "count": len(settings),
            "settings": flat,
            "by_category": by_category
        }

    except Exception as e:
        logger.error(f"settings_get_all error: {e}")
        raise Exception(f"取得所有設定失敗: {e}")
