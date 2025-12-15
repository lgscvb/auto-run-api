"""
續約流程管理工具
"""

from datetime import datetime
from typing import Optional
import logging

logger = logging.getLogger(__name__)

# 導入資料庫連接（將在 main.py 中設置）
postgrest_request = None

def set_postgrest_request(func):
    """設置 postgrest_request 函數"""
    global postgrest_request
    postgrest_request = func


# 有效的續約狀態
VALID_RENEWAL_STATUSES = [
    'none',       # 無需處理
    'notified',   # 已通知
    'confirmed',  # 已確認續約
    'paid',       # 已收款
    'invoiced',   # 已開發票
    'signed',     # 已簽約
    'completed'   # 完成
]

# 有效的發票狀態
VALID_INVOICE_STATUSES = [
    'pending_tax_id',    # 等待統編
    'issued_personal',   # 已開二聯
    'issued_business'    # 已開三聯
]


async def update_renewal_status(
    contract_id: int,
    renewal_status: str,
    notes: Optional[str] = None
) -> dict:
    """
    更新合約的續約狀態

    Args:
        contract_id: 合約 ID
        renewal_status: 新狀態 (notified/confirmed/paid/invoiced/signed/completed)
        notes: 備註

    Returns:
        更新結果
    """
    if renewal_status not in VALID_RENEWAL_STATUSES:
        return {
            "success": False,
            "error": f"無效的狀態: {renewal_status}。有效值: {', '.join(VALID_RENEWAL_STATUSES)}"
        }

    # 準備更新資料
    update_data = {
        "renewal_status": renewal_status
    }

    # 根據狀態更新對應的時間戳
    now = datetime.now().isoformat()
    status_timestamp_map = {
        'notified': 'renewal_notified_at',
        'confirmed': 'renewal_confirmed_at',
        'paid': 'renewal_paid_at',
        'invoiced': 'renewal_invoiced_at',
        'signed': 'renewal_signed_at'
    }

    if renewal_status in status_timestamp_map:
        update_data[status_timestamp_map[renewal_status]] = now

    if notes:
        update_data["renewal_notes"] = notes

    try:
        result = await postgrest_request(
            "PATCH",
            f"contracts?id=eq.{contract_id}",
            data=update_data,
            headers={"Prefer": "return=representation"}
        )

        return {
            "success": True,
            "contract_id": contract_id,
            "new_status": renewal_status,
            "updated_at": now
        }
    except Exception as e:
        logger.error(f"更新續約狀態失敗: {e}")
        return {
            "success": False,
            "error": str(e)
        }


async def update_invoice_status(
    contract_id: int,
    invoice_status: str,
    notes: Optional[str] = None
) -> dict:
    """
    更新合約的發票狀態

    Args:
        contract_id: 合約 ID
        invoice_status: 發票狀態 (pending_tax_id/issued_personal/issued_business)
        notes: 備註

    Returns:
        更新結果
    """
    if invoice_status not in VALID_INVOICE_STATUSES:
        return {
            "success": False,
            "error": f"無效的發票狀態: {invoice_status}。有效值: {', '.join(VALID_INVOICE_STATUSES)}"
        }

    update_data = {
        "invoice_status": invoice_status
    }

    if notes:
        update_data["renewal_notes"] = notes

    try:
        result = await postgrest_request(
            "PATCH",
            f"contracts?id=eq.{contract_id}",
            data=update_data,
            headers={"Prefer": "return=representation"}
        )

        return {
            "success": True,
            "contract_id": contract_id,
            "invoice_status": invoice_status
        }
    except Exception as e:
        logger.error(f"更新發票狀態失敗: {e}")
        return {
            "success": False,
            "error": str(e)
        }


async def get_renewal_status_summary(
    branch_id: Optional[int] = None
) -> dict:
    """
    取得續約狀態統計

    Args:
        branch_id: 場館 ID（可選）

    Returns:
        各狀態的數量統計
    """
    params = {}
    if branch_id:
        params["branch_id"] = f"eq.{branch_id}"

    try:
        result = await postgrest_request(
            "GET",
            "v_renewal_status_summary",
            params=params
        )

        # 整理成更易讀的格式
        summary = {}
        for row in result or []:
            status = row.get('renewal_status', 'none')
            if status not in summary:
                summary[status] = {
                    "count": 0,
                    "total_monthly_rent": 0
                }
            summary[status]["count"] += row.get('count', 0)
            summary[status]["total_monthly_rent"] += row.get('total_monthly_rent', 0)

        return {
            "success": True,
            "summary": summary,
            "raw": result
        }
    except Exception as e:
        logger.error(f"取得續約狀態統計失敗: {e}")
        return {
            "success": False,
            "error": str(e)
        }


async def renewal_set_flag(
    contract_id: int,
    flag: str,
    value: bool,
    notes: Optional[str] = None
) -> dict:
    """
    設定或清除續約 Checklist 的 flag（使用時間戳作為事實來源）

    實作 Cascade Logic:
    - 設定 paid/signed 時自動補上 confirmed
    - 清除 flag 時不會自動清除其他 flag

    Args:
        contract_id: 合約 ID
        flag: flag 名稱 (notified/confirmed/paid/signed)
        value: True = 設定, False = 清除
        notes: 備註

    Returns:
        更新結果，包含更新後的所有 flag 狀態
    """
    valid_flags = ['notified', 'confirmed', 'paid', 'signed']
    if flag not in valid_flags:
        return {
            "success": False,
            "error": f"無效的 flag: {flag}。有效值: {', '.join(valid_flags)}"
        }

    # 時間戳欄位對應
    flag_to_timestamp = {
        'notified': 'renewal_notified_at',
        'confirmed': 'renewal_confirmed_at',
        'paid': 'renewal_paid_at',
        'signed': 'renewal_signed_at'
    }

    now = datetime.now().isoformat()
    update_data = {}

    if value:
        # 設定 flag：寫入時間戳
        update_data[flag_to_timestamp[flag]] = now

        # Cascade Logic: 設定 paid 或 signed 時，自動補上 confirmed
        if flag in ['paid', 'signed']:
            # 先取得目前的合約狀態
            try:
                result = await postgrest_request(
                    "GET",
                    f"contracts?id=eq.{contract_id}&select=renewal_confirmed_at"
                )
                if result and not result[0].get('renewal_confirmed_at'):
                    # 如果尚未確認，自動補上
                    update_data['renewal_confirmed_at'] = now
            except Exception as e:
                logger.warning(f"取得合約狀態失敗，跳過 cascade: {e}")
    else:
        # 清除 flag：設為 null
        update_data[flag_to_timestamp[flag]] = None
        # 注意：清除時不會自動清除其他 flag

    if notes:
        update_data["renewal_notes"] = notes

    try:
        # 更新資料庫
        await postgrest_request(
            "PATCH",
            f"contracts?id=eq.{contract_id}",
            data=update_data,
            headers={"Prefer": "return=representation"}
        )

        # 取得更新後的完整狀態
        result = await postgrest_request(
            "GET",
            f"contracts?id=eq.{contract_id}&select=id,contract_number,renewal_notified_at,renewal_confirmed_at,renewal_paid_at,renewal_signed_at,invoice_status,renewal_status"
        )

        if result:
            contract = result[0]
            # 計算 computed flags
            flags = {
                "is_notified": bool(contract.get('renewal_notified_at')),
                "is_confirmed": bool(contract.get('renewal_confirmed_at')),
                "is_paid": bool(contract.get('renewal_paid_at')),
                "is_signed": bool(contract.get('renewal_signed_at')),
                "is_invoiced": contract.get('invoice_status') and contract.get('invoice_status') != 'pending_tax_id'
            }

            # 計算 progress 和 stage
            completed_count = sum(1 for v in flags.values() if v)

            if completed_count == 0:
                stage = 'pending'
            elif completed_count == 5:
                stage = 'completed'
            else:
                stage = 'in_progress'

            # 自動更新 renewal_status 欄位
            if stage != contract.get('renewal_status'):
                await postgrest_request(
                    "PATCH",
                    f"contracts?id=eq.{contract_id}",
                    data={"renewal_status": stage}
                )

            return {
                "success": True,
                "contract_id": contract_id,
                "contract_number": contract.get('contract_number'),
                "flag_updated": flag,
                "new_value": value,
                "flags": flags,
                "progress": completed_count,
                "stage": stage,
                "updated_at": now,
                "cascade_triggered": flag in ['paid', 'signed'] and value and 'renewal_confirmed_at' in update_data and update_data.get('renewal_confirmed_at') == now
            }

        return {
            "success": True,
            "contract_id": contract_id,
            "flag_updated": flag,
            "new_value": value,
            "updated_at": now
        }

    except Exception as e:
        logger.error(f"更新續約 flag 失敗: {e}")
        return {
            "success": False,
            "error": str(e)
        }


async def batch_update_renewal_status(
    contract_ids: list,
    renewal_status: str,
    notes: Optional[str] = None
) -> dict:
    """
    批次更新多個合約的續約狀態

    Args:
        contract_ids: 合約 ID 列表
        renewal_status: 新狀態
        notes: 備註

    Returns:
        更新結果
    """
    if renewal_status not in VALID_RENEWAL_STATUSES:
        return {
            "success": False,
            "error": f"無效的狀態: {renewal_status}"
        }

    if not contract_ids:
        return {
            "success": False,
            "error": "請提供至少一個合約 ID"
        }

    # 準備更新資料
    update_data = {
        "renewal_status": renewal_status
    }

    now = datetime.now().isoformat()
    status_timestamp_map = {
        'notified': 'renewal_notified_at',
        'confirmed': 'renewal_confirmed_at',
        'paid': 'renewal_paid_at',
        'invoiced': 'renewal_invoiced_at',
        'signed': 'renewal_signed_at'
    }

    if renewal_status in status_timestamp_map:
        update_data[status_timestamp_map[renewal_status]] = now

    if notes:
        update_data["renewal_notes"] = notes

    # 使用 PostgREST 的 in 語法批次更新
    ids_str = ",".join(str(id) for id in contract_ids)

    try:
        result = await postgrest_request(
            "PATCH",
            f"contracts?id=in.({ids_str})",
            data=update_data,
            headers={"Prefer": "return=representation"}
        )

        return {
            "success": True,
            "updated_count": len(contract_ids),
            "new_status": renewal_status,
            "contract_ids": contract_ids
        }
    except Exception as e:
        logger.error(f"批次更新續約狀態失敗: {e}")
        return {
            "success": False,
            "error": str(e)
        }
