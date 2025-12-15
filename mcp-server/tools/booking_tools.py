"""
Hour Jungle CRM - Booking Tools
會議室預約相關工具
"""

import logging
import os
from datetime import datetime, date, time, timedelta
from typing import Dict, Any, Optional, List

import httpx

from .google_calendar import get_calendar_service
from .line_tools import send_line_push

logger = logging.getLogger(__name__)

POSTGREST_URL = os.getenv("POSTGREST_URL", "http://postgrest:3000")

# PostgREST 請求函數（供此模組使用）
_postgrest_request = None


def set_postgrest_request(func):
    """設定 PostgREST 請求函數"""
    global _postgrest_request
    _postgrest_request = func


async def postgrest_get(endpoint: str, params: dict = None) -> Any:
    """PostgREST GET 請求"""
    if _postgrest_request:
        return await _postgrest_request("GET", endpoint, params=params)

    url = f"{POSTGREST_URL}/{endpoint}"
    async with httpx.AsyncClient() as client:
        response = await client.get(url, params=params, timeout=30.0)
        response.raise_for_status()
        return response.json()


async def postgrest_post(endpoint: str, data: dict, headers: dict = None) -> Any:
    """PostgREST POST 請求"""
    if _postgrest_request:
        # 確保 Prefer header 被傳入以取得回傳資料
        post_headers = {"Prefer": "return=representation"}
        if headers:
            post_headers.update(headers)
        return await _postgrest_request("POST", endpoint, data=data, headers=post_headers)

    url = f"{POSTGREST_URL}/{endpoint}"
    default_headers = {"Content-Type": "application/json", "Prefer": "return=representation"}
    if headers:
        default_headers.update(headers)
    async with httpx.AsyncClient() as client:
        response = await client.post(url, json=data, headers=default_headers, timeout=30.0)
        response.raise_for_status()
        return response.json()


async def postgrest_patch(endpoint: str, params: dict, data: dict) -> Any:
    """PostgREST PATCH 請求"""
    if _postgrest_request:
        return await _postgrest_request("PATCH", endpoint, params=params, data=data, headers={"Prefer": "return=representation"})

    url = f"{POSTGREST_URL}/{endpoint}"
    headers = {"Content-Type": "application/json", "Prefer": "return=representation"}
    async with httpx.AsyncClient() as client:
        response = await client.patch(url, params=params, json=data, headers=headers, timeout=30.0)
        response.raise_for_status()
        return response.json()


# ============================================================================
# 會議室預約工具
# ============================================================================

async def booking_list_rooms(branch_id: int = None) -> Dict[str, Any]:
    """
    列出會議室

    Args:
        branch_id: 場館ID（可選）

    Returns:
        會議室列表
    """
    try:
        params = {"is_active": "eq.true", "order": "branch_id,name"}
        if branch_id:
            params["branch_id"] = f"eq.{branch_id}"

        rooms = await postgrest_get("meeting_rooms", params)

        # 加入場館資訊
        branches = await postgrest_get("branches", {"select": "id,name"})
        branch_map = {b["id"]: b["name"] for b in branches}

        for room in rooms:
            room["branch_name"] = branch_map.get(room["branch_id"], "")

        return {
            "success": True,
            "rooms": rooms,
            "total": len(rooms)
        }
    except Exception as e:
        logger.error(f"booking_list_rooms error: {e}")
        raise Exception(f"取得會議室列表失敗: {e}")


async def booking_check_availability(
    room_id: int,
    date_str: str,
    start_time: str = None,
    end_time: str = None
) -> Dict[str, Any]:
    """
    查詢會議室可用時段

    Args:
        room_id: 會議室ID
        date_str: 日期 (YYYY-MM-DD)
        start_time: 開始時間 (HH:MM)，可選，用於檢查特定時段
        end_time: 結束時間 (HH:MM)，可選

    Returns:
        可用時段列表或特定時段可用性
    """
    try:
        # 取得會議室資訊
        rooms = await postgrest_get("meeting_rooms", {"id": f"eq.{room_id}"})
        if not rooms:
            return {"success": False, "error": "會議室不存在"}

        room = rooms[0]
        check_date = datetime.strptime(date_str, "%Y-%m-%d").date()

        # 取得該日已有預約
        bookings = await postgrest_get("meeting_room_bookings", {
            "meeting_room_id": f"eq.{room_id}",
            "booking_date": f"eq.{date_str}",
            "status": "eq.confirmed",
            "select": "id,start_time,end_time,customer_id"
        })

        # 如果有 Google Calendar ID，也從 Calendar 取得
        busy_times = []
        if room.get("google_calendar_id"):
            try:
                calendar_service = get_calendar_service()
                busy_result = calendar_service.get_busy_times(
                    room["google_calendar_id"],
                    check_date
                )
                if busy_result.get("success"):
                    busy_times = busy_result.get("busy_times", [])
            except Exception as e:
                logger.warning(f"Failed to get calendar busy times: {e}")

        # 合併資料庫預約和 Calendar 忙碌時段
        all_busy = []
        for b in bookings:
            all_busy.append({
                "start": b["start_time"][:5],  # HH:MM
                "end": b["end_time"][:5],
                "source": "database"
            })
        for bt in busy_times:
            all_busy.append({
                "start": bt["start"],
                "end": bt["end"],
                "source": "calendar"
            })

        # 如果要檢查特定時段
        if start_time and end_time:
            is_available = True
            for busy in all_busy:
                # 時段重疊檢查
                if not (end_time <= busy["start"] or start_time >= busy["end"]):
                    is_available = False
                    break

            return {
                "success": True,
                "room_id": room_id,
                "room_name": room["name"],
                "date": date_str,
                "start_time": start_time,
                "end_time": end_time,
                "is_available": is_available,
                "conflicts": [b for b in all_busy if not (end_time <= b["start"] or start_time >= b["end"])]
            }

        # 生成所有可用時段（30分鐘為單位）
        available_slots = []
        current = datetime.combine(check_date, time(9, 0))
        end_of_day = datetime.combine(check_date, time(18, 0))

        while current < end_of_day:
            slot_start = current.strftime("%H:%M")
            slot_end = (current + timedelta(minutes=30)).strftime("%H:%M")

            # 檢查是否可用
            is_slot_available = True
            for busy in all_busy:
                if not (slot_end <= busy["start"] or slot_start >= busy["end"]):
                    is_slot_available = False
                    break

            if is_slot_available:
                available_slots.append({
                    "start": slot_start,
                    "end": slot_end
                })

            current += timedelta(minutes=30)

        # 過濾已過去的時段（如果是今天）
        if check_date == date.today():
            now_str = datetime.now().strftime("%H:%M")
            available_slots = [s for s in available_slots if s["start"] > now_str]

        return {
            "success": True,
            "room_id": room_id,
            "room_name": room["name"],
            "date": date_str,
            "available_slots": available_slots,
            "total_available": len(available_slots),
            "busy_times": all_busy
        }
    except Exception as e:
        logger.error(f"booking_check_availability error: {e}")
        raise Exception(f"查詢可用時段失敗: {e}")


async def booking_create(
    room_id: int,
    customer_id: int,
    date_str: str,
    start_time: str,
    end_time: str,
    purpose: str = None,
    attendees_count: int = None,
    notes: str = None,
    created_by: str = "admin"
) -> Dict[str, Any]:
    """
    建立會議室預約

    Args:
        room_id: 會議室ID
        customer_id: 客戶ID
        date_str: 日期 (YYYY-MM-DD)
        start_time: 開始時間 (HH:MM)
        end_time: 結束時間 (HH:MM)
        purpose: 會議目的
        attendees_count: 預計人數
        notes: 備註
        created_by: 建立來源 (line/admin)

    Returns:
        預約結果
    """
    try:
        # 1. 檢查時段可用性
        availability = await booking_check_availability(room_id, date_str, start_time, end_time)
        if not availability.get("is_available"):
            return {
                "success": False,
                "error": "該時段已被預約",
                "conflicts": availability.get("conflicts", [])
            }

        # 2. 取得會議室資訊
        rooms = await postgrest_get("meeting_rooms", {"id": f"eq.{room_id}"})
        if not rooms:
            return {"success": False, "error": "會議室不存在"}
        room = rooms[0]

        # 3. 取得客戶資訊
        customers = await postgrest_get("customers", {"id": f"eq.{customer_id}"})
        if not customers:
            return {"success": False, "error": "客戶不存在"}
        customer = customers[0]

        # 4. 生成預約編號
        booking_number = await _generate_booking_number()

        # 5. 建立預約記錄
        booking_data = {
            "booking_number": booking_number,
            "meeting_room_id": room_id,
            "customer_id": customer_id,
            "booking_date": date_str,
            "start_time": start_time,
            "end_time": end_time,
            "status": "confirmed",
            "purpose": purpose,
            "attendees_count": attendees_count,
            "notes": notes,
            "created_by": created_by
        }

        result = await postgrest_post("meeting_room_bookings", booking_data)

        if not result:
            return {"success": False, "error": "建立預約失敗"}

        booking = result[0] if isinstance(result, list) else result

        # 6. 建立 Google Calendar 事件（如果有設定）
        google_event_id = None
        if room.get("google_calendar_id"):
            try:
                calendar_service = get_calendar_service()
                check_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                start_dt = datetime.combine(check_date, datetime.strptime(start_time, "%H:%M").time())
                end_dt = datetime.combine(check_date, datetime.strptime(end_time, "%H:%M").time())

                # 取得場館資訊
                branches = await postgrest_get("branches", {"id": f"eq.{room['branch_id']}"})
                branch_name = branches[0]["name"] if branches else ""

                event_title = f"[{booking_number}] {customer.get('company_name') or customer['name']}"
                event_desc = f"預約人: {customer['name']}\n"
                if purpose:
                    event_desc += f"目的: {purpose}\n"
                if attendees_count:
                    event_desc += f"人數: {attendees_count}\n"

                cal_result = calendar_service.create_event(
                    room["google_calendar_id"],
                    event_title,
                    start_dt,
                    end_dt,
                    description=event_desc,
                    location=f"{branch_name} {room['name']}"
                )

                if cal_result.get("success"):
                    google_event_id = cal_result["event_id"]
                    # 更新預約記錄
                    await postgrest_patch(
                        "meeting_room_bookings",
                        {"id": f"eq.{booking['id']}"},
                        {"google_event_id": google_event_id}
                    )
            except Exception as e:
                logger.warning(f"Failed to create calendar event: {e}")

        return {
            "success": True,
            "booking": {
                "id": booking["id"],
                "booking_number": booking_number,
                "room_name": room["name"],
                "customer_name": customer["name"],
                "date": date_str,
                "start_time": start_time,
                "end_time": end_time,
                "google_event_id": google_event_id
            },
            "message": f"預約成功！編號: {booking_number}"
        }
    except Exception as e:
        logger.error(f"booking_create error: {e}")
        raise Exception(f"建立預約失敗: {e}")


async def _generate_booking_number() -> str:
    """生成預約編號"""
    today_str = datetime.now().strftime("%Y%m%d")

    # 查詢今日最大序號
    bookings = await postgrest_get("meeting_room_bookings", {
        "booking_number": f"like.MR-{today_str}-%",
        "select": "booking_number",
        "order": "booking_number.desc",
        "limit": 1
    })

    if bookings:
        last_num = bookings[0]["booking_number"]
        seq = int(last_num[-4:]) + 1
    else:
        seq = 1

    return f"MR-{today_str}-{seq:04d}"


async def booking_cancel(
    booking_id: int,
    reason: str = None
) -> Dict[str, Any]:
    """
    取消會議室預約

    Args:
        booking_id: 預約ID
        reason: 取消原因

    Returns:
        取消結果
    """
    try:
        # 1. 取得預約資訊
        bookings = await postgrest_get("v_meeting_room_bookings", {"id": f"eq.{booking_id}"})
        if not bookings:
            return {"success": False, "error": "預約不存在"}

        booking = bookings[0]

        if booking["status"] == "cancelled":
            return {"success": False, "error": "此預約已取消"}

        # 2. 更新預約狀態
        await postgrest_patch(
            "meeting_room_bookings",
            {"id": f"eq.{booking_id}"},
            {
                "status": "cancelled",
                "cancelled_at": datetime.now().isoformat(),
                "cancel_reason": reason
            }
        )

        # 3. 刪除 Google Calendar 事件
        if booking.get("google_event_id"):
            try:
                rooms = await postgrest_get("meeting_rooms", {"id": f"eq.{booking['meeting_room_id']}"})
                if rooms and rooms[0].get("google_calendar_id"):
                    calendar_service = get_calendar_service()
                    calendar_service.delete_event(
                        rooms[0]["google_calendar_id"],
                        booking["google_event_id"]
                    )
            except Exception as e:
                logger.warning(f"Failed to delete calendar event: {e}")

        return {
            "success": True,
            "booking_number": booking["booking_number"],
            "message": f"預約 {booking['booking_number']} 已取消"
        }
    except Exception as e:
        logger.error(f"booking_cancel error: {e}")
        raise Exception(f"取消預約失敗: {e}")


async def booking_update(
    booking_id: int,
    date_str: str = None,
    start_time: str = None,
    end_time: str = None,
    purpose: str = None,
    attendees_count: int = None,
    notes: str = None
) -> Dict[str, Any]:
    """
    修改會議室預約

    Args:
        booking_id: 預約ID
        其他參數同 booking_create

    Returns:
        修改結果
    """
    try:
        # 1. 取得現有預約
        bookings = await postgrest_get("v_meeting_room_bookings", {"id": f"eq.{booking_id}"})
        if not bookings:
            return {"success": False, "error": "預約不存在"}

        booking = bookings[0]

        if booking["status"] != "confirmed":
            return {"success": False, "error": "只能修改確認中的預約"}

        # 2. 準備更新資料
        update_data = {}
        new_date = date_str or str(booking["booking_date"])
        new_start = start_time or booking["start_time"][:5]
        new_end = end_time or booking["end_time"][:5]

        # 3. 如果時間有變更，檢查可用性
        time_changed = (
            date_str and date_str != str(booking["booking_date"]) or
            start_time and start_time != booking["start_time"][:5] or
            end_time and end_time != booking["end_time"][:5]
        )

        if time_changed:
            # 檢查新時段（排除當前預約）
            availability = await booking_check_availability(
                booking["meeting_room_id"],
                new_date,
                new_start,
                new_end
            )

            # 移除自己的衝突
            conflicts = [
                c for c in availability.get("conflicts", [])
                if c.get("source") != "database" or
                not any(b["id"] == booking_id for b in await postgrest_get(
                    "meeting_room_bookings",
                    {"start_time": f"eq.{c['start']}:00", "booking_date": f"eq.{new_date}"}
                ))
            ]

            if conflicts:
                return {
                    "success": False,
                    "error": "新時段已被預約",
                    "conflicts": conflicts
                }

            update_data["booking_date"] = new_date
            update_data["start_time"] = new_start
            update_data["end_time"] = new_end

        if purpose is not None:
            update_data["purpose"] = purpose
        if attendees_count is not None:
            update_data["attendees_count"] = attendees_count
        if notes is not None:
            update_data["notes"] = notes

        if not update_data:
            return {"success": True, "message": "沒有需要更新的內容"}

        # 4. 更新資料庫
        await postgrest_patch(
            "meeting_room_bookings",
            {"id": f"eq.{booking_id}"},
            update_data
        )

        # 5. 更新 Google Calendar（如果時間有變更）
        if time_changed and booking.get("google_event_id"):
            try:
                rooms = await postgrest_get("meeting_rooms", {"id": f"eq.{booking['meeting_room_id']}"})
                if rooms and rooms[0].get("google_calendar_id"):
                    calendar_service = get_calendar_service()
                    check_date = datetime.strptime(new_date, "%Y-%m-%d").date()
                    start_dt = datetime.combine(check_date, datetime.strptime(new_start, "%H:%M").time())
                    end_dt = datetime.combine(check_date, datetime.strptime(new_end, "%H:%M").time())

                    calendar_service.update_event(
                        rooms[0]["google_calendar_id"],
                        booking["google_event_id"],
                        start_datetime=start_dt,
                        end_datetime=end_dt
                    )
            except Exception as e:
                logger.warning(f"Failed to update calendar event: {e}")

        return {
            "success": True,
            "booking_number": booking["booking_number"],
            "message": f"預約 {booking['booking_number']} 已更新"
        }
    except Exception as e:
        logger.error(f"booking_update error: {e}")
        raise Exception(f"修改預約失敗: {e}")


async def booking_list(
    customer_id: int = None,
    date_str: str = None,
    date_from: str = None,
    date_to: str = None,
    branch_id: int = None,
    status: str = None,
    limit: int = 50
) -> Dict[str, Any]:
    """
    列出會議室預約

    Args:
        customer_id: 客戶ID（可選）
        date_str: 特定日期（可選）
        date_from: 開始日期（可選）
        date_to: 結束日期（可選）
        branch_id: 場館ID（可選）
        status: 狀態（可選）
        limit: 回傳筆數

    Returns:
        預約列表
    """
    try:
        params = {"order": "booking_date.desc,start_time", "limit": limit}

        if customer_id:
            params["customer_id"] = f"eq.{customer_id}"
        if date_str:
            params["booking_date"] = f"eq.{date_str}"
        if date_from:
            params["booking_date"] = f"gte.{date_from}"
        if date_to:
            if "booking_date" in params:
                # 需要用 and 條件
                params["booking_date"] = f"gte.{date_from}"
                params["and"] = f"(booking_date.lte.{date_to})"
            else:
                params["booking_date"] = f"lte.{date_to}"
        if branch_id:
            params["branch_id"] = f"eq.{branch_id}"
        if status:
            params["status"] = f"eq.{status}"

        bookings = await postgrest_get("v_meeting_room_bookings", params)

        return {
            "success": True,
            "bookings": bookings,
            "total": len(bookings)
        }
    except Exception as e:
        logger.error(f"booking_list error: {e}")
        raise Exception(f"取得預約列表失敗: {e}")


async def booking_get(booking_id: int) -> Dict[str, Any]:
    """
    取得預約詳情

    Args:
        booking_id: 預約ID

    Returns:
        預約詳情
    """
    try:
        bookings = await postgrest_get("v_meeting_room_bookings", {"id": f"eq.{booking_id}"})

        if not bookings:
            return {"success": False, "error": "預約不存在"}

        return {
            "success": True,
            "booking": bookings[0]
        }
    except Exception as e:
        logger.error(f"booking_get error: {e}")
        raise Exception(f"取得預約詳情失敗: {e}")


async def booking_send_reminder(booking_id: int) -> Dict[str, Any]:
    """
    發送預約提醒

    Args:
        booking_id: 預約ID

    Returns:
        發送結果
    """
    try:
        # 取得預約詳情
        bookings = await postgrest_get("v_meeting_room_bookings", {"id": f"eq.{booking_id}"})
        if not bookings:
            return {"success": False, "error": "預約不存在"}

        booking = bookings[0]

        if not booking.get("line_user_id"):
            return {"success": False, "error": f"客戶 {booking['customer_name']} 沒有綁定 LINE"}

        # 格式化日期和時間
        booking_date = booking["booking_date"]
        if isinstance(booking_date, str):
            date_obj = datetime.strptime(booking_date, "%Y-%m-%d")
        else:
            date_obj = booking_date

        weekday_names = ["一", "二", "三", "四", "五", "六", "日"]
        weekday = weekday_names[date_obj.weekday()]

        date_formatted = f"{date_obj.month}/{date_obj.day}（{weekday}）"
        time_formatted = f"{booking['start_time'][:5]} - {booking['end_time'][:5]}"

        # 發送提醒訊息
        message = (
            f"📅 會議室預約提醒\n\n"
            f"親愛的 {booking['customer_name']} 您好，\n\n"
            f"提醒您即將到來的會議室預約：\n\n"
            f"📍 {booking['branch_name']} {booking['room_name']}\n"
            f"📆 {date_formatted}\n"
            f"⏰ {time_formatted}\n"
        )

        if booking.get("purpose"):
            message += f"📝 {booking['purpose']}\n"

        message += f"\n如需取消或變更，請提前告知我們 🙏"

        messages = [{"type": "text", "text": message}]
        result = await send_line_push(booking["line_user_id"], messages)

        if result.get("success"):
            # 更新提醒狀態
            await postgrest_patch(
                "meeting_room_bookings",
                {"id": f"eq.{booking_id}"},
                {"reminder_sent": True}
            )
            return {
                "success": True,
                "message": f"已發送提醒給 {booking['customer_name']}"
            }
        else:
            return result

    except Exception as e:
        logger.error(f"booking_send_reminder error: {e}")
        raise Exception(f"發送提醒失敗: {e}")


async def booking_get_by_line_user(line_user_id: str, upcoming_only: bool = True) -> Dict[str, Any]:
    """
    取得客戶的預約列表（給 LINE Bot 用）

    Args:
        line_user_id: LINE User ID
        upcoming_only: 是否只取得未來的預約

    Returns:
        預約列表
    """
    try:
        # 先取得客戶 ID
        customers = await postgrest_get("customers", {"line_user_id": f"eq.{line_user_id}"})
        if not customers:
            return {"success": False, "error": "找不到客戶資料，請先綁定 LINE"}

        customer = customers[0]

        params = {
            "customer_id": f"eq.{customer['id']}",
            "order": "booking_date,start_time",
            "limit": 10
        }

        if upcoming_only:
            params["booking_date"] = f"gte.{date.today().isoformat()}"
            params["status"] = "eq.confirmed"

        bookings = await postgrest_get("v_meeting_room_bookings", params)

        return {
            "success": True,
            "customer_id": customer["id"],
            "customer_name": customer["name"],
            "bookings": bookings,
            "total": len(bookings)
        }
    except Exception as e:
        logger.error(f"booking_get_by_line_user error: {e}")
        raise Exception(f"取得預約列表失敗: {e}")
