"""
Hour Jungle CRM - LINE Webhook Handler
LINE Bot Webhook 處理（會議室預約對話流程）
"""

import os
import json
import hmac
import hashlib
import base64
import logging
from datetime import datetime, date, timedelta
from typing import Dict, Any, Optional
from urllib.parse import parse_qs

import httpx
import redis.asyncio as redis

from .line_tools import send_line_push, log_to_brain
from .booking_tools import (
    booking_list_rooms,
    booking_check_availability,
    booking_create,
    booking_cancel,
    booking_get_by_line_user
)

logger = logging.getLogger(__name__)

# LINE 設定
LINE_CHANNEL_SECRET = os.getenv("LINE_CHANNEL_SECRET", "")
LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "")

# Redis 設定（對話狀態存儲）
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
STATE_TTL = 1800  # 對話狀態 TTL: 30 分鐘

# Redis 客戶端
_redis_client = None


async def get_redis() -> redis.Redis:
    """取得 Redis 客戶端"""
    global _redis_client
    if _redis_client is None:
        _redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    return _redis_client


def verify_signature(body: bytes, signature: str) -> bool:
    """驗證 LINE Webhook 簽名"""
    if not LINE_CHANNEL_SECRET:
        logger.warning("LINE_CHANNEL_SECRET not configured")
        return False

    hash_value = hmac.new(
        LINE_CHANNEL_SECRET.encode('utf-8'),
        body,
        hashlib.sha256
    ).digest()
    expected = base64.b64encode(hash_value).decode('utf-8')
    return hmac.compare_digest(signature, expected)


# ============================================================================
# 對話狀態管理
# ============================================================================

async def get_user_state(line_user_id: str) -> Optional[Dict]:
    """取得用戶對話狀態"""
    r = await get_redis()
    state = await r.get(f"booking_state:{line_user_id}")
    if state:
        return json.loads(state)
    return None


async def set_user_state(line_user_id: str, state: Dict):
    """設定用戶對話狀態"""
    r = await get_redis()
    await r.setex(
        f"booking_state:{line_user_id}",
        STATE_TTL,
        json.dumps(state, ensure_ascii=False)
    )


async def clear_user_state(line_user_id: str):
    """清除用戶對話狀態"""
    r = await get_redis()
    await r.delete(f"booking_state:{line_user_id}")


# ============================================================================
# Flex Message 模板
# ============================================================================

def create_room_selection_flex(rooms: list) -> Dict:
    """建立會議室選擇 Flex Message"""
    buttons = []
    for room in rooms:
        buttons.append({
            "type": "button",
            "action": {
                "type": "postback",
                "label": f"{room['branch_name']} {room['name']} ({room['capacity']}人)",
                "data": f"action=book&step=room&room_id={room['id']}"
            },
            "style": "primary",
            "margin": "sm"
        })

    return {
        "type": "flex",
        "altText": "選擇會議室",
        "contents": {
            "type": "bubble",
            "header": {
                "type": "box",
                "layout": "vertical",
                "contents": [
                    {"type": "text", "text": "📅 會議室預約", "weight": "bold", "size": "lg"}
                ]
            },
            "body": {
                "type": "box",
                "layout": "vertical",
                "contents": [
                    {"type": "text", "text": "請選擇會議室：", "margin": "md"},
                    *buttons
                ]
            }
        }
    }


def create_date_selection_flex() -> Dict:
    """建立日期選擇 Flex Message"""
    today = date.today()
    buttons = []

    for i in range(7):
        d = today + timedelta(days=i)
        weekday_names = ["一", "二", "三", "四", "五", "六", "日"]
        weekday = weekday_names[d.weekday()]

        if i == 0:
            label = f"今天 ({d.month}/{d.day})"
        elif i == 1:
            label = f"明天 ({d.month}/{d.day})"
        else:
            label = f"{d.month}/{d.day}（{weekday}）"

        buttons.append({
            "type": "button",
            "action": {
                "type": "postback",
                "label": label,
                "data": f"action=book&step=date&date={d.isoformat()}"
            },
            "style": "secondary",
            "margin": "sm"
        })

    return {
        "type": "flex",
        "altText": "選擇日期",
        "contents": {
            "type": "bubble",
            "header": {
                "type": "box",
                "layout": "vertical",
                "contents": [
                    {"type": "text", "text": "📆 選擇日期", "weight": "bold", "size": "lg"}
                ]
            },
            "body": {
                "type": "box",
                "layout": "vertical",
                "contents": buttons
            }
        }
    }


def create_time_selection_flex(available_slots: list, selected_date: str, all_busy_times: list = None) -> Dict:
    """建立時段選擇 Flex Message（顯示可預約和已被訂的時段）"""
    from datetime import datetime, time, timedelta

    # 生成所有時段（09:00 ~ 18:00，每小時一格）
    all_slots = []
    for hour in range(9, 18):
        slot_start = f"{hour:02d}:00"
        all_slots.append(slot_start)

    # 建立可用時段的 set（方便查詢）
    available_set = set(s["start"][:5] for s in available_slots)

    # 分成上午和下午
    morning_hours = [s for s in all_slots if int(s.split(":")[0]) < 12]
    afternoon_hours = [s for s in all_slots if int(s.split(":")[0]) >= 12]

    def create_time_buttons(hours: list) -> list:
        buttons = []
        for slot_start in hours:
            is_available = slot_start in available_set

            if is_available:
                # 可預約 - 綠色按鈕
                buttons.append({
                    "type": "button",
                    "action": {
                        "type": "postback",
                        "label": f"✅ {slot_start}",
                        "data": f"action=book&step=start_time&start={slot_start}"
                    },
                    "style": "primary",
                    "margin": "xs",
                    "height": "sm"
                })
            else:
                # 已被訂 - 灰色按鈕（不可點擊，使用 message action 顯示提示）
                buttons.append({
                    "type": "button",
                    "action": {
                        "type": "message",
                        "label": f"❌ {slot_start}",
                        "text": f"抱歉，{slot_start} 已被預約囉～請選擇其他時段"
                    },
                    "style": "secondary",
                    "margin": "xs",
                    "height": "sm"
                })
        return buttons

    bubbles = []

    if morning_hours:
        bubbles.append({
            "type": "bubble",
            "size": "kilo",
            "header": {
                "type": "box",
                "layout": "vertical",
                "contents": [
                    {"type": "text", "text": "🌅 上午", "weight": "bold", "size": "md"},
                    {"type": "text", "text": "✅可預約 ❌已被訂", "size": "xxs", "color": "#888888"}
                ]
            },
            "body": {
                "type": "box",
                "layout": "vertical",
                "contents": create_time_buttons(morning_hours)
            }
        })

    if afternoon_hours:
        bubbles.append({
            "type": "bubble",
            "size": "kilo",
            "header": {
                "type": "box",
                "layout": "vertical",
                "contents": [
                    {"type": "text", "text": "🌇 下午", "weight": "bold", "size": "md"},
                    {"type": "text", "text": "✅可預約 ❌已被訂", "size": "xxs", "color": "#888888"}
                ]
            },
            "body": {
                "type": "box",
                "layout": "vertical",
                "contents": create_time_buttons(afternoon_hours)
            }
        })

    # 檢查是否所有時段都已被訂
    if not available_slots:
        return {
            "type": "text",
            "text": f"😢 {selected_date} 已無可用時段，請選擇其他日期。"
        }

    return {
        "type": "flex",
        "altText": "選擇開始時間",
        "contents": {
            "type": "carousel",
            "contents": bubbles
        }
    }


def create_duration_selection_flex(start_time: str) -> Dict:
    """建立預約時長選擇"""
    # 計算可選的結束時間（最多到 18:00）
    start_hour, start_min = map(int, start_time.split(":"))
    durations = [30, 60, 90, 120, 150, 180]  # 30分到3小時

    buttons = []
    for dur in durations:
        end_hour = start_hour + (start_min + dur) // 60
        end_min = (start_min + dur) % 60
        end_time = f"{end_hour:02d}:{end_min:02d}"

        if end_hour > 18 or (end_hour == 18 and end_min > 0):
            break

        if dur < 60:
            label = f"{dur} 分鐘（到 {end_time}）"
        else:
            hours = dur // 60
            mins = dur % 60
            if mins:
                label = f"{hours} 小時 {mins} 分（到 {end_time}）"
            else:
                label = f"{hours} 小時（到 {end_time}）"

        buttons.append({
            "type": "button",
            "action": {
                "type": "postback",
                "label": label,
                "data": f"action=book&step=end_time&end={end_time}"
            },
            "style": "secondary",
            "margin": "sm"
        })

    return {
        "type": "flex",
        "altText": "選擇預約時長",
        "contents": {
            "type": "bubble",
            "header": {
                "type": "box",
                "layout": "vertical",
                "contents": [
                    {"type": "text", "text": "⏱️ 選擇時長", "weight": "bold", "size": "lg"}
                ]
            },
            "body": {
                "type": "box",
                "layout": "vertical",
                "contents": [
                    {"type": "text", "text": f"開始時間: {start_time}", "margin": "md"},
                    *buttons
                ]
            }
        }
    }


def create_confirm_booking_flex(state: Dict, room: Dict, customer_name: str) -> Dict:
    """建立預約確認 Flex Message"""
    booking_date = datetime.strptime(state["date"], "%Y-%m-%d")
    weekday_names = ["一", "二", "三", "四", "五", "六", "日"]
    weekday = weekday_names[booking_date.weekday()]

    return {
        "type": "flex",
        "altText": "確認預約",
        "contents": {
            "type": "bubble",
            "header": {
                "type": "box",
                "layout": "vertical",
                "contents": [
                    {"type": "text", "text": "✅ 確認預約", "weight": "bold", "size": "lg", "color": "#27ACB2"}
                ]
            },
            "body": {
                "type": "box",
                "layout": "vertical",
                "contents": [
                    {"type": "text", "text": f"📍 {room['branch_name']} {room['name']}", "margin": "md"},
                    {"type": "text", "text": f"📆 {booking_date.month}/{booking_date.day}（{weekday}）", "margin": "sm"},
                    {"type": "text", "text": f"⏰ {state['start_time']} - {state['end_time']}", "margin": "sm"},
                    {"type": "text", "text": f"👤 {customer_name}", "margin": "sm"},
                    {"type": "separator", "margin": "lg"},
                    {"type": "text", "text": "請確認以上預約內容", "margin": "md", "size": "sm", "color": "#888888"}
                ]
            },
            "footer": {
                "type": "box",
                "layout": "horizontal",
                "contents": [
                    {
                        "type": "button",
                        "action": {
                            "type": "postback",
                            "label": "確認預約",
                            "data": "action=book&step=confirm&confirm=yes"
                        },
                        "style": "primary"
                    },
                    {
                        "type": "button",
                        "action": {
                            "type": "postback",
                            "label": "取消",
                            "data": "action=book&step=confirm&confirm=no"
                        },
                        "style": "secondary",
                        "margin": "sm"
                    }
                ]
            }
        }
    }


def create_booking_success_flex(booking: Dict) -> Dict:
    """建立預約成功 Flex Message"""
    return {
        "type": "flex",
        "altText": "預約成功！",
        "contents": {
            "type": "bubble",
            "header": {
                "type": "box",
                "layout": "vertical",
                "backgroundColor": "#27ACB2",
                "contents": [
                    {"type": "text", "text": "🎉 預約成功！", "weight": "bold", "size": "lg", "color": "#ffffff"}
                ]
            },
            "body": {
                "type": "box",
                "layout": "vertical",
                "contents": [
                    {"type": "text", "text": f"預約編號: {booking['booking_number']}", "weight": "bold", "margin": "md"},
                    {"type": "text", "text": f"📍 {booking['room_name']}", "margin": "sm"},
                    {"type": "text", "text": f"📆 {booking['date']}", "margin": "sm"},
                    {"type": "text", "text": f"⏰ {booking['start_time']} - {booking['end_time']}", "margin": "sm"},
                    {"type": "separator", "margin": "lg"},
                    {"type": "text", "text": "我們會在會議前 1 小時提醒您", "margin": "md", "size": "sm", "color": "#888888"}
                ]
            },
            "footer": {
                "type": "box",
                "layout": "vertical",
                "contents": [
                    {
                        "type": "box",
                        "layout": "horizontal",
                        "contents": [
                            {
                                "type": "button",
                                "action": {
                                    "type": "postback",
                                    "label": "查看我的預約",
                                    "data": "action=list"
                                },
                                "style": "primary",
                                "flex": 1
                            },
                            {
                                "type": "button",
                                "action": {
                                    "type": "postback",
                                    "label": "重新預約",
                                    "data": "action=start"
                                },
                                "style": "secondary",
                                "flex": 1,
                                "margin": "sm"
                            }
                        ]
                    },
                    {
                        "type": "button",
                        "action": {
                            "type": "postback",
                            "label": "取消此預約",
                            "data": f"action=cancel&booking_id={booking['id']}"
                        },
                        "style": "link",
                        "height": "sm",
                        "margin": "sm"
                    }
                ]
            }
        }
    }


def create_my_bookings_flex(bookings: list) -> Dict:
    """建立我的預約列表 Flex Message"""
    if not bookings:
        return {
            "type": "text",
            "text": "📭 您目前沒有預約的會議室。\n\n輸入「預約」開始預約！"
        }

    bubbles = []
    for booking in bookings[:5]:  # 最多顯示 5 筆
        booking_date = booking["booking_date"]
        if isinstance(booking_date, str):
            date_obj = datetime.strptime(booking_date, "%Y-%m-%d")
        else:
            date_obj = booking_date

        weekday_names = ["一", "二", "三", "四", "五", "六", "日"]
        weekday = weekday_names[date_obj.weekday()]

        bubbles.append({
            "type": "bubble",
            "size": "kilo",
            "body": {
                "type": "box",
                "layout": "vertical",
                "contents": [
                    {"type": "text", "text": booking["booking_number"], "weight": "bold", "size": "sm"},
                    {"type": "text", "text": f"{booking['branch_name']} {booking['room_name']}", "margin": "sm"},
                    {"type": "text", "text": f"{date_obj.month}/{date_obj.day}（{weekday}）", "margin": "xs", "size": "sm"},
                    {"type": "text", "text": f"{booking['start_time'][:5]} - {booking['end_time'][:5]}", "size": "sm"}
                ]
            },
            "footer": {
                "type": "box",
                "layout": "vertical",
                "contents": [
                    {
                        "type": "button",
                        "action": {
                            "type": "postback",
                            "label": "取消預約",
                            "data": f"action=cancel&booking_id={booking['id']}"
                        },
                        "style": "secondary",
                        "height": "sm"
                    }
                ]
            }
        })

    return {
        "type": "flex",
        "altText": "您的預約",
        "contents": {
            "type": "carousel",
            "contents": bubbles
        }
    }


# ============================================================================
# 事件處理
# ============================================================================

async def handle_line_event(event: Dict) -> Dict[str, Any]:
    """處理 LINE 事件"""
    event_type = event.get("type")

    if event_type == "message":
        return await handle_message_event(event)
    elif event_type == "postback":
        return await handle_postback_event(event)
    elif event_type == "follow":
        return await handle_follow_event(event)

    return {"handled": False}


async def handle_message_event(event: Dict) -> Dict[str, Any]:
    """處理訊息事件"""
    import asyncio

    message = event.get("message", {})
    message_type = message.get("type")
    line_user_id = event["source"]["userId"]
    action_timestamp = datetime.utcnow().isoformat() + "Z"

    if message_type != "text":
        return {"handled": False}

    text = message.get("text", "").strip()

    # 檢查是否在對話流程中（用於取得用戶名稱）
    state = await get_user_state(line_user_id)
    customer_name = state.get("customer_name", "用戶") if state else "用戶"

    # 指令處理
    if text in ["預約", "預約會議室", "book", "booking"]:
        # 記錄用戶開始預約
        asyncio.create_task(log_to_brain(
            sender_id=line_user_id,
            sender_name=customer_name,
            content=f"輸入指令：{text}（開始預約流程）",
            message_type="user_action",
            timestamp=action_timestamp
        ))
        return await start_booking_flow(line_user_id)
    elif text in ["我的預約", "查詢預約", "mybooking", "查詢"]:
        asyncio.create_task(log_to_brain(
            sender_id=line_user_id,
            sender_name=customer_name,
            content=f"輸入指令：{text}（查詢預約）",
            message_type="user_action",
            timestamp=action_timestamp
        ))
        return await show_my_bookings(line_user_id)
    elif text in ["取消預約", "取消"]:
        asyncio.create_task(log_to_brain(
            sender_id=line_user_id,
            sender_name=customer_name,
            content=f"輸入指令：{text}（取消預約）",
            message_type="user_action",
            timestamp=action_timestamp
        ))
        return await show_cancel_options(line_user_id)
    elif text in ["幫助", "help", "？", "?"]:
        asyncio.create_task(log_to_brain(
            sender_id=line_user_id,
            sender_name=customer_name,
            content=f"輸入指令：{text}（查看幫助）",
            message_type="user_action",
            timestamp=action_timestamp
        ))
        return await send_help_message(line_user_id)

    # 檢查是否在對話流程中
    if state:
        # 可能是輸入目的等文字內容
        if state.get("awaiting_purpose"):
            # 記錄用戶輸入的會議目的
            asyncio.create_task(log_to_brain(
                sender_id=line_user_id,
                sender_name=customer_name,
                content=f"輸入會議目的：{text}",
                message_type="user_action",
                timestamp=action_timestamp
            ))
            state["purpose"] = text
            state["awaiting_purpose"] = False
            await set_user_state(line_user_id, state)
            # 繼續到確認步驟
            return await show_confirm_booking(line_user_id, state)

    return {"handled": False}


async def handle_postback_event(event: Dict) -> Dict[str, Any]:
    """處理 Postback 事件"""
    import asyncio

    line_user_id = event["source"]["userId"]
    data = event.get("postback", {}).get("data", "")
    action_timestamp = datetime.utcnow().isoformat() + "Z"

    # 解析 postback data
    params = dict(parse_qs(data))
    action = params.get("action", [""])[0]
    step = params.get("step", [""])[0]

    # 取得用戶名稱（從 state 或查詢客戶資料）
    state = await get_user_state(line_user_id)
    customer_name = state.get("customer_name", "用戶") if state else "用戶"

    if action == "book":
        return await handle_booking_postback(line_user_id, step, params)
    elif action == "cancel":
        booking_id = params.get("booking_id", [None])[0]
        if booking_id:
            # 記錄用戶取消預約操作
            asyncio.create_task(log_to_brain(
                sender_id=line_user_id,
                sender_name=customer_name,
                content=f"取消預約 (ID: {booking_id})",
                message_type="user_action",
                timestamp=action_timestamp
            ))
            return await cancel_booking(line_user_id, int(booking_id))
    elif action == "list":
        # 記錄用戶查看預約操作
        asyncio.create_task(log_to_brain(
            sender_id=line_user_id,
            sender_name=customer_name,
            content="查看我的預約",
            message_type="user_action",
            timestamp=action_timestamp
        ))
        return await show_my_bookings(line_user_id)
    elif action == "start":
        # 記錄用戶重新開始預約操作
        asyncio.create_task(log_to_brain(
            sender_id=line_user_id,
            sender_name=customer_name,
            content="開始新的會議室預約",
            message_type="user_action",
            timestamp=action_timestamp
        ))
        return await start_booking_flow(line_user_id)

    return {"handled": False}


async def handle_follow_event(event: Dict) -> Dict[str, Any]:
    """處理追蹤事件（用戶加入）"""
    line_user_id = event["source"]["userId"]

    welcome_message = (
        "歡迎使用 Hour Jungle 會議室預約系統！ 🎉\n\n"
        "您可以使用以下功能：\n"
        "📅 輸入「預約」- 預約會議室\n"
        "📋 輸入「我的預約」- 查看預約\n"
        "❌ 輸入「取消預約」- 取消預約\n"
        "❓ 輸入「幫助」- 查看說明"
    )

    await send_line_push(line_user_id, [{"type": "text", "text": welcome_message}])
    return {"handled": True}


# ============================================================================
# 預約流程
# ============================================================================

async def start_booking_flow(line_user_id: str) -> Dict[str, Any]:
    """開始預約流程"""
    # 驗證用戶是否為客戶
    from .booking_tools import postgrest_get

    customers = await postgrest_get("customers", {
        "line_user_id": f"eq.{line_user_id}",
        "status": "eq.active"
    })

    if not customers:
        await send_line_push(line_user_id, [{
            "type": "text",
            "text": "😢 抱歉，會議室預約服務僅限 Hour Jungle 現有客戶使用。\n\n如有需要，請聯繫我們的服務人員。"
        }])
        return {"handled": True}

    customer = customers[0]

    # 檢查客戶是否有合約（營業登記或辦公室）
    contracts = await postgrest_get("contracts", {
        "customer_id": f"eq.{customer['id']}",
        "status": "eq.active",
        "contract_type": "in.(virtual_office,coworking_fixed,coworking_flexible)"
    })

    if not contracts:
        await send_line_push(line_user_id, [{
            "type": "text",
            "text": "😢 抱歉，會議室預約服務僅限營業登記或辦公室合約客戶使用。"
        }])
        return {"handled": True}

    # 取得會議室列表
    rooms_result = await booking_list_rooms()
    if not rooms_result.get("success") or not rooms_result.get("rooms"):
        await send_line_push(line_user_id, [{
            "type": "text",
            "text": "😢 目前沒有可預約的會議室，請稍後再試。"
        }])
        return {"handled": True}

    rooms = rooms_result["rooms"]

    # 初始化對話狀態
    await set_user_state(line_user_id, {
        "action": "booking",
        "step": "select_room",
        "customer_id": customer["id"],
        "customer_name": customer["name"]
    })

    # 發送會議室選擇
    flex_message = create_room_selection_flex(rooms)
    await send_line_push(line_user_id, [flex_message])

    return {"handled": True}


async def handle_booking_postback(line_user_id: str, step: str, params: Dict) -> Dict[str, Any]:
    """處理預約流程 Postback"""
    import asyncio
    from .booking_tools import postgrest_get

    state = await get_user_state(line_user_id)
    if not state:
        await send_line_push(line_user_id, [{
            "type": "text",
            "text": "⏰ 對話已逾時，請重新輸入「預約」開始。"
        }])
        return {"handled": True}

    # 用於記錄到 Brain 的用戶名稱
    customer_name = state.get("customer_name", "用戶")
    action_timestamp = datetime.utcnow().isoformat() + "Z"

    if step == "room":
        # 選擇了會議室
        room_id = int(params.get("room_id", [0])[0])
        state["room_id"] = room_id
        state["step"] = "select_date"
        await set_user_state(line_user_id, state)

        # 記錄用戶操作到 Brain（取得會議室名稱）
        rooms = await postgrest_get("meeting_rooms", {"id": f"eq.{room_id}"})
        room_name = rooms[0]["name"] if rooms else f"會議室{room_id}"
        asyncio.create_task(log_to_brain(
            sender_id=line_user_id,
            sender_name=customer_name,
            content=f"選擇會議室：{room_name}",
            message_type="user_action",
            timestamp=action_timestamp
        ))

        # 發送日期選擇
        flex_message = create_date_selection_flex()
        await send_line_push(line_user_id, [flex_message])

    elif step == "date":
        # 選擇了日期
        selected_date = params.get("date", [""])[0]
        state["date"] = selected_date
        state["step"] = "select_time"
        await set_user_state(line_user_id, state)

        # 記錄用戶操作到 Brain
        asyncio.create_task(log_to_brain(
            sender_id=line_user_id,
            sender_name=customer_name,
            content=f"選擇日期：{selected_date}",
            message_type="user_action",
            timestamp=action_timestamp
        ))

        # 查詢可用時段
        availability = await booking_check_availability(state["room_id"], selected_date)
        if not availability.get("success"):
            await send_line_push(line_user_id, [{
                "type": "text",
                "text": "😢 查詢時段失敗，請重試。"
            }])
            return {"handled": True}

        # 發送時段選擇
        flex_message = create_time_selection_flex(
            availability.get("available_slots", []),
            selected_date
        )
        await send_line_push(line_user_id, [flex_message])

    elif step == "start_time":
        # 選擇了開始時間
        start_time = params.get("start", [""])[0]
        state["start_time"] = start_time
        state["step"] = "select_duration"
        await set_user_state(line_user_id, state)

        # 記錄用戶操作到 Brain
        asyncio.create_task(log_to_brain(
            sender_id=line_user_id,
            sender_name=customer_name,
            content=f"選擇開始時間：{start_time}",
            message_type="user_action",
            timestamp=action_timestamp
        ))

        # 發送時長選擇
        flex_message = create_duration_selection_flex(start_time)
        await send_line_push(line_user_id, [flex_message])

    elif step == "end_time":
        # 選擇了結束時間
        end_time = params.get("end", [""])[0]
        state["end_time"] = end_time
        state["step"] = "confirm"
        await set_user_state(line_user_id, state)

        # 記錄用戶操作到 Brain
        asyncio.create_task(log_to_brain(
            sender_id=line_user_id,
            sender_name=customer_name,
            content=f"選擇結束時間：{end_time}（{state.get('date')} {state.get('start_time')}-{end_time}）",
            message_type="user_action",
            timestamp=action_timestamp
        ))

        # 顯示確認畫面
        return await show_confirm_booking(line_user_id, state)

    elif step == "confirm":
        # 確認或取消
        confirm = params.get("confirm", [""])[0]
        if confirm == "yes":
            # 記錄用戶確認預約
            asyncio.create_task(log_to_brain(
                sender_id=line_user_id,
                sender_name=customer_name,
                content=f"確認預約會議室（{state.get('date')} {state.get('start_time')}-{state.get('end_time')}）",
                message_type="user_action",
                timestamp=action_timestamp
            ))
            return await execute_booking(line_user_id, state)
        else:
            # 記錄用戶取消預約
            asyncio.create_task(log_to_brain(
                sender_id=line_user_id,
                sender_name=customer_name,
                content="取消預約流程",
                message_type="user_action",
                timestamp=action_timestamp
            ))
            await clear_user_state(line_user_id)
            await send_line_push(line_user_id, [{
                "type": "text",
                "text": "已取消預約。\n\n輸入「預約」重新開始。"
            }])

    return {"handled": True}


async def show_confirm_booking(line_user_id: str, state: Dict) -> Dict[str, Any]:
    """顯示預約確認"""
    from .booking_tools import postgrest_get

    # 取得會議室資訊
    rooms = await postgrest_get("meeting_rooms", {"id": f"eq.{state['room_id']}"})
    if not rooms:
        await send_line_push(line_user_id, [{"type": "text", "text": "會議室資訊錯誤"}])
        return {"handled": True}

    room = rooms[0]

    # 取得場館名稱
    branches = await postgrest_get("branches", {"id": f"eq.{room['branch_id']}"})
    room["branch_name"] = branches[0]["name"] if branches else ""

    flex_message = create_confirm_booking_flex(state, room, state["customer_name"])
    await send_line_push(line_user_id, [flex_message])

    return {"handled": True}


async def execute_booking(line_user_id: str, state: Dict) -> Dict[str, Any]:
    """執行預約"""
    result = await booking_create(
        room_id=state["room_id"],
        customer_id=state["customer_id"],
        date_str=state["date"],
        start_time=state["start_time"],
        end_time=state["end_time"],
        purpose=state.get("purpose"),
        created_by="line"
    )

    await clear_user_state(line_user_id)

    if result.get("success"):
        flex_message = create_booking_success_flex(result["booking"])
        await send_line_push(line_user_id, [flex_message])
    else:
        error_msg = result.get("error", "預約失敗")
        await send_line_push(line_user_id, [{
            "type": "text",
            "text": f"😢 {error_msg}\n\n請輸入「預約」重試。"
        }])

    return {"handled": True}


async def show_my_bookings(line_user_id: str) -> Dict[str, Any]:
    """顯示我的預約"""
    result = await booking_get_by_line_user(line_user_id)

    if not result.get("success"):
        await send_line_push(line_user_id, [{
            "type": "text",
            "text": result.get("error", "查詢失敗")
        }])
        return {"handled": True}

    flex_message = create_my_bookings_flex(result.get("bookings", []))
    await send_line_push(line_user_id, [flex_message])

    return {"handled": True}


async def show_cancel_options(line_user_id: str) -> Dict[str, Any]:
    """顯示可取消的預約"""
    return await show_my_bookings(line_user_id)


async def cancel_booking(line_user_id: str, booking_id: int) -> Dict[str, Any]:
    """取消預約"""
    result = await booking_cancel(booking_id, reason="用戶透過 LINE 取消")

    if result.get("success"):
        await send_line_push(line_user_id, [{
            "type": "text",
            "text": f"✅ {result.get('message', '預約已取消')}"
        }])
    else:
        await send_line_push(line_user_id, [{
            "type": "text",
            "text": f"😢 {result.get('error', '取消失敗')}"
        }])

    return {"handled": True}


async def send_help_message(line_user_id: str) -> Dict[str, Any]:
    """發送幫助訊息"""
    help_text = (
        "📚 Hour Jungle 會議室預約說明\n\n"
        "【可用指令】\n"
        "📅 預約 - 預約會議室\n"
        "📋 我的預約 - 查看您的預約\n"
        "❌ 取消預約 - 取消預約\n\n"
        "【預約規則】\n"
        "• 僅限現有客戶使用\n"
        "• 營業時間 09:00-18:00\n"
        "• 最小預約單位 30 分鐘\n"
        "• 會議前 1 小時自動提醒\n\n"
        "如有問題請聯繫我們 📞"
    )

    await send_line_push(line_user_id, [{"type": "text", "text": help_text}])
    return {"handled": True}
