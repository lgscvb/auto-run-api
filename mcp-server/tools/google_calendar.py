"""
Hour Jungle CRM - Google Calendar Service
會議室預約 Google Calendar 整合服務
"""

import os
import logging
from datetime import datetime, date, time, timedelta
from typing import Dict, Any, Optional, List

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)

# 服務帳號 JSON 路徑
GOOGLE_CALENDAR_CREDENTIALS = os.getenv(
    "GOOGLE_CALENDAR_CREDENTIALS",
    "/secrets/calendar-sa.json"
)

# Calendar API 權限範圍
SCOPES = ['https://www.googleapis.com/auth/calendar']


class GoogleCalendarService:
    """Google Calendar 服務類別"""

    def __init__(self, credentials_path: str = None):
        """
        初始化 Google Calendar 服務

        Args:
            credentials_path: 服務帳號 JSON 路徑
        """
        self.credentials_path = credentials_path or GOOGLE_CALENDAR_CREDENTIALS
        self._service = None
        self._credentials = None

    def _get_credentials(self):
        """取得服務帳號憑證"""
        if not self._credentials:
            if not os.path.exists(self.credentials_path):
                raise FileNotFoundError(
                    f"Google Calendar credentials not found: {self.credentials_path}"
                )
            self._credentials = service_account.Credentials.from_service_account_file(
                self.credentials_path,
                scopes=SCOPES
            )
        return self._credentials

    def _get_service(self):
        """取得 Calendar API 服務"""
        if not self._service:
            credentials = self._get_credentials()
            self._service = build('calendar', 'v3', credentials=credentials)
        return self._service

    def create_calendar(self, summary: str, description: str = None) -> Dict[str, Any]:
        """
        建立新的 Calendar

        Args:
            summary: Calendar 名稱
            description: Calendar 描述

        Returns:
            新建立的 Calendar 資訊
        """
        service = self._get_service()

        calendar_body = {
            'summary': summary,
            'timeZone': 'Asia/Taipei'
        }
        if description:
            calendar_body['description'] = description

        try:
            calendar = service.calendars().insert(body=calendar_body).execute()
            logger.info(f"Created calendar: {calendar['id']}")
            return {
                'success': True,
                'calendar_id': calendar['id'],
                'summary': calendar['summary']
            }
        except HttpError as e:
            logger.error(f"Failed to create calendar: {e}")
            return {
                'success': False,
                'error': str(e)
            }

    def share_calendar(
        self,
        calendar_id: str,
        email: str,
        role: str = "reader"
    ) -> Dict[str, Any]:
        """
        分享行事曆給指定使用者

        Args:
            calendar_id: Calendar ID
            email: 要分享的 Email
            role: 權限角色 (reader, writer, owner)

        Returns:
            分享結果
        """
        service = self._get_service()

        rule = {
            'scope': {
                'type': 'user',
                'value': email
            },
            'role': role
        }

        try:
            created_rule = service.acl().insert(
                calendarId=calendar_id,
                body=rule
            ).execute()

            logger.info(f"Shared calendar {calendar_id} with {email} as {role}")
            return {
                'success': True,
                'email': email,
                'role': role,
                'rule_id': created_rule.get('id')
            }
        except HttpError as e:
            logger.error(f"Failed to share calendar: {e}")
            return {
                'success': False,
                'error': str(e)
            }

    def create_event(
        self,
        calendar_id: str,
        title: str,
        start_datetime: datetime,
        end_datetime: datetime,
        description: str = None,
        location: str = None,
        attendees: List[str] = None
    ) -> Dict[str, Any]:
        """
        建立 Calendar 事件

        Args:
            calendar_id: Calendar ID
            title: 事件標題
            start_datetime: 開始時間
            end_datetime: 結束時間
            description: 描述
            location: 地點
            attendees: 參與者 email 列表

        Returns:
            建立的事件資訊
        """
        service = self._get_service()

        event_body = {
            'summary': title,
            'start': {
                'dateTime': start_datetime.isoformat(),
                'timeZone': 'Asia/Taipei'
            },
            'end': {
                'dateTime': end_datetime.isoformat(),
                'timeZone': 'Asia/Taipei'
            }
        }

        if description:
            event_body['description'] = description
        if location:
            event_body['location'] = location
        if attendees:
            event_body['attendees'] = [{'email': email} for email in attendees]

        try:
            event = service.events().insert(
                calendarId=calendar_id,
                body=event_body
            ).execute()

            logger.info(f"Created event: {event['id']} in calendar {calendar_id}")
            return {
                'success': True,
                'event_id': event['id'],
                'html_link': event.get('htmlLink')
            }
        except HttpError as e:
            logger.error(f"Failed to create event: {e}")
            return {
                'success': False,
                'error': str(e)
            }

    def update_event(
        self,
        calendar_id: str,
        event_id: str,
        title: str = None,
        start_datetime: datetime = None,
        end_datetime: datetime = None,
        description: str = None,
        location: str = None
    ) -> Dict[str, Any]:
        """
        更新 Calendar 事件

        Args:
            calendar_id: Calendar ID
            event_id: Event ID
            其他參數同 create_event

        Returns:
            更新後的事件資訊
        """
        service = self._get_service()

        try:
            # 先取得現有事件
            event = service.events().get(
                calendarId=calendar_id,
                eventId=event_id
            ).execute()

            # 更新欄位
            if title:
                event['summary'] = title
            if start_datetime:
                event['start'] = {
                    'dateTime': start_datetime.isoformat(),
                    'timeZone': 'Asia/Taipei'
                }
            if end_datetime:
                event['end'] = {
                    'dateTime': end_datetime.isoformat(),
                    'timeZone': 'Asia/Taipei'
                }
            if description is not None:
                event['description'] = description
            if location is not None:
                event['location'] = location

            updated_event = service.events().update(
                calendarId=calendar_id,
                eventId=event_id,
                body=event
            ).execute()

            logger.info(f"Updated event: {event_id}")
            return {
                'success': True,
                'event_id': updated_event['id']
            }
        except HttpError as e:
            logger.error(f"Failed to update event: {e}")
            return {
                'success': False,
                'error': str(e)
            }

    def delete_event(self, calendar_id: str, event_id: str) -> Dict[str, Any]:
        """
        刪除 Calendar 事件

        Args:
            calendar_id: Calendar ID
            event_id: Event ID

        Returns:
            刪除結果
        """
        service = self._get_service()

        try:
            service.events().delete(
                calendarId=calendar_id,
                eventId=event_id
            ).execute()

            logger.info(f"Deleted event: {event_id}")
            return {'success': True}
        except HttpError as e:
            logger.error(f"Failed to delete event: {e}")
            return {
                'success': False,
                'error': str(e)
            }

    def get_busy_times(
        self,
        calendar_id: str,
        date_to_check: date,
        time_min: time = None,
        time_max: time = None
    ) -> Dict[str, Any]:
        """
        取得指定日期的忙碌時段

        Args:
            calendar_id: Calendar ID
            date_to_check: 要查詢的日期
            time_min: 查詢開始時間（預設 09:00）
            time_max: 查詢結束時間（預設 18:00）

        Returns:
            忙碌時段列表
        """
        service = self._get_service()

        # 預設營業時間
        if time_min is None:
            time_min = time(9, 0)
        if time_max is None:
            time_max = time(18, 0)

        # 建立時間範圍
        start_dt = datetime.combine(date_to_check, time_min)
        end_dt = datetime.combine(date_to_check, time_max)

        try:
            # 使用 freebusy API
            body = {
                'timeMin': start_dt.isoformat() + '+08:00',
                'timeMax': end_dt.isoformat() + '+08:00',
                'items': [{'id': calendar_id}],
                'timeZone': 'Asia/Taipei'
            }

            result = service.freebusy().query(body=body).execute()
            calendars = result.get('calendars', {})
            busy_times = calendars.get(calendar_id, {}).get('busy', [])

            # 轉換格式
            busy_slots = []
            for slot in busy_times:
                start = datetime.fromisoformat(slot['start'].replace('Z', '+00:00'))
                end = datetime.fromisoformat(slot['end'].replace('Z', '+00:00'))
                busy_slots.append({
                    'start': start.strftime('%H:%M'),
                    'end': end.strftime('%H:%M')
                })

            return {
                'success': True,
                'date': date_to_check.isoformat(),
                'busy_times': busy_slots
            }
        except HttpError as e:
            logger.error(f"Failed to get busy times: {e}")
            return {
                'success': False,
                'error': str(e)
            }

    def get_available_slots(
        self,
        calendar_id: str,
        date_to_check: date,
        slot_duration: int = 30,
        business_start: time = None,
        business_end: time = None
    ) -> Dict[str, Any]:
        """
        取得指定日期的可用時段

        Args:
            calendar_id: Calendar ID
            date_to_check: 要查詢的日期
            slot_duration: 時段長度（分鐘），預設 30
            business_start: 營業開始時間，預設 09:00
            business_end: 營業結束時間，預設 18:00

        Returns:
            可用時段列表
        """
        if business_start is None:
            business_start = time(9, 0)
        if business_end is None:
            business_end = time(18, 0)

        # 取得忙碌時段
        busy_result = self.get_busy_times(
            calendar_id,
            date_to_check,
            business_start,
            business_end
        )

        if not busy_result.get('success'):
            return busy_result

        busy_times = busy_result.get('busy_times', [])

        # 生成所有時段
        available_slots = []
        current_time = datetime.combine(date_to_check, business_start)
        end_time = datetime.combine(date_to_check, business_end)

        while current_time < end_time:
            slot_start = current_time.strftime('%H:%M')
            slot_end = (current_time + timedelta(minutes=slot_duration)).strftime('%H:%M')

            # 檢查是否與忙碌時段衝突
            is_available = True
            for busy in busy_times:
                # 簡單重疊檢查
                if not (slot_end <= busy['start'] or slot_start >= busy['end']):
                    is_available = False
                    break

            if is_available:
                available_slots.append({
                    'start': slot_start,
                    'end': slot_end
                })

            current_time += timedelta(minutes=slot_duration)

        # 如果是今天，過濾掉已過去的時段
        if date_to_check == date.today():
            now = datetime.now()
            current_time_str = now.strftime('%H:%M')
            available_slots = [
                slot for slot in available_slots
                if slot['start'] > current_time_str
            ]

        return {
            'success': True,
            'date': date_to_check.isoformat(),
            'available_slots': available_slots,
            'total_slots': len(available_slots)
        }

    def list_events(
        self,
        calendar_id: str,
        date_from: date = None,
        date_to: date = None,
        max_results: int = 50
    ) -> Dict[str, Any]:
        """
        列出 Calendar 事件

        Args:
            calendar_id: Calendar ID
            date_from: 開始日期
            date_to: 結束日期
            max_results: 最大回傳筆數

        Returns:
            事件列表
        """
        service = self._get_service()

        if date_from is None:
            date_from = date.today()
        if date_to is None:
            date_to = date_from + timedelta(days=30)

        time_min = datetime.combine(date_from, time(0, 0)).isoformat() + '+08:00'
        time_max = datetime.combine(date_to, time(23, 59)).isoformat() + '+08:00'

        try:
            events_result = service.events().list(
                calendarId=calendar_id,
                timeMin=time_min,
                timeMax=time_max,
                maxResults=max_results,
                singleEvents=True,
                orderBy='startTime'
            ).execute()

            events = events_result.get('items', [])

            return {
                'success': True,
                'events': [
                    {
                        'id': e['id'],
                        'summary': e.get('summary', ''),
                        'start': e['start'].get('dateTime', e['start'].get('date')),
                        'end': e['end'].get('dateTime', e['end'].get('date')),
                        'description': e.get('description', '')
                    }
                    for e in events
                ]
            }
        except HttpError as e:
            logger.error(f"Failed to list events: {e}")
            return {
                'success': False,
                'error': str(e)
            }


# 全域實例（延遲初始化）
_calendar_service = None


def get_calendar_service() -> GoogleCalendarService:
    """取得 Calendar 服務實例"""
    global _calendar_service
    if _calendar_service is None:
        _calendar_service = GoogleCalendarService()
    return _calendar_service
