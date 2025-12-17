-- ============================================================================
-- Hour Jungle CRM - Meeting Room Booking Schema
-- 014_meeting_rooms.sql - 會議室預約功能
-- ============================================================================

-- 1. 會議室表
CREATE TABLE IF NOT EXISTS meeting_rooms (
    id              SERIAL PRIMARY KEY,
    branch_id       INTEGER REFERENCES branches(id),
    name            VARCHAR(50) NOT NULL,       -- '會議室', 'A室', 'B室'
    capacity        INTEGER DEFAULT 6,          -- 座位數
    hourly_rate     NUMERIC(10,2) DEFAULT 0,    -- 每小時費率（0=免費）
    amenities       JSONB DEFAULT '[]',         -- 設備: ["投影機", "白板"]
    google_calendar_id VARCHAR(200),            -- Google Calendar ID
    is_active       BOOLEAN DEFAULT true,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- 2. 會議室預約表
CREATE TABLE IF NOT EXISTS meeting_room_bookings (
    id              SERIAL PRIMARY KEY,
    booking_number  VARCHAR(30) UNIQUE NOT NULL,  -- MR-20241215-0001
    meeting_room_id INTEGER NOT NULL REFERENCES meeting_rooms(id),
    customer_id     INTEGER REFERENCES customers(id),

    -- 時間
    booking_date    DATE NOT NULL,
    start_time      TIME NOT NULL,    -- 09:00
    end_time        TIME NOT NULL,    -- 10:30
    duration_minutes INTEGER,         -- 90

    -- Google Calendar
    google_event_id VARCHAR(200),

    -- 狀態
    status          VARCHAR(20) DEFAULT 'confirmed'
                    CHECK (status IN ('confirmed', 'cancelled', 'completed', 'no_show')),
    cancelled_at    TIMESTAMPTZ,
    cancel_reason   VARCHAR(200),

    -- 提醒
    reminder_sent   BOOLEAN DEFAULT false,  -- 1小時前提醒是否已發送

    -- 備註
    purpose         VARCHAR(200),   -- 會議目的
    attendees_count INTEGER,        -- 預計人數
    notes           TEXT,
    created_by      VARCHAR(50),    -- 'line' / 'admin'

    -- 時間戳
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW(),

    -- 約束：結束時間必須大於開始時間
    CONSTRAINT valid_time_range CHECK (end_time > start_time)
);

-- 3. 索引
CREATE INDEX IF NOT EXISTS idx_meeting_rooms_branch ON meeting_rooms(branch_id);
CREATE INDEX IF NOT EXISTS idx_bookings_date ON meeting_room_bookings(booking_date);
CREATE INDEX IF NOT EXISTS idx_bookings_customer ON meeting_room_bookings(customer_id);
CREATE INDEX IF NOT EXISTS idx_bookings_room ON meeting_room_bookings(meeting_room_id);
CREATE INDEX IF NOT EXISTS idx_bookings_status ON meeting_room_bookings(status, booking_date);
CREATE INDEX IF NOT EXISTS idx_bookings_reminder ON meeting_room_bookings(reminder_sent, status, booking_date, start_time);

-- 4. 視圖：會議室預約詳情
CREATE OR REPLACE VIEW v_meeting_room_bookings AS
SELECT
    b.id,
    b.booking_number,
    b.meeting_room_id,
    b.customer_id,
    b.booking_date,
    b.start_time,
    b.end_time,
    b.duration_minutes,
    b.google_event_id,
    b.status,
    b.cancelled_at,
    b.cancel_reason,
    b.reminder_sent,
    b.purpose,
    b.attendees_count,
    b.notes,
    b.created_by,
    b.created_at,
    b.updated_at,
    r.name AS room_name,
    r.capacity,
    r.amenities,
    br.id AS branch_id,
    br.name AS branch_name,
    c.name AS customer_name,
    c.company_name,
    c.phone AS customer_phone,
    c.email AS customer_email,
    c.line_user_id
FROM meeting_room_bookings b
JOIN meeting_rooms r ON b.meeting_room_id = r.id
JOIN branches br ON r.branch_id = br.id
LEFT JOIN customers c ON b.customer_id = c.id;

-- 5. 視圖：今日預約
CREATE OR REPLACE VIEW v_today_bookings AS
SELECT *
FROM v_meeting_room_bookings
WHERE booking_date = CURRENT_DATE
  AND status = 'confirmed'
ORDER BY start_time;

-- 6. 視圖：未來預約（提醒用）
CREATE OR REPLACE VIEW v_upcoming_bookings AS
SELECT *
FROM v_meeting_room_bookings
WHERE booking_date >= CURRENT_DATE
  AND status = 'confirmed'
ORDER BY booking_date, start_time;

-- 7. 函數：生成預約編號
CREATE OR REPLACE FUNCTION generate_booking_number()
RETURNS VARCHAR(30) AS $$
DECLARE
    today_str VARCHAR(8);
    seq_num INTEGER;
    new_number VARCHAR(30);
BEGIN
    today_str := TO_CHAR(CURRENT_DATE, 'YYYYMMDD');

    SELECT COALESCE(MAX(
        CAST(SUBSTRING(booking_number FROM 13 FOR 4) AS INTEGER)
    ), 0) + 1
    INTO seq_num
    FROM meeting_room_bookings
    WHERE booking_number LIKE 'MR-' || today_str || '-%';

    new_number := 'MR-' || today_str || '-' || LPAD(seq_num::TEXT, 4, '0');
    RETURN new_number;
END;
$$ LANGUAGE plpgsql;

-- 8. 觸發器：自動設定 duration_minutes 和更新時間
CREATE OR REPLACE FUNCTION booking_before_insert_update()
RETURNS TRIGGER AS $$
BEGIN
    -- 計算持續時間
    NEW.duration_minutes := EXTRACT(EPOCH FROM (NEW.end_time - NEW.start_time)) / 60;

    -- 更新時間戳
    NEW.updated_at := NOW();

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_booking_before_insert_update ON meeting_room_bookings;
CREATE TRIGGER trg_booking_before_insert_update
    BEFORE INSERT OR UPDATE ON meeting_room_bookings
    FOR EACH ROW
    EXECUTE FUNCTION booking_before_insert_update();

-- 9. 初始資料：台中館會議室
INSERT INTO meeting_rooms (branch_id, name, capacity, amenities, is_active)
SELECT
    1,  -- 假設 branch_id=1 是台中館/大忠館
    '會議室',
    10,
    '["投影機", "白板"]'::JSONB,
    true
WHERE NOT EXISTS (
    SELECT 1 FROM meeting_rooms WHERE branch_id = 1 AND name = '會議室'
);

-- 10. 函數：檢查時段是否可用
CREATE OR REPLACE FUNCTION check_room_availability(
    p_room_id INTEGER,
    p_date DATE,
    p_start_time TIME,
    p_end_time TIME,
    p_exclude_booking_id INTEGER DEFAULT NULL
)
RETURNS BOOLEAN AS $$
DECLARE
    conflict_count INTEGER;
BEGIN
    SELECT COUNT(*)
    INTO conflict_count
    FROM meeting_room_bookings
    WHERE meeting_room_id = p_room_id
      AND booking_date = p_date
      AND status = 'confirmed'
      AND (p_exclude_booking_id IS NULL OR id != p_exclude_booking_id)
      AND (
          (start_time < p_end_time AND end_time > p_start_time)
      );

    RETURN conflict_count = 0;
END;
$$ LANGUAGE plpgsql;

COMMENT ON TABLE meeting_rooms IS '會議室資料表';
COMMENT ON TABLE meeting_room_bookings IS '會議室預約記錄';
COMMENT ON VIEW v_meeting_room_bookings IS '會議室預約詳情視圖';
COMMENT ON FUNCTION generate_booking_number() IS '生成預約編號 MR-YYYYMMDD-NNNN';
COMMENT ON FUNCTION check_room_availability IS '檢查指定時段是否可預約';
