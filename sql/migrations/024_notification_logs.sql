-- Migration: 024_notification_logs
-- Description: 新增通知記錄表和自動通知設定
-- Date: 2025-12-19

-- ============================================================================
-- 1. 通知記錄表
-- ============================================================================

CREATE TABLE IF NOT EXISTS notification_logs (
    id SERIAL PRIMARY KEY,
    notification_type VARCHAR(50) NOT NULL,  -- payment_reminder, renewal_reminder
    customer_id INTEGER REFERENCES customers(id),
    contract_id INTEGER REFERENCES contracts(id),
    payment_id INTEGER REFERENCES payments(id),
    recipient_name VARCHAR(200),
    recipient_line_id VARCHAR(100),
    message_content TEXT,
    status VARCHAR(20) DEFAULT 'sent',  -- sent, failed, pending
    error_message TEXT,
    triggered_by VARCHAR(50) DEFAULT 'manual',  -- manual, scheduler, system
    created_at TIMESTAMP DEFAULT NOW()
);

-- 索引
CREATE INDEX IF NOT EXISTS idx_notification_logs_type ON notification_logs(notification_type);
CREATE INDEX IF NOT EXISTS idx_notification_logs_customer ON notification_logs(customer_id);
CREATE INDEX IF NOT EXISTS idx_notification_logs_created ON notification_logs(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_notification_logs_status ON notification_logs(status);

-- ============================================================================
-- 2. 系統設定表（如果不存在）
-- ============================================================================

CREATE TABLE IF NOT EXISTS system_settings (
    key VARCHAR(100) PRIMARY KEY,
    value TEXT,
    description TEXT,
    updated_at TIMESTAMP DEFAULT NOW()
);

-- 插入自動通知設定
INSERT INTO system_settings (key, value, description)
VALUES
    ('auto_payment_reminder', 'false', '自動催繳提醒開關'),
    ('auto_renewal_reminder', 'false', '自動續約提醒開關'),
    ('reminder_time', '09:00', '每日提醒時間'),
    ('overdue_reminder_days', '3,7,14,30', '逾期幾天發送提醒')
ON CONFLICT (key) DO NOTHING;

-- ============================================================================
-- 3. 通知記錄視圖
-- ============================================================================

CREATE OR REPLACE VIEW v_notification_logs AS
SELECT
    nl.id,
    nl.notification_type,
    nl.customer_id,
    nl.contract_id,
    nl.payment_id,
    nl.recipient_name,
    nl.recipient_line_id,
    nl.message_content,
    nl.status,
    nl.error_message,
    nl.triggered_by,
    nl.created_at,
    c.name as customer_name,
    b.name as branch_name,
    CASE nl.notification_type
        WHEN 'payment_reminder' THEN '催繳提醒'
        WHEN 'renewal_reminder' THEN '續約提醒'
        ELSE nl.notification_type
    END as type_label
FROM notification_logs nl
LEFT JOIN customers c ON nl.customer_id = c.id
LEFT JOIN contracts ct ON nl.contract_id = ct.id
LEFT JOIN branches b ON ct.branch_id = b.id
ORDER BY nl.created_at DESC;

-- ============================================================================
-- 4. 當月應催繳統計視圖
-- ============================================================================

CREATE OR REPLACE VIEW v_monthly_reminders_summary AS
SELECT
    'payment' as reminder_type,
    COUNT(*) as total_count,
    COUNT(*) FILTER (WHERE line_user_id IS NOT NULL) as can_notify_count,
    COALESCE(SUM(total_due), 0) as total_amount
FROM v_overdue_details
WHERE payment_period LIKE TO_CHAR(CURRENT_DATE, 'YYYY-MM') || '%'
   OR days_overdue > 0

UNION ALL

SELECT
    'renewal' as reminder_type,
    COUNT(*) as total_count,
    COUNT(*) FILTER (WHERE line_user_id IS NOT NULL) as can_notify_count,
    COALESCE(SUM(monthly_rent), 0) as total_amount
FROM v_renewal_reminders
WHERE days_remaining <= 30;

-- ============================================================================
-- 5. 今日通知統計
-- ============================================================================

CREATE OR REPLACE VIEW v_today_notifications AS
SELECT
    notification_type,
    COUNT(*) as sent_count,
    COUNT(*) FILTER (WHERE status = 'sent') as success_count,
    COUNT(*) FILTER (WHERE status = 'failed') as failed_count
FROM notification_logs
WHERE DATE(created_at) = CURRENT_DATE
GROUP BY notification_type;
