-- 010_settings.sql
-- 系統設定表

CREATE TABLE IF NOT EXISTS system_settings (
    id SERIAL PRIMARY KEY,
    key VARCHAR(100) UNIQUE NOT NULL,
    value JSONB NOT NULL DEFAULT '{}',
    description TEXT,
    category VARCHAR(50) DEFAULT 'general',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 預設設定值
INSERT INTO system_settings (key, value, description, category)
VALUES
    ('general', '{"system_name": "Hour Jungle CRM", "default_branch": null, "timezone": "Asia/Taipei", "language": "zh-TW"}', '一般設定', 'general'),
    ('notifications', '{"overdue_reminder": true, "renewal_reminder": true, "commission_reminder": true, "email_notification": false}', '通知設定', 'notifications'),
    ('renewal', '{"reminder_days": 30, "auto_notify": true}', '續約設定', 'renewal'),
    ('payment', '{"grace_period_days": 7, "late_fee_rate": 0.05}', '繳費設定', 'payment'),
    ('invoice', '{"auto_issue": false, "default_carrier_type": null}', '發票設定', 'invoice')
ON CONFLICT (key) DO NOTHING;

-- 更新時間觸發器
CREATE OR REPLACE FUNCTION update_settings_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS settings_updated_at ON system_settings;
CREATE TRIGGER settings_updated_at
    BEFORE UPDATE ON system_settings
    FOR EACH ROW
    EXECUTE FUNCTION update_settings_timestamp();

COMMENT ON TABLE system_settings IS '系統設定表';
COMMENT ON COLUMN system_settings.key IS '設定鍵值';
COMMENT ON COLUMN system_settings.value IS '設定值 (JSON)';
COMMENT ON COLUMN system_settings.category IS '分類 (general/notifications/renewal/payment/invoice)';
