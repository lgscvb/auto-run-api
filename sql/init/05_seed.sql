-- ============================================================================
-- Hour Jungle CRM - PostgreSQL Seed Data
-- 05_seed.sql - 初始資料
-- ============================================================================

-- ============================================================================
-- 1. 場館資料 (branches)
-- ============================================================================
INSERT INTO branches (id, code, name, rental_address, city, district, contact_phone, manager_name, status, allow_small_scale, has_good_relationship, tax_office_district, config, notes)
VALUES
    (1, 'DZ', '大忠館', '台中市西區大忠南街95號3樓', '台中市', '西區', NULL, NULL, 'active', TRUE, TRUE, '中區國稅局',
     '{"floor": "3F", "capacity": 50, "meeting_rooms": 2}',
     '主要場館，2019年開始營運'),

    (2, 'TWD', '台灣大道環瑞館', '台中市西區台灣大道二段285號20樓', '台中市', '西區', NULL, NULL, 'active', TRUE, FALSE, '中區國稅局',
     '{"floor": "20F", "capacity": 30, "meeting_rooms": 1}',
     '2024年3月新開幕')
ON CONFLICT (id) DO UPDATE SET
    name = EXCLUDED.name,
    rental_address = EXCLUDED.rental_address,
    config = EXCLUDED.config,
    updated_at = NOW();

-- 重設序列
SELECT setval('branches_id_seq', (SELECT MAX(id) FROM branches));

-- ============================================================================
-- 2. 系統設定 (system_settings)
-- ============================================================================
INSERT INTO system_settings (setting_key, setting_value, setting_type, description)
VALUES
    -- 繳費設定
    ('payment.default_due_day', '5', 'number', '預設繳費日'),
    ('payment.reminder_days_before', '3', 'number', '繳費提醒提前天數'),
    ('payment.overdue_grace_days', '3', 'number', '逾期寬限天數'),
    ('payment.late_fee_rate', '0.01', 'number', '逾期罰款比例 (每日)'),

    -- 合約設定
    ('contract.renewal_reminder_days', '30', 'number', '續約提醒提前天數'),
    ('contract.default_deposit_months', '1', 'number', '預設押金月數'),

    -- 佣金設定
    ('commission.eligibility_months', '6', 'number', '佣金資格所需月數'),
    ('commission.default_rate', '100', 'number', '預設佣金金額 (一個月租金)'),

    -- LINE 設定
    ('line.welcome_message', '歡迎加入 Hour Jungle！請問有什麼可以幫您的嗎？', 'string', 'LINE 歡迎訊息'),
    ('line.payment_reminder_template', '親愛的 {customer_name} 您好，您 {period} 的租金 ${amount} 將於 {due_date} 到期，請記得繳費喔！', 'string', '繳費提醒模板'),

    -- 系統設定
    ('system.timezone', 'Asia/Taipei', 'string', '系統時區'),
    ('system.currency', 'TWD', 'string', '系統幣別'),
    ('system.date_format', 'YYYY-MM-DD', 'string', '日期格式')
ON CONFLICT (setting_key) DO UPDATE SET
    setting_value = EXCLUDED.setting_value,
    updated_at = NOW();

-- ============================================================================
-- 3. 測試用會計事務所資料 (可選)
-- ============================================================================
-- 如果有需要測試，取消下方註解

/*
INSERT INTO accounting_firms (name, short_name, contact_person, phone, email, commission_rate, payment_terms, status, notes)
VALUES
    ('範例會計師事務所', '範例所', '王小明', '04-12345678', 'contact@example.com', 100.00, '簽約滿6個月', 'active', '測試用資料')
ON CONFLICT DO NOTHING;
*/

-- ============================================================================
-- 4. 驗證資料
-- ============================================================================
DO $$
DECLARE
    branch_count INTEGER;
    setting_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO branch_count FROM branches;
    SELECT COUNT(*) INTO setting_count FROM system_settings;

    RAISE NOTICE '=== 初始資料載入完成 ===';
    RAISE NOTICE '場館數量: %', branch_count;
    RAISE NOTICE '系統設定數量: %', setting_count;
END;
$$;
