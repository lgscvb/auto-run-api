-- Migration: 026_service_plans
-- Description: 建立服務價格表，支援報價單自動帶入
-- Date: 2025-12-20

-- ============================================================================
-- 1. 建立 service_plans 表
-- ============================================================================

CREATE TABLE IF NOT EXISTS service_plans (
    id SERIAL PRIMARY KEY,
    category TEXT NOT NULL,           -- 分類：空間服務, 登記服務, 代辦服務
    name TEXT NOT NULL,               -- 服務名稱（中文）
    code TEXT UNIQUE NOT NULL,        -- 服務代碼（用於程式識別）
    unit_price NUMERIC(12,2) NOT NULL,-- 單價
    unit TEXT NOT NULL,               -- 計價單位：月, 年, 小時, 次, 3小時
    billing_cycle TEXT,               -- 繳費週期：monthly, semi_annual, annual, one_time
    deposit NUMERIC(12,2) DEFAULT 0,  -- 押金
    original_price NUMERIC(12,2),     -- 原價（有優惠時）
    min_duration TEXT,                -- 最低租期：1個月, 1年, 2年
    notes TEXT,                       -- 備註
    is_active BOOLEAN DEFAULT TRUE,
    sort_order INTEGER DEFAULT 0,     -- 排序
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

COMMENT ON TABLE service_plans IS '服務價格表';
COMMENT ON COLUMN service_plans.category IS '分類：空間服務, 登記服務, 代辦服務';
COMMENT ON COLUMN service_plans.code IS '服務代碼（唯一識別碼）';
COMMENT ON COLUMN service_plans.billing_cycle IS '繳費週期：monthly=月繳, semi_annual=半年繳, annual=年繳, one_time=一次性';

-- ============================================================================
-- 2. 建立索引
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_service_plans_category ON service_plans(category);
CREATE INDEX IF NOT EXISTS idx_service_plans_code ON service_plans(code);
CREATE INDEX IF NOT EXISTS idx_service_plans_is_active ON service_plans(is_active);

-- ============================================================================
-- 3. 插入種子資料
-- ============================================================================

-- 空間服務
INSERT INTO service_plans (category, name, code, unit_price, unit, billing_cycle, deposit, original_price, min_duration, notes, sort_order) VALUES
-- 共享空間
('空間服務', '共享空間 - 時租', 'coworking_hourly', 80, '小時', 'one_time', 0, NULL, NULL, '開放座位/自由座', 10),
('空間服務', '共享空間 - 日租', 'coworking_daily', 350, '天', 'one_time', 0, NULL, NULL, '開放座位/自由座', 11),
('空間服務', '共享空間 - 月租', 'coworking_monthly', 3000, '月', 'monthly', 0, NULL, NULL, '不用綁年約，月繳月使用', 12),

-- 獨立辦公室
('空間服務', '獨立辦公室 E（年約）', 'private_office_e_annual', 15000, '月', 'monthly', 15000, 18000, '1年', '6~10人，有對外窗、採光通風良好', 20),

-- 會議室
('空間服務', '會議室 - 平日', 'meeting_room_weekday', 380, '小時', 'one_time', 0, NULL, NULL, '週一至週五 09:00~18:00，8~10人，含稅', 30),
('空間服務', '會議室 - 假日', 'meeting_room_weekend', 1650, '3小時', 'one_time', 0, NULL, '3小時', '最低起租3小時，8~10人，含稅', 31),

-- 活動場地
('空間服務', '活動場地 - 假日', 'event_space_weekend', 3600, '3小時', 'one_time', 0, NULL, '3小時', '週六 09:00~18:00，1~30人，含稅', 40),

-- 登記服務
('登記服務', '借址登記 - 兩年約', 'virtual_office_2year', 1490, '月', 'semi_annual', 6000, NULL, '2年', '半年繳', 50),
('登記服務', '借址登記 - 一年約', 'virtual_office_1year', 1800, '月', 'annual', 6000, NULL, '1年', '年繳', 51),

-- 續約價格（區分新約與續約）
('登記服務', '借址登記續約 - 兩年約', 'virtual_office_2year_renewal', 1800, '月', 'semi_annual', 0, NULL, '2年', '半年繳，續約價格', 52),
('登記服務', '借址登記續約 - 一年約', 'virtual_office_1year_renewal', 2000, '月', 'annual', 0, NULL, '1年', '年繳，續約價格', 53),

-- 代辦服務
('代辦服務', '新設立登記 - 公司', 'company_setup', 15000, '次', 'one_time', 0, NULL, NULL, '年營業額400萬以上及特許另報價', 60),
('代辦服務', '新設立登記 - 行號', 'business_setup', 8000, '次', 'one_time', 0, NULL, NULL, NULL, 61),
('代辦服務', '遷址代辦 - 台中市內', 'relocation_taichung', 6600, '次', 'one_time', 0, NULL, NULL, NULL, 70),
('代辦服務', '遷址代辦 - 外縣市', 'relocation_outside', 8000, '次', 'one_time', 0, NULL, NULL, '跨縣市遷址建議請找代辦', 71),

-- 會計服務
('代辦服務', '會計帳服務', 'accounting_service', 2000, '月', 'monthly', 0, NULL, '1年', '收14個月，年度總計 $28,000', 80);

-- ============================================================================
-- 4. 建立 updated_at 觸發器
-- ============================================================================

CREATE OR REPLACE FUNCTION update_service_plans_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_update_service_plans_updated_at ON service_plans;
CREATE TRIGGER trigger_update_service_plans_updated_at
    BEFORE UPDATE ON service_plans
    FOR EACH ROW
    EXECUTE FUNCTION update_service_plans_updated_at();

-- ============================================================================
-- 5. 授權 anon 角色存取（PostgREST）
-- ============================================================================

GRANT SELECT, INSERT, UPDATE, DELETE ON service_plans TO anon;
GRANT USAGE, SELECT ON SEQUENCE service_plans_id_seq TO anon;

-- ============================================================================
-- 6. 驗證
-- ============================================================================

SELECT category, name, code, unit_price, unit, deposit
FROM service_plans
ORDER BY sort_order;
