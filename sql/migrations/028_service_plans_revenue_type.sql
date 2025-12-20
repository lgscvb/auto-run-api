-- Migration: 028_service_plans_revenue_type
-- Description: 新增 revenue_type 欄位，區分自己收款與轉介服務
-- Date: 2025-12-20

-- ============================================================================
-- 費用分類說明
-- ============================================================================
-- own      : 自己收款（借址登記、空間租賃等）
-- referral : 轉介服務（代辦服務，由事務所收款，你收佣金）
-- deposit  : 押金
-- ============================================================================

-- 1. 新增 revenue_type 欄位
ALTER TABLE service_plans
ADD COLUMN IF NOT EXISTS revenue_type TEXT DEFAULT 'own';

-- 2. 新增 annual_months 欄位（用於會計服務等特殊計算，如年繳14個月）
ALTER TABLE service_plans
ADD COLUMN IF NOT EXISTS annual_months INTEGER DEFAULT 12;

-- 3. 新增約束
ALTER TABLE service_plans
DROP CONSTRAINT IF EXISTS chk_revenue_type;

ALTER TABLE service_plans
ADD CONSTRAINT chk_revenue_type
CHECK (revenue_type IN ('own', 'referral', 'deposit'));

-- 4. 更新現有資料的 revenue_type

-- 空間服務：自己收款
UPDATE service_plans SET revenue_type = 'own'
WHERE category = '空間服務';

-- 登記服務：自己收款
UPDATE service_plans SET revenue_type = 'own'
WHERE category = '登記服務';

-- 代辦服務：轉介（事務所收款）
UPDATE service_plans SET revenue_type = 'referral'
WHERE category = '代辦服務';

-- 會計服務：轉介 + 14個月/年
UPDATE service_plans SET revenue_type = 'referral', annual_months = 14
WHERE code = 'accounting_service';

-- 5. 更新 COMMENT
COMMENT ON COLUMN service_plans.revenue_type IS '營收類型：own=自己收款, referral=轉介（事務所收款）, deposit=押金';
COMMENT ON COLUMN service_plans.annual_months IS '年度月數（會計服務收14個月）';

-- 6. 驗證更新結果
SELECT category, name, code, revenue_type, annual_months
FROM service_plans
ORDER BY sort_order;
