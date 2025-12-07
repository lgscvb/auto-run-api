-- Migration: 004_renewal_status
-- Description: 新增續約流程追蹤欄位
-- Date: 2025-12-07

-- ============================================================================
-- 1. 新增續約狀態欄位
-- ============================================================================

-- 續約狀態：追蹤續約流程進度
ALTER TABLE contracts ADD COLUMN IF NOT EXISTS renewal_status VARCHAR(20) DEFAULT 'none';
COMMENT ON COLUMN contracts.renewal_status IS '續約狀態: none(無需處理), notified(已通知), confirmed(已確認), paid(已收款), invoiced(已開發票), signed(已簽約), completed(完成)';

-- 發票狀態：追蹤發票開立情況
ALTER TABLE contracts ADD COLUMN IF NOT EXISTS invoice_status VARCHAR(20);
COMMENT ON COLUMN contracts.invoice_status IS '發票狀態: pending_tax_id(等待統編), issued_personal(已開二聯), issued_business(已開三聯)';

-- ============================================================================
-- 2. 新增時間戳記欄位（追蹤每個步驟完成時間）
-- ============================================================================

ALTER TABLE contracts ADD COLUMN IF NOT EXISTS renewal_notified_at TIMESTAMP;
COMMENT ON COLUMN contracts.renewal_notified_at IS '通知客戶時間';

ALTER TABLE contracts ADD COLUMN IF NOT EXISTS renewal_confirmed_at TIMESTAMP;
COMMENT ON COLUMN contracts.renewal_confirmed_at IS '客戶確認續約時間';

ALTER TABLE contracts ADD COLUMN IF NOT EXISTS renewal_paid_at TIMESTAMP;
COMMENT ON COLUMN contracts.renewal_paid_at IS '收到續約款項時間';

ALTER TABLE contracts ADD COLUMN IF NOT EXISTS renewal_invoiced_at TIMESTAMP;
COMMENT ON COLUMN contracts.renewal_invoiced_at IS '發票開立時間';

ALTER TABLE contracts ADD COLUMN IF NOT EXISTS renewal_signed_at TIMESTAMP;
COMMENT ON COLUMN contracts.renewal_signed_at IS '合約簽署時間';

-- ============================================================================
-- 3. 新增備註欄位
-- ============================================================================

ALTER TABLE contracts ADD COLUMN IF NOT EXISTS renewal_notes TEXT;
COMMENT ON COLUMN contracts.renewal_notes IS '續約備註（如：等待公司登記核准）';

-- ============================================================================
-- 4. 建立索引以提升查詢效能
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_contracts_renewal_status ON contracts(renewal_status);
CREATE INDEX IF NOT EXISTS idx_contracts_invoice_status ON contracts(invoice_status);

-- ============================================================================
-- 5. 更新現有合約的續約狀態
-- ============================================================================

-- 將即將到期（30天內）且狀態為 active 的合約標記為需處理
UPDATE contracts
SET renewal_status = 'none'
WHERE renewal_status IS NULL;

-- ============================================================================
-- 6. 建立續約提醒 View（更新版）
-- ============================================================================

DROP VIEW IF EXISTS v_renewal_reminders;

CREATE VIEW v_renewal_reminders AS
SELECT
    c.id,
    c.contract_number,
    c.customer_id,
    cu.name AS customer_name,
    cu.company_name,
    cu.phone,
    cu.line_user_id,
    c.branch_id,
    b.name AS branch_name,
    c.start_date,
    c.end_date,
    c.monthly_rent,
    c.payment_cycle,
    c.status,
    c.renewal_status,
    c.invoice_status,
    c.renewal_notified_at,
    c.renewal_confirmed_at,
    c.renewal_paid_at,
    c.renewal_invoiced_at,
    c.renewal_signed_at,
    c.renewal_notes,
    (c.end_date - CURRENT_DATE) AS days_until_expiry,
    CASE
        WHEN c.end_date < CURRENT_DATE THEN 'expired'
        WHEN c.end_date <= CURRENT_DATE + INTERVAL '7 days' THEN 'critical'
        WHEN c.end_date <= CURRENT_DATE + INTERVAL '30 days' THEN 'warning'
        ELSE 'normal'
    END AS urgency_level
FROM contracts c
JOIN customers cu ON c.customer_id = cu.id
LEFT JOIN branches b ON c.branch_id = b.id
WHERE c.status = 'active'
  AND c.end_date <= CURRENT_DATE + INTERVAL '60 days'
ORDER BY c.end_date ASC;

-- 授權給 PostgREST 用戶
GRANT SELECT ON v_renewal_reminders TO anon, authenticated;

-- ============================================================================
-- 7. 建立續約狀態統計 View
-- ============================================================================

CREATE OR REPLACE VIEW v_renewal_status_summary AS
SELECT
    branch_id,
    b.name AS branch_name,
    renewal_status,
    COUNT(*) AS count,
    SUM(monthly_rent) AS total_monthly_rent
FROM contracts c
LEFT JOIN branches b ON c.branch_id = b.id
WHERE c.status = 'active'
  AND c.end_date <= CURRENT_DATE + INTERVAL '60 days'
GROUP BY branch_id, b.name, renewal_status
ORDER BY branch_id, renewal_status;

GRANT SELECT ON v_renewal_status_summary TO anon, authenticated;
