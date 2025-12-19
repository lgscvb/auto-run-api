-- Migration: 022_add_waived_status
-- Description: 新增 waived（免收）繳費狀態
-- Date: 2025-12-19
-- Purpose: 用於減免、免收的情況，不計入營收統計

-- ============================================================================
-- 1. 修改 payment_status CHECK 約束，新增 'waived' 值
-- ============================================================================

-- 刪除舊的 CHECK 約束
ALTER TABLE payments DROP CONSTRAINT IF EXISTS payments_payment_status_check;

-- 新增包含 waived 的 CHECK 約束
ALTER TABLE payments ADD CONSTRAINT payments_payment_status_check
    CHECK (payment_status IN ('pending', 'paid', 'overdue', 'waived'));

COMMENT ON COLUMN payments.payment_status IS '繳費狀態: pending=待繳, paid=已繳, overdue=逾期, waived=免收（不計入營收）';

-- ============================================================================
-- 2. 更新視圖：排除 waived 記錄的營收統計
-- ============================================================================

-- 更新分館營收視圖（如果存在）
CREATE OR REPLACE VIEW v_branch_revenue AS
SELECT
    b.id AS branch_id,
    b.name AS branch_name,
    COUNT(DISTINCT c.id) FILTER (WHERE c.status = 'active') AS active_contracts,
    COUNT(DISTINCT c.customer_id) FILTER (WHERE c.status = 'active') AS active_customers,
    COALESCE(SUM(p.amount) FILTER (
        WHERE p.payment_status = 'paid'
        AND p.payment_period = TO_CHAR(CURRENT_DATE, 'YYYY-MM')
    ), 0) AS current_month_revenue,
    COALESCE(SUM(p.amount) FILTER (
        WHERE p.payment_status = 'pending'
        AND p.payment_period = TO_CHAR(CURRENT_DATE, 'YYYY-MM')
    ), 0) AS current_month_pending,
    COALESCE(SUM(p.amount) FILTER (
        WHERE p.payment_status = 'overdue'
        AND p.payment_period = TO_CHAR(CURRENT_DATE, 'YYYY-MM')
    ), 0) AS current_month_overdue,
    COUNT(*) FILTER (
        WHERE p.payment_status = 'paid'
        AND p.payment_period = TO_CHAR(CURRENT_DATE, 'YYYY-MM')
    ) AS current_month_paid_count,
    COUNT(*) FILTER (
        WHERE p.payment_status = 'pending'
        AND p.payment_period = TO_CHAR(CURRENT_DATE, 'YYYY-MM')
    ) AS current_month_pending_count,
    COUNT(*) FILTER (
        WHERE p.payment_status = 'overdue'
        AND p.payment_period = TO_CHAR(CURRENT_DATE, 'YYYY-MM')
    ) AS current_month_overdue_count,
    COUNT(*) FILTER (
        WHERE c.end_date IS NOT NULL
        AND c.end_date <= CURRENT_DATE + INTERVAL '30 days'
        AND c.status = 'active'
    ) AS contracts_expiring_30days
FROM branches b
LEFT JOIN contracts c ON c.branch_id = b.id
LEFT JOIN payments p ON p.branch_id = b.id AND p.payment_status != 'waived'
GROUP BY b.id, b.name
ORDER BY b.name;

-- ============================================================================
-- 3. 修復現有資料：蕭家如 10月免收
-- ============================================================================

UPDATE payments
SET payment_status = 'waived', notes = '10月免收'
WHERE id = 301;

-- 驗證
SELECT id, customer_id, payment_period, amount, payment_status, notes
FROM payments
WHERE payment_status = 'waived';
