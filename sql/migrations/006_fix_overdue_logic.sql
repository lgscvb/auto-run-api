-- Migration: 006_fix_overdue_logic
-- Description: 修正逾期判斷邏輯，動態計算而非依賴靜態欄位
-- Date: 2025-12-07

-- ============================================================================
-- 問題：payment_status 欄位不會自動更新為 'overdue'
-- 解決：在 SQL 視圖中動態判斷：
--       - paid → 已付款
--       - pending + due_date < today → 逾期
--       - pending + due_date >= today → 待收
-- ============================================================================

-- 1. 更新場館營收摘要視圖（動態計算逾期）
DROP VIEW IF EXISTS v_branch_revenue_summary CASCADE;

CREATE OR REPLACE VIEW v_branch_revenue_summary AS
SELECT
    b.id AS branch_id,
    b.code AS branch_code,
    b.name AS branch_name,
    -- 本月數據（金額）- 動態計算逾期
    COALESCE(monthly.revenue, 0) AS current_month_revenue,
    COALESCE(monthly.pending_amount, 0) AS current_month_pending,
    COALESCE(monthly.overdue_amount, 0) AS current_month_overdue,
    -- 本月數據（筆數）
    COALESCE(monthly.paid_count, 0) AS current_month_paid_count,
    COALESCE(monthly.pending_count, 0) AS current_month_pending_count,
    COALESCE(monthly.overdue_count, 0) AS current_month_overdue_count,
    -- 客戶統計
    COALESCE(customer_stats.total_customers, 0) AS total_customers,
    COALESCE(customer_stats.active_customers, 0) AS active_customers,
    -- 合約統計
    COALESCE(contract_stats.total_contracts, 0) AS total_contracts,
    COALESCE(contract_stats.active_contracts, 0) AS active_contracts,
    COALESCE(contract_stats.expiring_soon, 0) AS contracts_expiring_30days
FROM branches b
LEFT JOIN LATERAL (
    SELECT
        -- 已付款
        SUM(amount) FILTER (WHERE payment_status = 'paid') AS revenue,
        COUNT(*) FILTER (WHERE payment_status = 'paid') AS paid_count,
        -- 待收（未到期）：pending 且 due_date >= 今天
        SUM(amount) FILTER (WHERE payment_status = 'pending' AND due_date >= CURRENT_DATE) AS pending_amount,
        COUNT(*) FILTER (WHERE payment_status = 'pending' AND due_date >= CURRENT_DATE) AS pending_count,
        -- 逾期（動態判斷）：pending 且 due_date < 今天，或原本就是 overdue
        SUM(amount) FILTER (WHERE
            payment_status = 'overdue'
            OR (payment_status = 'pending' AND due_date < CURRENT_DATE)
        ) AS overdue_amount,
        COUNT(*) FILTER (WHERE
            payment_status = 'overdue'
            OR (payment_status = 'pending' AND due_date < CURRENT_DATE)
        ) AS overdue_count
    FROM payments
    WHERE branch_id = b.id
      AND due_date >= DATE_TRUNC('month', CURRENT_DATE)
      AND due_date < DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month'
) monthly ON TRUE
LEFT JOIN LATERAL (
    SELECT
        COUNT(*) AS total_customers,
        COUNT(*) FILTER (WHERE status = 'active') AS active_customers
    FROM customers
    WHERE branch_id = b.id
) customer_stats ON TRUE
LEFT JOIN LATERAL (
    SELECT
        COUNT(*) AS total_contracts,
        COUNT(*) FILTER (WHERE status = 'active') AS active_contracts,
        COUNT(*) FILTER (WHERE status = 'active' AND end_date <= CURRENT_DATE + INTERVAL '30 days') AS expiring_soon
    FROM contracts
    WHERE branch_id = b.id
) contract_stats ON TRUE
WHERE b.status = 'active';

COMMENT ON VIEW v_branch_revenue_summary IS '場館營收摘要視圖，動態計算逾期狀態';

-- 2. 建立逾期款項詳情視圖（動態判斷）
DROP VIEW IF EXISTS v_overdue_details CASCADE;

CREATE OR REPLACE VIEW v_overdue_details AS
SELECT
    p.id AS payment_id,
    p.customer_id,
    cu.name AS customer_name,
    cu.company_name,
    cu.phone,
    cu.line_user_id,
    p.contract_id,
    c.contract_number,
    p.branch_id,
    b.name AS branch_name,
    p.amount AS total_due,
    p.due_date,
    p.payment_period,
    (CURRENT_DATE - p.due_date) AS days_overdue,
    CASE
        WHEN (CURRENT_DATE - p.due_date) <= 7 THEN 'warning'
        WHEN (CURRENT_DATE - p.due_date) <= 30 THEN 'danger'
        ELSE 'critical'
    END AS urgency_level
FROM payments p
JOIN customers cu ON p.customer_id = cu.id
LEFT JOIN contracts c ON p.contract_id = c.id
LEFT JOIN branches b ON p.branch_id = b.id
WHERE p.payment_status IN ('pending', 'overdue')
  AND p.due_date < CURRENT_DATE
ORDER BY p.due_date ASC;

COMMENT ON VIEW v_overdue_details IS '逾期款項詳情視圖，動態判斷逾期';

-- 授權
GRANT SELECT ON v_branch_revenue_summary TO anon, authenticated;
GRANT SELECT ON v_overdue_details TO anon, authenticated;

-- 3. 更新續約提醒視圖（保持 45 天）
DROP VIEW IF EXISTS v_renewal_reminders CASCADE;

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
    (c.end_date - CURRENT_DATE) AS days_remaining,
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
  AND c.end_date <= CURRENT_DATE + INTERVAL '45 days'
ORDER BY c.end_date ASC;

COMMENT ON VIEW v_renewal_reminders IS '續約提醒視圖，45天內到期的合約';

GRANT SELECT ON v_renewal_reminders TO anon, authenticated;

-- 4. 更新續約狀態統計視圖
DROP VIEW IF EXISTS v_renewal_status_summary CASCADE;

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
  AND c.end_date <= CURRENT_DATE + INTERVAL '45 days'
GROUP BY branch_id, b.name, renewal_status
ORDER BY branch_id, renewal_status;

GRANT SELECT ON v_renewal_status_summary TO anon, authenticated;

COMMENT ON VIEW v_renewal_status_summary IS '續約狀態統計視圖，45天內到期';
