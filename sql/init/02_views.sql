-- ============================================================================
-- Hour Jungle CRM - PostgreSQL Views
-- 02_views.sql - PostgREST 用的 Views
-- ============================================================================

-- ============================================================================
-- 1. 客戶摘要視圖 (v_customer_summary)
-- ============================================================================
CREATE OR REPLACE VIEW v_customer_summary AS
SELECT
    c.id,
    c.legacy_id,
    c.name,
    c.company_name,
    c.customer_type,
    c.phone,
    c.email,
    c.line_user_id,
    c.status,
    c.risk_level,
    c.source_channel,
    c.created_at,
    b.id AS branch_id,
    b.code AS branch_code,
    b.name AS branch_name,
    -- 合約統計
    COALESCE(contract_stats.total_contracts, 0) AS total_contracts,
    COALESCE(contract_stats.active_contracts, 0) AS active_contracts,
    contract_stats.latest_contract_end,
    -- 繳費統計
    COALESCE(payment_stats.total_paid, 0) AS total_paid,
    COALESCE(payment_stats.pending_amount, 0) AS pending_amount,
    COALESCE(payment_stats.overdue_count, 0) AS overdue_count,
    COALESCE(payment_stats.overdue_amount, 0) AS overdue_amount,
    -- 會計事務所
    af.id AS accounting_firm_id,
    af.name AS accounting_firm_name
FROM customers c
LEFT JOIN branches b ON c.branch_id = b.id
LEFT JOIN accounting_firms af ON c.accounting_firm_id = af.id
LEFT JOIN LATERAL (
    SELECT
        COUNT(*) AS total_contracts,
        COUNT(*) FILTER (WHERE status = 'active') AS active_contracts,
        MAX(end_date) AS latest_contract_end
    FROM contracts
    WHERE customer_id = c.id
) contract_stats ON TRUE
LEFT JOIN LATERAL (
    SELECT
        COALESCE(SUM(amount) FILTER (WHERE payment_status = 'paid'), 0) AS total_paid,
        COALESCE(SUM(amount) FILTER (WHERE payment_status = 'pending'), 0) AS pending_amount,
        COUNT(*) FILTER (WHERE payment_status = 'overdue') AS overdue_count,
        COALESCE(SUM(amount) FILTER (WHERE payment_status = 'overdue'), 0) AS overdue_amount
    FROM payments
    WHERE customer_id = c.id
) payment_stats ON TRUE;

COMMENT ON VIEW v_customer_summary IS '客戶摘要視圖，包含合約和繳費統計';

-- ============================================================================
-- 2. 應收款視圖 (v_payments_due)
-- ============================================================================
CREATE OR REPLACE VIEW v_payments_due AS
SELECT
    p.id,
    p.contract_id,
    p.customer_id,
    p.branch_id,
    p.payment_type,
    p.payment_period,
    p.amount,
    p.late_fee,
    p.due_date,
    p.payment_status,
    p.overdue_days,
    p.notes,
    -- 客戶資訊
    c.name AS customer_name,
    c.company_name,
    c.phone AS customer_phone,
    c.line_user_id,
    c.risk_level,
    -- 場館資訊
    b.code AS branch_code,
    b.name AS branch_name,
    -- 合約資訊
    ct.contract_number,
    ct.monthly_rent,
    ct.end_date AS contract_end_date,
    -- 緊急度計算
    CASE
        WHEN p.payment_status = 'overdue' AND p.overdue_days > 30 THEN 'critical'
        WHEN p.payment_status = 'overdue' AND p.overdue_days > 14 THEN 'high'
        WHEN p.payment_status = 'overdue' THEN 'medium'
        WHEN p.due_date <= CURRENT_DATE + INTERVAL '3 days' THEN 'upcoming'
        ELSE 'normal'
    END AS urgency,
    -- 總應收金額
    p.amount + COALESCE(p.late_fee, 0) AS total_due
FROM payments p
JOIN customers c ON p.customer_id = c.id
JOIN branches b ON p.branch_id = b.id
LEFT JOIN contracts ct ON p.contract_id = ct.id
WHERE p.payment_status IN ('pending', 'overdue')
ORDER BY
    CASE
        WHEN p.payment_status = 'overdue' THEN 0
        ELSE 1
    END,
    p.due_date ASC;

COMMENT ON VIEW v_payments_due IS '應收款列表，含緊急度標記';

-- ============================================================================
-- 3. 續約提醒視圖 (v_renewal_reminders)
-- ============================================================================
CREATE OR REPLACE VIEW v_renewal_reminders AS
SELECT
    ct.id AS contract_id,
    ct.contract_number,
    ct.customer_id,
    ct.branch_id,
    ct.contract_type,
    ct.plan_name,
    ct.start_date,
    ct.end_date,
    ct.monthly_rent,
    ct.status AS contract_status,
    -- 剩餘天數
    ct.end_date - CURRENT_DATE AS days_remaining,
    -- 客戶資訊
    c.name AS customer_name,
    c.company_name,
    c.phone AS customer_phone,
    c.email AS customer_email,
    c.line_user_id,
    c.status AS customer_status,
    -- 場館資訊
    b.code AS branch_code,
    b.name AS branch_name,
    -- 提醒優先級
    CASE
        WHEN ct.end_date - CURRENT_DATE <= 7 THEN 'urgent'
        WHEN ct.end_date - CURRENT_DATE <= 30 THEN 'high'
        WHEN ct.end_date - CURRENT_DATE <= 60 THEN 'medium'
        ELSE 'low'
    END AS priority,
    -- 合約歷史
    (SELECT COUNT(*) FROM contracts WHERE customer_id = ct.customer_id) AS total_contracts_history
FROM contracts ct
JOIN customers c ON ct.customer_id = c.id
JOIN branches b ON ct.branch_id = b.id
WHERE ct.status = 'active'
  AND ct.end_date <= CURRENT_DATE + INTERVAL '90 days'
  AND ct.end_date >= CURRENT_DATE
ORDER BY ct.end_date ASC;

COMMENT ON VIEW v_renewal_reminders IS '續約提醒視圖，90天內到期的合約';

-- ============================================================================
-- 4. 佣金追蹤視圖 (v_commission_tracker)
-- ============================================================================
CREATE OR REPLACE VIEW v_commission_tracker AS
SELECT
    cm.id,
    cm.accounting_firm_id,
    cm.customer_id,
    cm.contract_id,
    cm.amount AS commission_amount,
    cm.based_on_rent,
    cm.contract_start,
    cm.eligible_date,
    cm.status AS commission_status,
    cm.paid_at,
    cm.payment_method,
    cm.payment_reference,
    cm.notes,
    -- 會計事務所資訊
    af.name AS firm_name,
    af.short_name AS firm_short_name,
    af.contact_person AS firm_contact,
    af.phone AS firm_phone,
    af.commission_rate AS firm_rate,
    -- 客戶資訊
    c.name AS customer_name,
    c.company_name,
    -- 合約資訊
    ct.contract_number,
    ct.monthly_rent,
    ct.start_date AS contract_start_date,
    ct.end_date AS contract_end_date,
    ct.status AS contract_status,
    -- 場館資訊
    b.code AS branch_code,
    b.name AS branch_name,
    -- 是否已達付款條件（合約滿6個月）
    CASE
        WHEN ct.start_date + INTERVAL '6 months' <= CURRENT_DATE THEN TRUE
        ELSE FALSE
    END AS is_eligible_now,
    -- 距離可付款日
    CASE
        WHEN ct.start_date + INTERVAL '6 months' <= CURRENT_DATE THEN 0
        ELSE (ct.start_date + INTERVAL '6 months')::DATE - CURRENT_DATE
    END AS days_until_eligible
FROM commissions cm
LEFT JOIN accounting_firms af ON cm.accounting_firm_id = af.id
JOIN customers c ON cm.customer_id = c.id
JOIN contracts ct ON cm.contract_id = ct.id
JOIN branches b ON ct.branch_id = b.id
ORDER BY
    CASE cm.status
        WHEN 'eligible' THEN 1
        WHEN 'pending' THEN 2
        WHEN 'paid' THEN 3
        ELSE 4
    END,
    cm.eligible_date ASC;

COMMENT ON VIEW v_commission_tracker IS '佣金追蹤視圖，含付款資格計算';

-- ============================================================================
-- 5. 場館營收摘要視圖 (v_branch_revenue_summary)
-- ============================================================================
CREATE OR REPLACE VIEW v_branch_revenue_summary AS
SELECT
    b.id AS branch_id,
    b.code AS branch_code,
    b.name AS branch_name,
    -- 本月數據
    COALESCE(monthly.revenue, 0) AS current_month_revenue,
    COALESCE(monthly.paid_count, 0) AS current_month_paid_count,
    COALESCE(monthly.pending_amount, 0) AS current_month_pending,
    COALESCE(monthly.overdue_amount, 0) AS current_month_overdue,
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
        SUM(amount) FILTER (WHERE payment_status = 'paid') AS revenue,
        COUNT(*) FILTER (WHERE payment_status = 'paid') AS paid_count,
        SUM(amount) FILTER (WHERE payment_status = 'pending') AS pending_amount,
        SUM(amount) FILTER (WHERE payment_status = 'overdue') AS overdue_amount
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

COMMENT ON VIEW v_branch_revenue_summary IS '場館營收摘要視圖';

-- ============================================================================
-- 6. 逾期款項詳細視圖 (v_overdue_details)
-- ============================================================================
CREATE OR REPLACE VIEW v_overdue_details AS
SELECT
    p.id AS payment_id,
    p.customer_id,
    p.contract_id,
    p.branch_id,
    p.payment_period,
    p.amount,
    p.late_fee,
    p.amount + COALESCE(p.late_fee, 0) AS total_due,
    p.due_date,
    CURRENT_DATE - p.due_date AS days_overdue,
    -- 客戶資訊
    c.name AS customer_name,
    c.company_name,
    c.phone,
    c.email,
    c.line_user_id,
    c.risk_level,
    -- 場館
    b.code AS branch_code,
    b.name AS branch_name,
    -- 合約
    ct.contract_number,
    ct.monthly_rent,
    ct.status AS contract_status,
    -- 逾期等級
    CASE
        WHEN CURRENT_DATE - p.due_date > 60 THEN 'severe'
        WHEN CURRENT_DATE - p.due_date > 30 THEN 'high'
        WHEN CURRENT_DATE - p.due_date > 14 THEN 'medium'
        ELSE 'low'
    END AS overdue_level,
    -- 歷史逾期次數
    (SELECT COUNT(*) FROM payments
     WHERE customer_id = p.customer_id
       AND payment_status = 'overdue') AS historical_overdue_count
FROM payments p
JOIN customers c ON p.customer_id = c.id
JOIN branches b ON p.branch_id = b.id
LEFT JOIN contracts ct ON p.contract_id = ct.id
WHERE p.payment_status = 'overdue'
ORDER BY days_overdue DESC;

COMMENT ON VIEW v_overdue_details IS '逾期款項詳細視圖';

-- ============================================================================
-- 7. LINE 用戶查詢視圖 (v_line_user_lookup)
-- ============================================================================
CREATE OR REPLACE VIEW v_line_user_lookup AS
SELECT
    c.id AS customer_id,
    c.line_user_id,
    c.line_display_name,
    c.name,
    c.phone,
    c.status,
    c.branch_id,
    b.name AS branch_name,
    -- 活躍合約
    (SELECT COUNT(*) FROM contracts WHERE customer_id = c.id AND status = 'active') AS active_contracts,
    -- 待繳款項
    (SELECT COUNT(*) FROM payments WHERE customer_id = c.id AND payment_status IN ('pending', 'overdue')) AS pending_payments,
    -- 最近互動
    c.updated_at AS last_updated
FROM customers c
JOIN branches b ON c.branch_id = b.id
WHERE c.line_user_id IS NOT NULL
  AND c.line_user_id != '';

COMMENT ON VIEW v_line_user_lookup IS 'LINE 用戶快速查詢視圖';

-- ============================================================================
-- 8. 今日待辦視圖 (v_today_tasks)
-- ============================================================================
CREATE OR REPLACE VIEW v_today_tasks AS
WITH
-- 今日到期款項
due_today AS (
    SELECT
        'payment_due' AS task_type,
        p.id AS reference_id,
        c.name || ' - ' || p.payment_period || ' 租金到期' AS task_description,
        p.amount AS amount,
        p.customer_id,
        c.name AS customer_name,
        c.phone AS customer_phone,
        c.line_user_id,
        p.branch_id,
        b.name AS branch_name,
        'high' AS priority
    FROM payments p
    JOIN customers c ON p.customer_id = c.id
    JOIN branches b ON p.branch_id = b.id
    WHERE p.due_date = CURRENT_DATE
      AND p.payment_status = 'pending'
),
-- 合約今日到期
contract_expiring AS (
    SELECT
        'contract_expiring' AS task_type,
        ct.id AS reference_id,
        c.name || ' 合約今日到期' AS task_description,
        ct.monthly_rent AS amount,
        ct.customer_id,
        c.name AS customer_name,
        c.phone AS customer_phone,
        c.line_user_id,
        ct.branch_id,
        b.name AS branch_name,
        'urgent' AS priority
    FROM contracts ct
    JOIN customers c ON ct.customer_id = c.id
    JOIN branches b ON ct.branch_id = b.id
    WHERE ct.end_date = CURRENT_DATE
      AND ct.status = 'active'
),
-- 佣金可付款
commission_eligible AS (
    SELECT
        'commission_due' AS task_type,
        cm.id AS reference_id,
        COALESCE(af.short_name, af.name) || ' 佣金可付款 (' || c.name || ')' AS task_description,
        cm.amount AS amount,
        cm.customer_id,
        c.name AS customer_name,
        af.phone AS customer_phone,
        NULL AS line_user_id,
        ct.branch_id,
        b.name AS branch_name,
        'medium' AS priority
    FROM commissions cm
    LEFT JOIN accounting_firms af ON cm.accounting_firm_id = af.id
    JOIN customers c ON cm.customer_id = c.id
    JOIN contracts ct ON cm.contract_id = ct.id
    JOIN branches b ON ct.branch_id = b.id
    WHERE cm.status = 'pending'
      AND cm.eligible_date <= CURRENT_DATE
)
SELECT * FROM due_today
UNION ALL
SELECT * FROM contract_expiring
UNION ALL
SELECT * FROM commission_eligible
ORDER BY
    CASE priority
        WHEN 'urgent' THEN 1
        WHEN 'high' THEN 2
        WHEN 'medium' THEN 3
        ELSE 4
    END;

COMMENT ON VIEW v_today_tasks IS '今日待辦事項彙總視圖';
