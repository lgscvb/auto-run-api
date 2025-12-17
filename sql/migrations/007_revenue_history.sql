-- Migration: 007_revenue_history
-- Description: 建立營收歷史視圖，支援 YoY/MoM/QoQ 比較
-- Date: 2025-12-07

-- ============================================================================
-- 1. 月度營收統計視圖
-- ============================================================================

DROP VIEW IF EXISTS v_monthly_revenue CASCADE;

CREATE OR REPLACE VIEW v_monthly_revenue AS
SELECT
    DATE_TRUNC('month', p.due_date)::date AS period_start,
    TO_CHAR(p.due_date, 'YYYY-MM') AS period,
    EXTRACT(YEAR FROM p.due_date)::int AS year,
    EXTRACT(MONTH FROM p.due_date)::int AS month,
    b.id AS branch_id,
    b.name AS branch_name,
    -- 金額統計
    SUM(p.amount) FILTER (WHERE p.payment_status = 'paid') AS revenue,
    SUM(p.amount) FILTER (WHERE p.payment_status = 'pending' AND p.due_date >= CURRENT_DATE) AS pending,
    SUM(p.amount) FILTER (WHERE p.payment_status = 'overdue' OR (p.payment_status = 'pending' AND p.due_date < CURRENT_DATE)) AS overdue,
    SUM(p.amount) AS total_due,
    -- 筆數統計
    COUNT(*) FILTER (WHERE p.payment_status = 'paid') AS paid_count,
    COUNT(*) FILTER (WHERE p.payment_status = 'pending' AND p.due_date >= CURRENT_DATE) AS pending_count,
    COUNT(*) FILTER (WHERE p.payment_status = 'overdue' OR (p.payment_status = 'pending' AND p.due_date < CURRENT_DATE)) AS overdue_count,
    COUNT(*) AS total_count,
    -- 收款率
    ROUND(
        COALESCE(SUM(p.amount) FILTER (WHERE p.payment_status = 'paid') / NULLIF(SUM(p.amount), 0) * 100, 0),
        1
    ) AS collection_rate
FROM payments p
LEFT JOIN branches b ON p.branch_id = b.id
GROUP BY DATE_TRUNC('month', p.due_date), TO_CHAR(p.due_date, 'YYYY-MM'),
         EXTRACT(YEAR FROM p.due_date), EXTRACT(MONTH FROM p.due_date),
         b.id, b.name
ORDER BY period_start DESC, branch_name;

COMMENT ON VIEW v_monthly_revenue IS '月度營收統計視圖';

-- ============================================================================
-- 2. 季度營收統計視圖
-- ============================================================================

DROP VIEW IF EXISTS v_quarterly_revenue CASCADE;

CREATE OR REPLACE VIEW v_quarterly_revenue AS
SELECT
    DATE_TRUNC('quarter', p.due_date)::date AS period_start,
    TO_CHAR(p.due_date, 'YYYY') || '-Q' || EXTRACT(QUARTER FROM p.due_date) AS period,
    EXTRACT(YEAR FROM p.due_date)::int AS year,
    EXTRACT(QUARTER FROM p.due_date)::int AS quarter,
    b.id AS branch_id,
    b.name AS branch_name,
    -- 金額統計
    SUM(p.amount) FILTER (WHERE p.payment_status = 'paid') AS revenue,
    SUM(p.amount) FILTER (WHERE p.payment_status = 'pending' AND p.due_date >= CURRENT_DATE) AS pending,
    SUM(p.amount) FILTER (WHERE p.payment_status = 'overdue' OR (p.payment_status = 'pending' AND p.due_date < CURRENT_DATE)) AS overdue,
    SUM(p.amount) AS total_due,
    -- 筆數統計
    COUNT(*) FILTER (WHERE p.payment_status = 'paid') AS paid_count,
    COUNT(*) AS total_count,
    -- 收款率
    ROUND(
        COALESCE(SUM(p.amount) FILTER (WHERE p.payment_status = 'paid') / NULLIF(SUM(p.amount), 0) * 100, 0),
        1
    ) AS collection_rate
FROM payments p
LEFT JOIN branches b ON p.branch_id = b.id
GROUP BY DATE_TRUNC('quarter', p.due_date),
         TO_CHAR(p.due_date, 'YYYY') || '-Q' || EXTRACT(QUARTER FROM p.due_date),
         EXTRACT(YEAR FROM p.due_date), EXTRACT(QUARTER FROM p.due_date),
         b.id, b.name
ORDER BY period_start DESC, branch_name;

COMMENT ON VIEW v_quarterly_revenue IS '季度營收統計視圖';

-- ============================================================================
-- 3. 年度營收統計視圖
-- ============================================================================

DROP VIEW IF EXISTS v_yearly_revenue CASCADE;

CREATE OR REPLACE VIEW v_yearly_revenue AS
SELECT
    DATE_TRUNC('year', p.due_date)::date AS period_start,
    TO_CHAR(p.due_date, 'YYYY') AS period,
    EXTRACT(YEAR FROM p.due_date)::int AS year,
    b.id AS branch_id,
    b.name AS branch_name,
    -- 金額統計
    SUM(p.amount) FILTER (WHERE p.payment_status = 'paid') AS revenue,
    SUM(p.amount) FILTER (WHERE p.payment_status = 'pending' AND p.due_date >= CURRENT_DATE) AS pending,
    SUM(p.amount) FILTER (WHERE p.payment_status = 'overdue' OR (p.payment_status = 'pending' AND p.due_date < CURRENT_DATE)) AS overdue,
    SUM(p.amount) AS total_due,
    -- 筆數統計
    COUNT(*) FILTER (WHERE p.payment_status = 'paid') AS paid_count,
    COUNT(*) AS total_count,
    -- 收款率
    ROUND(
        COALESCE(SUM(p.amount) FILTER (WHERE p.payment_status = 'paid') / NULLIF(SUM(p.amount), 0) * 100, 0),
        1
    ) AS collection_rate
FROM payments p
LEFT JOIN branches b ON p.branch_id = b.id
GROUP BY DATE_TRUNC('year', p.due_date), TO_CHAR(p.due_date, 'YYYY'),
         EXTRACT(YEAR FROM p.due_date), b.id, b.name
ORDER BY period_start DESC, branch_name;

COMMENT ON VIEW v_yearly_revenue IS '年度營收統計視圖';

-- ============================================================================
-- 4. 全公司月度營收匯總（含 MoM 計算）
-- ============================================================================

DROP VIEW IF EXISTS v_company_monthly_revenue CASCADE;

CREATE OR REPLACE VIEW v_company_monthly_revenue AS
WITH monthly_totals AS (
    SELECT
        DATE_TRUNC('month', p.due_date)::date AS period_start,
        TO_CHAR(p.due_date, 'YYYY-MM') AS period,
        EXTRACT(YEAR FROM p.due_date)::int AS year,
        EXTRACT(MONTH FROM p.due_date)::int AS month,
        SUM(p.amount) FILTER (WHERE p.payment_status = 'paid') AS revenue,
        SUM(p.amount) AS total_due,
        COUNT(*) FILTER (WHERE p.payment_status = 'paid') AS paid_count,
        COUNT(*) AS total_count
    FROM payments p
    GROUP BY DATE_TRUNC('month', p.due_date), TO_CHAR(p.due_date, 'YYYY-MM'),
             EXTRACT(YEAR FROM p.due_date), EXTRACT(MONTH FROM p.due_date)
)
SELECT
    m.period_start,
    m.period,
    m.year,
    m.month,
    COALESCE(m.revenue, 0) AS revenue,
    COALESCE(m.total_due, 0) AS total_due,
    COALESCE(m.paid_count, 0) AS paid_count,
    COALESCE(m.total_count, 0) AS total_count,
    -- MoM (與上月比較)
    COALESCE(prev.revenue, 0) AS prev_month_revenue,
    CASE
        WHEN COALESCE(prev.revenue, 0) = 0 THEN NULL
        ELSE ROUND((COALESCE(m.revenue, 0) - COALESCE(prev.revenue, 0)) / prev.revenue * 100, 1)
    END AS mom_change,
    -- YoY (與去年同月比較)
    COALESCE(yoy.revenue, 0) AS prev_year_revenue,
    CASE
        WHEN COALESCE(yoy.revenue, 0) = 0 THEN NULL
        ELSE ROUND((COALESCE(m.revenue, 0) - COALESCE(yoy.revenue, 0)) / yoy.revenue * 100, 1)
    END AS yoy_change
FROM monthly_totals m
LEFT JOIN monthly_totals prev ON prev.period_start = m.period_start - INTERVAL '1 month'
LEFT JOIN monthly_totals yoy ON yoy.year = m.year - 1 AND yoy.month = m.month
ORDER BY m.period_start DESC;

COMMENT ON VIEW v_company_monthly_revenue IS '全公司月度營收匯總，含 MoM/YoY 比較';

-- ============================================================================
-- 5. 全公司季度營收匯總（含 QoQ 計算）
-- ============================================================================

DROP VIEW IF EXISTS v_company_quarterly_revenue CASCADE;

CREATE OR REPLACE VIEW v_company_quarterly_revenue AS
WITH quarterly_totals AS (
    SELECT
        DATE_TRUNC('quarter', p.due_date)::date AS period_start,
        TO_CHAR(p.due_date, 'YYYY') || '-Q' || EXTRACT(QUARTER FROM p.due_date) AS period,
        EXTRACT(YEAR FROM p.due_date)::int AS year,
        EXTRACT(QUARTER FROM p.due_date)::int AS quarter,
        SUM(p.amount) FILTER (WHERE p.payment_status = 'paid') AS revenue,
        SUM(p.amount) AS total_due,
        COUNT(*) FILTER (WHERE p.payment_status = 'paid') AS paid_count,
        COUNT(*) AS total_count
    FROM payments p
    GROUP BY DATE_TRUNC('quarter', p.due_date),
             TO_CHAR(p.due_date, 'YYYY') || '-Q' || EXTRACT(QUARTER FROM p.due_date),
             EXTRACT(YEAR FROM p.due_date), EXTRACT(QUARTER FROM p.due_date)
)
SELECT
    q.period_start,
    q.period,
    q.year,
    q.quarter,
    COALESCE(q.revenue, 0) AS revenue,
    COALESCE(q.total_due, 0) AS total_due,
    COALESCE(q.paid_count, 0) AS paid_count,
    COALESCE(q.total_count, 0) AS total_count,
    -- QoQ (與上季比較)
    COALESCE(prev.revenue, 0) AS prev_quarter_revenue,
    CASE
        WHEN COALESCE(prev.revenue, 0) = 0 THEN NULL
        ELSE ROUND((COALESCE(q.revenue, 0) - COALESCE(prev.revenue, 0)) / prev.revenue * 100, 1)
    END AS qoq_change,
    -- YoY (與去年同季比較)
    COALESCE(yoy.revenue, 0) AS prev_year_revenue,
    CASE
        WHEN COALESCE(yoy.revenue, 0) = 0 THEN NULL
        ELSE ROUND((COALESCE(q.revenue, 0) - COALESCE(yoy.revenue, 0)) / yoy.revenue * 100, 1)
    END AS yoy_change
FROM quarterly_totals q
LEFT JOIN quarterly_totals prev ON prev.period_start = q.period_start - INTERVAL '3 months'
LEFT JOIN quarterly_totals yoy ON yoy.year = q.year - 1 AND yoy.quarter = q.quarter
ORDER BY q.period_start DESC;

COMMENT ON VIEW v_company_quarterly_revenue IS '全公司季度營收匯總，含 QoQ/YoY 比較';

-- ============================================================================
-- 6. 全公司年度營收匯總（含 YoY 計算）
-- ============================================================================

DROP VIEW IF EXISTS v_company_yearly_revenue CASCADE;

CREATE OR REPLACE VIEW v_company_yearly_revenue AS
WITH yearly_totals AS (
    SELECT
        DATE_TRUNC('year', p.due_date)::date AS period_start,
        TO_CHAR(p.due_date, 'YYYY') AS period,
        EXTRACT(YEAR FROM p.due_date)::int AS year,
        SUM(p.amount) FILTER (WHERE p.payment_status = 'paid') AS revenue,
        SUM(p.amount) AS total_due,
        COUNT(*) FILTER (WHERE p.payment_status = 'paid') AS paid_count,
        COUNT(*) AS total_count
    FROM payments p
    GROUP BY DATE_TRUNC('year', p.due_date), TO_CHAR(p.due_date, 'YYYY'),
             EXTRACT(YEAR FROM p.due_date)
)
SELECT
    y.period_start,
    y.period,
    y.year,
    COALESCE(y.revenue, 0) AS revenue,
    COALESCE(y.total_due, 0) AS total_due,
    COALESCE(y.paid_count, 0) AS paid_count,
    COALESCE(y.total_count, 0) AS total_count,
    -- YoY (與去年比較)
    COALESCE(prev.revenue, 0) AS prev_year_revenue,
    CASE
        WHEN COALESCE(prev.revenue, 0) = 0 THEN NULL
        ELSE ROUND((COALESCE(y.revenue, 0) - COALESCE(prev.revenue, 0)) / prev.revenue * 100, 1)
    END AS yoy_change
FROM yearly_totals y
LEFT JOIN yearly_totals prev ON prev.year = y.year - 1
ORDER BY y.period_start DESC;

COMMENT ON VIEW v_company_yearly_revenue IS '全公司年度營收匯總，含 YoY 比較';

-- ============================================================================
-- 授權
-- ============================================================================

GRANT SELECT ON v_monthly_revenue TO anon, authenticated;
GRANT SELECT ON v_quarterly_revenue TO anon, authenticated;
GRANT SELECT ON v_yearly_revenue TO anon, authenticated;
GRANT SELECT ON v_company_monthly_revenue TO anon, authenticated;
GRANT SELECT ON v_company_quarterly_revenue TO anon, authenticated;
GRANT SELECT ON v_company_yearly_revenue TO anon, authenticated;
