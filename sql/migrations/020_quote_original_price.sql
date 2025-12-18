-- ============================================================================
-- 020_quote_original_price.sql
-- 報價單新增原價欄位
-- ============================================================================

-- 新增 original_price 欄位到 quotes 表
-- 用於記錄服務的原價（例如營業登記原價 3000）
-- 折扣後的價格會在 items 中的 unit_price 體現
ALTER TABLE quotes ADD COLUMN IF NOT EXISTS original_price NUMERIC(10,2);

-- 先刪除舊視圖再重建（避免欄位順序衝突）
DROP VIEW IF EXISTS v_quotes;

-- 重建視圖加入 original_price
CREATE VIEW v_quotes AS
SELECT
    q.id,
    q.quote_number,
    q.customer_id,
    COALESCE(c.name, q.customer_name) AS customer_name,
    COALESCE(c.phone, q.customer_phone) AS customer_phone,
    COALESCE(c.email, q.customer_email) AS customer_email,
    COALESCE(c.company_name, q.company_name) AS company_name,
    q.branch_id,
    b.name AS branch_name,
    q.contract_type,
    q.plan_name,
    q.contract_months,
    q.proposed_start_date,
    q.original_price,
    q.items,
    q.subtotal,
    q.discount_amount,
    q.discount_note,
    q.tax_amount,
    q.total_amount,
    q.deposit_amount,
    q.valid_from,
    q.valid_until,
    q.status,
    CASE
        WHEN q.status IN ('accepted', 'converted') THEN FALSE
        WHEN q.valid_until < CURRENT_DATE THEN TRUE
        ELSE FALSE
    END AS is_expired,
    q.converted_contract_id,
    q.internal_notes,
    q.customer_notes,
    q.created_by,
    q.sent_at,
    q.viewed_at,
    q.responded_at,
    q.created_at,
    q.updated_at
FROM quotes q
LEFT JOIN customers c ON q.customer_id = c.id
LEFT JOIN branches b ON q.branch_id = b.id
ORDER BY q.created_at DESC;

-- 授予 anon 角色查詢權限（DROP VIEW 後權限會遺失）
GRANT SELECT ON v_quotes TO anon;

COMMENT ON COLUMN quotes.original_price IS '服務原價（用於合約，例如營業登記原價 3000）';
