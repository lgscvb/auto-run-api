-- ============================================================================
-- Hour Jungle CRM - Migration 011: 報價單轉合約欄位
-- ============================================================================

-- 報價單增加轉換時間
ALTER TABLE quotes ADD COLUMN IF NOT EXISTS converted_at TIMESTAMPTZ;

-- 合約表增加來源報價單
ALTER TABLE contracts ADD COLUMN IF NOT EXISTS quote_id INTEGER REFERENCES quotes(id);

-- 索引
CREATE INDEX IF NOT EXISTS idx_contracts_quote ON contracts(quote_id);

-- 更新 v_quotes 視圖，加入 converted_at
DROP VIEW IF EXISTS v_quotes;
CREATE OR REPLACE VIEW v_quotes AS
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
    q.converted_at,
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

COMMENT ON COLUMN quotes.converted_at IS '轉換為合約的時間';
COMMENT ON COLUMN contracts.quote_id IS '來源報價單ID';
