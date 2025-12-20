-- Migration: 027_v_quotes_add_line_user_id
-- Description: 更新 v_quotes 視圖加入 line_user_id 欄位
-- Date: 2025-12-20

-- 重建視圖（PostgreSQL 需要 DROP 再 CREATE 來新增欄位）
DROP VIEW IF EXISTS v_quotes;

CREATE VIEW v_quotes AS
SELECT q.id,
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
        WHEN q.status::text = ANY (ARRAY['accepted'::character varying, 'converted'::character varying]::text[]) THEN false
        WHEN q.valid_until < CURRENT_DATE THEN true
        ELSE false
    END AS is_expired,
    q.converted_contract_id,
    q.internal_notes,
    q.customer_notes,
    q.created_by,
    q.sent_at,
    q.viewed_at,
    q.responded_at,
    q.created_at,
    q.updated_at,
    q.line_user_id
FROM quotes q
LEFT JOIN customers c ON q.customer_id = c.id
LEFT JOIN branches b ON q.branch_id = b.id
ORDER BY q.created_at DESC;

-- 授權
GRANT SELECT ON v_quotes TO anon;
GRANT SELECT ON v_quotes TO authenticated;

COMMENT ON VIEW v_quotes IS '報價單視圖（包含客戶和場館資訊）';
