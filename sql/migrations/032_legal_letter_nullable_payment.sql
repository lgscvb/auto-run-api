-- Migration: 032_legal_letter_nullable_payment
-- Description: 允許從合約直接建立存證信函（無需 payment_id）
-- Date: 2025-12-21

-- 移除 payment_id 的 NOT NULL 約束
ALTER TABLE legal_letters ALTER COLUMN payment_id DROP NOT NULL;

-- 更新欄位註解
COMMENT ON COLUMN legal_letters.payment_id IS '付款ID（從逾期付款建立時填入，手動從合約建立時可為 NULL）';

-- 更新視圖以處理 NULL payment_id
DROP VIEW IF EXISTS v_pending_legal_letters CASCADE;

CREATE VIEW v_pending_legal_letters AS
SELECT
    ll.id,
    ll.letter_number,
    ll.payment_id,
    ll.contract_id,
    ll.customer_id,
    ll.branch_id,

    -- 收件人
    ll.recipient_name,
    ll.recipient_address,

    -- 金額
    ll.overdue_amount,
    ll.overdue_days,
    ll.reminder_count,

    -- 狀態
    ll.status,
    ll.pdf_path,
    ll.pdf_generated_at,
    ll.approved_by,
    ll.approved_at,
    ll.sent_at,
    ll.tracking_number,
    ll.notified_at,
    ll.notes,

    -- 時間
    ll.created_at,
    ll.updated_at,

    -- 關聯資訊
    cu.name AS customer_name,
    cu.company_name,
    cu.phone,
    cu.line_user_id,
    c.contract_number,
    b.name AS branch_name,

    -- 狀態標籤
    CASE ll.status
        WHEN 'draft' THEN '草稿'
        WHEN 'approved' THEN '已審核'
        WHEN 'sent' THEN '已寄送'
        WHEN 'cancelled' THEN '已取消'
    END AS status_label,

    -- 建立來源
    CASE
        WHEN ll.payment_id IS NOT NULL THEN 'payment'
        ELSE 'contract'
    END AS source_type

FROM legal_letters ll
JOIN customers cu ON ll.customer_id = cu.id
JOIN contracts c ON ll.contract_id = c.id
JOIN branches b ON ll.branch_id = b.id
WHERE ll.status != 'cancelled'
ORDER BY
    CASE ll.status
        WHEN 'draft' THEN 1
        WHEN 'approved' THEN 2
        WHEN 'sent' THEN 3
    END,
    ll.created_at DESC;

COMMENT ON VIEW v_pending_legal_letters IS '待處理存證信函視圖（含手動從合約建立的存證信函）';

GRANT SELECT ON v_pending_legal_letters TO anon, authenticated;
