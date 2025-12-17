-- Migration: 013_legal_letter
-- Description: 存證信函自動生成功能 - 資料庫結構
-- Date: 2025-12-11

-- ============================================================================
-- 1. payments 表新增催繳追蹤欄位
-- ============================================================================

ALTER TABLE payments ADD COLUMN IF NOT EXISTS reminder_count INTEGER DEFAULT 0;
ALTER TABLE payments ADD COLUMN IF NOT EXISTS last_reminder_at TIMESTAMPTZ;

COMMENT ON COLUMN payments.reminder_count IS '催繳次數';
COMMENT ON COLUMN payments.last_reminder_at IS '最後催繳時間';

-- ============================================================================
-- 2. customers 表新增地址欄位
-- ============================================================================

ALTER TABLE customers ADD COLUMN IF NOT EXISTS registered_address TEXT;
ALTER TABLE customers ADD COLUMN IF NOT EXISTS household_address TEXT;

COMMENT ON COLUMN customers.registered_address IS '公司登記地址（公司類型使用）';
COMMENT ON COLUMN customers.household_address IS '戶籍地址（行號/個人類型使用）';

-- ============================================================================
-- 3. 建立存證信函記錄表 legal_letters
-- ============================================================================

CREATE TABLE IF NOT EXISTS legal_letters (
    id              SERIAL PRIMARY KEY,
    payment_id      INTEGER NOT NULL REFERENCES payments(id),
    customer_id     INTEGER NOT NULL REFERENCES customers(id),
    contract_id     INTEGER NOT NULL REFERENCES contracts(id),
    branch_id       INTEGER NOT NULL REFERENCES branches(id),

    -- 存證信函資訊
    letter_number   VARCHAR(50) UNIQUE,           -- 存證信函編號

    -- 收件人資訊（快照）
    recipient_name  VARCHAR(100) NOT NULL,        -- 收件人姓名
    recipient_address TEXT NOT NULL,               -- 收件人地址

    -- 內容
    content         TEXT NOT NULL,                 -- 存證信函內容（LLM 生成）

    -- 金額資訊（快照）
    overdue_amount  NUMERIC(10,2) NOT NULL,       -- 逾期金額
    overdue_days    INTEGER NOT NULL,              -- 逾期天數
    reminder_count  INTEGER NOT NULL,              -- 催繳次數（快照）

    -- 狀態流程: draft → approved → sent
    status          VARCHAR(20) DEFAULT 'draft'
                    CHECK (status IN ('draft', 'approved', 'sent', 'cancelled')),

    -- PDF
    pdf_path        VARCHAR(500),                  -- GCS 路徑
    pdf_generated_at TIMESTAMPTZ,

    -- 審核
    approved_by     VARCHAR(100),
    approved_at     TIMESTAMPTZ,

    -- 寄送
    sent_at         DATE,
    tracking_number VARCHAR(50),                   -- 郵局掛號號碼

    -- LINE 通知
    notified_at     TIMESTAMPTZ,
    notified_to     VARCHAR(100),                  -- LINE user ID

    -- 備註
    notes           TEXT,

    -- 系統欄位
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    created_by      INTEGER
);

-- 索引
CREATE INDEX IF NOT EXISTS idx_legal_letters_payment_id ON legal_letters(payment_id);
CREATE INDEX IF NOT EXISTS idx_legal_letters_customer_id ON legal_letters(customer_id);
CREATE INDEX IF NOT EXISTS idx_legal_letters_branch_id ON legal_letters(branch_id);
CREATE INDEX IF NOT EXISTS idx_legal_letters_status ON legal_letters(status);
CREATE INDEX IF NOT EXISTS idx_legal_letters_created_at ON legal_letters(created_at);

COMMENT ON TABLE legal_letters IS '存證信函記錄表';

-- ============================================================================
-- 4. 建立候選視圖 v_legal_letter_candidates
-- 條件：逾期 > 14 天 且 催繳次數 >= 5 且 尚未建立存證信函
-- ============================================================================

DROP VIEW IF EXISTS v_legal_letter_candidates CASCADE;

CREATE VIEW v_legal_letter_candidates AS
SELECT
    p.id AS payment_id,
    p.contract_id,
    p.customer_id,
    p.branch_id,
    p.amount AS overdue_amount,
    p.due_date,
    (CURRENT_DATE - p.due_date) AS days_overdue,
    p.reminder_count,
    p.last_reminder_at,
    p.payment_period,

    -- 客戶資訊
    cu.name AS customer_name,
    cu.company_name,
    cu.customer_type,
    cu.phone,
    cu.email,
    cu.line_user_id,
    -- 根據客戶類型選擇地址
    CASE
        WHEN cu.customer_type = 'company' THEN COALESCE(cu.registered_address, cu.address)
        ELSE COALESCE(cu.household_address, cu.address)
    END AS legal_address,

    -- 合約資訊
    c.contract_number,
    c.monthly_rent,

    -- 場館資訊
    b.name AS branch_name,

    -- 緊急程度
    CASE
        WHEN (CURRENT_DATE - p.due_date) > 60 THEN 'critical'
        WHEN (CURRENT_DATE - p.due_date) > 30 THEN 'high'
        ELSE 'medium'
    END AS urgency_level

FROM payments p
JOIN customers cu ON p.customer_id = cu.id
JOIN contracts c ON p.contract_id = c.id
JOIN branches b ON p.branch_id = b.id
WHERE
    -- 逾期狀態
    p.payment_status IN ('pending', 'overdue')
    AND p.due_date < CURRENT_DATE
    -- 逾期超過 14 天
    AND (CURRENT_DATE - p.due_date) > 14
    -- 催繳次數 >= 5
    AND COALESCE(p.reminder_count, 0) >= 5
    -- 尚未建立存證信函
    AND NOT EXISTS (
        SELECT 1 FROM legal_letters ll
        WHERE ll.payment_id = p.id
        AND ll.status != 'cancelled'
    )
ORDER BY
    (CURRENT_DATE - p.due_date) DESC,
    p.amount DESC;

COMMENT ON VIEW v_legal_letter_candidates IS '存證信函候選客戶視圖：逾期>14天且催繳>=5次';

-- ============================================================================
-- 5. 建立待處理視圖 v_pending_legal_letters
-- ============================================================================

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
    END AS status_label

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

COMMENT ON VIEW v_pending_legal_letters IS '待處理存證信函視圖';

-- ============================================================================
-- 6. 授權給 PostgREST 用戶
-- ============================================================================

GRANT SELECT ON v_legal_letter_candidates TO anon, authenticated;
GRANT SELECT ON v_pending_legal_letters TO anon, authenticated;
GRANT ALL ON legal_letters TO anon, authenticated;
GRANT USAGE, SELECT ON SEQUENCE legal_letters_id_seq TO anon, authenticated;

-- ============================================================================
-- 7. 更新 updated_at 觸發器
-- ============================================================================

CREATE OR REPLACE FUNCTION update_legal_letters_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_legal_letters_updated_at ON legal_letters;
CREATE TRIGGER trigger_legal_letters_updated_at
    BEFORE UPDATE ON legal_letters
    FOR EACH ROW
    EXECUTE FUNCTION update_legal_letters_updated_at();

-- ============================================================================
-- 8. 生成存證信函編號函數
-- ============================================================================

CREATE OR REPLACE FUNCTION generate_legal_letter_number()
RETURNS VARCHAR(50) AS $$
DECLARE
    today_str VARCHAR(8);
    seq_num INTEGER;
    new_number VARCHAR(50);
BEGIN
    today_str := TO_CHAR(CURRENT_DATE, 'YYYYMMDD');

    SELECT COALESCE(MAX(
        CAST(SUBSTRING(letter_number FROM 'LL' || today_str || '-(\d+)') AS INTEGER)
    ), 0) + 1
    INTO seq_num
    FROM legal_letters
    WHERE letter_number LIKE 'LL' || today_str || '-%';

    new_number := 'LL' || today_str || '-' || LPAD(seq_num::TEXT, 3, '0');

    RETURN new_number;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION generate_legal_letter_number() IS '生成存證信函編號：LL{YYYYMMDD}-{序號}';
