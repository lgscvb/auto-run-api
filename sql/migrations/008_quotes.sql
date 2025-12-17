-- ============================================================================
-- Hour Jungle CRM - Migration 008: 報價單系統
-- ============================================================================

-- 報價單表 (quotes)
CREATE TABLE IF NOT EXISTS quotes (
    id              SERIAL PRIMARY KEY,
    quote_number    VARCHAR(50) UNIQUE NOT NULL,
    customer_id     INTEGER REFERENCES customers(id),
    branch_id       INTEGER NOT NULL REFERENCES branches(id),

    -- 客戶資訊（未建立客戶時使用）
    customer_name   VARCHAR(100),
    customer_phone  VARCHAR(20),
    customer_email  VARCHAR(100),
    company_name    VARCHAR(200),

    -- 方案資訊
    contract_type   VARCHAR(30) DEFAULT 'virtual_office'
                    CHECK (contract_type IN ('virtual_office', 'coworking_fixed', 'coworking_flexible', 'meeting_room')),
    plan_name       VARCHAR(100),

    -- 期間
    contract_months INTEGER DEFAULT 12,
    proposed_start_date DATE,

    -- 費用明細
    items           JSONB DEFAULT '[]',
    -- 格式: [{"name": "商登月租費", "quantity": 12, "unit_price": 5000, "amount": 60000}]

    subtotal        NUMERIC(12,2) DEFAULT 0,
    discount_amount NUMERIC(12,2) DEFAULT 0,
    discount_note   VARCHAR(200),
    tax_amount      NUMERIC(12,2) DEFAULT 0,
    total_amount    NUMERIC(12,2) DEFAULT 0,

    -- 押金
    deposit_amount  NUMERIC(12,2) DEFAULT 0,

    -- 有效期
    valid_from      DATE DEFAULT CURRENT_DATE,
    valid_until     DATE DEFAULT (CURRENT_DATE + INTERVAL '30 days'),

    -- 狀態
    status          VARCHAR(20) DEFAULT 'draft'
                    CHECK (status IN ('draft', 'sent', 'viewed', 'accepted', 'rejected', 'expired', 'converted')),

    -- 轉換後的合約
    converted_contract_id INTEGER REFERENCES contracts(id),

    -- 備註
    internal_notes  TEXT,
    customer_notes  TEXT,

    -- 建立者
    created_by      VARCHAR(100),

    -- 時間戳
    sent_at         TIMESTAMPTZ,
    viewed_at       TIMESTAMPTZ,
    responded_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- 索引
CREATE INDEX IF NOT EXISTS idx_quotes_customer ON quotes(customer_id);
CREATE INDEX IF NOT EXISTS idx_quotes_branch ON quotes(branch_id);
CREATE INDEX IF NOT EXISTS idx_quotes_status ON quotes(status);
CREATE INDEX IF NOT EXISTS idx_quotes_valid_until ON quotes(valid_until);

-- 報價單號生成函數
CREATE OR REPLACE FUNCTION generate_quote_number()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.quote_number IS NULL THEN
        NEW.quote_number := 'Q' || TO_CHAR(NOW(), 'YYYYMMDD') || '-' ||
                          LPAD(NEXTVAL('quotes_id_seq')::TEXT, 4, '0');
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 觸發器
DROP TRIGGER IF EXISTS trg_quote_number ON quotes;
CREATE TRIGGER trg_quote_number
    BEFORE INSERT ON quotes
    FOR EACH ROW
    EXECUTE FUNCTION generate_quote_number();

-- 更新時間觸發器
CREATE OR REPLACE FUNCTION update_quotes_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_quotes_timestamp ON quotes;
CREATE TRIGGER trg_quotes_timestamp
    BEFORE UPDATE ON quotes
    FOR EACH ROW
    EXECUTE FUNCTION update_quotes_timestamp();

-- 報價單視圖
CREATE OR REPLACE VIEW v_quotes AS
SELECT
    q.id,
    q.quote_number,
    q.customer_id,
    COALESCE(c.name, q.customer_name) AS customer_name,
    COALESCE(c.phone, q.customer_phone) AS customer_phone,
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

COMMENT ON TABLE quotes IS '報價單';
COMMENT ON VIEW v_quotes IS '報價單視圖（含客戶和分館資訊）';
