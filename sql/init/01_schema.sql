-- ============================================================================
-- Hour Jungle CRM - PostgreSQL Schema
-- 01_schema.sql - 核心資料表
-- ============================================================================

-- 啟用必要擴展
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ============================================================================
-- 1. 場館表 (branches)
-- ============================================================================
CREATE TABLE branches (
    id              SERIAL PRIMARY KEY,
    code            VARCHAR(20) UNIQUE NOT NULL,
    name            VARCHAR(100) NOT NULL,
    rental_address  VARCHAR(200) NOT NULL,
    city            VARCHAR(50) DEFAULT '台中市',
    district        VARCHAR(50),
    contact_phone   VARCHAR(20),
    manager_name    VARCHAR(50),
    status          VARCHAR(20) DEFAULT 'active'
                    CHECK (status IN ('active', 'preparing', 'closed')),
    allow_small_scale BOOLEAN DEFAULT FALSE,
    has_good_relationship BOOLEAN DEFAULT FALSE,
    tax_office_district VARCHAR(100),
    config          JSONB DEFAULT '{}',
    notes           TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================================
-- 2. 會計事務所表 (accounting_firms)
-- ============================================================================
CREATE TABLE accounting_firms (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(200) NOT NULL,
    short_name      VARCHAR(50),
    contact_person  VARCHAR(100),
    phone           VARCHAR(20),
    email           VARCHAR(100),
    address         TEXT,
    commission_rate NUMERIC(5,2) DEFAULT 100.00,
    payment_terms   VARCHAR(200) DEFAULT '簽約滿6個月',
    status          VARCHAR(20) DEFAULT 'active'
                    CHECK (status IN ('active', 'inactive', 'suspended')),
    total_referrals INTEGER DEFAULT 0,
    total_commission_paid NUMERIC(15,2) DEFAULT 0,
    metadata        JSONB DEFAULT '{}',
    notes           TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================================
-- 3. 客戶表 (customers)
-- ============================================================================
CREATE TABLE customers (
    id              SERIAL PRIMARY KEY,
    legacy_id       VARCHAR(20) UNIQUE,
    branch_id       INTEGER NOT NULL REFERENCES branches(id),

    -- 基本資料
    customer_type   VARCHAR(30) DEFAULT 'individual'
                    CHECK (customer_type IN ('individual', 'sole_proprietorship', 'company')),
    name            VARCHAR(100) NOT NULL,
    company_name    VARCHAR(200),
    company_tax_id  VARCHAR(8),
    id_number       VARCHAR(10),
    birthday        DATE,

    -- 聯絡資訊
    phone           VARCHAR(20),
    email           VARCHAR(100),
    address         TEXT,
    line_user_id    VARCHAR(100),
    line_display_name VARCHAR(100),

    -- 發票設定
    invoice_title   VARCHAR(200),
    invoice_tax_id  VARCHAR(8),
    invoice_delivery VARCHAR(20) DEFAULT 'email'
                    CHECK (invoice_delivery IN ('email', 'carrier', 'personal')),
    invoice_carrier VARCHAR(20),

    -- 客戶來源
    source_channel  VARCHAR(50) DEFAULT 'others',
    source_detail   VARCHAR(200),
    referrer_id     INTEGER REFERENCES customers(id),
    accounting_firm_id INTEGER REFERENCES accounting_firms(id),

    -- 狀態與風險
    status          VARCHAR(20) DEFAULT 'active'
                    CHECK (status IN ('prospect', 'active', 'suspended', 'churned')),
    risk_level      VARCHAR(10) DEFAULT 'low'
                    CHECK (risk_level IN ('low', 'medium', 'high')),
    risk_notes      TEXT,

    -- 擴展欄位 (KYC 等靈活欄位)
    metadata        JSONB DEFAULT '{}',
    industry_notes  TEXT,
    notes           TEXT,

    -- 系統欄位
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    created_by      INTEGER
);

-- 客戶表索引
CREATE INDEX idx_customers_branch_id ON customers(branch_id);
CREATE INDEX idx_customers_status ON customers(status);
CREATE INDEX idx_customers_line_user_id ON customers(line_user_id);
CREATE INDEX idx_customers_phone ON customers(phone);
CREATE INDEX idx_customers_company_tax_id ON customers(company_tax_id);
CREATE INDEX idx_customers_accounting_firm_id ON customers(accounting_firm_id);
CREATE INDEX idx_customers_metadata ON customers USING GIN(metadata);

-- ============================================================================
-- 4. 合約表 (contracts)
-- ============================================================================
CREATE TABLE contracts (
    id              SERIAL PRIMARY KEY,
    contract_number VARCHAR(50) UNIQUE,
    customer_id     INTEGER NOT NULL REFERENCES customers(id),
    branch_id       INTEGER NOT NULL REFERENCES branches(id),

    -- 合約類型
    contract_type   VARCHAR(30) DEFAULT 'virtual_office'
                    CHECK (contract_type IN ('virtual_office', 'coworking_fixed', 'coworking_flexible', 'meeting_room')),
    plan_name       VARCHAR(100),
    rental_address  VARCHAR(200),

    -- 期間
    start_date      DATE NOT NULL,
    end_date        DATE NOT NULL,
    signed_at       DATE,

    -- 費用
    original_price  NUMERIC(10,2),
    discount_rate   NUMERIC(5,2) DEFAULT 100.00,
    monthly_rent    NUMERIC(10,2) NOT NULL,
    deposit         NUMERIC(10,2) DEFAULT 0,
    deposit_status  VARCHAR(20) DEFAULT 'held'
                    CHECK (deposit_status IN ('held', 'refunded', 'forfeited')),

    -- 繳費設定
    payment_cycle   VARCHAR(20) DEFAULT 'monthly'
                    CHECK (payment_cycle IN ('monthly', 'quarterly', 'semi_annual', 'annual', 'biennial')),
    payment_day     INTEGER DEFAULT 5 CHECK (payment_day BETWEEN 1 AND 31),

    -- 狀態
    status          VARCHAR(20) DEFAULT 'draft'
                    CHECK (status IN ('draft', 'pending_sign', 'active', 'expired', 'terminated', 'cancelled')),

    -- 介紹人/佣金
    broker_name     VARCHAR(100),
    broker_firm_id  INTEGER REFERENCES accounting_firms(id),
    commission_eligible BOOLEAN DEFAULT FALSE,
    commission_paid BOOLEAN DEFAULT FALSE,
    commission_due_date DATE,

    -- 加值服務
    addon_services  JSONB DEFAULT '[]',

    -- 擴展欄位
    metadata        JSONB DEFAULT '{}',
    notes           TEXT,

    -- 系統欄位
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    created_by      INTEGER,

    -- 約束
    CONSTRAINT valid_date_range CHECK (end_date > start_date)
);

-- 合約表索引
CREATE INDEX idx_contracts_customer_id ON contracts(customer_id);
CREATE INDEX idx_contracts_branch_id ON contracts(branch_id);
CREATE INDEX idx_contracts_status ON contracts(status);
CREATE INDEX idx_contracts_end_date ON contracts(end_date);
CREATE INDEX idx_contracts_broker_firm_id ON contracts(broker_firm_id);

-- ============================================================================
-- 5. 付款表 (payments)
-- ============================================================================
CREATE TABLE payments (
    id              SERIAL PRIMARY KEY,
    contract_id     INTEGER NOT NULL REFERENCES contracts(id),
    customer_id     INTEGER NOT NULL REFERENCES customers(id),
    branch_id       INTEGER NOT NULL REFERENCES branches(id),

    -- 付款類型
    payment_type    VARCHAR(20) NOT NULL
                    CHECK (payment_type IN ('deposit', 'rent', 'addon', 'penalty', 'refund')),
    payment_period  VARCHAR(20),

    -- 金額
    amount          NUMERIC(10,2) NOT NULL,
    late_fee        NUMERIC(10,2) DEFAULT 0,

    -- 付款資訊
    payment_method  VARCHAR(30)
                    CHECK (payment_method IN ('cash', 'transfer', 'credit_card', 'line_pay', NULL)),
    payment_status  VARCHAR(20) DEFAULT 'pending'
                    CHECK (payment_status IN ('pending', 'paid', 'overdue', 'cancelled', 'refunded')),

    -- 日期
    due_date        DATE NOT NULL,
    paid_at         TIMESTAMPTZ,

    -- 發票
    invoice_number  VARCHAR(20),
    invoice_date    DATE,
    invoice_status  VARCHAR(20) DEFAULT 'pending'
                    CHECK (invoice_status IN ('pending', 'issued', 'void')),

    -- 逾期
    overdue_days    INTEGER DEFAULT 0,

    -- 擴展欄位
    metadata        JSONB DEFAULT '{}',
    notes           TEXT,

    -- 系統欄位
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- 付款表索引
CREATE INDEX idx_payments_contract_id ON payments(contract_id);
CREATE INDEX idx_payments_customer_id ON payments(customer_id);
CREATE INDEX idx_payments_branch_id ON payments(branch_id);
CREATE INDEX idx_payments_due_date ON payments(due_date);
CREATE INDEX idx_payments_status ON payments(payment_status);
CREATE INDEX idx_payments_period ON payments(payment_period);

-- ============================================================================
-- 6. 佣金表 (commissions)
-- ============================================================================
CREATE TABLE commissions (
    id              SERIAL PRIMARY KEY,
    accounting_firm_id INTEGER REFERENCES accounting_firms(id),
    customer_id     INTEGER NOT NULL REFERENCES customers(id),
    contract_id     INTEGER NOT NULL REFERENCES contracts(id),

    -- 金額
    amount          NUMERIC(10,2) NOT NULL,
    based_on_rent   NUMERIC(10,2),

    -- 時間
    contract_start  DATE,
    eligible_date   DATE,

    -- 狀態
    status          VARCHAR(20) DEFAULT 'pending'
                    CHECK (status IN ('pending', 'eligible', 'paid', 'cancelled')),
    paid_at         DATE,
    payment_method  VARCHAR(50),
    payment_reference VARCHAR(100),

    -- 擴展欄位
    metadata        JSONB DEFAULT '{}',
    notes           TEXT,

    -- 系統欄位
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- 佣金表索引
CREATE INDEX idx_commissions_firm_id ON commissions(accounting_firm_id);
CREATE INDEX idx_commissions_customer_id ON commissions(customer_id);
CREATE INDEX idx_commissions_contract_id ON commissions(contract_id);
CREATE INDEX idx_commissions_status ON commissions(status);
CREATE INDEX idx_commissions_eligible_date ON commissions(eligible_date);

-- ============================================================================
-- 7. 審計日誌表 (audit_logs)
-- ============================================================================
CREATE TABLE audit_logs (
    id              BIGSERIAL PRIMARY KEY,
    table_name      VARCHAR(50) NOT NULL,
    record_id       INTEGER NOT NULL,
    action          VARCHAR(10) NOT NULL
                    CHECK (action IN ('INSERT', 'UPDATE', 'DELETE')),
    old_data        JSONB,
    new_data        JSONB,
    changed_fields  TEXT[],
    user_id         INTEGER,
    user_role       VARCHAR(20),
    ip_address      INET,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_audit_logs_table ON audit_logs(table_name, record_id);
CREATE INDEX idx_audit_logs_created_at ON audit_logs(created_at);

-- ============================================================================
-- 8. 通知隊列表 (notification_queue)
-- ============================================================================
CREATE TABLE notification_queue (
    id              BIGSERIAL PRIMARY KEY,
    notification_type VARCHAR(50) NOT NULL,
    channel         VARCHAR(20) DEFAULT 'line'
                    CHECK (channel IN ('line', 'email', 'sms')),
    recipient_id    INTEGER REFERENCES customers(id),
    recipient_line_id VARCHAR(100),
    payload         JSONB NOT NULL,
    scheduled_at    TIMESTAMPTZ NOT NULL,
    status          VARCHAR(20) DEFAULT 'pending'
                    CHECK (status IN ('pending', 'processing', 'sent', 'failed')),
    sent_at         TIMESTAMPTZ,
    error_message   TEXT,
    retry_count     INTEGER DEFAULT 0,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_notification_queue_status ON notification_queue(status, scheduled_at);
CREATE INDEX idx_notification_queue_recipient ON notification_queue(recipient_id);

-- ============================================================================
-- 9. 系統設定表 (system_settings)
-- ============================================================================
CREATE TABLE system_settings (
    id              SERIAL PRIMARY KEY,
    setting_key     VARCHAR(100) UNIQUE NOT NULL,
    setting_value   TEXT,
    setting_type    VARCHAR(20) DEFAULT 'string'
                    CHECK (setting_type IN ('string', 'number', 'boolean', 'json')),
    description     TEXT,
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_by      INTEGER
);
