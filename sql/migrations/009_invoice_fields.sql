-- 009_invoice_fields.sql
-- 新增發票相關欄位到 payments 表

-- 發票號碼
ALTER TABLE payments
ADD COLUMN IF NOT EXISTS invoice_number VARCHAR(20);

-- 發票日期
ALTER TABLE payments
ADD COLUMN IF NOT EXISTS invoice_date DATE;

-- 發票狀態 (issued=已開立, voided=已作廢)
ALTER TABLE payments
ADD COLUMN IF NOT EXISTS invoice_status VARCHAR(20) DEFAULT NULL;

-- 發票號碼索引
CREATE INDEX IF NOT EXISTS idx_payments_invoice_number
ON payments(invoice_number) WHERE invoice_number IS NOT NULL;

-- 發票狀態索引
CREATE INDEX IF NOT EXISTS idx_payments_invoice_status
ON payments(invoice_status) WHERE invoice_status IS NOT NULL;

COMMENT ON COLUMN payments.invoice_number IS '電子發票號碼';
COMMENT ON COLUMN payments.invoice_date IS '發票開立日期';
COMMENT ON COLUMN payments.invoice_status IS '發票狀態 (issued=已開立, voided=已作廢)';
