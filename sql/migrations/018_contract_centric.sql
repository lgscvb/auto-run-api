-- ============================================================================
-- 018_contract_centric.sql
-- 「以合約為主體」重構 - 合約內儲存客戶資訊
-- ============================================================================

-- 1. 擴展 contracts 表：新增客戶資訊欄位
-- 這些欄位是冗餘儲存，用於：
--   - 合約 PDF 生成時不需要 JOIN customers
--   - 同一客戶不同合約可能對應不同公司
--   - 歷史合約快照不受客戶資料修改影響

ALTER TABLE contracts ADD COLUMN IF NOT EXISTS company_name VARCHAR(200);
ALTER TABLE contracts ADD COLUMN IF NOT EXISTS representative_name VARCHAR(100);
ALTER TABLE contracts ADD COLUMN IF NOT EXISTS representative_address TEXT;
ALTER TABLE contracts ADD COLUMN IF NOT EXISTS id_number VARCHAR(20);
ALTER TABLE contracts ADD COLUMN IF NOT EXISTS company_tax_id VARCHAR(8);
ALTER TABLE contracts ADD COLUMN IF NOT EXISTS phone VARCHAR(20);
ALTER TABLE contracts ADD COLUMN IF NOT EXISTS email VARCHAR(100);
ALTER TABLE contracts ADD COLUMN IF NOT EXISTS renewed_from_id INTEGER REFERENCES contracts(id);

-- 2. 新增合約狀態 'renewed'（已續約）
-- 需要先移除舊的 CHECK 約束，再建立新的
ALTER TABLE contracts DROP CONSTRAINT IF EXISTS contracts_status_check;
ALTER TABLE contracts ADD CONSTRAINT contracts_status_check
    CHECK (status IN ('draft', 'pending_sign', 'active', 'expired', 'terminated', 'cancelled', 'renewed'));

-- 3. 新增索引
CREATE INDEX IF NOT EXISTS idx_contracts_company_tax_id ON contracts(company_tax_id);
CREATE INDEX IF NOT EXISTS idx_contracts_phone ON contracts(phone);
CREATE INDEX IF NOT EXISTS idx_contracts_renewed_from_id ON contracts(renewed_from_id);

-- 4. 資料遷移：填補現有合約的客戶資訊
UPDATE contracts c SET
    company_name = cu.company_name,
    representative_name = cu.name,
    id_number = cu.id_number,
    company_tax_id = cu.company_tax_id,
    phone = cu.phone,
    email = cu.email
FROM customers cu
WHERE c.customer_id = cu.id
  AND c.company_name IS NULL;

-- 5. 客戶自動關聯觸發器（匹配優先順序：統編 > 電話）
-- 當建立合約時，如果沒有提供 customer_id，自動查找或建立客戶
CREATE OR REPLACE FUNCTION auto_link_customer()
RETURNS TRIGGER AS $$
DECLARE
    found_customer_id INTEGER;
BEGIN
    -- 如果已有 customer_id 就不處理
    IF NEW.customer_id IS NOT NULL THEN
        RETURN NEW;
    END IF;

    -- 1. 用統編匹配（最準確，公司唯一）
    IF NEW.company_tax_id IS NOT NULL AND NEW.company_tax_id != '' THEN
        SELECT id INTO found_customer_id FROM customers
        WHERE company_tax_id = NEW.company_tax_id
        LIMIT 1;
    END IF;

    -- 2. 用電話匹配（個人唯一）
    IF found_customer_id IS NULL AND NEW.phone IS NOT NULL AND NEW.phone != '' THEN
        SELECT id INTO found_customer_id FROM customers
        WHERE phone = NEW.phone
        LIMIT 1;
    END IF;

    IF found_customer_id IS NOT NULL THEN
        -- 找到現有客戶，關聯之
        NEW.customer_id := found_customer_id;
    ELSE
        -- 自動建立新客戶
        INSERT INTO customers (
            name,
            phone,
            email,
            company_name,
            company_tax_id,
            id_number,
            branch_id,
            status,
            created_at
        ) VALUES (
            COALESCE(NEW.representative_name, NEW.company_name, '未命名客戶'),
            NEW.phone,
            NEW.email,
            NEW.company_name,
            NEW.company_tax_id,
            NEW.id_number,
            NEW.branch_id,
            'active',
            NOW()
        )
        RETURNING id INTO NEW.customer_id;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 移除舊觸發器（如果存在）
DROP TRIGGER IF EXISTS trg_contract_auto_link_customer ON contracts;

-- 建立新觸發器
CREATE TRIGGER trg_contract_auto_link_customer
    BEFORE INSERT ON contracts
    FOR EACH ROW
    EXECUTE FUNCTION auto_link_customer();

-- 6. 建立重複客戶檢測視圖
CREATE OR REPLACE VIEW v_duplicate_customers AS
SELECT
    phone,
    COUNT(*) as customer_count,
    array_agg(id) as customer_ids,
    array_agg(name) as customer_names
FROM customers
WHERE phone IS NOT NULL AND phone != ''
GROUP BY phone
HAVING COUNT(*) > 1;

-- 7. 驗證腳本（Migration 後應執行）
-- SELECT COUNT(*) FROM contracts WHERE customer_id IS NULL;
-- 結果應為 0

COMMENT ON COLUMN contracts.company_name IS '公司名稱（冗餘儲存）';
COMMENT ON COLUMN contracts.representative_name IS '負責人姓名';
COMMENT ON COLUMN contracts.representative_address IS '負責人地址';
COMMENT ON COLUMN contracts.id_number IS '身分證/居留證號碼';
COMMENT ON COLUMN contracts.company_tax_id IS '公司統編（可為空，新設立公司）';
COMMENT ON COLUMN contracts.phone IS '聯絡電話';
COMMENT ON COLUMN contracts.email IS '電子郵件';
COMMENT ON COLUMN contracts.renewed_from_id IS '續約來源合約 ID';
