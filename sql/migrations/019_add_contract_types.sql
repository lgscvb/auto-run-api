-- ============================================================================
-- 019_add_contract_types.sql
-- 新增合約類型：辦公室租賃 (office) 和自由座 (flex_seat)
-- ============================================================================

-- 移除舊的 contract_type CHECK 約束
ALTER TABLE contracts DROP CONSTRAINT IF EXISTS contracts_contract_type_check;

-- 建立新的 contract_type CHECK 約束（包含所有現有類型 + 新類型）
ALTER TABLE contracts ADD CONSTRAINT contracts_contract_type_check
    CHECK (contract_type IN (
        'virtual_office',      -- 營業登記
        'office',              -- 辦公室租賃（新增）
        'flex_seat',           -- 自由座（新增）
        'coworking_fixed',     -- 固定座（舊有）
        'coworking_flexible'   -- 彈性座（舊有）
    ));

-- 更新欄位註解
COMMENT ON COLUMN contracts.contract_type IS '合約類型：virtual_office=營業登記, office=辦公室租賃, flex_seat=自由座, coworking_fixed=固定座, coworking_flexible=彈性座';
