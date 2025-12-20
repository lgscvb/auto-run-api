-- Migration: 新增 custom 到 contracts.contract_type CHECK 約束
-- 支援從報價單轉換的自訂組合合約

ALTER TABLE contracts DROP CONSTRAINT IF EXISTS contracts_contract_type_check;

ALTER TABLE contracts ADD CONSTRAINT contracts_contract_type_check
    CHECK (contract_type IN (
        'virtual_office',
        'office',
        'flex_seat',
        'coworking_fixed',
        'coworking_flexible',
        'custom'  -- 自訂組合（多種服務）
    ));
