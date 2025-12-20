-- Migration: 新增 custom 到 quotes.contract_type CHECK 約束
-- 支援多種服務組合的報價單

ALTER TABLE quotes DROP CONSTRAINT IF EXISTS quotes_contract_type_check;

ALTER TABLE quotes ADD CONSTRAINT quotes_contract_type_check
    CHECK (contract_type IN (
        'virtual_office',
        'coworking_fixed',
        'coworking_flexible',
        'meeting_room',
        'custom'  -- 自訂組合（多種服務）
    ));
