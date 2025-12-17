-- 新增 position_number 欄位
ALTER TABLE contracts ADD COLUMN IF NOT EXISTS position_number INTEGER;

-- 建立索引以便查詢
CREATE INDEX IF NOT EXISTS idx_contracts_position_number ON contracts(position_number);

-- 更新續約客戶的位置
UPDATE contracts SET position_number = 5 WHERE id = 553;
UPDATE contracts SET position_number = 6 WHERE id = 547;
UPDATE contracts SET position_number = 69 WHERE id = 608;
UPDATE contracts SET position_number = 93 WHERE id = 573;
