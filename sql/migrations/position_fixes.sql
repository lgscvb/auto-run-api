-- 位置修正 SQL
-- 這些是名稱有差異但確認為同一公司的修正

-- 位置 31: PPT=泉家鑫企業社 → DB=泉佳鑫企業社
UPDATE contracts SET position_number = 31 WHERE id = 616;

-- 位置 46: PPT=新遞國際物流有限公司 → DB=新遞國際開發有限公司
UPDATE contracts SET position_number = 46 WHERE id = 563;

-- 位置 62: PPT=短腿基商舖 → DB=短腿基商鋪
UPDATE contracts SET position_number = 62 WHERE id = 575;

-- 位置 70: PPT=步臻有限公司 → DB=步臻低碳策略有限公司
UPDATE contracts SET position_number = 70 WHERE id = 583;

