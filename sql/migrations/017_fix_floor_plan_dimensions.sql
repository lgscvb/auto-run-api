-- 017_fix_floor_plan_dimensions.sql
-- 修正平面圖尺寸和座標縮放
-- 問題：GCS 圖片 (2457x1609) 與資料庫記錄 (853x959) 不符

-- 計算縮放比例
-- scale_x = 2457 / 853 = 2.8804
-- scale_y = 1609 / 959 = 1.6778

-- 1. 更新 floor_plans 表的尺寸
UPDATE floor_plans
SET width = 2457, height = 1609, updated_at = NOW()
WHERE id = 1;

-- 2. 更新所有位置座標（乘以縮放比例）
UPDATE floor_positions
SET
    x = ROUND(x * 2.8804)::INTEGER,
    y = ROUND(y * 1.6778)::INTEGER,
    width = ROUND(width * 2.8804)::INTEGER,
    height = ROUND(height * 1.6778)::INTEGER
WHERE floor_plan_id = 1;

-- 確認更新結果
-- SELECT id, name, width, height FROM floor_plans WHERE id = 1;
-- SELECT position_number, x, y, width, height FROM floor_positions WHERE floor_plan_id = 1 ORDER BY position_number LIMIT 10;
