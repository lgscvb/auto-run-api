-- ============================================================================
-- Hour Jungle CRM - Floor Plan 平面圖功能
-- Migration: 015_floor_plan.sql
-- ============================================================================

-- 1. 場館平面圖表
CREATE TABLE IF NOT EXISTS floor_plans (
    id SERIAL PRIMARY KEY,
    branch_id INTEGER REFERENCES branches(id),
    name VARCHAR(100) NOT NULL,           -- 例：大忠本館
    image_filename VARCHAR(255),          -- 底圖檔案名稱
    image_url TEXT,                       -- GCS URL
    width INTEGER DEFAULT 853,            -- 圖片寬度 px
    height INTEGER DEFAULT 959,           -- 圖片高度 px
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- 2. 位置坐標映射表
CREATE TABLE IF NOT EXISTS floor_positions (
    id SERIAL PRIMARY KEY,
    floor_plan_id INTEGER REFERENCES floor_plans(id),
    position_number INTEGER NOT NULL,     -- 位置編號 1-107
    x INTEGER NOT NULL,                   -- X 坐標 (px)
    y INTEGER NOT NULL,                   -- Y 坐標 (px)
    width INTEGER DEFAULT 68,             -- 文字框寬度
    height INTEGER DEFAULT 21,            -- 文字框高度
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(floor_plan_id, position_number)
);

-- 3. 在 contracts 表新增 position_number 欄位（關聯租戶與位置）
ALTER TABLE contracts ADD COLUMN IF NOT EXISTS position_number INTEGER;

COMMENT ON COLUMN contracts.position_number IS '位置編號，對應平面圖的位置';

-- 4. 建立索引
CREATE INDEX IF NOT EXISTS idx_floor_positions_floor_plan ON floor_positions(floor_plan_id);
CREATE INDEX IF NOT EXISTS idx_contracts_position ON contracts(position_number) WHERE position_number IS NOT NULL;

-- 5. 建立視圖：位置與租戶對照
CREATE OR REPLACE VIEW v_floor_positions AS
SELECT
    fp.id,
    fp.floor_plan_id,
    fp.position_number,
    fp.x,
    fp.y,
    fp.width,
    fp.height,
    plan.branch_id,
    plan.name as floor_plan_name,
    c.id as contract_id,
    c.customer_id,
    cust.name as customer_name,
    cust.company_name,
    c.status as contract_status,
    c.end_date as contract_end_date
FROM floor_positions fp
JOIN floor_plans plan ON fp.floor_plan_id = plan.id
LEFT JOIN contracts c ON c.position_number = fp.position_number
    AND c.branch_id = plan.branch_id
    AND c.status = 'active'
LEFT JOIN customers cust ON c.customer_id = cust.id
ORDER BY fp.position_number;

-- 6. 插入大忠本館平面圖
INSERT INTO floor_plans (branch_id, name, image_filename, width, height)
VALUES (1, '大忠本館', 'dazhong_floor_plan.png', 853, 959)
ON CONFLICT DO NOTHING;

-- 7. 批量插入 107 個位置坐標（從 PPT 提取的數據）
-- 取得剛建立的 floor_plan_id
DO $$
DECLARE
    plan_id INTEGER;
BEGIN
    SELECT id INTO plan_id FROM floor_plans WHERE name = '大忠本館' LIMIT 1;

    IF plan_id IS NOT NULL THEN
        -- 插入所有位置坐標
        INSERT INTO floor_positions (floor_plan_id, position_number, x, y, width, height) VALUES
        (plan_id, 1, 57, 631, 48, 21),
        (plan_id, 2, 60, 683, 56, 21),
        (plan_id, 3, 48, 734, 80, 21),
        (plan_id, 4, 44, 798, 88, 21),
        (plan_id, 5, 141, 637, 80, 21),
        (plan_id, 6, 145, 695, 72, 21),
        (plan_id, 7, 153, 747, 56, 21),
        (plan_id, 8, 158, 798, 64, 21),
        (plan_id, 9, 241, 637, 56, 21),
        (plan_id, 10, 225, 706, 68, 21),
        (plan_id, 11, 200, 746, 100, 21),
        (plan_id, 12, 204, 812, 92, 21),
        (plan_id, 13, 291, 653, 60, 21),
        (plan_id, 14, 278, 687, 87, 30),
        (plan_id, 15, 295, 759, 52, 21),
        (plan_id, 16, 287, 825, 68, 21),
        (plan_id, 17, 345, 613, 108, 21),
        (plan_id, 18, 349, 673, 100, 21),
        (plan_id, 19, 348, 725, 68, 21),
        (plan_id, 20, 365, 786, 68, 21),
        (plan_id, 21, 437, 637, 84, 21),
        (plan_id, 22, 419, 686, 108, 21),
        (plan_id, 23, 429, 727, 100, 21),
        (plan_id, 24, 449, 798, 60, 21),
        (plan_id, 25, 10, 888, 52, 32),
        (plan_id, 26, 16, 919, 84, 21),
        (plan_id, 27, 66, 880, 60, 32),
        (plan_id, 28, 107, 915, 76, 30),
        (plan_id, 29, 131, 887, 100, 21),
        (plan_id, 30, 221, 903, 76, 21),
        (plan_id, 31, 38, 279, 68, 21),
        (plan_id, 32, 54, 342, 52, 21),
        (plan_id, 33, 30, 407, 84, 21),
        (plan_id, 34, 583, 300, 60, 42),
        (plan_id, 35, 643, 305, 52, 62),
        (plan_id, 36, 690, 302, 52, 32),
        (plan_id, 37, 743, 305, 44, 32),
        (plan_id, 38, 783, 313, 52, 21),
        (plan_id, 39, 515, 378, 60, 21),
        (plan_id, 40, 558, 420, 52, 21),
        (plan_id, 41, 587, 385, 84, 21),
        (plan_id, 42, 651, 373, 63, 30),
        (plan_id, 43, 678, 427, 76, 21),
        (plan_id, 44, 739, 385, 52, 21),
        (plan_id, 45, 759, 428, 84, 21),
        (plan_id, 46, 255, 343, 100, 19),
        (plan_id, 47, 255, 306, 68, 21),
        (plan_id, 48, 247, 270, 116, 21),
        (plan_id, 49, 250, 239, 95, 21),
        (plan_id, 50, 279, 215, 52, 21),
        (plan_id, 51, 322, 187, 68, 21),
        (plan_id, 52, 366, 233, 84, 21),
        (plan_id, 53, 387, 267, 60, 21),
        (plan_id, 54, 382, 302, 52, 21),
        (plan_id, 55, 318, 324, 116, 21),
        (plan_id, 56, 383, 176, 68, 32),
        (plan_id, 57, 147, 88, 68, 32),
        (plan_id, 58, 216, 88, 68, 21),
        (plan_id, 59, 271, 119, 68, 32),
        (plan_id, 60, 95, 46, 68, 21),
        (plan_id, 61, 156, 41, 68, 32),
        (plan_id, 62, 255, 46, 68, 21),
        (plan_id, 63, 468, 155, 68, 21),
        (plan_id, 64, 515, 197, 68, 32),
        (plan_id, 65, 558, 155, 68, 21),
        (plan_id, 66, 599, 197, 68, 21),
        (plan_id, 67, 648, 161, 68, 21),
        (plan_id, 68, 767, 196, 68, 21),
        (plan_id, 69, 515, 230, 68, 32),
        (plan_id, 70, 550, 279, 68, 21),
        (plan_id, 71, 586, 228, 68, 32),
        (plan_id, 72, 633, 280, 68, 21),
        (plan_id, 73, 674, 234, 68, 32),
        (plan_id, 74, 714, 279, 68, 32),
        (plan_id, 75, 763, 228, 68, 32),
        (plan_id, 76, 708, 630, 68, 32),
        (plan_id, 77, 550, 488, 68, 32),
        (plan_id, 78, 609, 493, 68, 21),
        (plan_id, 79, 665, 495, 68, 21),
        (plan_id, 80, 763, 490, 84, 21),
        (plan_id, 81, 512, 539, 116, 21),
        (plan_id, 82, 561, 573, 76, 21),
        (plan_id, 83, 612, 541, 84, 21),
        (plan_id, 84, 657, 569, 84, 21),
        (plan_id, 85, 690, 541, 100, 21),
        (plan_id, 86, 519, 641, 84, 21),
        (plan_id, 87, 576, 600, 52, 21),
        (plan_id, 88, 597, 641, 108, 21),
        (plan_id, 89, 657, 597, 84, 21),
        (plan_id, 90, 568, 714, 68, 21),
        (plan_id, 91, 605, 673, 84, 21),
        (plan_id, 92, 644, 707, 76, 21),
        (plan_id, 93, 696, 674, 92, 21),
        (plan_id, 94, 574, 772, 60, 21),
        (plan_id, 95, 595, 736, 100, 21),
        (plan_id, 96, 659, 772, 76, 21),
        (plan_id, 97, 685, 730, 100, 21),
        (plan_id, 98, 485, 818, 95, 30),
        (plan_id, 99, 550, 855, 84, 21),
        (plan_id, 100, 592, 809, 83, 30),
        (plan_id, 101, 636, 852, 91, 40),
        (plan_id, 102, 0, 372, 81, 21),
        (plan_id, 103, 672, 818, 89, 21),
        (plan_id, 104, 7, 324, 89, 21),
        (plan_id, 105, 0, 302, 65, 21),
        (plan_id, 106, 145, 392, 89, 21),
        (plan_id, 107, 141, 461, 97, 21)
        ON CONFLICT (floor_plan_id, position_number) DO NOTHING;
    END IF;
END $$;

-- 8. PostgREST 權限
GRANT SELECT ON floor_plans TO web_anon;
GRANT SELECT ON floor_positions TO web_anon;
GRANT SELECT ON v_floor_positions TO web_anon;
