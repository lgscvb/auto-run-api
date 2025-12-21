-- Migration: 025_quotes_line_user_id
-- Description: 報價單添加 LINE User ID 欄位（來自 Brain 詢問）
-- Date: 2025-12-20

-- 添加 line_user_id 欄位
ALTER TABLE quotes
ADD COLUMN IF NOT EXISTS line_user_id TEXT;

-- 添加 sent_at 欄位（記錄發送時間）
ALTER TABLE quotes
ADD COLUMN IF NOT EXISTS sent_at TIMESTAMPTZ;

-- 添加索引
CREATE INDEX IF NOT EXISTS idx_quotes_line_user_id ON quotes(line_user_id);

-- 更新 v_quotes 視圖（如果存在）
CREATE OR REPLACE VIEW v_quotes AS
SELECT
    q.*,
    b.name as branch_name,
    c.name as customer_name_from_db,
    COALESCE(q.customer_name, c.name) as display_customer_name
FROM quotes q
LEFT JOIN branches b ON q.branch_id = b.id
LEFT JOIN customers c ON q.customer_id = c.id;

COMMENT ON COLUMN quotes.line_user_id IS 'LINE User ID（來自 Brain 詢問，用於發送報價單）';
COMMENT ON COLUMN quotes.sent_at IS '報價單發送時間';
