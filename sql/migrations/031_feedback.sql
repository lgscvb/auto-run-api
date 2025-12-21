-- Migration: 031_feedback
-- Description: 使用者回報問題與建議
-- Date: 2025-12-21

-- ============================================================================
-- 建立 feedback 資料表
-- 用於收集 CRM 使用者透過 AI 助手回報的問題和建議
-- ============================================================================

CREATE TABLE IF NOT EXISTS feedback (
    id SERIAL PRIMARY KEY,

    -- 回報類型
    feedback_type TEXT NOT NULL CHECK (feedback_type IN ('bug', 'feature', 'improvement', 'question', 'other')),

    -- 優先級
    priority TEXT DEFAULT 'medium' CHECK (priority IN ('low', 'medium', 'high', 'critical')),

    -- 標題與內容
    title TEXT NOT NULL,
    description TEXT,

    -- 上下文資訊
    page_url TEXT,                    -- 回報時所在頁面
    related_feature TEXT,             -- 相關功能（如：繳費管理、報表）

    -- 狀態追蹤
    status TEXT DEFAULT 'open' CHECK (status IN ('open', 'reviewing', 'in_progress', 'resolved', 'wontfix', 'duplicate')),
    resolved_at TIMESTAMPTZ,
    resolution_notes TEXT,

    -- 元資料
    submitted_by TEXT,                -- 提交者名稱
    submitted_via TEXT DEFAULT 'ai_assistant',  -- 提交管道

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- 索引
CREATE INDEX IF NOT EXISTS idx_feedback_status ON feedback(status);
CREATE INDEX IF NOT EXISTS idx_feedback_type ON feedback(feedback_type);
CREATE INDEX IF NOT EXISTS idx_feedback_created_at ON feedback(created_at DESC);

-- 觸發器：自動更新 updated_at
CREATE OR REPLACE FUNCTION update_feedback_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_update_feedback_timestamp ON feedback;
CREATE TRIGGER trigger_update_feedback_timestamp
    BEFORE UPDATE ON feedback
    FOR EACH ROW
    EXECUTE FUNCTION update_feedback_timestamp();

-- 權限
GRANT SELECT, INSERT, UPDATE ON feedback TO anon, authenticated;
GRANT USAGE, SELECT ON SEQUENCE feedback_id_seq TO anon, authenticated;

COMMENT ON TABLE feedback IS '使用者回報的問題與建議';
COMMENT ON COLUMN feedback.feedback_type IS '類型：bug=錯誤, feature=新功能, improvement=改進, question=問題, other=其他';
COMMENT ON COLUMN feedback.priority IS '優先級：low=低, medium=中, high=高, critical=緊急';
COMMENT ON COLUMN feedback.status IS '狀態：open=待處理, reviewing=審核中, in_progress=處理中, resolved=已解決, wontfix=不修復, duplicate=重複';

-- ============================================================================
-- 建立視圖：回報統計
-- ============================================================================
CREATE OR REPLACE VIEW v_feedback_summary AS
SELECT
    feedback_type,
    status,
    COUNT(*) as count,
    COUNT(*) FILTER (WHERE created_at >= CURRENT_DATE - INTERVAL '7 days') as last_7_days,
    COUNT(*) FILTER (WHERE created_at >= CURRENT_DATE - INTERVAL '30 days') as last_30_days
FROM feedback
GROUP BY feedback_type, status
ORDER BY feedback_type, status;

GRANT SELECT ON v_feedback_summary TO anon, authenticated;
