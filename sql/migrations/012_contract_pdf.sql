-- ============================================================================
-- Hour Jungle CRM - Migration 012: 合約 PDF 欄位
-- ============================================================================

-- 合約表增加 PDF 路徑
ALTER TABLE contracts ADD COLUMN IF NOT EXISTS pdf_path VARCHAR(500);
ALTER TABLE contracts ADD COLUMN IF NOT EXISTS pdf_generated_at TIMESTAMPTZ;

COMMENT ON COLUMN contracts.pdf_path IS 'GCS 中的 PDF 路徑';
COMMENT ON COLUMN contracts.pdf_generated_at IS 'PDF 生成時間';
