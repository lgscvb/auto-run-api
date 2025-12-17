-- ============================================================================
-- Hour Jungle CRM - 修正 payment_cycle
-- 生成時間: 2025-12-07 13:46:14
-- 共 1 筆需要更新
-- 來源: 客戶資料表 + 繳費表 交叉驗證
-- ============================================================================

-- DZ-232 陳姵伶: monthly → triennial (Excel: 3Y)
UPDATE contracts SET payment_cycle = 'triennial' WHERE id = 635;

-- 驗證
SELECT payment_cycle, COUNT(*) as count FROM contracts WHERE status = 'active' GROUP BY payment_cycle ORDER BY payment_cycle;