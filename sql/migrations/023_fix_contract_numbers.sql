-- Migration: 023_fix_contract_numbers
-- Description: 統一合約編號格式
-- Date: 2025-12-19
-- Purpose:
--   1. 大忠館：DZ-XXX（去掉 -2025 後綴）
--   2. 環瑞館：HR-VXX（去掉 -2025- 中綴）
--   3. 刪除測試資料（HJ-202512-* 系列）

-- ============================================================================
-- 1. 大忠館：去掉 -2025 後綴（DZ-004-2025 → DZ-004）
-- ============================================================================

UPDATE contracts
SET contract_number = REPLACE(contract_number, '-2025', '')
WHERE contract_number LIKE 'DZ-%-2025'
  AND contract_number NOT LIKE 'DZ-2025-%';

-- 特殊格式修正：DZ-2025-264 → DZ-264
UPDATE contracts
SET contract_number = 'DZ-264'
WHERE contract_number = 'DZ-2025-264';

-- ============================================================================
-- 2. 環瑞館：去掉 -2025- 中綴（HR-2025-V27 → HR-V27）
-- ============================================================================

UPDATE contracts
SET contract_number = REPLACE(contract_number, '-2025-', '-')
WHERE contract_number LIKE 'HR-2025-V%';

-- ============================================================================
-- 3. 刪除測試資料（HJ-202512-* 系列，都是 terminated）
-- ============================================================================

-- 先刪除相關的 payments 記錄（外鍵約束）
DELETE FROM payments
WHERE contract_id IN (
    SELECT id FROM contracts WHERE contract_number LIKE 'HJ-202512-%'
);

-- 刪除測試合約
DELETE FROM contracts
WHERE contract_number LIKE 'HJ-202512-%';

-- ============================================================================
-- 4. 驗證結果
-- ============================================================================

-- 檢查是否還有不正確的格式
SELECT 'remaining_bad_formats' as check_type, contract_number, branch_id, status
FROM contracts
WHERE contract_number LIKE '%2025%'
   OR contract_number LIKE 'HJ-%';

-- 確認大忠館編號格式
SELECT 'dz_samples' as check_type, contract_number, status
FROM contracts
WHERE contract_number LIKE 'DZ-%'
ORDER BY id
LIMIT 5;

-- 確認環瑞館編號格式
SELECT 'hr_samples' as check_type, contract_number, status
FROM contracts
WHERE contract_number LIKE 'HR-%'
ORDER BY id
LIMIT 5;

-- 統計
SELECT
    CASE
        WHEN contract_number LIKE 'DZ-%' THEN '大忠館 (DZ)'
        WHEN contract_number LIKE 'HR-%' THEN '環瑞館 (HR)'
        ELSE '其他'
    END as branch_format,
    COUNT(*) as count
FROM contracts
WHERE contract_number IS NOT NULL
GROUP BY 1
ORDER BY 1;
