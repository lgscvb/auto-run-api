-- Migration: 030_customer_traits
-- Description: 新增客戶特性標籤欄位
-- Date: 2025-12-21

-- ============================================================================
-- 新增 traits 欄位到 customers 表
-- 用於儲存客戶特性標籤和備註
-- ============================================================================

-- 新增 traits 欄位（JSONB 格式）
-- 結構：{ "tags": ["payment_risk", "cooperative"], "notes": "備註說明" }
ALTER TABLE customers
ADD COLUMN IF NOT EXISTS traits JSONB DEFAULT NULL;

COMMENT ON COLUMN customers.traits IS '客戶特性標籤，格式：{"tags": ["tag_id1", "tag_id2"], "notes": "備註"}';

-- 可用的標籤定義（供參考，實際定義在前端）:
-- payment_risk: 易拖欠款項
-- far_location: 住很遠不便
-- cooperative: 配合度高
-- strict: 一板一眼
-- cautious: 需謹慎應對
-- vip: VIP 客戶
-- referral: 轉介來源
