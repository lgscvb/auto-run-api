-- 完整位置更新 SQL
-- 執行: docker exec -i hourjungle-crm-postgres-1 psql -U postgres -d crm < complete_position_update.sql
-- 產生時間: 2025-12-16

-- ============================================================================
-- 第一部分：新增 position_number 欄位
-- ============================================================================

ALTER TABLE contracts ADD COLUMN IF NOT EXISTS position_number INTEGER;
CREATE INDEX IF NOT EXISTS idx_contracts_position_number ON contracts(position_number);

-- ============================================================================
-- 第二部分：自動匹配的 92 筆
-- ============================================================================

UPDATE contracts SET position_number = 2 WHERE id = 622;
UPDATE contracts SET position_number = 3 WHERE id = 549;
UPDATE contracts SET position_number = 4 WHERE id = 554;
UPDATE contracts SET position_number = 7 WHERE id = 567;
UPDATE contracts SET position_number = 8 WHERE id = 643;
UPDATE contracts SET position_number = 9 WHERE id = 625;
UPDATE contracts SET position_number = 10 WHERE id = 651;
UPDATE contracts SET position_number = 11 WHERE id = 578;
UPDATE contracts SET position_number = 13 WHERE id = 602;
UPDATE contracts SET position_number = 14 WHERE id = 615;
UPDATE contracts SET position_number = 15 WHERE id = 634;
UPDATE contracts SET position_number = 16 WHERE id = 628;
UPDATE contracts SET position_number = 17 WHERE id = 648;
UPDATE contracts SET position_number = 19 WHERE id = 568;
UPDATE contracts SET position_number = 20 WHERE id = 551;
UPDATE contracts SET position_number = 21 WHERE id = 635;
UPDATE contracts SET position_number = 22 WHERE id = 607;
UPDATE contracts SET position_number = 23 WHERE id = 552;
UPDATE contracts SET position_number = 24 WHERE id = 555;
UPDATE contracts SET position_number = 25 WHERE id = 606;
UPDATE contracts SET position_number = 26 WHERE id = 641;
UPDATE contracts SET position_number = 27 WHERE id = 556;
UPDATE contracts SET position_number = 28 WHERE id = 660;
UPDATE contracts SET position_number = 29 WHERE id = 642;
UPDATE contracts SET position_number = 30 WHERE id = 557;
UPDATE contracts SET position_number = 32 WHERE id = 558;
UPDATE contracts SET position_number = 33 WHERE id = 656;
UPDATE contracts SET position_number = 34 WHERE id = 559;
UPDATE contracts SET position_number = 35 WHERE id = 633;
UPDATE contracts SET position_number = 36 WHERE id = 621;
UPDATE contracts SET position_number = 37 WHERE id = 560;
UPDATE contracts SET position_number = 38 WHERE id = 595;
UPDATE contracts SET position_number = 39 WHERE id = 561;
UPDATE contracts SET position_number = 40 WHERE id = 562;
UPDATE contracts SET position_number = 41 WHERE id = 652;
UPDATE contracts SET position_number = 43 WHERE id = 581;
UPDATE contracts SET position_number = 44 WHERE id = 612;
UPDATE contracts SET position_number = 45 WHERE id = 564;
UPDATE contracts SET position_number = 47 WHERE id = 565;
UPDATE contracts SET position_number = 48 WHERE id = 566;
UPDATE contracts SET position_number = 49 WHERE id = 620;
UPDATE contracts SET position_number = 50 WHERE id = 569;
UPDATE contracts SET position_number = 51 WHERE id = 600;
UPDATE contracts SET position_number = 52 WHERE id = 623;
UPDATE contracts SET position_number = 53 WHERE id = 570;
UPDATE contracts SET position_number = 54 WHERE id = 613;
UPDATE contracts SET position_number = 56 WHERE id = 572;
UPDATE contracts SET position_number = 57 WHERE id = 571;
UPDATE contracts SET position_number = 58 WHERE id = 637;
UPDATE contracts SET position_number = 59 WHERE id = 610;
UPDATE contracts SET position_number = 63 WHERE id = 655;
UPDATE contracts SET position_number = 64 WHERE id = 576;
UPDATE contracts SET position_number = 65 WHERE id = 577;
UPDATE contracts SET position_number = 66 WHERE id = 579;
UPDATE contracts SET position_number = 67 WHERE id = 618;
UPDATE contracts SET position_number = 68 WHERE id = 614;
UPDATE contracts SET position_number = 71 WHERE id = 584;
UPDATE contracts SET position_number = 72 WHERE id = 585;
UPDATE contracts SET position_number = 73 WHERE id = 587;
UPDATE contracts SET position_number = 74 WHERE id = 661;
UPDATE contracts SET position_number = 75 WHERE id = 650;
UPDATE contracts SET position_number = 76 WHERE id = 589;
UPDATE contracts SET position_number = 77 WHERE id = 638;
UPDATE contracts SET position_number = 78 WHERE id = 586;
UPDATE contracts SET position_number = 79 WHERE id = 631;
UPDATE contracts SET position_number = 80 WHERE id = 588;
UPDATE contracts SET position_number = 81 WHERE id = 632;
UPDATE contracts SET position_number = 82 WHERE id = 582;
UPDATE contracts SET position_number = 83 WHERE id = 580;
UPDATE contracts SET position_number = 84 WHERE id = 590;
UPDATE contracts SET position_number = 85 WHERE id = 626;
UPDATE contracts SET position_number = 86 WHERE id = 592;
UPDATE contracts SET position_number = 87 WHERE id = 662;
UPDATE contracts SET position_number = 88 WHERE id = 797;
UPDATE contracts SET position_number = 89 WHERE id = 605;
UPDATE contracts SET position_number = 90 WHERE id = 596;
UPDATE contracts SET position_number = 91 WHERE id = 654;
UPDATE contracts SET position_number = 92 WHERE id = 594;
UPDATE contracts SET position_number = 94 WHERE id = 591;
UPDATE contracts SET position_number = 95 WHERE id = 593;
UPDATE contracts SET position_number = 96 WHERE id = 646;
UPDATE contracts SET position_number = 97 WHERE id = 649;
UPDATE contracts SET position_number = 98 WHERE id = 617;
UPDATE contracts SET position_number = 99 WHERE id = 657;
UPDATE contracts SET position_number = 100 WHERE id = 647;
UPDATE contracts SET position_number = 101 WHERE id = 598;
UPDATE contracts SET position_number = 102 WHERE id = 599;
UPDATE contracts SET position_number = 103 WHERE id = 609;
UPDATE contracts SET position_number = 104 WHERE id = 630;
UPDATE contracts SET position_number = 105 WHERE id = 601;
UPDATE contracts SET position_number = 106 WHERE id = 644;
UPDATE contracts SET position_number = 107 WHERE id = 640;

-- ============================================================================
-- 第三部分：名稱差異手動修正（5 筆）
-- ============================================================================

-- 位置 1: 廖氏商行（使用正確的合約 #548）
UPDATE contracts SET position_number = 1 WHERE id = 548;

-- 位置 31: 泉家鑫 → 泉佳鑫
UPDATE contracts SET position_number = 31 WHERE id = 616;

-- 位置 46: 新遞國際物流 → 新遞國際開發
UPDATE contracts SET position_number = 46 WHERE id = 563;

-- 位置 62: 短腿基商舖 → 短腿基商鋪
UPDATE contracts SET position_number = 62 WHERE id = 575;

-- 位置 70: 步臻 → 步臻低碳策略
UPDATE contracts SET position_number = 70 WHERE id = 583;

-- ============================================================================
-- 第四部分：續約客戶位置（4 筆）
-- ============================================================================

-- 一貝兒美容工作室 - 位置 5
UPDATE contracts SET position_number = 5 WHERE id = 553;

-- 吉爾哈登工作室 - 位置 6
UPDATE contracts SET position_number = 6 WHERE id = 547;

-- 七分之二的探索 - 位置 69
UPDATE contracts SET position_number = 69 WHERE id = 608;

-- 小倩媽咪 - 位置 93
UPDATE contracts SET position_number = 93 WHERE id = 573;

-- ============================================================================
-- 驗證結果
-- ============================================================================

SELECT
    '總計更新' as info,
    COUNT(*) as count
FROM contracts
WHERE position_number IS NOT NULL;
