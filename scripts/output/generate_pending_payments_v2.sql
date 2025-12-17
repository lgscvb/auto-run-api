-- ============================================================================
-- Hour Jungle CRM - 2025-12 待繳記錄生成（正確版）
-- 生成時間: 2025-12-07 14:19:49
-- 邏輯: 根據合約開始日期和繳費週期計算正確的繳費時間點
-- ============================================================================

-- 刪除之前錯誤生成的 2025-12 pending 記錄
DELETE FROM payments WHERE payment_period = '2025-12' AND payment_status = 'pending';

-- DZ-057 李冠葳 (semi_annual, x6)
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (554, 1904, 1, 'rent', '2025-12', 10140, '2025-12-08', 'pending')
ON CONFLICT DO NOTHING;
-- DZ-058 洪庭琦 (semi_annual, x6)
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (555, 1905, 1, 'rent', '2025-12', 8940, '2025-12-07', 'pending')
ON CONFLICT DO NOTHING;
-- DZ-063 呂育豪 (semi_annual, x6)
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (556, 1906, 1, 'rent', '2025-12', 8940, '2025-12-22', 'pending')
ON CONFLICT DO NOTHING;
-- DZ-085 湯詠為 (semi_annual, x6)
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (563, 1913, 1, 'rent', '2025-12', 12000, '2025-12-01', 'pending')
ON CONFLICT DO NOTHING;
-- DZ-088 陳為彤 (semi_annual, x6)
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (566, 1916, 1, 'rent', '2025-12', 10140, '2025-12-28', 'pending')
ON CONFLICT DO NOTHING;
-- DZ-112 黃仲瑛(green) (annual, x12)
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (575, 1925, 1, 'rent', '2025-12', 21600, '2025-12-13', 'pending')
ON CONFLICT DO NOTHING;
-- DZ-004 陳孟暄 (monthly, x1)
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (546, 1896, 1, 'rent', '2025-12', 12000, '2025-12-10', 'pending')
ON CONFLICT DO NOTHING;
-- DZ-258 潘玫雯 (monthly, x1)
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (659, 2009, 1, 'rent', '2025-12', 3000, '2025-12-10', 'pending')
ON CONFLICT DO NOTHING;
-- DZ-262 Ravin Wadhawan (annual, x12)
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (663, 2013, 1, 'rent', '2025-12', 148200, '2025-12-01', 'pending')
ON CONFLICT DO NOTHING;
-- DZ-263 劉翁昌 (monthly, x1)
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (664, 2014, 1, 'rent', '2025-12', 3000, '2025-12-11', 'pending')
ON CONFLICT DO NOTHING;
-- HR-005 陳玉美 (semi_annual, x6)
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (668, 2018, 2, 'rent', '2025-12', 10800, '2025-12-10', 'pending')
ON CONFLICT DO NOTHING;
-- DZ-164 曾宥榆 (semi_annual, x6)
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (598, 1948, 1, 'rent', '2025-12', 10800, '2025-12-14', 'pending')
ON CONFLICT DO NOTHING;

-- 統計: 12 筆待繳, 總金額 $259,560

-- 驗證
SELECT payment_status, COUNT(*) as count, SUM(amount) as total FROM payments WHERE payment_period = '2025-12' GROUP BY payment_status;