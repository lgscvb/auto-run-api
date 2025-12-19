-- ============================================================================
-- Hour Jungle CRM - 繳費記錄補匯入
-- 生成時間: 2025-12-18 20:59:21
-- 來源: payments_ALL_with_method_20251203_115506.csv
-- ============================================================================

-- 2025 年已付款記錄: 46 筆

-- 賴宗政 (semi_annual)
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (1126, 2308, 1, 'rent', '2025-01', 8940.0, '2025-01-05', 'paid')
ON CONFLICT DO NOTHING;
-- 包奇艷  (semi_annual)
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (1125, 2307, 1, 'rent', '2025-01', 12000.0, '2025-01-05', 'paid')
ON CONFLICT DO NOTHING;
-- 劉基寅 (monthly)
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (597, 1947, 1, 'rent', '2025-01', 9880.0, '2025-01-15', 'paid')
ON CONFLICT DO NOTHING;
-- 李沁澐 (annual)
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (1110, 2292, 1, 'rent', '2025-01', 1.0, '2025-01-05', 'paid')
ON CONFLICT DO NOTHING;
-- 劉基寅 (monthly)
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (597, 1947, 1, 'rent', '2025-02', 9880.0, '2025-02-15', 'paid')
ON CONFLICT DO NOTHING;
-- 湯詠為 (quarterly)
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (1019, 2197, 1, 'rent', '2025-03', 6000.0, '2025-03-05', 'paid')
ON CONFLICT DO NOTHING;
-- 曾華田 (semi_annual)
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (1116, 2298, 1, 'rent', '2025-03', 10800.0, '2025-03-05', 'paid')
ON CONFLICT DO NOTHING;
-- 江怡霈 (semi_annual)
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (622, 1972, 1, 'rent', '2025-03', 1650.0, '2025-03-13', 'paid')
ON CONFLICT DO NOTHING;
-- 劉基寅 (monthly)
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (597, 1947, 1, 'rent', '2025-03', 9880.0, '2025-03-15', 'paid')
ON CONFLICT DO NOTHING;
-- 陳建誠 (annual)
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (1124, 2306, 1, 'rent', '2025-03', 1800.0, '2025-03-05', 'paid')
ON CONFLICT DO NOTHING;
-- 鐘韋程 (annual)
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (1120, 2302, 1, 'rent', '2025-03', 1800.0, '2025-03-05', 'paid')
ON CONFLICT DO NOTHING;
-- 張祐寧 (annual)
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (1111, 2293, 1, 'rent', '2025-03', 3.0, '2025-03-05', 'paid')
ON CONFLICT DO NOTHING;
-- 曾湘稘 (semi_annual)
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (1136, 2323, 1, 'rent', '2025-03', 1490.0, '2025-03-05', 'paid')
ON CONFLICT DO NOTHING;
-- 廖佑泓 (semi_annual)
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (1023, 2201, 1, 'rent', '2025-04', 8940.0, '2025-04-05', 'paid')
ON CONFLICT DO NOTHING;
-- 朱栢逸 (semi_annual)
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (1131, 2313, 1, 'rent', '2025-04', 10140.0, '2025-04-05', 'paid')
ON CONFLICT DO NOTHING;
-- 劉基寅 (monthly)
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (597, 1947, 1, 'rent', '2025-04', 9880.0, '2025-04-15', 'paid')
ON CONFLICT DO NOTHING;
-- 吳世多 (monthly)
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (1150, 2325, 1, 'rent', '2025-04', 3000.0, '2025-04-05', 'paid')
ON CONFLICT DO NOTHING;
-- 吳振領 (annual)
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (1112, 2294, 1, 'rent', '2025-04', 1800.0, '2025-04-05', 'paid')
ON CONFLICT DO NOTHING;
-- 胡良輝 (semi_annual)
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (1138, 2348, 1, 'rent', '2025-04', 4.0, '2025-04-05', 'paid')
ON CONFLICT DO NOTHING;
-- 蔣榮宗 (semi_annual)
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (1141, 2329, 1, 'rent', '2025-04', 11250.0, '2025-04-05', 'paid')
ON CONFLICT DO NOTHING;
-- 劉基寅 (monthly)
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (597, 1947, 1, 'rent', '2025-05', 9880.0, '2025-05-15', 'paid')
ON CONFLICT DO NOTHING;
-- 蕭家如 (monthly)
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (603, 1953, 1, 'rent', '2025-05', 3000.0, '2025-05-05', 'paid')
ON CONFLICT DO NOTHING;
-- 吳世多 (monthly)
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (1150, 2325, 1, 'rent', '2025-05', 3000.0, '2025-05-05', 'paid')
ON CONFLICT DO NOTHING;
-- 賴宗政 (semi_annual)
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (1126, 2308, 1, 'rent', '2025-05', 6000.0, '2025-05-05', 'paid')
ON CONFLICT DO NOTHING;
-- 巫成妍 (annual)
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (1122, 2304, 1, 'rent', '2025-05', 5.0, '2025-05-05', 'paid')
ON CONFLICT DO NOTHING;
-- 湯詠為 (quarterly)
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (1019, 2197, 1, 'rent', '2025-06', 12000.0, '2025-06-05', 'paid')
ON CONFLICT DO NOTHING;
-- 湯詠為 (semi_annual)
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (1019, 2197, 1, 'rent', '2025-06', 8940.0, '2025-06-05', 'paid')
ON CONFLICT DO NOTHING;
-- 劉基寅 (monthly)
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (597, 1947, 1, 'rent', '2025-07', 10880.0, '2025-07-15', 'paid')
ON CONFLICT DO NOTHING;
-- 包奇艷  (semi_annual)
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (1125, 2307, 1, 'rent', '2025-07', 2000.0, '2025-07-05', 'paid')
ON CONFLICT DO NOTHING;
-- 賴宗政 (semi_annual)
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (1126, 2308, 1, 'rent', '2025-07', 1490.0, '2025-07-05', 'paid')
ON CONFLICT DO NOTHING;
-- 劉基寅 (monthly)
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (597, 1947, 1, 'rent', '2025-08', 10880.0, '2025-08-15', 'paid')
ON CONFLICT DO NOTHING;
-- 賴宗政 (biennial)
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (1126, 2308, 1, 'rent', '2025-08', 8.0, '2025-08-05', 'paid')
ON CONFLICT DO NOTHING;
-- 劉怡廷 (annual)
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (547, 1897, 1, 'rent', '2025-09', 153900.0, '2025-09-05', 'paid')
ON CONFLICT DO NOTHING;
-- 朱建勳 (biennial)
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (1049, 2227, 1, 'rent', '2025-09', 20280.0, '2025-09-05', 'paid')
ON CONFLICT DO NOTHING;
-- 劉基寅 (monthly)
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (597, 1947, 1, 'rent', '2025-09', 10880.0, '2025-09-15', 'paid')
ON CONFLICT DO NOTHING;
-- 曾華田 (semi_annual)
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (1116, 2298, 1, 'rent', '2025-09', 10800.0, '2025-09-05', 'paid')
ON CONFLICT DO NOTHING;
-- 楊鴻興 (semi_annual)
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (585, 1935, 1, 'rent', '2025-09', 1800.0, '2025-09-01', 'paid')
ON CONFLICT DO NOTHING;
-- 劉基寅 (monthly)
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (597, 1947, 1, 'rent', '2025-10', 10880.0, '2025-10-15', 'paid')
ON CONFLICT DO NOTHING;
-- 廖佑泓 (semi_annual)
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (1023, 2201, 1, 'rent', '2025-10', 8940.0, '2025-10-05', 'paid')
ON CONFLICT DO NOTHING;
-- 朱栢逸 (semi_annual)
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (1131, 2313, 1, 'rent', '2025-10', 1690.0, '2025-10-05', 'paid')
ON CONFLICT DO NOTHING;
-- 劉基寅 (monthly)
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (597, 1947, 1, 'rent', '2025-11', 10880.0, '2025-11-15', 'paid')
ON CONFLICT DO NOTHING;
-- 陳印泰 (semi_annual)
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (1183, 2368, 2, 'rent', '2025-11', 10140.0, '2025-11-10', 'paid')
ON CONFLICT DO NOTHING;
-- 江雅宣 (annual)
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (1132, 2314, 1, 'rent', '2025-11', 1800.0, '2025-11-05', 'paid')
ON CONFLICT DO NOTHING;
-- 沈孟輝 (semi_annual)
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (1155, 682, 2, 'rent', '2025-04', 16800.0, '2025-04-05', 'paid')
ON CONFLICT DO NOTHING;
-- 洪莉晴 (annual)
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (995, 698, 2, 'rent', '2025-08', 26280.0, '2025-08-01', 'paid')
ON CONFLICT DO NOTHING;
-- 沈孟輝 (semi_annual)
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (1155, 682, 2, 'rent', '2025-10', 1800.0, '2025-10-05', 'paid')
ON CONFLICT DO NOTHING;

-- 統計: 46 筆已付款, 總金額 $474,091

-- ============================================================================
-- 2026 年待繳記錄: 92 筆（可選匯入）
-- 建議：使用 generate_monthly_payments 函數生成，而非手動匯入
-- ============================================================================