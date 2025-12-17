-- ============================================================================
-- Hour Jungle CRM - 2026 繳費記錄匯入
-- 生成時間: 2025-12-07 12:14:22
-- ============================================================================

INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (547, 1897, 1, 'rent', '2026-06', 18000, '2026-06-01', 'paid', '2025-05-23', 'transfer')
ON CONFLICT DO NOTHING;
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (552, 1902, 1, 'rent', '2026-06', 21600, '2026-06-01', 'paid', '2025-06-04', 'transfer')
ON CONFLICT DO NOTHING;
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (596, 1946, 1, 'rent', '2026-06', 21600, '2026-06-01', 'paid', '2025-06-18', 'transfer')
ON CONFLICT DO NOTHING;

-- 統計:
-- 已繳 (paid):       3 筆  $      61,200
-- 待繳 (pending):    0 筆  $           0

-- 驗證
SELECT payment_period, payment_status, COUNT(*) as count, SUM(amount) as total FROM payments WHERE payment_period LIKE '2026-%' GROUP BY payment_period, payment_status ORDER BY payment_period;