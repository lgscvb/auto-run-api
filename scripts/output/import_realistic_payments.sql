-- ============================================================================
-- Hour Jungle CRM - 真實繳費記錄生成腳本
-- 生成時間: 2025-12-07 01:04:30
-- ============================================================================

-- 清空現有繳費記錄
TRUNCATE payments RESTART IDENTITY;

INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (547, 1897, 1, 'rent', '2024-06', 18000.0, '2024-06-01', 'paid', '2024-06-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (547, 1897, 1, 'rent', '2025-06', 18000.0, '2025-06-01', 'paid', '2025-06-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (548, 1898, 1, 'rent', '2025-06', 1490.0, '2025-06-01', 'paid', '2025-06-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (548, 1898, 1, 'rent', '2025-07', 1490.0, '2025-07-01', 'paid', '2025-07-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (548, 1898, 1, 'rent', '2025-08', 1490.0, '2025-08-01', 'paid', '2025-08-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (548, 1898, 1, 'rent', '2025-09', 1490.0, '2025-09-01', 'paid', '2025-09-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (548, 1898, 1, 'rent', '2025-10', 1490.0, '2025-10-01', 'paid', '2025-10-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (548, 1898, 1, 'rent', '2025-11', 1490.0, '2025-11-01', 'paid', '2025-11-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, overdue_days)
VALUES (548, 1898, 1, 'rent', '2025-11', 1490.0, '2025-11-01', 'overdue', 36);
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (548, 1898, 1, 'rent', '2025-12', 1490.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (548, 1898, 1, 'rent', '2026-01', 1490.0, '2026-01-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (548, 1898, 1, 'rent', '2026-02', 1490.0, '2026-02-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (549, 1899, 1, 'rent', '2025-06', 1490.0, '2025-06-01', 'paid', '2025-06-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (549, 1899, 1, 'rent', '2025-07', 1490.0, '2025-07-01', 'paid', '2025-07-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (549, 1899, 1, 'rent', '2025-08', 1490.0, '2025-08-01', 'paid', '2025-08-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (549, 1899, 1, 'rent', '2025-09', 1490.0, '2025-09-01', 'paid', '2025-09-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (549, 1899, 1, 'rent', '2025-10', 1490.0, '2025-10-01', 'paid', '2025-10-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (549, 1899, 1, 'rent', '2025-11', 1490.0, '2025-11-01', 'paid', '2025-11-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (549, 1899, 1, 'rent', '2025-11', 1490.0, '2025-11-01', 'paid', '2025-11-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (549, 1899, 1, 'rent', '2025-12', 1490.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (549, 1899, 1, 'rent', '2026-01', 1490.0, '2026-01-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (549, 1899, 1, 'rent', '2026-02', 1490.0, '2026-02-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (550, 1900, 1, 'rent', '2024-10', 18000.0, '2024-10-01', 'paid', '2024-10-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (550, 1900, 1, 'rent', '2025-10', 18000.0, '2025-10-01', 'paid', '2025-10-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (551, 1901, 1, 'rent', '2025-11', 40560.0, '2025-11-01', 'paid', '2025-11-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (551, 1901, 1, 'rent', '2025-12', 40560.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (552, 1902, 1, 'rent', '2025-11', 21600.0, '2025-11-01', 'paid', '2025-11-13', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (552, 1902, 1, 'rent', '2025-12', 21600.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (553, 1903, 1, 'rent', '2023-10', 1490.0, '2023-10-01', 'paid', '2023-10-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (553, 1903, 1, 'rent', '2023-11', 1490.0, '2023-11-01', 'paid', '2023-11-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (553, 1903, 1, 'rent', '2023-12', 1490.0, '2023-12-01', 'paid', '2023-12-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (553, 1903, 1, 'rent', '2024-01', 1490.0, '2024-01-01', 'paid', '2024-01-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (553, 1903, 1, 'rent', '2024-02', 1490.0, '2024-02-01', 'paid', '2024-02-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (553, 1903, 1, 'rent', '2024-03', 1490.0, '2024-03-01', 'paid', '2024-03-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (553, 1903, 1, 'rent', '2024-04', 1490.0, '2024-04-01', 'paid', '2024-04-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (553, 1903, 1, 'rent', '2024-05', 1490.0, '2024-05-01', 'paid', '2024-05-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (553, 1903, 1, 'rent', '2024-06', 1490.0, '2024-06-01', 'paid', '2024-06-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (553, 1903, 1, 'rent', '2024-07', 1490.0, '2024-07-01', 'paid', '2024-07-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (553, 1903, 1, 'rent', '2024-08', 1490.0, '2024-08-01', 'paid', '2024-08-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (553, 1903, 1, 'rent', '2024-09', 1490.0, '2024-09-01', 'paid', '2024-09-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (554, 1904, 1, 'rent', '2025-06', 1690.0, '2025-06-01', 'paid', '2025-06-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (554, 1904, 1, 'rent', '2025-07', 1690.0, '2025-07-01', 'paid', '2025-07-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (554, 1904, 1, 'rent', '2025-08', 1690.0, '2025-08-01', 'paid', '2025-08-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (554, 1904, 1, 'rent', '2025-09', 1690.0, '2025-09-01', 'paid', '2025-09-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (554, 1904, 1, 'rent', '2025-10', 1690.0, '2025-10-01', 'paid', '2025-10-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (554, 1904, 1, 'rent', '2025-11', 1690.0, '2025-11-01', 'paid', '2025-11-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, overdue_days)
VALUES (554, 1904, 1, 'rent', '2025-11', 1690.0, '2025-11-01', 'overdue', 36);
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (554, 1904, 1, 'rent', '2025-12', 1690.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (554, 1904, 1, 'rent', '2026-01', 1690.0, '2026-01-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (554, 1904, 1, 'rent', '2026-02', 1690.0, '2026-02-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (555, 1905, 1, 'rent', '2025-06', 1490.0, '2025-06-01', 'paid', '2025-06-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (555, 1905, 1, 'rent', '2025-07', 1490.0, '2025-07-01', 'paid', '2025-07-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (555, 1905, 1, 'rent', '2025-08', 1490.0, '2025-08-01', 'paid', '2025-08-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (555, 1905, 1, 'rent', '2025-09', 1490.0, '2025-09-01', 'paid', '2025-09-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (555, 1905, 1, 'rent', '2025-10', 1490.0, '2025-10-01', 'paid', '2025-10-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (555, 1905, 1, 'rent', '2025-11', 1490.0, '2025-11-01', 'paid', '2025-11-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (555, 1905, 1, 'rent', '2025-11', 1490.0, '2025-11-01', 'paid', '2025-11-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (555, 1905, 1, 'rent', '2025-12', 1490.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (556, 1906, 1, 'rent', '2025-06', 1490.0, '2025-06-01', 'paid', '2025-06-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (556, 1906, 1, 'rent', '2025-07', 1490.0, '2025-07-01', 'paid', '2025-07-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (556, 1906, 1, 'rent', '2025-08', 1490.0, '2025-08-01', 'paid', '2025-08-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (556, 1906, 1, 'rent', '2025-09', 1490.0, '2025-09-01', 'paid', '2025-09-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (556, 1906, 1, 'rent', '2025-10', 1490.0, '2025-10-01', 'paid', '2025-10-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (556, 1906, 1, 'rent', '2025-11', 1490.0, '2025-11-01', 'paid', '2025-11-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (556, 1906, 1, 'rent', '2025-11', 1490.0, '2025-11-01', 'paid', '2025-11-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (556, 1906, 1, 'rent', '2025-12', 1490.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (557, 1907, 1, 'rent', '2025-06', 1490.0, '2025-06-01', 'paid', '2025-06-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (557, 1907, 1, 'rent', '2025-07', 1490.0, '2025-07-01', 'paid', '2025-07-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (557, 1907, 1, 'rent', '2025-08', 1490.0, '2025-08-01', 'paid', '2025-08-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (557, 1907, 1, 'rent', '2025-09', 1490.0, '2025-09-01', 'paid', '2025-09-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (557, 1907, 1, 'rent', '2025-10', 1490.0, '2025-10-01', 'paid', '2025-10-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (557, 1907, 1, 'rent', '2025-11', 1490.0, '2025-11-01', 'paid', '2025-11-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (557, 1907, 1, 'rent', '2025-11', 1490.0, '2025-11-01', 'paid', '2025-11-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (557, 1907, 1, 'rent', '2025-12', 1490.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (557, 1907, 1, 'rent', '2026-01', 1490.0, '2026-01-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (558, 1908, 1, 'rent', '2025-11', 24000.0, '2025-11-01', 'paid', '2025-11-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (558, 1908, 1, 'rent', '2025-12', 24000.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (559, 1909, 1, 'rent', '2025-06', 1650.0, '2025-06-01', 'paid', '2025-06-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (559, 1909, 1, 'rent', '2025-07', 1650.0, '2025-07-01', 'paid', '2025-07-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (559, 1909, 1, 'rent', '2025-08', 1650.0, '2025-08-01', 'paid', '2025-08-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (559, 1909, 1, 'rent', '2025-09', 1650.0, '2025-09-01', 'paid', '2025-09-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (559, 1909, 1, 'rent', '2025-10', 1650.0, '2025-10-01', 'paid', '2025-10-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (559, 1909, 1, 'rent', '2025-11', 1650.0, '2025-11-01', 'paid', '2025-11-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (559, 1909, 1, 'rent', '2025-11', 1650.0, '2025-11-01', 'paid', '2025-11-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (559, 1909, 1, 'rent', '2025-12', 1650.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (559, 1909, 1, 'rent', '2026-01', 1650.0, '2026-01-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (559, 1909, 1, 'rent', '2026-02', 1650.0, '2026-02-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (560, 1910, 1, 'rent', '2025-11', 43200.0, '2025-11-01', 'paid', '2025-11-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (560, 1910, 1, 'rent', '2025-12', 43200.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, overdue_days)
VALUES (562, 1912, 1, 'rent', '2025-11', 43200.0, '2025-11-01', 'overdue', 36);
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (562, 1912, 1, 'rent', '2025-12', 43200.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (563, 1913, 1, 'rent', '2025-06', 2000.0, '2025-06-01', 'paid', '2025-06-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (563, 1913, 1, 'rent', '2025-07', 2000.0, '2025-07-01', 'paid', '2025-07-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (563, 1913, 1, 'rent', '2025-08', 2000.0, '2025-08-01', 'paid', '2025-08-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (563, 1913, 1, 'rent', '2025-09', 2000.0, '2025-09-01', 'paid', '2025-09-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (563, 1913, 1, 'rent', '2025-10', 2000.0, '2025-10-01', 'paid', '2025-10-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (563, 1913, 1, 'rent', '2025-11', 2000.0, '2025-11-01', 'paid', '2025-11-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (563, 1913, 1, 'rent', '2025-11', 2000.0, '2025-11-01', 'paid', '2025-11-15', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (563, 1913, 1, 'rent', '2025-12', 2000.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (563, 1913, 1, 'rent', '2026-01', 2000.0, '2026-01-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (563, 1913, 1, 'rent', '2026-02', 2000.0, '2026-02-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (564, 1914, 1, 'rent', '2025-06', 1690.0, '2025-06-01', 'paid', '2025-06-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (564, 1914, 1, 'rent', '2025-07', 1690.0, '2025-07-01', 'paid', '2025-07-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (564, 1914, 1, 'rent', '2025-08', 1690.0, '2025-08-01', 'paid', '2025-08-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (564, 1914, 1, 'rent', '2025-09', 1690.0, '2025-09-01', 'paid', '2025-09-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (564, 1914, 1, 'rent', '2025-10', 1690.0, '2025-10-01', 'paid', '2025-10-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (564, 1914, 1, 'rent', '2025-11', 1690.0, '2025-11-01', 'paid', '2025-11-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (564, 1914, 1, 'rent', '2025-11', 1690.0, '2025-11-01', 'paid', '2025-11-13', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (564, 1914, 1, 'rent', '2025-12', 1690.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (564, 1914, 1, 'rent', '2026-01', 1690.0, '2026-01-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (564, 1914, 1, 'rent', '2026-02', 1690.0, '2026-02-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, overdue_days)
VALUES (565, 1915, 1, 'rent', '2025-11', 40560.0, '2025-11-01', 'overdue', 36);
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (565, 1915, 1, 'rent', '2025-12', 40560.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (566, 1916, 1, 'rent', '2025-06', 1690.0, '2025-06-01', 'paid', '2025-06-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (566, 1916, 1, 'rent', '2025-07', 1690.0, '2025-07-01', 'paid', '2025-07-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (566, 1916, 1, 'rent', '2025-08', 1690.0, '2025-08-01', 'paid', '2025-08-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (566, 1916, 1, 'rent', '2025-09', 1690.0, '2025-09-01', 'paid', '2025-09-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (566, 1916, 1, 'rent', '2025-10', 1690.0, '2025-10-01', 'paid', '2025-10-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (566, 1916, 1, 'rent', '2025-11', 1690.0, '2025-11-01', 'paid', '2025-11-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (566, 1916, 1, 'rent', '2025-11', 1690.0, '2025-11-01', 'paid', '2025-11-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (566, 1916, 1, 'rent', '2025-12', 1690.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (566, 1916, 1, 'rent', '2026-01', 1690.0, '2026-01-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (566, 1916, 1, 'rent', '2026-02', 1690.0, '2026-02-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (567, 1917, 1, 'rent', '2025-07', 1800.0, '2025-07-01', 'paid', '2025-07-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (567, 1917, 1, 'rent', '2025-08', 1800.0, '2025-08-01', 'paid', '2025-08-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (567, 1917, 1, 'rent', '2025-09', 1800.0, '2025-09-01', 'paid', '2025-09-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (567, 1917, 1, 'rent', '2025-10', 1800.0, '2025-10-01', 'paid', '2025-10-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (567, 1917, 1, 'rent', '2025-11', 1800.0, '2025-11-01', 'paid', '2025-11-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, overdue_days)
VALUES (567, 1917, 1, 'rent', '2025-11', 1800.0, '2025-11-01', 'overdue', 36);
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (567, 1917, 1, 'rent', '2025-12', 1800.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (567, 1917, 1, 'rent', '2026-01', 1800.0, '2026-01-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (567, 1917, 1, 'rent', '2026-02', 1800.0, '2026-02-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (568, 1918, 1, 'rent', '2025-08', 1690.0, '2025-08-01', 'paid', '2025-08-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (568, 1918, 1, 'rent', '2025-09', 1690.0, '2025-09-01', 'paid', '2025-09-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (568, 1918, 1, 'rent', '2025-10', 1690.0, '2025-10-01', 'paid', '2025-10-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (568, 1918, 1, 'rent', '2025-11', 1690.0, '2025-11-01', 'paid', '2025-11-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (568, 1918, 1, 'rent', '2025-11', 1690.0, '2025-11-01', 'paid', '2025-11-15', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (568, 1918, 1, 'rent', '2025-12', 1690.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (568, 1918, 1, 'rent', '2026-01', 1690.0, '2026-01-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (568, 1918, 1, 'rent', '2026-02', 1690.0, '2026-02-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (569, 1919, 1, 'rent', '2025-08', 1800.0, '2025-08-01', 'paid', '2025-08-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (569, 1919, 1, 'rent', '2025-09', 1800.0, '2025-09-01', 'paid', '2025-09-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (569, 1919, 1, 'rent', '2025-10', 1800.0, '2025-10-01', 'paid', '2025-10-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (569, 1919, 1, 'rent', '2025-11', 1800.0, '2025-11-01', 'paid', '2025-11-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (569, 1919, 1, 'rent', '2025-11', 1800.0, '2025-11-01', 'paid', '2025-11-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (569, 1919, 1, 'rent', '2025-12', 1800.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (569, 1919, 1, 'rent', '2026-01', 1800.0, '2026-01-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (569, 1919, 1, 'rent', '2026-02', 1800.0, '2026-02-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (570, 1920, 1, 'rent', '2025-06', 2000.0, '2025-06-01', 'paid', '2025-06-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (570, 1920, 1, 'rent', '2025-07', 2000.0, '2025-07-01', 'paid', '2025-07-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (570, 1920, 1, 'rent', '2025-08', 2000.0, '2025-08-01', 'paid', '2025-08-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (570, 1920, 1, 'rent', '2025-09', 2000.0, '2025-09-01', 'paid', '2025-09-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (570, 1920, 1, 'rent', '2025-10', 2000.0, '2025-10-01', 'paid', '2025-10-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (570, 1920, 1, 'rent', '2025-11', 2000.0, '2025-11-01', 'paid', '2025-11-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (570, 1920, 1, 'rent', '2025-11', 2000.0, '2025-11-01', 'paid', '2025-11-12', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (570, 1920, 1, 'rent', '2025-12', 2000.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (570, 1920, 1, 'rent', '2026-01', 2000.0, '2026-01-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (570, 1920, 1, 'rent', '2026-02', 2000.0, '2026-02-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (571, 1921, 1, 'rent', '2025-11', 1800.0, '2025-11-01', 'paid', '2025-11-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (571, 1921, 1, 'rent', '2025-11', 1800.0, '2025-11-01', 'paid', '2025-11-15', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (571, 1921, 1, 'rent', '2025-12', 1800.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (571, 1921, 1, 'rent', '2026-01', 1800.0, '2026-01-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (571, 1921, 1, 'rent', '2026-02', 1800.0, '2026-02-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (572, 1922, 1, 'rent', '2025-11', 1800.0, '2025-11-01', 'paid', '2025-11-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (572, 1922, 1, 'rent', '2025-11', 1800.0, '2025-11-01', 'paid', '2025-11-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (572, 1922, 1, 'rent', '2025-12', 1800.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (572, 1922, 1, 'rent', '2026-01', 1800.0, '2026-01-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (572, 1922, 1, 'rent', '2026-02', 1800.0, '2026-02-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (573, 1923, 1, 'rent', '2024-12', 1800.0, '2024-12-01', 'paid', '2024-12-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (573, 1923, 1, 'rent', '2025-01', 1800.0, '2025-01-01', 'paid', '2025-01-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (573, 1923, 1, 'rent', '2025-02', 1800.0, '2025-02-01', 'paid', '2025-02-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (573, 1923, 1, 'rent', '2025-03', 1800.0, '2025-03-01', 'paid', '2025-03-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (573, 1923, 1, 'rent', '2025-04', 1800.0, '2025-04-01', 'paid', '2025-04-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (573, 1923, 1, 'rent', '2025-05', 1800.0, '2025-05-01', 'paid', '2025-05-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (573, 1923, 1, 'rent', '2025-06', 1800.0, '2025-06-01', 'paid', '2025-06-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (573, 1923, 1, 'rent', '2025-07', 1800.0, '2025-07-01', 'paid', '2025-07-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (573, 1923, 1, 'rent', '2025-08', 1800.0, '2025-08-01', 'paid', '2025-08-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (573, 1923, 1, 'rent', '2025-09', 1800.0, '2025-09-01', 'paid', '2025-09-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (573, 1923, 1, 'rent', '2025-10', 1800.0, '2025-10-01', 'paid', '2025-10-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (573, 1923, 1, 'rent', '2025-11', 1800.0, '2025-11-01', 'paid', '2025-11-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (574, 1924, 1, 'rent', '2022-12', 1490.0, '2022-12-01', 'paid', '2022-12-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (574, 1924, 1, 'rent', '2023-01', 1490.0, '2023-01-01', 'paid', '2023-01-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (574, 1924, 1, 'rent', '2023-02', 1490.0, '2023-02-01', 'paid', '2023-02-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (574, 1924, 1, 'rent', '2023-03', 1490.0, '2023-03-01', 'paid', '2023-03-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (574, 1924, 1, 'rent', '2023-04', 1490.0, '2023-04-01', 'paid', '2023-04-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (574, 1924, 1, 'rent', '2023-05', 1490.0, '2023-05-01', 'paid', '2023-05-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (574, 1924, 1, 'rent', '2023-06', 1490.0, '2023-06-01', 'paid', '2023-06-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (574, 1924, 1, 'rent', '2023-07', 1490.0, '2023-07-01', 'paid', '2023-07-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (574, 1924, 1, 'rent', '2023-08', 1490.0, '2023-08-01', 'paid', '2023-08-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (574, 1924, 1, 'rent', '2023-09', 1490.0, '2023-09-01', 'paid', '2023-09-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (574, 1924, 1, 'rent', '2023-10', 1490.0, '2023-10-01', 'paid', '2023-10-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (574, 1924, 1, 'rent', '2023-11', 1490.0, '2023-11-01', 'paid', '2023-11-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (575, 1925, 1, 'rent', '2025-06', 1800.0, '2025-06-01', 'paid', '2025-06-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (575, 1925, 1, 'rent', '2025-07', 1800.0, '2025-07-01', 'paid', '2025-07-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (575, 1925, 1, 'rent', '2025-08', 1800.0, '2025-08-01', 'paid', '2025-08-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (575, 1925, 1, 'rent', '2025-09', 1800.0, '2025-09-01', 'paid', '2025-09-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (575, 1925, 1, 'rent', '2025-10', 1800.0, '2025-10-01', 'paid', '2025-10-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (575, 1925, 1, 'rent', '2025-11', 1800.0, '2025-11-01', 'paid', '2025-11-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (575, 1925, 1, 'rent', '2025-11', 1800.0, '2025-11-01', 'paid', '2025-11-14', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (575, 1925, 1, 'rent', '2025-12', 1800.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (576, 1926, 1, 'rent', '2025-06', 1490.0, '2025-06-01', 'paid', '2025-06-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (576, 1926, 1, 'rent', '2025-07', 1490.0, '2025-07-01', 'paid', '2025-07-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (576, 1926, 1, 'rent', '2025-08', 1490.0, '2025-08-01', 'paid', '2025-08-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (576, 1926, 1, 'rent', '2025-09', 1490.0, '2025-09-01', 'paid', '2025-09-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (576, 1926, 1, 'rent', '2025-10', 1490.0, '2025-10-01', 'paid', '2025-10-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (576, 1926, 1, 'rent', '2025-11', 1490.0, '2025-11-01', 'paid', '2025-11-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, overdue_days)
VALUES (576, 1926, 1, 'rent', '2025-11', 1490.0, '2025-11-01', 'overdue', 36);
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (576, 1926, 1, 'rent', '2025-12', 1490.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (576, 1926, 1, 'rent', '2026-01', 1490.0, '2026-01-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (577, 1927, 1, 'rent', '2025-06', 1800.0, '2025-06-01', 'paid', '2025-06-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (577, 1927, 1, 'rent', '2025-07', 1800.0, '2025-07-01', 'paid', '2025-07-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (577, 1927, 1, 'rent', '2025-08', 1800.0, '2025-08-01', 'paid', '2025-08-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (577, 1927, 1, 'rent', '2025-09', 1800.0, '2025-09-01', 'paid', '2025-09-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (577, 1927, 1, 'rent', '2025-10', 1800.0, '2025-10-01', 'paid', '2025-10-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (577, 1927, 1, 'rent', '2025-11', 1800.0, '2025-11-01', 'paid', '2025-11-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (577, 1927, 1, 'rent', '2025-11', 1800.0, '2025-11-01', 'paid', '2025-11-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (577, 1927, 1, 'rent', '2025-12', 1800.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (577, 1927, 1, 'rent', '2026-01', 1800.0, '2026-01-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (577, 1927, 1, 'rent', '2026-02', 1800.0, '2026-02-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (578, 1928, 1, 'rent', '2025-06', 1690.0, '2025-06-01', 'paid', '2025-06-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (578, 1928, 1, 'rent', '2025-07', 1690.0, '2025-07-01', 'paid', '2025-07-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (578, 1928, 1, 'rent', '2025-08', 1690.0, '2025-08-01', 'paid', '2025-08-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (578, 1928, 1, 'rent', '2025-09', 1690.0, '2025-09-01', 'paid', '2025-09-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (578, 1928, 1, 'rent', '2025-10', 1690.0, '2025-10-01', 'paid', '2025-10-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (578, 1928, 1, 'rent', '2025-11', 1690.0, '2025-11-01', 'paid', '2025-11-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (578, 1928, 1, 'rent', '2025-11', 1690.0, '2025-11-01', 'paid', '2025-11-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (578, 1928, 1, 'rent', '2025-12', 1690.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (578, 1928, 1, 'rent', '2026-01', 1690.0, '2026-01-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (578, 1928, 1, 'rent', '2026-02', 1690.0, '2026-02-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (579, 1929, 1, 'rent', '2025-06', 1800.0, '2025-06-01', 'paid', '2025-06-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (579, 1929, 1, 'rent', '2025-07', 1800.0, '2025-07-01', 'paid', '2025-07-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (579, 1929, 1, 'rent', '2025-08', 1800.0, '2025-08-01', 'paid', '2025-08-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (579, 1929, 1, 'rent', '2025-09', 1800.0, '2025-09-01', 'paid', '2025-09-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (579, 1929, 1, 'rent', '2025-10', 1800.0, '2025-10-01', 'paid', '2025-10-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (579, 1929, 1, 'rent', '2025-11', 1800.0, '2025-11-01', 'paid', '2025-11-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, overdue_days)
VALUES (579, 1929, 1, 'rent', '2025-11', 1800.0, '2025-11-01', 'overdue', 36);
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (579, 1929, 1, 'rent', '2025-12', 1800.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (579, 1929, 1, 'rent', '2026-01', 1800.0, '2026-01-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (579, 1929, 1, 'rent', '2026-02', 1800.0, '2026-02-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (580, 1930, 1, 'rent', '2025-06', 2000.0, '2025-06-01', 'paid', '2025-06-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (580, 1930, 1, 'rent', '2025-07', 2000.0, '2025-07-01', 'paid', '2025-07-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (580, 1930, 1, 'rent', '2025-08', 2000.0, '2025-08-01', 'paid', '2025-08-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (580, 1930, 1, 'rent', '2025-09', 2000.0, '2025-09-01', 'paid', '2025-09-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (580, 1930, 1, 'rent', '2025-10', 2000.0, '2025-10-01', 'paid', '2025-10-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (580, 1930, 1, 'rent', '2025-11', 2000.0, '2025-11-01', 'paid', '2025-11-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, overdue_days)
VALUES (580, 1930, 1, 'rent', '2025-11', 2000.0, '2025-11-01', 'overdue', 36);
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (580, 1930, 1, 'rent', '2025-12', 2000.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (580, 1930, 1, 'rent', '2026-01', 2000.0, '2026-01-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (580, 1930, 1, 'rent', '2026-02', 2000.0, '2026-02-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (581, 1931, 1, 'rent', '2025-06', 1800.0, '2025-06-01', 'paid', '2025-06-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (581, 1931, 1, 'rent', '2025-07', 1800.0, '2025-07-01', 'paid', '2025-07-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (581, 1931, 1, 'rent', '2025-08', 1800.0, '2025-08-01', 'paid', '2025-08-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (581, 1931, 1, 'rent', '2025-09', 1800.0, '2025-09-01', 'paid', '2025-09-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (581, 1931, 1, 'rent', '2025-10', 1800.0, '2025-10-01', 'paid', '2025-10-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (581, 1931, 1, 'rent', '2025-11', 1800.0, '2025-11-01', 'paid', '2025-11-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (581, 1931, 1, 'rent', '2025-11', 1800.0, '2025-11-01', 'paid', '2025-11-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (581, 1931, 1, 'rent', '2025-12', 1800.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (581, 1931, 1, 'rent', '2026-01', 1800.0, '2026-01-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (581, 1931, 1, 'rent', '2026-02', 1800.0, '2026-02-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (582, 1932, 1, 'rent', '2025-06', 2000.0, '2025-06-01', 'paid', '2025-06-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (582, 1932, 1, 'rent', '2025-07', 2000.0, '2025-07-01', 'paid', '2025-07-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (582, 1932, 1, 'rent', '2025-08', 2000.0, '2025-08-01', 'paid', '2025-08-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (582, 1932, 1, 'rent', '2025-09', 2000.0, '2025-09-01', 'paid', '2025-09-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (582, 1932, 1, 'rent', '2025-10', 2000.0, '2025-10-01', 'paid', '2025-10-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (582, 1932, 1, 'rent', '2025-11', 2000.0, '2025-11-01', 'paid', '2025-11-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, overdue_days)
VALUES (582, 1932, 1, 'rent', '2025-11', 2000.0, '2025-11-01', 'overdue', 36);
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (582, 1932, 1, 'rent', '2025-12', 2000.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (582, 1932, 1, 'rent', '2026-01', 2000.0, '2026-01-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (582, 1932, 1, 'rent', '2026-02', 2000.0, '2026-02-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (583, 1933, 1, 'rent', '2025-06', 1800.0, '2025-06-01', 'paid', '2025-06-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (583, 1933, 1, 'rent', '2025-07', 1800.0, '2025-07-01', 'paid', '2025-07-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (583, 1933, 1, 'rent', '2025-08', 1800.0, '2025-08-01', 'paid', '2025-08-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (583, 1933, 1, 'rent', '2025-09', 1800.0, '2025-09-01', 'paid', '2025-09-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (583, 1933, 1, 'rent', '2025-10', 1800.0, '2025-10-01', 'paid', '2025-10-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (583, 1933, 1, 'rent', '2025-11', 1800.0, '2025-11-01', 'paid', '2025-11-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, overdue_days)
VALUES (583, 1933, 1, 'rent', '2025-11', 1800.0, '2025-11-01', 'overdue', 36);
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (583, 1933, 1, 'rent', '2025-12', 1800.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (583, 1933, 1, 'rent', '2026-01', 1800.0, '2026-01-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (583, 1933, 1, 'rent', '2026-02', 1800.0, '2026-02-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (584, 1934, 1, 'rent', '2025-06', 1800.0, '2025-06-01', 'paid', '2025-06-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (584, 1934, 1, 'rent', '2025-07', 1800.0, '2025-07-01', 'paid', '2025-07-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (584, 1934, 1, 'rent', '2025-08', 1800.0, '2025-08-01', 'paid', '2025-08-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (584, 1934, 1, 'rent', '2025-09', 1800.0, '2025-09-01', 'paid', '2025-09-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (584, 1934, 1, 'rent', '2025-10', 1800.0, '2025-10-01', 'paid', '2025-10-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (584, 1934, 1, 'rent', '2025-11', 1800.0, '2025-11-01', 'paid', '2025-11-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (584, 1934, 1, 'rent', '2025-11', 1800.0, '2025-11-01', 'paid', '2025-11-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (584, 1934, 1, 'rent', '2025-12', 1800.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (584, 1934, 1, 'rent', '2026-01', 1800.0, '2026-01-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (584, 1934, 1, 'rent', '2026-02', 1800.0, '2026-02-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (585, 1935, 1, 'rent', '2025-11', 43200.0, '2025-11-01', 'paid', '2025-11-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (585, 1935, 1, 'rent', '2025-12', 43200.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (586, 1936, 1, 'rent', '2025-06', 1800.0, '2025-06-01', 'paid', '2025-06-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (586, 1936, 1, 'rent', '2025-07', 1800.0, '2025-07-01', 'paid', '2025-07-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (586, 1936, 1, 'rent', '2025-08', 1800.0, '2025-08-01', 'paid', '2025-08-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (586, 1936, 1, 'rent', '2025-09', 1800.0, '2025-09-01', 'paid', '2025-09-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (586, 1936, 1, 'rent', '2025-10', 1800.0, '2025-10-01', 'paid', '2025-10-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (586, 1936, 1, 'rent', '2025-11', 1800.0, '2025-11-01', 'paid', '2025-11-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (586, 1936, 1, 'rent', '2025-11', 1800.0, '2025-11-01', 'paid', '2025-11-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (586, 1936, 1, 'rent', '2025-12', 1800.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (586, 1936, 1, 'rent', '2026-01', 1800.0, '2026-01-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (586, 1936, 1, 'rent', '2026-02', 1800.0, '2026-02-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (587, 1937, 1, 'rent', '2025-06', 1800.0, '2025-06-01', 'paid', '2025-06-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (587, 1937, 1, 'rent', '2025-07', 1800.0, '2025-07-01', 'paid', '2025-07-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (587, 1937, 1, 'rent', '2025-08', 1800.0, '2025-08-01', 'paid', '2025-08-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (587, 1937, 1, 'rent', '2025-09', 1800.0, '2025-09-01', 'paid', '2025-09-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (587, 1937, 1, 'rent', '2025-10', 1800.0, '2025-10-01', 'paid', '2025-10-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (587, 1937, 1, 'rent', '2025-11', 1800.0, '2025-11-01', 'paid', '2025-11-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (587, 1937, 1, 'rent', '2025-11', 1800.0, '2025-11-01', 'paid', '2025-11-16', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (587, 1937, 1, 'rent', '2025-12', 1800.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (587, 1937, 1, 'rent', '2026-01', 1800.0, '2026-01-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (587, 1937, 1, 'rent', '2026-02', 1800.0, '2026-02-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (588, 1938, 1, 'rent', '2025-06', 1800.0, '2025-06-01', 'paid', '2025-06-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (588, 1938, 1, 'rent', '2025-07', 1800.0, '2025-07-01', 'paid', '2025-07-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (588, 1938, 1, 'rent', '2025-08', 1800.0, '2025-08-01', 'paid', '2025-08-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (588, 1938, 1, 'rent', '2025-09', 1800.0, '2025-09-01', 'paid', '2025-09-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (588, 1938, 1, 'rent', '2025-10', 1800.0, '2025-10-01', 'paid', '2025-10-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (588, 1938, 1, 'rent', '2025-11', 1800.0, '2025-11-01', 'paid', '2025-11-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (588, 1938, 1, 'rent', '2025-11', 1800.0, '2025-11-01', 'paid', '2025-11-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (588, 1938, 1, 'rent', '2025-12', 1800.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (588, 1938, 1, 'rent', '2026-01', 1800.0, '2026-01-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (588, 1938, 1, 'rent', '2026-02', 1800.0, '2026-02-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (589, 1939, 1, 'rent', '2025-06', 2000.0, '2025-06-01', 'paid', '2025-06-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (589, 1939, 1, 'rent', '2025-07', 2000.0, '2025-07-01', 'paid', '2025-07-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (589, 1939, 1, 'rent', '2025-08', 2000.0, '2025-08-01', 'paid', '2025-08-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (589, 1939, 1, 'rent', '2025-09', 2000.0, '2025-09-01', 'paid', '2025-09-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (589, 1939, 1, 'rent', '2025-10', 2000.0, '2025-10-01', 'paid', '2025-10-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (589, 1939, 1, 'rent', '2025-11', 2000.0, '2025-11-01', 'paid', '2025-11-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (589, 1939, 1, 'rent', '2025-11', 2000.0, '2025-11-01', 'paid', '2025-11-13', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (589, 1939, 1, 'rent', '2025-12', 2000.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (589, 1939, 1, 'rent', '2026-01', 2000.0, '2026-01-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (589, 1939, 1, 'rent', '2026-02', 2000.0, '2026-02-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (590, 1940, 1, 'rent', '2025-06', 1800.0, '2025-06-01', 'paid', '2025-06-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (590, 1940, 1, 'rent', '2025-07', 1800.0, '2025-07-01', 'paid', '2025-07-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (590, 1940, 1, 'rent', '2025-08', 1800.0, '2025-08-01', 'paid', '2025-08-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (590, 1940, 1, 'rent', '2025-09', 1800.0, '2025-09-01', 'paid', '2025-09-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (590, 1940, 1, 'rent', '2025-10', 1800.0, '2025-10-01', 'paid', '2025-10-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (590, 1940, 1, 'rent', '2025-11', 1800.0, '2025-11-01', 'paid', '2025-11-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (590, 1940, 1, 'rent', '2025-11', 1800.0, '2025-11-01', 'paid', '2025-11-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (590, 1940, 1, 'rent', '2025-12', 1800.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (590, 1940, 1, 'rent', '2026-01', 1800.0, '2026-01-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (590, 1940, 1, 'rent', '2026-02', 1800.0, '2026-02-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (591, 1941, 1, 'rent', '2025-06', 1800.0, '2025-06-01', 'paid', '2025-06-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (591, 1941, 1, 'rent', '2025-07', 1800.0, '2025-07-01', 'paid', '2025-07-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (591, 1941, 1, 'rent', '2025-08', 1800.0, '2025-08-01', 'paid', '2025-08-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (591, 1941, 1, 'rent', '2025-09', 1800.0, '2025-09-01', 'paid', '2025-09-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (591, 1941, 1, 'rent', '2025-10', 1800.0, '2025-10-01', 'paid', '2025-10-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (591, 1941, 1, 'rent', '2025-11', 1800.0, '2025-11-01', 'paid', '2025-11-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, overdue_days)
VALUES (591, 1941, 1, 'rent', '2025-11', 1800.0, '2025-11-01', 'overdue', 36);
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (591, 1941, 1, 'rent', '2025-12', 1800.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (591, 1941, 1, 'rent', '2026-01', 1800.0, '2026-01-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (591, 1941, 1, 'rent', '2026-02', 1800.0, '2026-02-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (592, 1942, 1, 'rent', '2025-06', 1800.0, '2025-06-01', 'paid', '2025-06-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (592, 1942, 1, 'rent', '2025-07', 1800.0, '2025-07-01', 'paid', '2025-07-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (592, 1942, 1, 'rent', '2025-08', 1800.0, '2025-08-01', 'paid', '2025-08-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (592, 1942, 1, 'rent', '2025-09', 1800.0, '2025-09-01', 'paid', '2025-09-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (592, 1942, 1, 'rent', '2025-10', 1800.0, '2025-10-01', 'paid', '2025-10-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (592, 1942, 1, 'rent', '2025-11', 1800.0, '2025-11-01', 'paid', '2025-11-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (592, 1942, 1, 'rent', '2025-11', 1800.0, '2025-11-01', 'paid', '2025-11-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (592, 1942, 1, 'rent', '2025-12', 1800.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (592, 1942, 1, 'rent', '2026-01', 1800.0, '2026-01-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (592, 1942, 1, 'rent', '2026-02', 1800.0, '2026-02-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (593, 1943, 1, 'rent', '2025-06', 1800.0, '2025-06-01', 'paid', '2025-06-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (593, 1943, 1, 'rent', '2025-07', 1800.0, '2025-07-01', 'paid', '2025-07-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (593, 1943, 1, 'rent', '2025-08', 1800.0, '2025-08-01', 'paid', '2025-08-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (593, 1943, 1, 'rent', '2025-09', 1800.0, '2025-09-01', 'paid', '2025-09-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (593, 1943, 1, 'rent', '2025-10', 1800.0, '2025-10-01', 'paid', '2025-10-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (593, 1943, 1, 'rent', '2025-11', 1800.0, '2025-11-01', 'paid', '2025-11-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (593, 1943, 1, 'rent', '2025-11', 1800.0, '2025-11-01', 'paid', '2025-11-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (593, 1943, 1, 'rent', '2025-12', 1800.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (593, 1943, 1, 'rent', '2026-01', 1800.0, '2026-01-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (593, 1943, 1, 'rent', '2026-02', 1800.0, '2026-02-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (594, 1944, 1, 'rent', '2025-06', 1800.0, '2025-06-01', 'paid', '2025-06-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (594, 1944, 1, 'rent', '2025-07', 1800.0, '2025-07-01', 'paid', '2025-07-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (594, 1944, 1, 'rent', '2025-08', 1800.0, '2025-08-01', 'paid', '2025-08-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (594, 1944, 1, 'rent', '2025-09', 1800.0, '2025-09-01', 'paid', '2025-09-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (594, 1944, 1, 'rent', '2025-10', 1800.0, '2025-10-01', 'paid', '2025-10-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (594, 1944, 1, 'rent', '2025-11', 1800.0, '2025-11-01', 'paid', '2025-11-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (594, 1944, 1, 'rent', '2025-11', 1800.0, '2025-11-01', 'paid', '2025-11-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (594, 1944, 1, 'rent', '2025-12', 1800.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (594, 1944, 1, 'rent', '2026-01', 1800.0, '2026-01-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (594, 1944, 1, 'rent', '2026-02', 1800.0, '2026-02-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (595, 1945, 1, 'rent', '2025-06', 2000.0, '2025-06-01', 'paid', '2025-06-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (595, 1945, 1, 'rent', '2025-07', 2000.0, '2025-07-01', 'paid', '2025-07-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (595, 1945, 1, 'rent', '2025-08', 2000.0, '2025-08-01', 'paid', '2025-08-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (595, 1945, 1, 'rent', '2025-09', 2000.0, '2025-09-01', 'paid', '2025-09-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (595, 1945, 1, 'rent', '2025-10', 2000.0, '2025-10-01', 'paid', '2025-10-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (595, 1945, 1, 'rent', '2025-11', 2000.0, '2025-11-01', 'paid', '2025-11-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, overdue_days)
VALUES (595, 1945, 1, 'rent', '2025-11', 2000.0, '2025-11-01', 'overdue', 36);
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (595, 1945, 1, 'rent', '2025-12', 2000.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (595, 1945, 1, 'rent', '2026-01', 2000.0, '2026-01-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (595, 1945, 1, 'rent', '2026-02', 2000.0, '2026-02-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (596, 1946, 1, 'rent', '2025-06', 1800.0, '2025-06-01', 'paid', '2025-06-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (596, 1946, 1, 'rent', '2025-07', 1800.0, '2025-07-01', 'paid', '2025-07-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (596, 1946, 1, 'rent', '2025-08', 1800.0, '2025-08-01', 'paid', '2025-08-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (596, 1946, 1, 'rent', '2025-09', 1800.0, '2025-09-01', 'paid', '2025-09-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (596, 1946, 1, 'rent', '2025-10', 1800.0, '2025-10-01', 'paid', '2025-10-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (596, 1946, 1, 'rent', '2025-11', 1800.0, '2025-11-01', 'paid', '2025-11-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (596, 1946, 1, 'rent', '2025-11', 1800.0, '2025-11-01', 'paid', '2025-11-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (596, 1946, 1, 'rent', '2025-12', 1800.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (596, 1946, 1, 'rent', '2026-01', 1800.0, '2026-01-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (596, 1946, 1, 'rent', '2026-02', 1800.0, '2026-02-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (597, 1947, 1, 'rent', '2025-06', 1490.0, '2025-06-01', 'paid', '2025-06-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (597, 1947, 1, 'rent', '2025-07', 1490.0, '2025-07-01', 'paid', '2025-07-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (597, 1947, 1, 'rent', '2025-08', 1490.0, '2025-08-01', 'paid', '2025-08-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (597, 1947, 1, 'rent', '2025-09', 1490.0, '2025-09-01', 'paid', '2025-09-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (597, 1947, 1, 'rent', '2025-10', 1490.0, '2025-10-01', 'paid', '2025-10-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (597, 1947, 1, 'rent', '2025-11', 1490.0, '2025-11-01', 'paid', '2025-11-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (597, 1947, 1, 'rent', '2025-11', 1490.0, '2025-11-01', 'paid', '2025-11-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (597, 1947, 1, 'rent', '2025-12', 1490.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (597, 1947, 1, 'rent', '2026-01', 1490.0, '2026-01-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (597, 1947, 1, 'rent', '2026-02', 1490.0, '2026-02-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (599, 1949, 1, 'rent', '2025-07', 1800.0, '2025-07-01', 'paid', '2025-07-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (599, 1949, 1, 'rent', '2025-08', 1800.0, '2025-08-01', 'paid', '2025-08-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (599, 1949, 1, 'rent', '2025-09', 1800.0, '2025-09-01', 'paid', '2025-09-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (599, 1949, 1, 'rent', '2025-10', 1800.0, '2025-10-01', 'paid', '2025-10-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (599, 1949, 1, 'rent', '2025-11', 1800.0, '2025-11-01', 'paid', '2025-11-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (599, 1949, 1, 'rent', '2025-11', 1800.0, '2025-11-01', 'paid', '2025-11-16', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (599, 1949, 1, 'rent', '2025-12', 1800.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (599, 1949, 1, 'rent', '2026-01', 1800.0, '2026-01-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (599, 1949, 1, 'rent', '2026-02', 1800.0, '2026-02-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (600, 1950, 1, 'rent', '2025-07', 1490.0, '2025-07-01', 'paid', '2025-07-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (600, 1950, 1, 'rent', '2025-08', 1490.0, '2025-08-01', 'paid', '2025-08-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (600, 1950, 1, 'rent', '2025-09', 1490.0, '2025-09-01', 'paid', '2025-09-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (600, 1950, 1, 'rent', '2025-10', 1490.0, '2025-10-01', 'paid', '2025-10-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (600, 1950, 1, 'rent', '2025-11', 1490.0, '2025-11-01', 'paid', '2025-11-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, overdue_days)
VALUES (600, 1950, 1, 'rent', '2025-11', 1490.0, '2025-11-01', 'overdue', 36);
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (600, 1950, 1, 'rent', '2025-12', 1490.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (600, 1950, 1, 'rent', '2026-01', 1490.0, '2026-01-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (600, 1950, 1, 'rent', '2026-02', 1490.0, '2026-02-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (601, 1951, 1, 'rent', '2025-07', 1800.0, '2025-07-01', 'paid', '2025-07-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (601, 1951, 1, 'rent', '2025-08', 1800.0, '2025-08-01', 'paid', '2025-08-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (601, 1951, 1, 'rent', '2025-09', 1800.0, '2025-09-01', 'paid', '2025-09-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (601, 1951, 1, 'rent', '2025-10', 1800.0, '2025-10-01', 'paid', '2025-10-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (601, 1951, 1, 'rent', '2025-11', 1800.0, '2025-11-01', 'paid', '2025-11-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (601, 1951, 1, 'rent', '2025-11', 1800.0, '2025-11-01', 'paid', '2025-11-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (601, 1951, 1, 'rent', '2025-12', 1800.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (601, 1951, 1, 'rent', '2026-01', 1800.0, '2026-01-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (601, 1951, 1, 'rent', '2026-02', 1800.0, '2026-02-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (602, 1952, 1, 'rent', '2025-08', 1800.0, '2025-08-01', 'paid', '2025-08-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (602, 1952, 1, 'rent', '2025-09', 1800.0, '2025-09-01', 'paid', '2025-09-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (602, 1952, 1, 'rent', '2025-10', 1800.0, '2025-10-01', 'paid', '2025-10-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (602, 1952, 1, 'rent', '2025-11', 1800.0, '2025-11-01', 'paid', '2025-11-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (602, 1952, 1, 'rent', '2025-11', 1800.0, '2025-11-01', 'paid', '2025-11-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (602, 1952, 1, 'rent', '2025-12', 1800.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (602, 1952, 1, 'rent', '2026-01', 1800.0, '2026-01-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (602, 1952, 1, 'rent', '2026-02', 1800.0, '2026-02-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (603, 1953, 1, 'rent', '2023-08', 3000.0, '2023-08-01', 'paid', '2023-08-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (603, 1953, 1, 'rent', '2023-09', 3000.0, '2023-09-01', 'paid', '2023-09-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (603, 1953, 1, 'rent', '2023-10', 3000.0, '2023-10-01', 'paid', '2023-10-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (603, 1953, 1, 'rent', '2023-11', 3000.0, '2023-11-01', 'paid', '2023-11-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (603, 1953, 1, 'rent', '2023-12', 3000.0, '2023-12-01', 'paid', '2023-12-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (603, 1953, 1, 'rent', '2024-01', 3000.0, '2024-01-01', 'paid', '2024-01-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (603, 1953, 1, 'rent', '2024-02', 3000.0, '2024-02-01', 'paid', '2024-02-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (603, 1953, 1, 'rent', '2024-03', 3000.0, '2024-03-01', 'paid', '2024-03-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (603, 1953, 1, 'rent', '2024-04', 3000.0, '2024-04-01', 'paid', '2024-04-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (603, 1953, 1, 'rent', '2024-05', 3000.0, '2024-05-01', 'paid', '2024-05-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (603, 1953, 1, 'rent', '2024-06', 3000.0, '2024-06-01', 'paid', '2024-06-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (603, 1953, 1, 'rent', '2024-07', 3000.0, '2024-07-01', 'paid', '2024-07-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (604, 1954, 1, 'rent', '2025-09', 12825.0, '2025-09-01', 'paid', '2025-09-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (604, 1954, 1, 'rent', '2025-10', 12825.0, '2025-10-01', 'paid', '2025-10-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (604, 1954, 1, 'rent', '2025-11', 12825.0, '2025-11-01', 'paid', '2025-11-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, overdue_days)
VALUES (604, 1954, 1, 'rent', '2025-11', 12825.0, '2025-11-01', 'overdue', 36);
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (604, 1954, 1, 'rent', '2025-12', 12825.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (604, 1954, 1, 'rent', '2026-01', 12825.0, '2026-01-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (604, 1954, 1, 'rent', '2026-02', 12825.0, '2026-02-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (605, 1955, 1, 'rent', '2025-09', 1490.0, '2025-09-01', 'paid', '2025-09-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (605, 1955, 1, 'rent', '2025-10', 1490.0, '2025-10-01', 'paid', '2025-10-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (605, 1955, 1, 'rent', '2025-11', 1490.0, '2025-11-01', 'paid', '2025-11-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (605, 1955, 1, 'rent', '2025-11', 1490.0, '2025-11-01', 'paid', '2025-11-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (605, 1955, 1, 'rent', '2025-12', 1490.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (605, 1955, 1, 'rent', '2026-01', 1490.0, '2026-01-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (605, 1955, 1, 'rent', '2026-02', 1490.0, '2026-02-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (606, 1956, 1, 'rent', '2025-09', 1800.0, '2025-09-01', 'paid', '2025-09-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (606, 1956, 1, 'rent', '2025-10', 1800.0, '2025-10-01', 'paid', '2025-10-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (606, 1956, 1, 'rent', '2025-11', 1800.0, '2025-11-01', 'paid', '2025-11-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, overdue_days)
VALUES (606, 1956, 1, 'rent', '2025-11', 1800.0, '2025-11-01', 'overdue', 36);
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (606, 1956, 1, 'rent', '2025-12', 1800.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (606, 1956, 1, 'rent', '2026-01', 1800.0, '2026-01-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (606, 1956, 1, 'rent', '2026-02', 1800.0, '2026-02-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (607, 1957, 1, 'rent', '2025-10', 1490.0, '2025-10-01', 'paid', '2025-10-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (607, 1957, 1, 'rent', '2025-11', 1490.0, '2025-11-01', 'paid', '2025-11-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, overdue_days)
VALUES (607, 1957, 1, 'rent', '2025-11', 1490.0, '2025-11-01', 'overdue', 36);
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (607, 1957, 1, 'rent', '2025-12', 1490.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (607, 1957, 1, 'rent', '2026-01', 1490.0, '2026-01-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (607, 1957, 1, 'rent', '2026-02', 1490.0, '2026-02-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (608, 1958, 1, 'rent', '2023-09', 1490.0, '2023-09-01', 'paid', '2023-09-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (608, 1958, 1, 'rent', '2023-10', 1490.0, '2023-10-01', 'paid', '2023-10-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (608, 1958, 1, 'rent', '2023-11', 1490.0, '2023-11-01', 'paid', '2023-11-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (608, 1958, 1, 'rent', '2023-12', 1490.0, '2023-12-01', 'paid', '2023-12-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (608, 1958, 1, 'rent', '2024-01', 1490.0, '2024-01-01', 'paid', '2024-01-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (608, 1958, 1, 'rent', '2024-02', 1490.0, '2024-02-01', 'paid', '2024-02-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (608, 1958, 1, 'rent', '2024-03', 1490.0, '2024-03-01', 'paid', '2024-03-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (608, 1958, 1, 'rent', '2024-04', 1490.0, '2024-04-01', 'paid', '2024-04-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (608, 1958, 1, 'rent', '2024-05', 1490.0, '2024-05-01', 'paid', '2024-05-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (608, 1958, 1, 'rent', '2024-06', 1490.0, '2024-06-01', 'paid', '2024-06-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (608, 1958, 1, 'rent', '2024-07', 1490.0, '2024-07-01', 'paid', '2024-07-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (608, 1958, 1, 'rent', '2024-08', 1490.0, '2024-08-01', 'paid', '2024-08-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (609, 1959, 1, 'rent', '2025-07', 1490.0, '2025-07-01', 'paid', '2025-07-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (609, 1959, 1, 'rent', '2025-08', 1490.0, '2025-08-01', 'paid', '2025-08-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (609, 1959, 1, 'rent', '2025-09', 1490.0, '2025-09-01', 'paid', '2025-09-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (609, 1959, 1, 'rent', '2025-10', 1490.0, '2025-10-01', 'paid', '2025-10-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (609, 1959, 1, 'rent', '2025-11', 1490.0, '2025-11-01', 'paid', '2025-11-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, overdue_days)
VALUES (609, 1959, 1, 'rent', '2025-11', 1490.0, '2025-11-01', 'overdue', 36);
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (609, 1959, 1, 'rent', '2025-12', 1490.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (609, 1959, 1, 'rent', '2026-01', 1490.0, '2026-01-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (609, 1959, 1, 'rent', '2026-02', 1490.0, '2026-02-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, overdue_days)
VALUES (610, 1960, 1, 'rent', '2025-11', 10800.0, '2025-11-01', 'overdue', 36);
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (610, 1960, 1, 'rent', '2025-12', 10800.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (610, 1960, 1, 'rent', '2026-06', 10800.0, '2026-06-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (611, 1961, 1, 'rent', '2024-11', 1800.0, '2024-11-01', 'paid', '2024-11-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (611, 1961, 1, 'rent', '2024-12', 1800.0, '2024-12-01', 'paid', '2024-12-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (611, 1961, 1, 'rent', '2025-01', 1800.0, '2025-01-01', 'paid', '2025-01-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (611, 1961, 1, 'rent', '2025-02', 1800.0, '2025-02-01', 'paid', '2025-02-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (611, 1961, 1, 'rent', '2025-03', 1800.0, '2025-03-01', 'paid', '2025-03-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (611, 1961, 1, 'rent', '2025-04', 1800.0, '2025-04-01', 'paid', '2025-04-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (611, 1961, 1, 'rent', '2025-05', 1800.0, '2025-05-01', 'paid', '2025-05-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (611, 1961, 1, 'rent', '2025-06', 1800.0, '2025-06-01', 'paid', '2025-06-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (611, 1961, 1, 'rent', '2025-07', 1800.0, '2025-07-01', 'paid', '2025-07-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (611, 1961, 1, 'rent', '2025-08', 1800.0, '2025-08-01', 'paid', '2025-08-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (611, 1961, 1, 'rent', '2025-09', 1800.0, '2025-09-01', 'paid', '2025-09-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (611, 1961, 1, 'rent', '2025-10', 1800.0, '2025-10-01', 'paid', '2025-10-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (612, 1962, 1, 'rent', '2025-11', 1690.0, '2025-11-01', 'paid', '2025-11-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (612, 1962, 1, 'rent', '2025-11', 1690.0, '2025-11-01', 'paid', '2025-11-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (612, 1962, 1, 'rent', '2025-12', 1690.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (612, 1962, 1, 'rent', '2026-01', 1690.0, '2026-01-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (612, 1962, 1, 'rent', '2026-02', 1690.0, '2026-02-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (614, 1964, 1, 'rent', '2023-12', 8940.0, '2023-12-01', 'paid', '2023-12-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (614, 1964, 1, 'rent', '2024-06', 8940.0, '2024-06-01', 'paid', '2024-06-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (614, 1964, 1, 'rent', '2024-12', 8940.0, '2024-12-01', 'paid', '2024-12-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (614, 1964, 1, 'rent', '2025-06', 8940.0, '2025-06-01', 'paid', '2025-06-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, overdue_days)
VALUES (614, 1964, 1, 'rent', '2025-11', 8940.0, '2025-11-01', 'overdue', 36);
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (614, 1964, 1, 'rent', '2025-12', 8940.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (615, 1965, 1, 'rent', '2025-06', 1490.0, '2025-06-01', 'paid', '2025-06-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (615, 1965, 1, 'rent', '2025-07', 1490.0, '2025-07-01', 'paid', '2025-07-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (615, 1965, 1, 'rent', '2025-08', 1490.0, '2025-08-01', 'paid', '2025-08-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (615, 1965, 1, 'rent', '2025-09', 1490.0, '2025-09-01', 'paid', '2025-09-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (615, 1965, 1, 'rent', '2025-10', 1490.0, '2025-10-01', 'paid', '2025-10-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (615, 1965, 1, 'rent', '2025-11', 1490.0, '2025-11-01', 'paid', '2025-11-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, overdue_days)
VALUES (615, 1965, 1, 'rent', '2025-11', 1490.0, '2025-11-01', 'overdue', 36);
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (615, 1965, 1, 'rent', '2025-12', 1490.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (616, 1966, 1, 'rent', '2024-06', 8940.0, '2024-06-01', 'paid', '2024-06-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (616, 1966, 1, 'rent', '2024-12', 8940.0, '2024-12-01', 'paid', '2024-12-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (616, 1966, 1, 'rent', '2025-06', 8940.0, '2025-06-01', 'paid', '2025-06-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (616, 1966, 1, 'rent', '2025-11', 8940.0, '2025-11-01', 'paid', '2025-11-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (616, 1966, 1, 'rent', '2025-12', 8940.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (617, 1967, 1, 'rent', '2024-06', 8940.0, '2024-06-01', 'paid', '2024-06-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (617, 1967, 1, 'rent', '2024-12', 8940.0, '2024-12-01', 'paid', '2024-12-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (617, 1967, 1, 'rent', '2025-06', 8940.0, '2025-06-01', 'paid', '2025-06-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (617, 1967, 1, 'rent', '2025-11', 8940.0, '2025-11-01', 'paid', '2025-11-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (617, 1967, 1, 'rent', '2025-12', 8940.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (618, 1968, 1, 'rent', '2025-06', 1800.0, '2025-06-01', 'paid', '2025-06-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (618, 1968, 1, 'rent', '2025-07', 1800.0, '2025-07-01', 'paid', '2025-07-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (618, 1968, 1, 'rent', '2025-08', 1800.0, '2025-08-01', 'paid', '2025-08-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (618, 1968, 1, 'rent', '2025-09', 1800.0, '2025-09-01', 'paid', '2025-09-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (618, 1968, 1, 'rent', '2025-10', 1800.0, '2025-10-01', 'paid', '2025-10-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (618, 1968, 1, 'rent', '2025-11', 1800.0, '2025-11-01', 'paid', '2025-11-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (618, 1968, 1, 'rent', '2025-11', 1800.0, '2025-11-01', 'paid', '2025-11-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (618, 1968, 1, 'rent', '2025-12', 1800.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (618, 1968, 1, 'rent', '2026-01', 1800.0, '2026-01-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (619, 1969, 1, 'rent', '2025-06', 12000.0, '2025-06-01', 'paid', '2025-06-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (619, 1969, 1, 'rent', '2025-07', 12000.0, '2025-07-01', 'paid', '2025-07-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (619, 1969, 1, 'rent', '2025-08', 12000.0, '2025-08-01', 'paid', '2025-08-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (619, 1969, 1, 'rent', '2025-09', 12000.0, '2025-09-01', 'paid', '2025-09-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (619, 1969, 1, 'rent', '2025-10', 12000.0, '2025-10-01', 'paid', '2025-10-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (619, 1969, 1, 'rent', '2025-11', 12000.0, '2025-11-01', 'paid', '2025-11-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (619, 1969, 1, 'rent', '2025-11', 12000.0, '2025-11-01', 'paid', '2025-11-15', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (619, 1969, 1, 'rent', '2025-12', 12000.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (619, 1969, 1, 'rent', '2026-01', 12000.0, '2026-01-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (619, 1969, 1, 'rent', '2026-02', 12000.0, '2026-02-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (620, 1970, 1, 'rent', '2024-06', 9900.0, '2024-06-01', 'paid', '2024-06-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (620, 1970, 1, 'rent', '2024-12', 9900.0, '2024-12-01', 'paid', '2024-12-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (620, 1970, 1, 'rent', '2025-06', 9900.0, '2025-06-01', 'paid', '2025-06-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (620, 1970, 1, 'rent', '2025-11', 9900.0, '2025-11-01', 'paid', '2025-11-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (620, 1970, 1, 'rent', '2025-12', 9900.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (621, 1971, 1, 'rent', '2025-06', 10800.0, '2025-06-01', 'paid', '2025-06-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (621, 1971, 1, 'rent', '2025-11', 10800.0, '2025-11-01', 'paid', '2025-11-14', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (621, 1971, 1, 'rent', '2025-12', 10800.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (621, 1971, 1, 'rent', '2026-06', 10800.0, '2026-06-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (621, 1971, 1, 'rent', '2026-12', 10800.0, '2026-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (622, 1972, 1, 'rent', '2024-06', 9900.0, '2024-06-01', 'paid', '2024-06-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (622, 1972, 1, 'rent', '2024-12', 9900.0, '2024-12-01', 'paid', '2024-12-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (622, 1972, 1, 'rent', '2025-06', 9900.0, '2025-06-01', 'paid', '2025-06-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (622, 1972, 1, 'rent', '2025-11', 9900.0, '2025-11-01', 'paid', '2025-11-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (622, 1972, 1, 'rent', '2025-12', 9900.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (623, 1973, 1, 'rent', '2024-06', 9900.0, '2024-06-01', 'paid', '2024-06-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (623, 1973, 1, 'rent', '2024-12', 9900.0, '2024-12-01', 'paid', '2024-12-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (623, 1973, 1, 'rent', '2025-06', 9900.0, '2025-06-01', 'paid', '2025-06-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (623, 1973, 1, 'rent', '2025-11', 9900.0, '2025-11-01', 'paid', '2025-11-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (623, 1973, 1, 'rent', '2025-12', 9900.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (624, 1974, 1, 'rent', '2024-05', 12000.0, '2024-05-01', 'paid', '2024-05-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (624, 1974, 1, 'rent', '2024-06', 12000.0, '2024-06-01', 'paid', '2024-06-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (624, 1974, 1, 'rent', '2024-07', 12000.0, '2024-07-01', 'paid', '2024-07-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (624, 1974, 1, 'rent', '2024-08', 12000.0, '2024-08-01', 'paid', '2024-08-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (624, 1974, 1, 'rent', '2024-09', 12000.0, '2024-09-01', 'paid', '2024-09-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (624, 1974, 1, 'rent', '2024-10', 12000.0, '2024-10-01', 'paid', '2024-10-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (624, 1974, 1, 'rent', '2024-11', 12000.0, '2024-11-01', 'paid', '2024-11-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (624, 1974, 1, 'rent', '2024-12', 12000.0, '2024-12-01', 'paid', '2024-12-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (624, 1974, 1, 'rent', '2025-01', 12000.0, '2025-01-01', 'paid', '2025-01-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (624, 1974, 1, 'rent', '2025-02', 12000.0, '2025-02-01', 'paid', '2025-02-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (624, 1974, 1, 'rent', '2025-03', 12000.0, '2025-03-01', 'paid', '2025-03-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (624, 1974, 1, 'rent', '2025-04', 12000.0, '2025-04-01', 'paid', '2025-04-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (625, 1975, 1, 'rent', '2025-06', 10800.0, '2025-06-01', 'paid', '2025-06-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, overdue_days)
VALUES (625, 1975, 1, 'rent', '2025-11', 10800.0, '2025-11-01', 'overdue', 36);
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (625, 1975, 1, 'rent', '2025-12', 10800.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (625, 1975, 1, 'rent', '2026-06', 10800.0, '2026-06-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (625, 1975, 1, 'rent', '2026-12', 10800.0, '2026-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (626, 1976, 1, 'rent', '2024-06', 10140.0, '2024-06-01', 'paid', '2024-06-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (626, 1976, 1, 'rent', '2024-12', 10140.0, '2024-12-01', 'paid', '2024-12-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (626, 1976, 1, 'rent', '2025-06', 10140.0, '2025-06-01', 'paid', '2025-06-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, overdue_days)
VALUES (626, 1976, 1, 'rent', '2025-11', 10140.0, '2025-11-01', 'overdue', 36);
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (626, 1976, 1, 'rent', '2025-12', 10140.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (627, 1977, 1, 'rent', '2024-06', 10140.0, '2024-06-01', 'paid', '2024-06-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (627, 1977, 1, 'rent', '2024-12', 10140.0, '2024-12-01', 'paid', '2024-12-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (627, 1977, 1, 'rent', '2025-06', 10140.0, '2025-06-01', 'paid', '2025-06-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (627, 1977, 1, 'rent', '2025-11', 10140.0, '2025-11-01', 'paid', '2025-11-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (627, 1977, 1, 'rent', '2025-12', 10140.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (628, 1978, 1, 'rent', '2024-06', 10140.0, '2024-06-01', 'paid', '2024-06-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (628, 1978, 1, 'rent', '2024-12', 10140.0, '2024-12-01', 'paid', '2024-12-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (628, 1978, 1, 'rent', '2025-06', 10140.0, '2025-06-01', 'paid', '2025-06-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (628, 1978, 1, 'rent', '2025-11', 10140.0, '2025-11-01', 'paid', '2025-11-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (628, 1978, 1, 'rent', '2025-12', 10140.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (629, 1979, 1, 'rent', '2025-06', 10880.0, '2025-06-01', 'paid', '2025-06-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (629, 1979, 1, 'rent', '2025-07', 10880.0, '2025-07-01', 'paid', '2025-07-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (629, 1979, 1, 'rent', '2025-08', 10880.0, '2025-08-01', 'paid', '2025-08-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (629, 1979, 1, 'rent', '2025-09', 10880.0, '2025-09-01', 'paid', '2025-09-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (629, 1979, 1, 'rent', '2025-10', 10880.0, '2025-10-01', 'paid', '2025-10-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (629, 1979, 1, 'rent', '2025-11', 10880.0, '2025-11-01', 'paid', '2025-11-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (629, 1979, 1, 'rent', '2025-11', 10880.0, '2025-11-01', 'paid', '2025-11-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (629, 1979, 1, 'rent', '2025-12', 10880.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (629, 1979, 1, 'rent', '2026-01', 10880.0, '2026-01-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (629, 1979, 1, 'rent', '2026-02', 10880.0, '2026-02-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (630, 1980, 1, 'rent', '2024-12', 10140.0, '2024-12-01', 'paid', '2024-12-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (630, 1980, 1, 'rent', '2025-06', 10140.0, '2025-06-01', 'paid', '2025-06-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (630, 1980, 1, 'rent', '2025-11', 10140.0, '2025-11-01', 'paid', '2025-11-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (630, 1980, 1, 'rent', '2025-12', 10140.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (630, 1980, 1, 'rent', '2026-06', 10140.0, '2026-06-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (631, 1981, 1, 'rent', '2025-11', 12000.0, '2025-11-01', 'paid', '2025-11-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (631, 1981, 1, 'rent', '2025-12', 12000.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (631, 1981, 1, 'rent', '2026-06', 12000.0, '2026-06-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (631, 1981, 1, 'rent', '2026-12', 12000.0, '2026-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (632, 1982, 1, 'rent', '2024-12', 10140.0, '2024-12-01', 'paid', '2024-12-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (632, 1982, 1, 'rent', '2025-06', 10140.0, '2025-06-01', 'paid', '2025-06-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (632, 1982, 1, 'rent', '2025-11', 10140.0, '2025-11-01', 'paid', '2025-11-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (632, 1982, 1, 'rent', '2025-12', 10140.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (632, 1982, 1, 'rent', '2026-06', 10140.0, '2026-06-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (633, 1983, 1, 'rent', '2024-12', 10140.0, '2024-12-01', 'paid', '2024-12-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (633, 1983, 1, 'rent', '2025-06', 10140.0, '2025-06-01', 'paid', '2025-06-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (633, 1983, 1, 'rent', '2025-11', 10140.0, '2025-11-01', 'paid', '2025-11-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (633, 1983, 1, 'rent', '2025-12', 10140.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (633, 1983, 1, 'rent', '2026-06', 10140.0, '2026-06-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (634, 1984, 1, 'rent', '2024-12', 10140.0, '2024-12-01', 'paid', '2024-12-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (634, 1984, 1, 'rent', '2025-06', 10140.0, '2025-06-01', 'paid', '2025-06-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (634, 1984, 1, 'rent', '2025-11', 10140.0, '2025-11-01', 'paid', '2025-11-12', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (634, 1984, 1, 'rent', '2025-12', 10140.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (634, 1984, 1, 'rent', '2026-06', 10140.0, '2026-06-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (635, 1985, 1, 'rent', '2025-06', 1800.0, '2025-06-01', 'paid', '2025-06-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (635, 1985, 1, 'rent', '2025-07', 1800.0, '2025-07-01', 'paid', '2025-07-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (635, 1985, 1, 'rent', '2025-08', 1800.0, '2025-08-01', 'paid', '2025-08-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (635, 1985, 1, 'rent', '2025-09', 1800.0, '2025-09-01', 'paid', '2025-09-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (635, 1985, 1, 'rent', '2025-10', 1800.0, '2025-10-01', 'paid', '2025-10-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (635, 1985, 1, 'rent', '2025-11', 1800.0, '2025-11-01', 'paid', '2025-11-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, overdue_days)
VALUES (635, 1985, 1, 'rent', '2025-11', 1800.0, '2025-11-01', 'overdue', 36);
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (635, 1985, 1, 'rent', '2025-12', 1800.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (635, 1985, 1, 'rent', '2026-01', 1800.0, '2026-01-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (635, 1985, 1, 'rent', '2026-02', 1800.0, '2026-02-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (636, 1986, 1, 'rent', '2024-11', 21600.0, '2024-11-01', 'paid', '2024-11-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (636, 1986, 1, 'rent', '2025-11', 21600.0, '2025-11-01', 'paid', '2025-11-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (637, 1987, 1, 'rent', '2025-11', 40560.0, '2025-11-01', 'paid', '2025-11-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (637, 1987, 1, 'rent', '2025-12', 40560.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (638, 1988, 1, 'rent', '2024-12', 10140.0, '2024-12-01', 'paid', '2024-12-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (638, 1988, 1, 'rent', '2025-06', 10140.0, '2025-06-01', 'paid', '2025-06-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (638, 1988, 1, 'rent', '2025-11', 10140.0, '2025-11-01', 'paid', '2025-11-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (638, 1988, 1, 'rent', '2025-12', 10140.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (638, 1988, 1, 'rent', '2026-06', 10140.0, '2026-06-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (639, 1989, 1, 'rent', '2024-12', 10140.0, '2024-12-01', 'paid', '2024-12-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (639, 1989, 1, 'rent', '2025-06', 10140.0, '2025-06-01', 'paid', '2025-06-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (639, 1989, 1, 'rent', '2025-12', 10140.0, '2025-12-01', 'paid', '2025-12-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (640, 1990, 1, 'rent', '2025-06', 10800.0, '2025-06-01', 'paid', '2025-06-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (640, 1990, 1, 'rent', '2025-11', 10800.0, '2025-11-01', 'paid', '2025-11-12', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (640, 1990, 1, 'rent', '2025-12', 10800.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (640, 1990, 1, 'rent', '2026-06', 10800.0, '2026-06-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (640, 1990, 1, 'rent', '2026-12', 10800.0, '2026-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, overdue_days)
VALUES (641, 1991, 1, 'rent', '2025-11', 24000.0, '2025-11-01', 'overdue', 36);
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (641, 1991, 1, 'rent', '2025-12', 24000.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (642, 1992, 1, 'rent', '2025-06', 10800.0, '2025-06-01', 'paid', '2025-06-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (642, 1992, 1, 'rent', '2025-11', 10800.0, '2025-11-01', 'paid', '2025-11-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (642, 1992, 1, 'rent', '2025-12', 10800.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (642, 1992, 1, 'rent', '2026-06', 10800.0, '2026-06-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (642, 1992, 1, 'rent', '2026-12', 10800.0, '2026-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (643, 1993, 1, 'rent', '2025-06', 10800.0, '2025-06-01', 'paid', '2025-06-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (643, 1993, 1, 'rent', '2025-11', 10800.0, '2025-11-01', 'paid', '2025-11-12', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (643, 1993, 1, 'rent', '2025-12', 10800.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (643, 1993, 1, 'rent', '2026-06', 10800.0, '2026-06-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (643, 1993, 1, 'rent', '2026-12', 10800.0, '2026-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (644, 1994, 1, 'rent', '2025-06', 10800.0, '2025-06-01', 'paid', '2025-06-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (644, 1994, 1, 'rent', '2025-11', 10800.0, '2025-11-01', 'paid', '2025-11-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (644, 1994, 1, 'rent', '2025-12', 10800.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (644, 1994, 1, 'rent', '2026-06', 10800.0, '2026-06-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (644, 1994, 1, 'rent', '2026-12', 10800.0, '2026-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (645, 1995, 1, 'rent', '2025-11', 24000.0, '2025-11-01', 'paid', '2025-11-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (645, 1995, 1, 'rent', '2025-12', 24000.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (646, 1996, 1, 'rent', '2025-06', 10800.0, '2025-06-01', 'paid', '2025-06-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (646, 1996, 1, 'rent', '2025-11', 10800.0, '2025-11-01', 'paid', '2025-11-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (646, 1996, 1, 'rent', '2025-12', 10800.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (646, 1996, 1, 'rent', '2026-06', 10800.0, '2026-06-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (646, 1996, 1, 'rent', '2026-12', 10800.0, '2026-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, overdue_days)
VALUES (647, 1997, 1, 'rent', '2025-11', 24000.0, '2025-11-01', 'overdue', 36);
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (647, 1997, 1, 'rent', '2025-12', 24000.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (648, 1998, 1, 'rent', '2025-06', 10800.0, '2025-06-01', 'paid', '2025-06-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (648, 1998, 1, 'rent', '2025-11', 10800.0, '2025-11-01', 'paid', '2025-11-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (648, 1998, 1, 'rent', '2025-12', 10800.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (648, 1998, 1, 'rent', '2026-06', 10800.0, '2026-06-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (648, 1998, 1, 'rent', '2026-12', 10800.0, '2026-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (649, 1999, 1, 'rent', '2025-11', 24000.0, '2025-11-01', 'paid', '2025-11-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (649, 1999, 1, 'rent', '2025-12', 24000.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (546, 1896, 1, 'rent', '2025-06', 12000.0, '2025-06-01', 'paid', '2025-06-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (546, 1896, 1, 'rent', '2025-07', 12000.0, '2025-07-01', 'paid', '2025-07-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (546, 1896, 1, 'rent', '2025-08', 12000.0, '2025-08-01', 'paid', '2025-08-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (546, 1896, 1, 'rent', '2025-09', 12000.0, '2025-09-01', 'paid', '2025-09-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (546, 1896, 1, 'rent', '2025-10', 12000.0, '2025-10-01', 'paid', '2025-10-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (546, 1896, 1, 'rent', '2025-11', 12000.0, '2025-11-01', 'paid', '2025-11-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (546, 1896, 1, 'rent', '2025-11', 12000.0, '2025-11-01', 'paid', '2025-11-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (546, 1896, 1, 'rent', '2025-12', 12000.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (546, 1896, 1, 'rent', '2026-01', 12000.0, '2026-01-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (546, 1896, 1, 'rent', '2026-02', 12000.0, '2026-02-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (561, 1911, 1, 'rent', '2025-06', 1800.0, '2025-06-01', 'paid', '2025-06-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (561, 1911, 1, 'rent', '2025-07', 1800.0, '2025-07-01', 'paid', '2025-07-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (561, 1911, 1, 'rent', '2025-08', 1800.0, '2025-08-01', 'paid', '2025-08-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (561, 1911, 1, 'rent', '2025-09', 1800.0, '2025-09-01', 'paid', '2025-09-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (561, 1911, 1, 'rent', '2025-10', 1800.0, '2025-10-01', 'paid', '2025-10-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (561, 1911, 1, 'rent', '2025-11', 1800.0, '2025-11-01', 'paid', '2025-11-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, overdue_days)
VALUES (561, 1911, 1, 'rent', '2025-11', 1800.0, '2025-11-01', 'overdue', 36);
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (561, 1911, 1, 'rent', '2025-12', 1800.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (561, 1911, 1, 'rent', '2026-01', 1800.0, '2026-01-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (561, 1911, 1, 'rent', '2026-02', 1800.0, '2026-02-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (598, 1948, 1, 'rent', '2025-06', 1800.0, '2025-06-01', 'paid', '2025-06-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (598, 1948, 1, 'rent', '2025-07', 1800.0, '2025-07-01', 'paid', '2025-07-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (598, 1948, 1, 'rent', '2025-08', 1800.0, '2025-08-01', 'paid', '2025-08-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (598, 1948, 1, 'rent', '2025-09', 1800.0, '2025-09-01', 'paid', '2025-09-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (598, 1948, 1, 'rent', '2025-10', 1800.0, '2025-10-01', 'paid', '2025-10-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (598, 1948, 1, 'rent', '2025-11', 1800.0, '2025-11-01', 'paid', '2025-11-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (598, 1948, 1, 'rent', '2025-11', 1800.0, '2025-11-01', 'paid', '2025-11-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (598, 1948, 1, 'rent', '2025-12', 1800.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (598, 1948, 1, 'rent', '2026-01', 1800.0, '2026-01-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (598, 1948, 1, 'rent', '2026-02', 1800.0, '2026-02-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (613, 1963, 1, 'rent', '2025-11', 6000.0, '2025-11-01', 'paid', '2025-11-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (613, 1963, 1, 'rent', '2025-12', 6000.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (613, 1963, 1, 'rent', '2026-06', 6000.0, '2026-06-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (650, 2000, 1, 'rent', '2025-06', 10800.0, '2025-06-01', 'paid', '2025-06-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (650, 2000, 1, 'rent', '2025-11', 10800.0, '2025-11-01', 'paid', '2025-11-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (650, 2000, 1, 'rent', '2025-12', 10800.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (650, 2000, 1, 'rent', '2026-06', 10800.0, '2026-06-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (650, 2000, 1, 'rent', '2026-12', 10800.0, '2026-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (651, 2001, 1, 'rent', '2025-11', 24000.0, '2025-11-01', 'paid', '2025-11-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (651, 2001, 1, 'rent', '2025-12', 24000.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (652, 2002, 1, 'rent', '2025-11', 28800.0, '2025-11-01', 'paid', '2025-11-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (652, 2002, 1, 'rent', '2025-12', 28800.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, overdue_days)
VALUES (797, 2003, 1, 'rent', '2025-11', 28800.0, '2025-11-01', 'overdue', 36);
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (797, 2003, 1, 'rent', '2025-12', 28800.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (654, 2004, 1, 'rent', '2025-11', 28800.0, '2025-11-01', 'paid', '2025-11-12', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (654, 2004, 1, 'rent', '2025-12', 28800.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (655, 2005, 1, 'rent', '2025-11', 10140.0, '2025-11-01', 'paid', '2025-11-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (655, 2005, 1, 'rent', '2025-12', 10140.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (655, 2005, 1, 'rent', '2026-06', 10140.0, '2026-06-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (655, 2005, 1, 'rent', '2026-12', 10140.0, '2026-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (656, 2006, 1, 'rent', '2025-11', 19080.0, '2025-11-01', 'paid', '2025-11-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (656, 2006, 1, 'rent', '2025-12', 19080.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (657, 2007, 1, 'rent', '2025-11', 24000.0, '2025-11-01', 'paid', '2025-11-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (657, 2007, 1, 'rent', '2025-12', 24000.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (658, 2008, 1, 'rent', '2025-11', 21600.0, '2025-11-01', 'paid', '2025-11-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (658, 2008, 1, 'rent', '2025-12', 21600.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (659, 2009, 1, 'rent', '2025-09', 3000.0, '2025-09-01', 'paid', '2025-09-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (659, 2009, 1, 'rent', '2025-10', 3000.0, '2025-10-01', 'paid', '2025-10-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (659, 2009, 1, 'rent', '2025-11', 3000.0, '2025-11-01', 'paid', '2025-11-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, overdue_days)
VALUES (659, 2009, 1, 'rent', '2025-11', 3000.0, '2025-11-01', 'overdue', 36);
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (659, 2009, 1, 'rent', '2025-12', 3000.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (659, 2009, 1, 'rent', '2026-01', 3000.0, '2026-01-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (659, 2009, 1, 'rent', '2026-02', 3000.0, '2026-02-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, overdue_days)
VALUES (660, 2010, 1, 'rent', '2025-11', 21600.0, '2025-11-01', 'overdue', 36);
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (660, 2010, 1, 'rent', '2025-12', 21600.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (661, 2011, 1, 'rent', '2025-11', 24000.0, '2025-11-01', 'paid', '2025-11-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (661, 2011, 1, 'rent', '2025-12', 24000.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (662, 2012, 1, 'rent', '2025-11', 21600.0, '2025-11-01', 'paid', '2025-11-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (662, 2012, 1, 'rent', '2025-12', 21600.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (663, 2013, 1, 'rent', '2025-12', 148200.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (664, 2014, 1, 'rent', '2025-11', 3000.0, '2025-11-01', 'paid', '2025-11-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (664, 2014, 1, 'rent', '2025-11', 3000.0, '2025-11-01', 'paid', '2025-11-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (664, 2014, 1, 'rent', '2025-12', 3000.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (664, 2014, 1, 'rent', '2026-01', 3000.0, '2026-01-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (664, 2014, 1, 'rent', '2026-02', 3000.0, '2026-02-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (665, 2015, 2, 'rent', '2025-11', 24000.0, '2025-11-01', 'paid', '2025-11-13', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (665, 2015, 2, 'rent', '2025-12', 24000.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, overdue_days)
VALUES (666, 2016, 2, 'rent', '2025-11', 24000.0, '2025-11-01', 'overdue', 36);
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (666, 2016, 2, 'rent', '2025-12', 24000.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (667, 2017, 2, 'rent', '2025-06', 10800.0, '2025-06-01', 'paid', '2025-06-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, overdue_days)
VALUES (667, 2017, 2, 'rent', '2025-11', 10800.0, '2025-11-01', 'overdue', 36);
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (667, 2017, 2, 'rent', '2025-12', 10800.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (667, 2017, 2, 'rent', '2026-06', 10800.0, '2026-06-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (667, 2017, 2, 'rent', '2026-12', 10800.0, '2026-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (668, 2018, 2, 'rent', '2025-06', 10800.0, '2025-06-01', 'paid', '2025-06-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (668, 2018, 2, 'rent', '2025-11', 10800.0, '2025-11-01', 'paid', '2025-11-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (668, 2018, 2, 'rent', '2025-12', 10800.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (668, 2018, 2, 'rent', '2026-06', 10800.0, '2026-06-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (668, 2018, 2, 'rent', '2026-12', 10800.0, '2026-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (669, 2019, 2, 'rent', '2025-11', 28800.0, '2025-11-01', 'paid', '2025-11-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (669, 2019, 2, 'rent', '2025-12', 28800.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, overdue_days)
VALUES (670, 2020, 2, 'rent', '2025-11', 24000.0, '2025-11-01', 'overdue', 36);
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (670, 2020, 2, 'rent', '2025-12', 24000.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (671, 2021, 2, 'rent', '2025-06', 1000.0, '2025-06-01', 'paid', '2025-06-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (671, 2021, 2, 'rent', '2025-07', 1000.0, '2025-07-01', 'paid', '2025-07-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (671, 2021, 2, 'rent', '2025-08', 1000.0, '2025-08-01', 'paid', '2025-08-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (671, 2021, 2, 'rent', '2025-09', 1000.0, '2025-09-01', 'paid', '2025-09-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (671, 2021, 2, 'rent', '2025-10', 1000.0, '2025-10-01', 'paid', '2025-10-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (671, 2021, 2, 'rent', '2025-11', 1000.0, '2025-11-01', 'paid', '2025-11-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (671, 2021, 2, 'rent', '2025-11', 1000.0, '2025-11-01', 'paid', '2025-11-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (671, 2021, 2, 'rent', '2025-12', 1000.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (671, 2021, 2, 'rent', '2026-01', 1000.0, '2026-01-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (671, 2021, 2, 'rent', '2026-02', 1000.0, '2026-02-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (672, 2022, 2, 'rent', '2025-06', 1000.0, '2025-06-01', 'paid', '2025-06-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (672, 2022, 2, 'rent', '2025-07', 1000.0, '2025-07-01', 'paid', '2025-07-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (672, 2022, 2, 'rent', '2025-08', 1000.0, '2025-08-01', 'paid', '2025-08-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (672, 2022, 2, 'rent', '2025-09', 1000.0, '2025-09-01', 'paid', '2025-09-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (672, 2022, 2, 'rent', '2025-10', 1000.0, '2025-10-01', 'paid', '2025-10-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (672, 2022, 2, 'rent', '2025-11', 1000.0, '2025-11-01', 'paid', '2025-11-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, overdue_days)
VALUES (672, 2022, 2, 'rent', '2025-11', 1000.0, '2025-11-01', 'overdue', 36);
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (672, 2022, 2, 'rent', '2025-12', 1000.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (672, 2022, 2, 'rent', '2026-01', 1000.0, '2026-01-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (672, 2022, 2, 'rent', '2026-02', 1000.0, '2026-02-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (673, 2023, 2, 'rent', '2025-06', 1000.0, '2025-06-01', 'paid', '2025-06-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (673, 2023, 2, 'rent', '2025-07', 1000.0, '2025-07-01', 'paid', '2025-07-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (673, 2023, 2, 'rent', '2025-08', 1000.0, '2025-08-01', 'paid', '2025-08-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (673, 2023, 2, 'rent', '2025-09', 1000.0, '2025-09-01', 'paid', '2025-09-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (673, 2023, 2, 'rent', '2025-10', 1000.0, '2025-10-01', 'paid', '2025-10-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (673, 2023, 2, 'rent', '2025-11', 1000.0, '2025-11-01', 'paid', '2025-11-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (673, 2023, 2, 'rent', '2025-11', 1000.0, '2025-11-01', 'paid', '2025-11-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (673, 2023, 2, 'rent', '2025-12', 1000.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (673, 2023, 2, 'rent', '2026-01', 1000.0, '2026-01-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (673, 2023, 2, 'rent', '2026-02', 1000.0, '2026-02-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (674, 2024, 2, 'rent', '2025-11', 24000.0, '2025-11-01', 'paid', '2025-11-13', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (674, 2024, 2, 'rent', '2025-12', 24000.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (675, 2025, 2, 'rent', '2025-11', 28800.0, '2025-11-01', 'paid', '2025-11-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (675, 2025, 2, 'rent', '2025-12', 28800.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, overdue_days)
VALUES (676, 2026, 2, 'rent', '2025-11', 19080.0, '2025-11-01', 'overdue', 36);
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (676, 2026, 2, 'rent', '2025-12', 19080.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, overdue_days)
VALUES (677, 2027, 2, 'rent', '2025-11', 10140.0, '2025-11-01', 'overdue', 36);
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (677, 2027, 2, 'rent', '2025-12', 10140.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (677, 2027, 2, 'rent', '2026-06', 10140.0, '2026-06-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (677, 2027, 2, 'rent', '2026-12', 10140.0, '2026-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (678, 2028, 2, 'rent', '2025-11', 28800.0, '2025-11-01', 'paid', '2025-11-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (678, 2028, 2, 'rent', '2025-12', 28800.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (679, 2029, 2, 'rent', '2025-11', 19080.0, '2025-11-01', 'paid', '2025-11-15', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (679, 2029, 2, 'rent', '2025-12', 19080.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (680, 2030, 2, 'rent', '2025-11', 19080.0, '2025-11-01', 'paid', '2025-11-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (680, 2030, 2, 'rent', '2025-12', 19080.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, overdue_days)
VALUES (681, 2031, 2, 'rent', '2025-11', 40560.0, '2025-11-01', 'overdue', 36);
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (681, 2031, 2, 'rent', '2025-12', 40560.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (682, 2032, 2, 'rent', '2025-11', 20280.0, '2025-11-01', 'paid', '2025-11-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (682, 2032, 2, 'rent', '2025-12', 20280.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, overdue_days)
VALUES (683, 2033, 2, 'rent', '2025-11', 19080.0, '2025-11-01', 'overdue', 36);
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (683, 2033, 2, 'rent', '2025-12', 19080.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (684, 2034, 2, 'rent', '2025-11', 21600.0, '2025-11-01', 'paid', '2025-11-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (684, 2034, 2, 'rent', '2025-12', 21600.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (685, 2035, 2, 'rent', '2025-11', 21600.0, '2025-11-01', 'paid', '2025-11-16', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (685, 2035, 2, 'rent', '2025-12', 21600.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (686, 2036, 2, 'rent', '2025-11', 21600.0, '2025-11-01', 'paid', '2025-11-15', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (686, 2036, 2, 'rent', '2025-12', 21600.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, overdue_days)
VALUES (687, 2037, 2, 'rent', '2025-11', 21600.0, '2025-11-01', 'overdue', 36);
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (687, 2037, 2, 'rent', '2025-12', 21600.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (688, 2038, 2, 'rent', '2025-11', 21600.0, '2025-11-01', 'paid', '2025-11-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (688, 2038, 2, 'rent', '2025-12', 21600.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (689, 2039, 2, 'rent', '2025-12', 21600.0, '2025-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES (689, 2039, 2, 'rent', '2026-12', 21600.0, '2026-12-01', 'pending');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (837, 2187, 1, 'rent', '2020-01', 110.0, '2020-01-01', 'paid', '2020-01-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (837, 2187, 1, 'rent', '2020-02', 110.0, '2020-02-01', 'paid', '2020-02-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (837, 2187, 1, 'rent', '2020-03', 110.0, '2020-03-01', 'paid', '2020-03-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (837, 2187, 1, 'rent', '2020-04', 110.0, '2020-04-01', 'paid', '2020-04-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (837, 2187, 1, 'rent', '2020-05', 110.0, '2020-05-01', 'paid', '2020-05-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (837, 2187, 1, 'rent', '2020-06', 110.0, '2020-06-01', 'paid', '2020-06-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (837, 2187, 1, 'rent', '2020-07', 110.0, '2020-07-01', 'paid', '2020-07-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (837, 2187, 1, 'rent', '2020-08', 110.0, '2020-08-01', 'paid', '2020-08-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (837, 2187, 1, 'rent', '2020-09', 110.0, '2020-09-01', 'paid', '2020-09-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (837, 2187, 1, 'rent', '2020-10', 110.0, '2020-10-01', 'paid', '2020-10-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (837, 2187, 1, 'rent', '2020-11', 110.0, '2020-11-01', 'paid', '2020-11-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (837, 2187, 1, 'rent', '2020-12', 110.0, '2020-12-01', 'paid', '2020-12-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (838, 2189, 1, 'rent', '2020-01', 110.0, '2020-01-01', 'paid', '2020-01-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (838, 2189, 1, 'rent', '2020-02', 110.0, '2020-02-01', 'paid', '2020-02-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (838, 2189, 1, 'rent', '2020-03', 110.0, '2020-03-01', 'paid', '2020-03-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (838, 2189, 1, 'rent', '2020-04', 110.0, '2020-04-01', 'paid', '2020-04-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (838, 2189, 1, 'rent', '2020-05', 110.0, '2020-05-01', 'paid', '2020-05-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (838, 2189, 1, 'rent', '2020-06', 110.0, '2020-06-01', 'paid', '2020-06-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (838, 2189, 1, 'rent', '2020-07', 110.0, '2020-07-01', 'paid', '2020-07-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (838, 2189, 1, 'rent', '2020-08', 110.0, '2020-08-01', 'paid', '2020-08-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (838, 2189, 1, 'rent', '2020-09', 110.0, '2020-09-01', 'paid', '2020-09-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (838, 2189, 1, 'rent', '2020-10', 110.0, '2020-10-01', 'paid', '2020-10-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (838, 2189, 1, 'rent', '2020-11', 110.0, '2020-11-01', 'paid', '2020-11-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (838, 2189, 1, 'rent', '2020-12', 110.0, '2020-12-01', 'paid', '2020-12-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (839, 2191, 1, 'rent', '2020-01', 110.0, '2020-01-01', 'paid', '2020-01-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (839, 2191, 1, 'rent', '2020-02', 110.0, '2020-02-01', 'paid', '2020-02-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (839, 2191, 1, 'rent', '2020-03', 110.0, '2020-03-01', 'paid', '2020-03-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (839, 2191, 1, 'rent', '2020-04', 110.0, '2020-04-01', 'paid', '2020-04-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (839, 2191, 1, 'rent', '2020-05', 110.0, '2020-05-01', 'paid', '2020-05-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (839, 2191, 1, 'rent', '2020-06', 110.0, '2020-06-01', 'paid', '2020-06-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (839, 2191, 1, 'rent', '2020-07', 110.0, '2020-07-01', 'paid', '2020-07-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (839, 2191, 1, 'rent', '2020-08', 110.0, '2020-08-01', 'paid', '2020-08-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (839, 2191, 1, 'rent', '2020-09', 110.0, '2020-09-01', 'paid', '2020-09-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (839, 2191, 1, 'rent', '2020-10', 110.0, '2020-10-01', 'paid', '2020-10-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (839, 2191, 1, 'rent', '2020-11', 110.0, '2020-11-01', 'paid', '2020-11-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (839, 2191, 1, 'rent', '2020-12', 110.0, '2020-12-01', 'paid', '2020-12-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (840, 2192, 1, 'rent', '2020-01', 110.0, '2020-01-01', 'paid', '2020-01-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (840, 2192, 1, 'rent', '2020-02', 110.0, '2020-02-01', 'paid', '2020-02-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (840, 2192, 1, 'rent', '2020-03', 110.0, '2020-03-01', 'paid', '2020-03-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (840, 2192, 1, 'rent', '2020-04', 110.0, '2020-04-01', 'paid', '2020-04-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (840, 2192, 1, 'rent', '2020-05', 110.0, '2020-05-01', 'paid', '2020-05-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (840, 2192, 1, 'rent', '2020-06', 110.0, '2020-06-01', 'paid', '2020-06-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (840, 2192, 1, 'rent', '2020-07', 110.0, '2020-07-01', 'paid', '2020-07-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (840, 2192, 1, 'rent', '2020-08', 110.0, '2020-08-01', 'paid', '2020-08-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (840, 2192, 1, 'rent', '2020-09', 110.0, '2020-09-01', 'paid', '2020-09-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (840, 2192, 1, 'rent', '2020-10', 110.0, '2020-10-01', 'paid', '2020-10-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (840, 2192, 1, 'rent', '2020-11', 110.0, '2020-11-01', 'paid', '2020-11-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (840, 2192, 1, 'rent', '2020-12', 110.0, '2020-12-01', 'paid', '2020-12-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (841, 2193, 1, 'rent', '2020-01', 110.0, '2020-01-01', 'paid', '2020-01-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (841, 2193, 1, 'rent', '2020-02', 110.0, '2020-02-01', 'paid', '2020-02-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (841, 2193, 1, 'rent', '2020-03', 110.0, '2020-03-01', 'paid', '2020-03-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (841, 2193, 1, 'rent', '2020-04', 110.0, '2020-04-01', 'paid', '2020-04-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (841, 2193, 1, 'rent', '2020-05', 110.0, '2020-05-01', 'paid', '2020-05-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (841, 2193, 1, 'rent', '2020-06', 110.0, '2020-06-01', 'paid', '2020-06-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (841, 2193, 1, 'rent', '2020-07', 110.0, '2020-07-01', 'paid', '2020-07-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (841, 2193, 1, 'rent', '2020-08', 110.0, '2020-08-01', 'paid', '2020-08-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (841, 2193, 1, 'rent', '2020-09', 110.0, '2020-09-01', 'paid', '2020-09-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (841, 2193, 1, 'rent', '2020-10', 110.0, '2020-10-01', 'paid', '2020-10-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (841, 2193, 1, 'rent', '2020-11', 110.0, '2020-11-01', 'paid', '2020-11-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (841, 2193, 1, 'rent', '2020-12', 110.0, '2020-12-01', 'paid', '2020-12-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (842, 2194, 1, 'rent', '2020-01', 109.0, '2020-01-01', 'paid', '2020-01-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (842, 2194, 1, 'rent', '2020-02', 109.0, '2020-02-01', 'paid', '2020-02-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (842, 2194, 1, 'rent', '2020-03', 109.0, '2020-03-01', 'paid', '2020-03-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (842, 2194, 1, 'rent', '2020-04', 109.0, '2020-04-01', 'paid', '2020-04-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (842, 2194, 1, 'rent', '2020-05', 109.0, '2020-05-01', 'paid', '2020-05-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (842, 2194, 1, 'rent', '2020-06', 109.0, '2020-06-01', 'paid', '2020-06-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (842, 2194, 1, 'rent', '2020-07', 109.0, '2020-07-01', 'paid', '2020-07-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (842, 2194, 1, 'rent', '2020-08', 109.0, '2020-08-01', 'paid', '2020-08-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (842, 2194, 1, 'rent', '2020-09', 109.0, '2020-09-01', 'paid', '2020-09-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (842, 2194, 1, 'rent', '2020-10', 109.0, '2020-10-01', 'paid', '2020-10-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (842, 2194, 1, 'rent', '2020-11', 109.0, '2020-11-01', 'paid', '2020-11-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (842, 2194, 1, 'rent', '2020-12', 109.0, '2020-12-01', 'paid', '2020-12-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (843, 2196, 1, 'rent', '2020-01', 110.0, '2020-01-01', 'paid', '2020-01-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (843, 2196, 1, 'rent', '2020-02', 110.0, '2020-02-01', 'paid', '2020-02-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (843, 2196, 1, 'rent', '2020-03', 110.0, '2020-03-01', 'paid', '2020-03-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (843, 2196, 1, 'rent', '2020-04', 110.0, '2020-04-01', 'paid', '2020-04-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (843, 2196, 1, 'rent', '2020-05', 110.0, '2020-05-01', 'paid', '2020-05-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (843, 2196, 1, 'rent', '2020-06', 110.0, '2020-06-01', 'paid', '2020-06-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (843, 2196, 1, 'rent', '2020-07', 110.0, '2020-07-01', 'paid', '2020-07-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (843, 2196, 1, 'rent', '2020-08', 110.0, '2020-08-01', 'paid', '2020-08-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (843, 2196, 1, 'rent', '2020-09', 110.0, '2020-09-01', 'paid', '2020-09-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (843, 2196, 1, 'rent', '2020-10', 110.0, '2020-10-01', 'paid', '2020-10-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (843, 2196, 1, 'rent', '2020-11', 110.0, '2020-11-01', 'paid', '2020-11-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (843, 2196, 1, 'rent', '2020-12', 110.0, '2020-12-01', 'paid', '2020-12-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (844, 2197, 1, 'rent', '2020-01', 110.0, '2020-01-01', 'paid', '2020-01-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (844, 2197, 1, 'rent', '2020-02', 110.0, '2020-02-01', 'paid', '2020-02-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (844, 2197, 1, 'rent', '2020-03', 110.0, '2020-03-01', 'paid', '2020-03-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (844, 2197, 1, 'rent', '2020-04', 110.0, '2020-04-01', 'paid', '2020-04-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (844, 2197, 1, 'rent', '2020-05', 110.0, '2020-05-01', 'paid', '2020-05-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (844, 2197, 1, 'rent', '2020-06', 110.0, '2020-06-01', 'paid', '2020-06-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (844, 2197, 1, 'rent', '2020-07', 110.0, '2020-07-01', 'paid', '2020-07-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (844, 2197, 1, 'rent', '2020-08', 110.0, '2020-08-01', 'paid', '2020-08-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (844, 2197, 1, 'rent', '2020-09', 110.0, '2020-09-01', 'paid', '2020-09-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (844, 2197, 1, 'rent', '2020-10', 110.0, '2020-10-01', 'paid', '2020-10-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (844, 2197, 1, 'rent', '2020-11', 110.0, '2020-11-01', 'paid', '2020-11-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (844, 2197, 1, 'rent', '2020-12', 110.0, '2020-12-01', 'paid', '2020-12-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (845, 2198, 1, 'rent', '2020-01', 110.0, '2020-01-01', 'paid', '2020-01-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (845, 2198, 1, 'rent', '2020-02', 110.0, '2020-02-01', 'paid', '2020-02-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (845, 2198, 1, 'rent', '2020-03', 110.0, '2020-03-01', 'paid', '2020-03-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (845, 2198, 1, 'rent', '2020-04', 110.0, '2020-04-01', 'paid', '2020-04-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (845, 2198, 1, 'rent', '2020-05', 110.0, '2020-05-01', 'paid', '2020-05-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (845, 2198, 1, 'rent', '2020-06', 110.0, '2020-06-01', 'paid', '2020-06-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (845, 2198, 1, 'rent', '2020-07', 110.0, '2020-07-01', 'paid', '2020-07-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (845, 2198, 1, 'rent', '2020-08', 110.0, '2020-08-01', 'paid', '2020-08-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (845, 2198, 1, 'rent', '2020-09', 110.0, '2020-09-01', 'paid', '2020-09-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (845, 2198, 1, 'rent', '2020-10', 110.0, '2020-10-01', 'paid', '2020-10-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (845, 2198, 1, 'rent', '2020-11', 110.0, '2020-11-01', 'paid', '2020-11-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (845, 2198, 1, 'rent', '2020-12', 110.0, '2020-12-01', 'paid', '2020-12-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (846, 2199, 1, 'rent', '2020-01', 10912.0, '2020-01-01', 'paid', '2020-01-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (846, 2199, 1, 'rent', '2020-02', 10912.0, '2020-02-01', 'paid', '2020-02-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (846, 2199, 1, 'rent', '2020-03', 10912.0, '2020-03-01', 'paid', '2020-03-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (846, 2199, 1, 'rent', '2020-04', 10912.0, '2020-04-01', 'paid', '2020-04-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (846, 2199, 1, 'rent', '2020-05', 10912.0, '2020-05-01', 'paid', '2020-05-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (846, 2199, 1, 'rent', '2020-06', 10912.0, '2020-06-01', 'paid', '2020-06-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (846, 2199, 1, 'rent', '2020-07', 10912.0, '2020-07-01', 'paid', '2020-07-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (846, 2199, 1, 'rent', '2020-08', 10912.0, '2020-08-01', 'paid', '2020-08-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (846, 2199, 1, 'rent', '2020-09', 10912.0, '2020-09-01', 'paid', '2020-09-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (846, 2199, 1, 'rent', '2020-10', 10912.0, '2020-10-01', 'paid', '2020-10-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (846, 2199, 1, 'rent', '2020-11', 10912.0, '2020-11-01', 'paid', '2020-11-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (846, 2199, 1, 'rent', '2020-12', 10912.0, '2020-12-01', 'paid', '2020-12-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (847, 2200, 1, 'rent', '2020-01', 110.0, '2020-01-01', 'paid', '2020-01-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (847, 2200, 1, 'rent', '2020-02', 110.0, '2020-02-01', 'paid', '2020-02-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (847, 2200, 1, 'rent', '2020-03', 110.0, '2020-03-01', 'paid', '2020-03-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (847, 2200, 1, 'rent', '2020-04', 110.0, '2020-04-01', 'paid', '2020-04-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (847, 2200, 1, 'rent', '2020-05', 110.0, '2020-05-01', 'paid', '2020-05-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (847, 2200, 1, 'rent', '2020-06', 110.0, '2020-06-01', 'paid', '2020-06-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (847, 2200, 1, 'rent', '2020-07', 110.0, '2020-07-01', 'paid', '2020-07-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (847, 2200, 1, 'rent', '2020-08', 110.0, '2020-08-01', 'paid', '2020-08-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (847, 2200, 1, 'rent', '2020-09', 110.0, '2020-09-01', 'paid', '2020-09-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (847, 2200, 1, 'rent', '2020-10', 110.0, '2020-10-01', 'paid', '2020-10-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (847, 2200, 1, 'rent', '2020-11', 110.0, '2020-11-01', 'paid', '2020-11-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (847, 2200, 1, 'rent', '2020-12', 110.0, '2020-12-01', 'paid', '2020-12-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (848, 2201, 1, 'rent', '2020-01', 110.0, '2020-01-01', 'paid', '2020-01-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (848, 2201, 1, 'rent', '2020-02', 110.0, '2020-02-01', 'paid', '2020-02-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (848, 2201, 1, 'rent', '2020-03', 110.0, '2020-03-01', 'paid', '2020-03-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (848, 2201, 1, 'rent', '2020-04', 110.0, '2020-04-01', 'paid', '2020-04-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (848, 2201, 1, 'rent', '2020-05', 110.0, '2020-05-01', 'paid', '2020-05-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (848, 2201, 1, 'rent', '2020-06', 110.0, '2020-06-01', 'paid', '2020-06-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (848, 2201, 1, 'rent', '2020-07', 110.0, '2020-07-01', 'paid', '2020-07-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (848, 2201, 1, 'rent', '2020-08', 110.0, '2020-08-01', 'paid', '2020-08-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (848, 2201, 1, 'rent', '2020-09', 110.0, '2020-09-01', 'paid', '2020-09-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (848, 2201, 1, 'rent', '2020-10', 110.0, '2020-10-01', 'paid', '2020-10-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (848, 2201, 1, 'rent', '2020-11', 110.0, '2020-11-01', 'paid', '2020-11-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (848, 2201, 1, 'rent', '2020-12', 110.0, '2020-12-01', 'paid', '2020-12-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (849, 2202, 1, 'rent', '2020-01', 110.0, '2020-01-01', 'paid', '2020-01-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (849, 2202, 1, 'rent', '2020-02', 110.0, '2020-02-01', 'paid', '2020-02-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (849, 2202, 1, 'rent', '2020-03', 110.0, '2020-03-01', 'paid', '2020-03-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (849, 2202, 1, 'rent', '2020-04', 110.0, '2020-04-01', 'paid', '2020-04-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (849, 2202, 1, 'rent', '2020-05', 110.0, '2020-05-01', 'paid', '2020-05-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (849, 2202, 1, 'rent', '2020-06', 110.0, '2020-06-01', 'paid', '2020-06-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (849, 2202, 1, 'rent', '2020-07', 110.0, '2020-07-01', 'paid', '2020-07-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (849, 2202, 1, 'rent', '2020-08', 110.0, '2020-08-01', 'paid', '2020-08-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (849, 2202, 1, 'rent', '2020-09', 110.0, '2020-09-01', 'paid', '2020-09-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (849, 2202, 1, 'rent', '2020-10', 110.0, '2020-10-01', 'paid', '2020-10-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (849, 2202, 1, 'rent', '2020-11', 110.0, '2020-11-01', 'paid', '2020-11-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (849, 2202, 1, 'rent', '2020-12', 110.0, '2020-12-01', 'paid', '2020-12-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (850, 2203, 1, 'rent', '2020-01', 110.0, '2020-01-01', 'paid', '2020-01-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (850, 2203, 1, 'rent', '2020-02', 110.0, '2020-02-01', 'paid', '2020-02-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (850, 2203, 1, 'rent', '2020-03', 110.0, '2020-03-01', 'paid', '2020-03-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (850, 2203, 1, 'rent', '2020-04', 110.0, '2020-04-01', 'paid', '2020-04-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (850, 2203, 1, 'rent', '2020-05', 110.0, '2020-05-01', 'paid', '2020-05-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (850, 2203, 1, 'rent', '2020-06', 110.0, '2020-06-01', 'paid', '2020-06-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (850, 2203, 1, 'rent', '2020-07', 110.0, '2020-07-01', 'paid', '2020-07-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (850, 2203, 1, 'rent', '2020-08', 110.0, '2020-08-01', 'paid', '2020-08-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (850, 2203, 1, 'rent', '2020-09', 110.0, '2020-09-01', 'paid', '2020-09-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (850, 2203, 1, 'rent', '2020-10', 110.0, '2020-10-01', 'paid', '2020-10-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (850, 2203, 1, 'rent', '2020-11', 110.0, '2020-11-01', 'paid', '2020-11-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (850, 2203, 1, 'rent', '2020-12', 110.0, '2020-12-01', 'paid', '2020-12-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (851, 2204, 1, 'rent', '2020-01', 110.0, '2020-01-01', 'paid', '2020-01-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (851, 2204, 1, 'rent', '2020-02', 110.0, '2020-02-01', 'paid', '2020-02-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (851, 2204, 1, 'rent', '2020-03', 110.0, '2020-03-01', 'paid', '2020-03-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (851, 2204, 1, 'rent', '2020-04', 110.0, '2020-04-01', 'paid', '2020-04-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (851, 2204, 1, 'rent', '2020-05', 110.0, '2020-05-01', 'paid', '2020-05-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (851, 2204, 1, 'rent', '2020-06', 110.0, '2020-06-01', 'paid', '2020-06-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (851, 2204, 1, 'rent', '2020-07', 110.0, '2020-07-01', 'paid', '2020-07-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (851, 2204, 1, 'rent', '2020-08', 110.0, '2020-08-01', 'paid', '2020-08-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (851, 2204, 1, 'rent', '2020-09', 110.0, '2020-09-01', 'paid', '2020-09-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (851, 2204, 1, 'rent', '2020-10', 110.0, '2020-10-01', 'paid', '2020-10-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (851, 2204, 1, 'rent', '2020-11', 110.0, '2020-11-01', 'paid', '2020-11-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (851, 2204, 1, 'rent', '2020-12', 110.0, '2020-12-01', 'paid', '2020-12-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (853, 2206, 1, 'rent', '2020-01', 110.0, '2020-01-01', 'paid', '2020-01-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (853, 2206, 1, 'rent', '2020-02', 110.0, '2020-02-01', 'paid', '2020-02-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (853, 2206, 1, 'rent', '2020-03', 110.0, '2020-03-01', 'paid', '2020-03-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (853, 2206, 1, 'rent', '2020-04', 110.0, '2020-04-01', 'paid', '2020-04-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (853, 2206, 1, 'rent', '2020-05', 110.0, '2020-05-01', 'paid', '2020-05-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (853, 2206, 1, 'rent', '2020-06', 110.0, '2020-06-01', 'paid', '2020-06-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (853, 2206, 1, 'rent', '2020-07', 110.0, '2020-07-01', 'paid', '2020-07-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (853, 2206, 1, 'rent', '2020-08', 110.0, '2020-08-01', 'paid', '2020-08-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (853, 2206, 1, 'rent', '2020-09', 110.0, '2020-09-01', 'paid', '2020-09-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (853, 2206, 1, 'rent', '2020-10', 110.0, '2020-10-01', 'paid', '2020-10-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (853, 2206, 1, 'rent', '2020-11', 110.0, '2020-11-01', 'paid', '2020-11-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (853, 2206, 1, 'rent', '2020-12', 110.0, '2020-12-01', 'paid', '2020-12-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (854, 2207, 1, 'rent', '2020-01', 110.0, '2020-01-01', 'paid', '2020-01-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (854, 2207, 1, 'rent', '2020-02', 110.0, '2020-02-01', 'paid', '2020-02-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (854, 2207, 1, 'rent', '2020-03', 110.0, '2020-03-01', 'paid', '2020-03-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (854, 2207, 1, 'rent', '2020-04', 110.0, '2020-04-01', 'paid', '2020-04-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (854, 2207, 1, 'rent', '2020-05', 110.0, '2020-05-01', 'paid', '2020-05-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (854, 2207, 1, 'rent', '2020-06', 110.0, '2020-06-01', 'paid', '2020-06-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (854, 2207, 1, 'rent', '2020-07', 110.0, '2020-07-01', 'paid', '2020-07-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (854, 2207, 1, 'rent', '2020-08', 110.0, '2020-08-01', 'paid', '2020-08-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (854, 2207, 1, 'rent', '2020-09', 110.0, '2020-09-01', 'paid', '2020-09-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (854, 2207, 1, 'rent', '2020-10', 110.0, '2020-10-01', 'paid', '2020-10-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (854, 2207, 1, 'rent', '2020-11', 110.0, '2020-11-01', 'paid', '2020-11-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (854, 2207, 1, 'rent', '2020-12', 110.0, '2020-12-01', 'paid', '2020-12-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (855, 2208, 1, 'rent', '2020-01', 110.0, '2020-01-01', 'paid', '2020-01-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (855, 2208, 1, 'rent', '2020-02', 110.0, '2020-02-01', 'paid', '2020-02-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (855, 2208, 1, 'rent', '2020-03', 110.0, '2020-03-01', 'paid', '2020-03-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (855, 2208, 1, 'rent', '2020-04', 110.0, '2020-04-01', 'paid', '2020-04-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (855, 2208, 1, 'rent', '2020-05', 110.0, '2020-05-01', 'paid', '2020-05-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (855, 2208, 1, 'rent', '2020-06', 110.0, '2020-06-01', 'paid', '2020-06-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (855, 2208, 1, 'rent', '2020-07', 110.0, '2020-07-01', 'paid', '2020-07-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (855, 2208, 1, 'rent', '2020-08', 110.0, '2020-08-01', 'paid', '2020-08-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (855, 2208, 1, 'rent', '2020-09', 110.0, '2020-09-01', 'paid', '2020-09-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (855, 2208, 1, 'rent', '2020-10', 110.0, '2020-10-01', 'paid', '2020-10-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (855, 2208, 1, 'rent', '2020-11', 110.0, '2020-11-01', 'paid', '2020-11-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (855, 2208, 1, 'rent', '2020-12', 110.0, '2020-12-01', 'paid', '2020-12-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (856, 2209, 1, 'rent', '2020-01', 110.0, '2020-01-01', 'paid', '2020-01-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (856, 2209, 1, 'rent', '2020-02', 110.0, '2020-02-01', 'paid', '2020-02-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (856, 2209, 1, 'rent', '2020-03', 110.0, '2020-03-01', 'paid', '2020-03-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (856, 2209, 1, 'rent', '2020-04', 110.0, '2020-04-01', 'paid', '2020-04-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (856, 2209, 1, 'rent', '2020-05', 110.0, '2020-05-01', 'paid', '2020-05-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (856, 2209, 1, 'rent', '2020-06', 110.0, '2020-06-01', 'paid', '2020-06-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (856, 2209, 1, 'rent', '2020-07', 110.0, '2020-07-01', 'paid', '2020-07-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (856, 2209, 1, 'rent', '2020-08', 110.0, '2020-08-01', 'paid', '2020-08-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (856, 2209, 1, 'rent', '2020-09', 110.0, '2020-09-01', 'paid', '2020-09-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (856, 2209, 1, 'rent', '2020-10', 110.0, '2020-10-01', 'paid', '2020-10-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (856, 2209, 1, 'rent', '2020-11', 110.0, '2020-11-01', 'paid', '2020-11-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (856, 2209, 1, 'rent', '2020-12', 110.0, '2020-12-01', 'paid', '2020-12-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (857, 2210, 1, 'rent', '2020-01', 110.0, '2020-01-01', 'paid', '2020-01-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (857, 2210, 1, 'rent', '2020-02', 110.0, '2020-02-01', 'paid', '2020-02-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (857, 2210, 1, 'rent', '2020-03', 110.0, '2020-03-01', 'paid', '2020-03-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (857, 2210, 1, 'rent', '2020-04', 110.0, '2020-04-01', 'paid', '2020-04-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (857, 2210, 1, 'rent', '2020-05', 110.0, '2020-05-01', 'paid', '2020-05-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (857, 2210, 1, 'rent', '2020-06', 110.0, '2020-06-01', 'paid', '2020-06-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (857, 2210, 1, 'rent', '2020-07', 110.0, '2020-07-01', 'paid', '2020-07-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (857, 2210, 1, 'rent', '2020-08', 110.0, '2020-08-01', 'paid', '2020-08-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (857, 2210, 1, 'rent', '2020-09', 110.0, '2020-09-01', 'paid', '2020-09-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (857, 2210, 1, 'rent', '2020-10', 110.0, '2020-10-01', 'paid', '2020-10-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (857, 2210, 1, 'rent', '2020-11', 110.0, '2020-11-01', 'paid', '2020-11-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (857, 2210, 1, 'rent', '2020-12', 110.0, '2020-12-01', 'paid', '2020-12-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (858, 2212, 1, 'rent', '2020-01', 110.0, '2020-01-01', 'paid', '2020-01-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (858, 2212, 1, 'rent', '2020-02', 110.0, '2020-02-01', 'paid', '2020-02-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (858, 2212, 1, 'rent', '2020-03', 110.0, '2020-03-01', 'paid', '2020-03-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (858, 2212, 1, 'rent', '2020-04', 110.0, '2020-04-01', 'paid', '2020-04-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (858, 2212, 1, 'rent', '2020-05', 110.0, '2020-05-01', 'paid', '2020-05-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (858, 2212, 1, 'rent', '2020-06', 110.0, '2020-06-01', 'paid', '2020-06-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (858, 2212, 1, 'rent', '2020-07', 110.0, '2020-07-01', 'paid', '2020-07-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (858, 2212, 1, 'rent', '2020-08', 110.0, '2020-08-01', 'paid', '2020-08-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (858, 2212, 1, 'rent', '2020-09', 110.0, '2020-09-01', 'paid', '2020-09-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (858, 2212, 1, 'rent', '2020-10', 110.0, '2020-10-01', 'paid', '2020-10-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (858, 2212, 1, 'rent', '2020-11', 110.0, '2020-11-01', 'paid', '2020-11-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (858, 2212, 1, 'rent', '2020-12', 110.0, '2020-12-01', 'paid', '2020-12-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (859, 2214, 1, 'rent', '2020-01', 110.0, '2020-01-01', 'paid', '2020-01-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (859, 2214, 1, 'rent', '2020-02', 110.0, '2020-02-01', 'paid', '2020-02-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (859, 2214, 1, 'rent', '2020-03', 110.0, '2020-03-01', 'paid', '2020-03-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (859, 2214, 1, 'rent', '2020-04', 110.0, '2020-04-01', 'paid', '2020-04-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (859, 2214, 1, 'rent', '2020-05', 110.0, '2020-05-01', 'paid', '2020-05-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (859, 2214, 1, 'rent', '2020-06', 110.0, '2020-06-01', 'paid', '2020-06-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (859, 2214, 1, 'rent', '2020-07', 110.0, '2020-07-01', 'paid', '2020-07-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (859, 2214, 1, 'rent', '2020-08', 110.0, '2020-08-01', 'paid', '2020-08-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (859, 2214, 1, 'rent', '2020-09', 110.0, '2020-09-01', 'paid', '2020-09-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (859, 2214, 1, 'rent', '2020-10', 110.0, '2020-10-01', 'paid', '2020-10-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (859, 2214, 1, 'rent', '2020-11', 110.0, '2020-11-01', 'paid', '2020-11-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (859, 2214, 1, 'rent', '2020-12', 110.0, '2020-12-01', 'paid', '2020-12-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (860, 2215, 1, 'rent', '2020-01', 110.0, '2020-01-01', 'paid', '2020-01-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (860, 2215, 1, 'rent', '2020-02', 110.0, '2020-02-01', 'paid', '2020-02-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (860, 2215, 1, 'rent', '2020-03', 110.0, '2020-03-01', 'paid', '2020-03-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (860, 2215, 1, 'rent', '2020-04', 110.0, '2020-04-01', 'paid', '2020-04-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (860, 2215, 1, 'rent', '2020-05', 110.0, '2020-05-01', 'paid', '2020-05-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (860, 2215, 1, 'rent', '2020-06', 110.0, '2020-06-01', 'paid', '2020-06-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (860, 2215, 1, 'rent', '2020-07', 110.0, '2020-07-01', 'paid', '2020-07-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (860, 2215, 1, 'rent', '2020-08', 110.0, '2020-08-01', 'paid', '2020-08-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (860, 2215, 1, 'rent', '2020-09', 110.0, '2020-09-01', 'paid', '2020-09-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (860, 2215, 1, 'rent', '2020-10', 110.0, '2020-10-01', 'paid', '2020-10-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (860, 2215, 1, 'rent', '2020-11', 110.0, '2020-11-01', 'paid', '2020-11-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (860, 2215, 1, 'rent', '2020-12', 110.0, '2020-12-01', 'paid', '2020-12-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (861, 2216, 1, 'rent', '2020-01', 110.0, '2020-01-01', 'paid', '2020-01-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (861, 2216, 1, 'rent', '2020-02', 110.0, '2020-02-01', 'paid', '2020-02-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (861, 2216, 1, 'rent', '2020-03', 110.0, '2020-03-01', 'paid', '2020-03-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (861, 2216, 1, 'rent', '2020-04', 110.0, '2020-04-01', 'paid', '2020-04-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (861, 2216, 1, 'rent', '2020-05', 110.0, '2020-05-01', 'paid', '2020-05-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (861, 2216, 1, 'rent', '2020-06', 110.0, '2020-06-01', 'paid', '2020-06-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (861, 2216, 1, 'rent', '2020-07', 110.0, '2020-07-01', 'paid', '2020-07-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (861, 2216, 1, 'rent', '2020-08', 110.0, '2020-08-01', 'paid', '2020-08-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (861, 2216, 1, 'rent', '2020-09', 110.0, '2020-09-01', 'paid', '2020-09-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (861, 2216, 1, 'rent', '2020-10', 110.0, '2020-10-01', 'paid', '2020-10-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (861, 2216, 1, 'rent', '2020-11', 110.0, '2020-11-01', 'paid', '2020-11-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (861, 2216, 1, 'rent', '2020-12', 110.0, '2020-12-01', 'paid', '2020-12-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (862, 2217, 1, 'rent', '2020-01', 110.0, '2020-01-01', 'paid', '2020-01-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (862, 2217, 1, 'rent', '2020-02', 110.0, '2020-02-01', 'paid', '2020-02-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (862, 2217, 1, 'rent', '2020-03', 110.0, '2020-03-01', 'paid', '2020-03-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (862, 2217, 1, 'rent', '2020-04', 110.0, '2020-04-01', 'paid', '2020-04-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (862, 2217, 1, 'rent', '2020-05', 110.0, '2020-05-01', 'paid', '2020-05-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (862, 2217, 1, 'rent', '2020-06', 110.0, '2020-06-01', 'paid', '2020-06-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (862, 2217, 1, 'rent', '2020-07', 110.0, '2020-07-01', 'paid', '2020-07-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (862, 2217, 1, 'rent', '2020-08', 110.0, '2020-08-01', 'paid', '2020-08-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (862, 2217, 1, 'rent', '2020-09', 110.0, '2020-09-01', 'paid', '2020-09-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (862, 2217, 1, 'rent', '2020-10', 110.0, '2020-10-01', 'paid', '2020-10-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (862, 2217, 1, 'rent', '2020-11', 110.0, '2020-11-01', 'paid', '2020-11-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (862, 2217, 1, 'rent', '2020-12', 110.0, '2020-12-01', 'paid', '2020-12-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (863, 2218, 1, 'rent', '2020-01', 110.0, '2020-01-01', 'paid', '2020-01-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (863, 2218, 1, 'rent', '2020-02', 110.0, '2020-02-01', 'paid', '2020-02-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (863, 2218, 1, 'rent', '2020-03', 110.0, '2020-03-01', 'paid', '2020-03-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (863, 2218, 1, 'rent', '2020-04', 110.0, '2020-04-01', 'paid', '2020-04-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (863, 2218, 1, 'rent', '2020-05', 110.0, '2020-05-01', 'paid', '2020-05-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (863, 2218, 1, 'rent', '2020-06', 110.0, '2020-06-01', 'paid', '2020-06-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (863, 2218, 1, 'rent', '2020-07', 110.0, '2020-07-01', 'paid', '2020-07-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (863, 2218, 1, 'rent', '2020-08', 110.0, '2020-08-01', 'paid', '2020-08-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (863, 2218, 1, 'rent', '2020-09', 110.0, '2020-09-01', 'paid', '2020-09-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (863, 2218, 1, 'rent', '2020-10', 110.0, '2020-10-01', 'paid', '2020-10-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (863, 2218, 1, 'rent', '2020-11', 110.0, '2020-11-01', 'paid', '2020-11-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (863, 2218, 1, 'rent', '2020-12', 110.0, '2020-12-01', 'paid', '2020-12-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (864, 2219, 1, 'rent', '2020-01', 110.0, '2020-01-01', 'paid', '2020-01-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (864, 2219, 1, 'rent', '2020-02', 110.0, '2020-02-01', 'paid', '2020-02-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (864, 2219, 1, 'rent', '2020-03', 110.0, '2020-03-01', 'paid', '2020-03-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (864, 2219, 1, 'rent', '2020-04', 110.0, '2020-04-01', 'paid', '2020-04-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (864, 2219, 1, 'rent', '2020-05', 110.0, '2020-05-01', 'paid', '2020-05-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (864, 2219, 1, 'rent', '2020-06', 110.0, '2020-06-01', 'paid', '2020-06-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (864, 2219, 1, 'rent', '2020-07', 110.0, '2020-07-01', 'paid', '2020-07-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (864, 2219, 1, 'rent', '2020-08', 110.0, '2020-08-01', 'paid', '2020-08-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (864, 2219, 1, 'rent', '2020-09', 110.0, '2020-09-01', 'paid', '2020-09-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (864, 2219, 1, 'rent', '2020-10', 110.0, '2020-10-01', 'paid', '2020-10-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (864, 2219, 1, 'rent', '2020-11', 110.0, '2020-11-01', 'paid', '2020-11-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (864, 2219, 1, 'rent', '2020-12', 110.0, '2020-12-01', 'paid', '2020-12-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (865, 2220, 1, 'rent', '2020-01', 110.0, '2020-01-01', 'paid', '2020-01-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (865, 2220, 1, 'rent', '2020-02', 110.0, '2020-02-01', 'paid', '2020-02-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (865, 2220, 1, 'rent', '2020-03', 110.0, '2020-03-01', 'paid', '2020-03-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (865, 2220, 1, 'rent', '2020-04', 110.0, '2020-04-01', 'paid', '2020-04-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (865, 2220, 1, 'rent', '2020-05', 110.0, '2020-05-01', 'paid', '2020-05-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (865, 2220, 1, 'rent', '2020-06', 110.0, '2020-06-01', 'paid', '2020-06-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (865, 2220, 1, 'rent', '2020-07', 110.0, '2020-07-01', 'paid', '2020-07-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (865, 2220, 1, 'rent', '2020-08', 110.0, '2020-08-01', 'paid', '2020-08-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (865, 2220, 1, 'rent', '2020-09', 110.0, '2020-09-01', 'paid', '2020-09-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (865, 2220, 1, 'rent', '2020-10', 110.0, '2020-10-01', 'paid', '2020-10-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (865, 2220, 1, 'rent', '2020-11', 110.0, '2020-11-01', 'paid', '2020-11-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (865, 2220, 1, 'rent', '2020-12', 110.0, '2020-12-01', 'paid', '2020-12-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (866, 2221, 1, 'rent', '2020-01', 111.0, '2020-01-01', 'paid', '2020-01-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (866, 2221, 1, 'rent', '2020-02', 111.0, '2020-02-01', 'paid', '2020-02-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (866, 2221, 1, 'rent', '2020-03', 111.0, '2020-03-01', 'paid', '2020-03-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (866, 2221, 1, 'rent', '2020-04', 111.0, '2020-04-01', 'paid', '2020-04-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (866, 2221, 1, 'rent', '2020-05', 111.0, '2020-05-01', 'paid', '2020-05-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (866, 2221, 1, 'rent', '2020-06', 111.0, '2020-06-01', 'paid', '2020-06-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (866, 2221, 1, 'rent', '2020-07', 111.0, '2020-07-01', 'paid', '2020-07-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (866, 2221, 1, 'rent', '2020-08', 111.0, '2020-08-01', 'paid', '2020-08-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (866, 2221, 1, 'rent', '2020-09', 111.0, '2020-09-01', 'paid', '2020-09-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (866, 2221, 1, 'rent', '2020-10', 111.0, '2020-10-01', 'paid', '2020-10-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (866, 2221, 1, 'rent', '2020-11', 111.0, '2020-11-01', 'paid', '2020-11-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (866, 2221, 1, 'rent', '2020-12', 111.0, '2020-12-01', 'paid', '2020-12-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (867, 2222, 1, 'rent', '2020-01', 111.0, '2020-01-01', 'paid', '2020-01-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (867, 2222, 1, 'rent', '2020-02', 111.0, '2020-02-01', 'paid', '2020-02-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (867, 2222, 1, 'rent', '2020-03', 111.0, '2020-03-01', 'paid', '2020-03-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (867, 2222, 1, 'rent', '2020-04', 111.0, '2020-04-01', 'paid', '2020-04-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (867, 2222, 1, 'rent', '2020-05', 111.0, '2020-05-01', 'paid', '2020-05-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (867, 2222, 1, 'rent', '2020-06', 111.0, '2020-06-01', 'paid', '2020-06-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (867, 2222, 1, 'rent', '2020-07', 111.0, '2020-07-01', 'paid', '2020-07-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (867, 2222, 1, 'rent', '2020-08', 111.0, '2020-08-01', 'paid', '2020-08-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (867, 2222, 1, 'rent', '2020-09', 111.0, '2020-09-01', 'paid', '2020-09-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (867, 2222, 1, 'rent', '2020-10', 111.0, '2020-10-01', 'paid', '2020-10-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (867, 2222, 1, 'rent', '2020-11', 111.0, '2020-11-01', 'paid', '2020-11-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (867, 2222, 1, 'rent', '2020-12', 111.0, '2020-12-01', 'paid', '2020-12-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (868, 2223, 1, 'rent', '2020-01', 111.0, '2020-01-01', 'paid', '2020-01-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (868, 2223, 1, 'rent', '2020-02', 111.0, '2020-02-01', 'paid', '2020-02-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (868, 2223, 1, 'rent', '2020-03', 111.0, '2020-03-01', 'paid', '2020-03-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (868, 2223, 1, 'rent', '2020-04', 111.0, '2020-04-01', 'paid', '2020-04-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (868, 2223, 1, 'rent', '2020-05', 111.0, '2020-05-01', 'paid', '2020-05-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (868, 2223, 1, 'rent', '2020-06', 111.0, '2020-06-01', 'paid', '2020-06-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (868, 2223, 1, 'rent', '2020-07', 111.0, '2020-07-01', 'paid', '2020-07-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (868, 2223, 1, 'rent', '2020-08', 111.0, '2020-08-01', 'paid', '2020-08-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (868, 2223, 1, 'rent', '2020-09', 111.0, '2020-09-01', 'paid', '2020-09-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (868, 2223, 1, 'rent', '2020-10', 111.0, '2020-10-01', 'paid', '2020-10-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (868, 2223, 1, 'rent', '2020-11', 111.0, '2020-11-01', 'paid', '2020-11-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (868, 2223, 1, 'rent', '2020-12', 111.0, '2020-12-01', 'paid', '2020-12-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (870, 2228, 1, 'rent', '2021-12', 1.0, '2021-12-01', 'paid', '2021-12-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (872, 2237, 1, 'rent', '2021-03', 2500.0, '2021-03-01', 'paid', '2021-03-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (872, 2237, 1, 'rent', '2021-04', 2500.0, '2021-04-01', 'paid', '2021-04-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (872, 2237, 1, 'rent', '2021-05', 2500.0, '2021-05-01', 'paid', '2021-05-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (872, 2237, 1, 'rent', '2021-06', 2500.0, '2021-06-01', 'paid', '2021-06-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (872, 2237, 1, 'rent', '2021-07', 2500.0, '2021-07-01', 'paid', '2021-07-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (872, 2237, 1, 'rent', '2021-08', 2500.0, '2021-08-01', 'paid', '2021-08-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (872, 2237, 1, 'rent', '2021-09', 2500.0, '2021-09-01', 'paid', '2021-09-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (872, 2237, 1, 'rent', '2021-10', 2500.0, '2021-10-01', 'paid', '2021-10-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (872, 2237, 1, 'rent', '2021-11', 2500.0, '2021-11-01', 'paid', '2021-11-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (872, 2237, 1, 'rent', '2021-12', 2500.0, '2021-12-01', 'paid', '2021-12-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (872, 2237, 1, 'rent', '2022-01', 2500.0, '2022-01-01', 'paid', '2022-01-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (872, 2237, 1, 'rent', '2022-02', 2500.0, '2022-02-01', 'paid', '2022-02-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (874, 2239, 1, 'rent', '2020-01', 2500.0, '2020-01-01', 'paid', '2020-01-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (874, 2239, 1, 'rent', '2020-02', 2500.0, '2020-02-01', 'paid', '2020-02-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (874, 2239, 1, 'rent', '2020-03', 2500.0, '2020-03-01', 'paid', '2020-03-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (874, 2239, 1, 'rent', '2020-04', 2500.0, '2020-04-01', 'paid', '2020-04-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (874, 2239, 1, 'rent', '2020-05', 2500.0, '2020-05-01', 'paid', '2020-05-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (874, 2239, 1, 'rent', '2020-06', 2500.0, '2020-06-01', 'paid', '2020-06-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (874, 2239, 1, 'rent', '2020-07', 2500.0, '2020-07-01', 'paid', '2020-07-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (874, 2239, 1, 'rent', '2020-08', 2500.0, '2020-08-01', 'paid', '2020-08-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (874, 2239, 1, 'rent', '2020-09', 2500.0, '2020-09-01', 'paid', '2020-09-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (874, 2239, 1, 'rent', '2020-10', 2500.0, '2020-10-01', 'paid', '2020-10-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (874, 2239, 1, 'rent', '2020-11', 2500.0, '2020-11-01', 'paid', '2020-11-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (874, 2239, 1, 'rent', '2020-12', 2500.0, '2020-12-01', 'paid', '2020-12-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (875, 2244, 1, 'rent', '2020-01', 18000.0, '2020-01-01', 'paid', '2020-01-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (875, 2244, 1, 'rent', '2021-01', 18000.0, '2021-01-01', 'paid', '2021-01-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (875, 2244, 1, 'rent', '2022-01', 18000.0, '2022-01-01', 'paid', '2022-01-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (876, 2245, 1, 'rent', '2020-01', 12000.0, '2020-01-01', 'paid', '2020-01-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (876, 2245, 1, 'rent', '2021-01', 12000.0, '2021-01-01', 'paid', '2021-01-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (876, 2245, 1, 'rent', '2022-01', 12000.0, '2022-01-01', 'paid', '2022-01-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (877, 2246, 1, 'rent', '2020-01', 1490.0, '2020-01-01', 'paid', '2020-01-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (877, 2246, 1, 'rent', '2020-02', 1490.0, '2020-02-01', 'paid', '2020-02-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (877, 2246, 1, 'rent', '2020-03', 1490.0, '2020-03-01', 'paid', '2020-03-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (877, 2246, 1, 'rent', '2020-04', 1490.0, '2020-04-01', 'paid', '2020-04-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (877, 2246, 1, 'rent', '2020-05', 1490.0, '2020-05-01', 'paid', '2020-05-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (877, 2246, 1, 'rent', '2020-06', 1490.0, '2020-06-01', 'paid', '2020-06-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (877, 2246, 1, 'rent', '2020-07', 1490.0, '2020-07-01', 'paid', '2020-07-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (877, 2246, 1, 'rent', '2020-08', 1490.0, '2020-08-01', 'paid', '2020-08-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (877, 2246, 1, 'rent', '2020-09', 1490.0, '2020-09-01', 'paid', '2020-09-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (877, 2246, 1, 'rent', '2020-10', 1490.0, '2020-10-01', 'paid', '2020-10-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (877, 2246, 1, 'rent', '2020-11', 1490.0, '2020-11-01', 'paid', '2020-11-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (877, 2246, 1, 'rent', '2020-12', 1490.0, '2020-12-01', 'paid', '2020-12-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (878, 2247, 1, 'rent', '2020-01', 1800.0, '2020-01-01', 'paid', '2020-01-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (878, 2247, 1, 'rent', '2020-02', 1800.0, '2020-02-01', 'paid', '2020-02-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (878, 2247, 1, 'rent', '2020-03', 1800.0, '2020-03-01', 'paid', '2020-03-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (878, 2247, 1, 'rent', '2020-04', 1800.0, '2020-04-01', 'paid', '2020-04-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (878, 2247, 1, 'rent', '2020-05', 1800.0, '2020-05-01', 'paid', '2020-05-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (878, 2247, 1, 'rent', '2020-06', 1800.0, '2020-06-01', 'paid', '2020-06-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (878, 2247, 1, 'rent', '2020-07', 1800.0, '2020-07-01', 'paid', '2020-07-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (878, 2247, 1, 'rent', '2020-08', 1800.0, '2020-08-01', 'paid', '2020-08-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (878, 2247, 1, 'rent', '2020-09', 1800.0, '2020-09-01', 'paid', '2020-09-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (878, 2247, 1, 'rent', '2020-10', 1800.0, '2020-10-01', 'paid', '2020-10-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (878, 2247, 1, 'rent', '2020-11', 1800.0, '2020-11-01', 'paid', '2020-11-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (878, 2247, 1, 'rent', '2020-12', 1800.0, '2020-12-01', 'paid', '2020-12-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (879, 2248, 1, 'rent', '2020-01', 21600.0, '2020-01-01', 'paid', '2020-01-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (879, 2248, 1, 'rent', '2021-01', 21600.0, '2021-01-01', 'paid', '2021-01-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (879, 2248, 1, 'rent', '2022-01', 21600.0, '2022-01-01', 'paid', '2022-01-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (880, 2249, 1, 'rent', '2020-01', 15000.0, '2020-01-01', 'paid', '2020-01-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (880, 2249, 1, 'rent', '2020-02', 15000.0, '2020-02-01', 'paid', '2020-02-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (880, 2249, 1, 'rent', '2020-03', 15000.0, '2020-03-01', 'paid', '2020-03-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (880, 2249, 1, 'rent', '2020-04', 15000.0, '2020-04-01', 'paid', '2020-04-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (880, 2249, 1, 'rent', '2020-05', 15000.0, '2020-05-01', 'paid', '2020-05-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (880, 2249, 1, 'rent', '2020-06', 15000.0, '2020-06-01', 'paid', '2020-06-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (880, 2249, 1, 'rent', '2020-07', 15000.0, '2020-07-01', 'paid', '2020-07-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (880, 2249, 1, 'rent', '2020-08', 15000.0, '2020-08-01', 'paid', '2020-08-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (880, 2249, 1, 'rent', '2020-09', 15000.0, '2020-09-01', 'paid', '2020-09-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (880, 2249, 1, 'rent', '2020-10', 15000.0, '2020-10-01', 'paid', '2020-10-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (880, 2249, 1, 'rent', '2020-11', 15000.0, '2020-11-01', 'paid', '2020-11-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (880, 2249, 1, 'rent', '2020-12', 15000.0, '2020-12-01', 'paid', '2020-12-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (881, 2250, 1, 'rent', '2020-01', 13500.0, '2020-01-01', 'paid', '2020-01-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (881, 2250, 1, 'rent', '2020-02', 13500.0, '2020-02-01', 'paid', '2020-02-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (881, 2250, 1, 'rent', '2020-03', 13500.0, '2020-03-01', 'paid', '2020-03-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (881, 2250, 1, 'rent', '2020-04', 13500.0, '2020-04-01', 'paid', '2020-04-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (881, 2250, 1, 'rent', '2020-05', 13500.0, '2020-05-01', 'paid', '2020-05-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (881, 2250, 1, 'rent', '2020-06', 13500.0, '2020-06-01', 'paid', '2020-06-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (881, 2250, 1, 'rent', '2020-07', 13500.0, '2020-07-01', 'paid', '2020-07-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (881, 2250, 1, 'rent', '2020-08', 13500.0, '2020-08-01', 'paid', '2020-08-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (881, 2250, 1, 'rent', '2020-09', 13500.0, '2020-09-01', 'paid', '2020-09-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (881, 2250, 1, 'rent', '2020-10', 13500.0, '2020-10-01', 'paid', '2020-10-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (881, 2250, 1, 'rent', '2020-11', 13500.0, '2020-11-01', 'paid', '2020-11-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (881, 2250, 1, 'rent', '2020-12', 13500.0, '2020-12-01', 'paid', '2020-12-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (882, 2251, 1, 'rent', '2020-01', 21600.0, '2020-01-01', 'paid', '2020-01-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (882, 2251, 1, 'rent', '2021-01', 21600.0, '2021-01-01', 'paid', '2021-01-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (882, 2251, 1, 'rent', '2022-01', 21600.0, '2022-01-01', 'paid', '2022-01-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (883, 2252, 1, 'rent', '2020-01', 36000.0, '2020-01-01', 'paid', '2020-01-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (883, 2252, 1, 'rent', '2021-01', 36000.0, '2021-01-01', 'paid', '2021-01-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (883, 2252, 1, 'rent', '2022-01', 36000.0, '2022-01-01', 'paid', '2022-01-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (884, 2253, 1, 'rent', '2020-01', 2000.0, '2020-01-01', 'paid', '2020-01-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (884, 2253, 1, 'rent', '2020-02', 2000.0, '2020-02-01', 'paid', '2020-02-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (884, 2253, 1, 'rent', '2020-03', 2000.0, '2020-03-01', 'paid', '2020-03-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (884, 2253, 1, 'rent', '2020-04', 2000.0, '2020-04-01', 'paid', '2020-04-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (884, 2253, 1, 'rent', '2020-05', 2000.0, '2020-05-01', 'paid', '2020-05-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (884, 2253, 1, 'rent', '2020-06', 2000.0, '2020-06-01', 'paid', '2020-06-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (884, 2253, 1, 'rent', '2020-07', 2000.0, '2020-07-01', 'paid', '2020-07-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (884, 2253, 1, 'rent', '2020-08', 2000.0, '2020-08-01', 'paid', '2020-08-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (884, 2253, 1, 'rent', '2020-09', 2000.0, '2020-09-01', 'paid', '2020-09-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (884, 2253, 1, 'rent', '2020-10', 2000.0, '2020-10-01', 'paid', '2020-10-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (884, 2253, 1, 'rent', '2020-11', 2000.0, '2020-11-01', 'paid', '2020-11-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (884, 2253, 1, 'rent', '2020-12', 2000.0, '2020-12-01', 'paid', '2020-12-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (885, 2254, 1, 'rent', '2020-01', 1490.0, '2020-01-01', 'paid', '2020-01-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (885, 2254, 1, 'rent', '2020-02', 1490.0, '2020-02-01', 'paid', '2020-02-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (885, 2254, 1, 'rent', '2020-03', 1490.0, '2020-03-01', 'paid', '2020-03-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (885, 2254, 1, 'rent', '2020-04', 1490.0, '2020-04-01', 'paid', '2020-04-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (885, 2254, 1, 'rent', '2020-05', 1490.0, '2020-05-01', 'paid', '2020-05-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (885, 2254, 1, 'rent', '2020-06', 1490.0, '2020-06-01', 'paid', '2020-06-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (885, 2254, 1, 'rent', '2020-07', 1490.0, '2020-07-01', 'paid', '2020-07-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (885, 2254, 1, 'rent', '2020-08', 1490.0, '2020-08-01', 'paid', '2020-08-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (885, 2254, 1, 'rent', '2020-09', 1490.0, '2020-09-01', 'paid', '2020-09-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (885, 2254, 1, 'rent', '2020-10', 1490.0, '2020-10-01', 'paid', '2020-10-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (885, 2254, 1, 'rent', '2020-11', 1490.0, '2020-11-01', 'paid', '2020-11-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (885, 2254, 1, 'rent', '2020-12', 1490.0, '2020-12-01', 'paid', '2020-12-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (886, 2255, 1, 'rent', '2020-01', 21600.0, '2020-01-01', 'paid', '2020-01-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (886, 2255, 1, 'rent', '2021-01', 21600.0, '2021-01-01', 'paid', '2021-01-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (886, 2255, 1, 'rent', '2022-01', 21600.0, '2022-01-01', 'paid', '2022-01-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (887, 2256, 1, 'rent', '2020-01', 21600.0, '2020-01-01', 'paid', '2020-01-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (887, 2256, 1, 'rent', '2021-01', 21600.0, '2021-01-01', 'paid', '2021-01-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (887, 2256, 1, 'rent', '2022-01', 21600.0, '2022-01-01', 'paid', '2022-01-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (887, 2256, 1, 'rent', '2023-01', 21600.0, '2023-01-01', 'paid', '2023-01-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (888, 2257, 1, 'rent', '2020-01', 21600.0, '2020-01-01', 'paid', '2020-01-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (888, 2257, 1, 'rent', '2021-01', 21600.0, '2021-01-01', 'paid', '2021-01-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (888, 2257, 1, 'rent', '2022-01', 21600.0, '2022-01-01', 'paid', '2022-01-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (888, 2257, 1, 'rent', '2023-01', 21600.0, '2023-01-01', 'paid', '2023-01-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (889, 2258, 1, 'rent', '2020-01', 12000.0, '2020-01-01', 'paid', '2020-01-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (889, 2258, 1, 'rent', '2021-01', 12000.0, '2021-01-01', 'paid', '2021-01-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (889, 2258, 1, 'rent', '2022-01', 12000.0, '2022-01-01', 'paid', '2022-01-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (889, 2258, 1, 'rent', '2023-01', 12000.0, '2023-01-01', 'paid', '2023-01-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (890, 2259, 1, 'rent', '2020-01', 21600.0, '2020-01-01', 'paid', '2020-01-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (890, 2259, 1, 'rent', '2021-01', 21600.0, '2021-01-01', 'paid', '2021-01-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (890, 2259, 1, 'rent', '2022-01', 21600.0, '2022-01-01', 'paid', '2022-01-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (890, 2259, 1, 'rent', '2023-01', 21600.0, '2023-01-01', 'paid', '2023-01-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (891, 2260, 1, 'rent', '2020-01', 21600.0, '2020-01-01', 'paid', '2020-01-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (891, 2260, 1, 'rent', '2021-01', 21600.0, '2021-01-01', 'paid', '2021-01-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (891, 2260, 1, 'rent', '2022-01', 21600.0, '2022-01-01', 'paid', '2022-01-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (892, 2261, 1, 'rent', '2020-01', 30000.0, '2020-01-01', 'paid', '2020-01-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (892, 2261, 1, 'rent', '2021-01', 30000.0, '2021-01-01', 'paid', '2021-01-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (892, 2261, 1, 'rent', '2022-01', 30000.0, '2022-01-01', 'paid', '2022-01-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (893, 2262, 1, 'rent', '2020-01', 1490.0, '2020-01-01', 'paid', '2020-01-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (893, 2262, 1, 'rent', '2020-02', 1490.0, '2020-02-01', 'paid', '2020-02-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (893, 2262, 1, 'rent', '2020-03', 1490.0, '2020-03-01', 'paid', '2020-03-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (893, 2262, 1, 'rent', '2020-04', 1490.0, '2020-04-01', 'paid', '2020-04-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (893, 2262, 1, 'rent', '2020-05', 1490.0, '2020-05-01', 'paid', '2020-05-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (893, 2262, 1, 'rent', '2020-06', 1490.0, '2020-06-01', 'paid', '2020-06-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (893, 2262, 1, 'rent', '2020-07', 1490.0, '2020-07-01', 'paid', '2020-07-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (893, 2262, 1, 'rent', '2020-08', 1490.0, '2020-08-01', 'paid', '2020-08-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (893, 2262, 1, 'rent', '2020-09', 1490.0, '2020-09-01', 'paid', '2020-09-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (893, 2262, 1, 'rent', '2020-10', 1490.0, '2020-10-01', 'paid', '2020-10-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (893, 2262, 1, 'rent', '2020-11', 1490.0, '2020-11-01', 'paid', '2020-11-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (893, 2262, 1, 'rent', '2020-12', 1490.0, '2020-12-01', 'paid', '2020-12-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (894, 2263, 1, 'rent', '2020-01', 1800.0, '2020-01-01', 'paid', '2020-01-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (894, 2263, 1, 'rent', '2020-02', 1800.0, '2020-02-01', 'paid', '2020-02-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (894, 2263, 1, 'rent', '2020-03', 1800.0, '2020-03-01', 'paid', '2020-03-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (894, 2263, 1, 'rent', '2020-04', 1800.0, '2020-04-01', 'paid', '2020-04-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (894, 2263, 1, 'rent', '2020-05', 1800.0, '2020-05-01', 'paid', '2020-05-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (894, 2263, 1, 'rent', '2020-06', 1800.0, '2020-06-01', 'paid', '2020-06-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (894, 2263, 1, 'rent', '2020-07', 1800.0, '2020-07-01', 'paid', '2020-07-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (894, 2263, 1, 'rent', '2020-08', 1800.0, '2020-08-01', 'paid', '2020-08-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (894, 2263, 1, 'rent', '2020-09', 1800.0, '2020-09-01', 'paid', '2020-09-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (894, 2263, 1, 'rent', '2020-10', 1800.0, '2020-10-01', 'paid', '2020-10-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (894, 2263, 1, 'rent', '2020-11', 1800.0, '2020-11-01', 'paid', '2020-11-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (894, 2263, 1, 'rent', '2020-12', 1800.0, '2020-12-01', 'paid', '2020-12-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (895, 2264, 1, 'rent', '2020-01', 1800.0, '2020-01-01', 'paid', '2020-01-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (895, 2264, 1, 'rent', '2020-02', 1800.0, '2020-02-01', 'paid', '2020-02-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (895, 2264, 1, 'rent', '2020-03', 1800.0, '2020-03-01', 'paid', '2020-03-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (895, 2264, 1, 'rent', '2020-04', 1800.0, '2020-04-01', 'paid', '2020-04-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (895, 2264, 1, 'rent', '2020-05', 1800.0, '2020-05-01', 'paid', '2020-05-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (895, 2264, 1, 'rent', '2020-06', 1800.0, '2020-06-01', 'paid', '2020-06-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (895, 2264, 1, 'rent', '2020-07', 1800.0, '2020-07-01', 'paid', '2020-07-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (895, 2264, 1, 'rent', '2020-08', 1800.0, '2020-08-01', 'paid', '2020-08-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (895, 2264, 1, 'rent', '2020-09', 1800.0, '2020-09-01', 'paid', '2020-09-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (895, 2264, 1, 'rent', '2020-10', 1800.0, '2020-10-01', 'paid', '2020-10-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (895, 2264, 1, 'rent', '2020-11', 1800.0, '2020-11-01', 'paid', '2020-11-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (895, 2264, 1, 'rent', '2020-12', 1800.0, '2020-12-01', 'paid', '2020-12-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (896, 2265, 1, 'rent', '2020-01', 1490.0, '2020-01-01', 'paid', '2020-01-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (896, 2265, 1, 'rent', '2020-02', 1490.0, '2020-02-01', 'paid', '2020-02-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (896, 2265, 1, 'rent', '2020-03', 1490.0, '2020-03-01', 'paid', '2020-03-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (896, 2265, 1, 'rent', '2020-04', 1490.0, '2020-04-01', 'paid', '2020-04-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (896, 2265, 1, 'rent', '2020-05', 1490.0, '2020-05-01', 'paid', '2020-05-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (896, 2265, 1, 'rent', '2020-06', 1490.0, '2020-06-01', 'paid', '2020-06-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (896, 2265, 1, 'rent', '2020-07', 1490.0, '2020-07-01', 'paid', '2020-07-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (896, 2265, 1, 'rent', '2020-08', 1490.0, '2020-08-01', 'paid', '2020-08-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (896, 2265, 1, 'rent', '2020-09', 1490.0, '2020-09-01', 'paid', '2020-09-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (896, 2265, 1, 'rent', '2020-10', 1490.0, '2020-10-01', 'paid', '2020-10-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (896, 2265, 1, 'rent', '2020-11', 1490.0, '2020-11-01', 'paid', '2020-11-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (896, 2265, 1, 'rent', '2020-12', 1490.0, '2020-12-01', 'paid', '2020-12-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (897, 2266, 1, 'rent', '2020-01', 1490.0, '2020-01-01', 'paid', '2020-01-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (897, 2266, 1, 'rent', '2020-02', 1490.0, '2020-02-01', 'paid', '2020-02-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (897, 2266, 1, 'rent', '2020-03', 1490.0, '2020-03-01', 'paid', '2020-03-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (897, 2266, 1, 'rent', '2020-04', 1490.0, '2020-04-01', 'paid', '2020-04-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (897, 2266, 1, 'rent', '2020-05', 1490.0, '2020-05-01', 'paid', '2020-05-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (897, 2266, 1, 'rent', '2020-06', 1490.0, '2020-06-01', 'paid', '2020-06-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (897, 2266, 1, 'rent', '2020-07', 1490.0, '2020-07-01', 'paid', '2020-07-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (897, 2266, 1, 'rent', '2020-08', 1490.0, '2020-08-01', 'paid', '2020-08-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (897, 2266, 1, 'rent', '2020-09', 1490.0, '2020-09-01', 'paid', '2020-09-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (897, 2266, 1, 'rent', '2020-10', 1490.0, '2020-10-01', 'paid', '2020-10-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (897, 2266, 1, 'rent', '2020-11', 1490.0, '2020-11-01', 'paid', '2020-11-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (897, 2266, 1, 'rent', '2020-12', 1490.0, '2020-12-01', 'paid', '2020-12-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (898, 2267, 1, 'rent', '2020-01', 18000.0, '2020-01-01', 'paid', '2020-01-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (898, 2267, 1, 'rent', '2021-01', 18000.0, '2021-01-01', 'paid', '2021-01-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (898, 2267, 1, 'rent', '2022-01', 18000.0, '2022-01-01', 'paid', '2022-01-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (898, 2267, 1, 'rent', '2023-01', 18000.0, '2023-01-01', 'paid', '2023-01-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (899, 2268, 1, 'rent', '2020-01', 1500.0, '2020-01-01', 'paid', '2020-01-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (899, 2268, 1, 'rent', '2020-02', 1500.0, '2020-02-01', 'paid', '2020-02-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (899, 2268, 1, 'rent', '2020-03', 1500.0, '2020-03-01', 'paid', '2020-03-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (899, 2268, 1, 'rent', '2020-04', 1500.0, '2020-04-01', 'paid', '2020-04-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (899, 2268, 1, 'rent', '2020-05', 1500.0, '2020-05-01', 'paid', '2020-05-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (899, 2268, 1, 'rent', '2020-06', 1500.0, '2020-06-01', 'paid', '2020-06-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (899, 2268, 1, 'rent', '2020-07', 1500.0, '2020-07-01', 'paid', '2020-07-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (899, 2268, 1, 'rent', '2020-08', 1500.0, '2020-08-01', 'paid', '2020-08-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (899, 2268, 1, 'rent', '2020-09', 1500.0, '2020-09-01', 'paid', '2020-09-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (899, 2268, 1, 'rent', '2020-10', 1500.0, '2020-10-01', 'paid', '2020-10-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (899, 2268, 1, 'rent', '2020-11', 1500.0, '2020-11-01', 'paid', '2020-11-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (899, 2268, 1, 'rent', '2020-12', 1500.0, '2020-12-01', 'paid', '2020-12-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (900, 2269, 1, 'rent', '2020-01', 12500.0, '2020-01-01', 'paid', '2020-01-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (900, 2269, 1, 'rent', '2020-02', 12500.0, '2020-02-01', 'paid', '2020-02-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (900, 2269, 1, 'rent', '2020-03', 12500.0, '2020-03-01', 'paid', '2020-03-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (900, 2269, 1, 'rent', '2020-04', 12500.0, '2020-04-01', 'paid', '2020-04-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (900, 2269, 1, 'rent', '2020-05', 12500.0, '2020-05-01', 'paid', '2020-05-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (900, 2269, 1, 'rent', '2020-06', 12500.0, '2020-06-01', 'paid', '2020-06-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (900, 2269, 1, 'rent', '2020-07', 12500.0, '2020-07-01', 'paid', '2020-07-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (900, 2269, 1, 'rent', '2020-08', 12500.0, '2020-08-01', 'paid', '2020-08-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (900, 2269, 1, 'rent', '2020-09', 12500.0, '2020-09-01', 'paid', '2020-09-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (900, 2269, 1, 'rent', '2020-10', 12500.0, '2020-10-01', 'paid', '2020-10-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (900, 2269, 1, 'rent', '2020-11', 12500.0, '2020-11-01', 'paid', '2020-11-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (900, 2269, 1, 'rent', '2020-12', 12500.0, '2020-12-01', 'paid', '2020-12-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (901, 2270, 1, 'rent', '2020-01', 1490.0, '2020-01-01', 'paid', '2020-01-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (901, 2270, 1, 'rent', '2020-02', 1490.0, '2020-02-01', 'paid', '2020-02-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (901, 2270, 1, 'rent', '2020-03', 1490.0, '2020-03-01', 'paid', '2020-03-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (901, 2270, 1, 'rent', '2020-04', 1490.0, '2020-04-01', 'paid', '2020-04-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (901, 2270, 1, 'rent', '2020-05', 1490.0, '2020-05-01', 'paid', '2020-05-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (901, 2270, 1, 'rent', '2020-06', 1490.0, '2020-06-01', 'paid', '2020-06-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (901, 2270, 1, 'rent', '2020-07', 1490.0, '2020-07-01', 'paid', '2020-07-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (901, 2270, 1, 'rent', '2020-08', 1490.0, '2020-08-01', 'paid', '2020-08-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (901, 2270, 1, 'rent', '2020-09', 1490.0, '2020-09-01', 'paid', '2020-09-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (901, 2270, 1, 'rent', '2020-10', 1490.0, '2020-10-01', 'paid', '2020-10-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (901, 2270, 1, 'rent', '2020-11', 1490.0, '2020-11-01', 'paid', '2020-11-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (901, 2270, 1, 'rent', '2020-12', 1490.0, '2020-12-01', 'paid', '2020-12-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (902, 2271, 1, 'rent', '2020-01', 21600.0, '2020-01-01', 'paid', '2020-01-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (902, 2271, 1, 'rent', '2021-01', 21600.0, '2021-01-01', 'paid', '2021-01-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (902, 2271, 1, 'rent', '2022-01', 21600.0, '2022-01-01', 'paid', '2022-01-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (902, 2271, 1, 'rent', '2023-01', 21600.0, '2023-01-01', 'paid', '2023-01-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (903, 2272, 1, 'rent', '2020-01', 21600.0, '2020-01-01', 'paid', '2020-01-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (903, 2272, 1, 'rent', '2021-01', 21600.0, '2021-01-01', 'paid', '2021-01-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (903, 2272, 1, 'rent', '2022-01', 21600.0, '2022-01-01', 'paid', '2022-01-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (903, 2272, 1, 'rent', '2023-01', 21600.0, '2023-01-01', 'paid', '2023-01-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (904, 2273, 1, 'rent', '2020-01', 3000.0, '2020-01-01', 'paid', '2020-01-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (904, 2273, 1, 'rent', '2020-02', 3000.0, '2020-02-01', 'paid', '2020-02-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (904, 2273, 1, 'rent', '2020-03', 3000.0, '2020-03-01', 'paid', '2020-03-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (904, 2273, 1, 'rent', '2020-04', 3000.0, '2020-04-01', 'paid', '2020-04-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (904, 2273, 1, 'rent', '2020-05', 3000.0, '2020-05-01', 'paid', '2020-05-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (904, 2273, 1, 'rent', '2020-06', 3000.0, '2020-06-01', 'paid', '2020-06-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (904, 2273, 1, 'rent', '2020-07', 3000.0, '2020-07-01', 'paid', '2020-07-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (904, 2273, 1, 'rent', '2020-08', 3000.0, '2020-08-01', 'paid', '2020-08-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (904, 2273, 1, 'rent', '2020-09', 3000.0, '2020-09-01', 'paid', '2020-09-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (904, 2273, 1, 'rent', '2020-10', 3000.0, '2020-10-01', 'paid', '2020-10-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (904, 2273, 1, 'rent', '2020-11', 3000.0, '2020-11-01', 'paid', '2020-11-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (904, 2273, 1, 'rent', '2020-12', 3000.0, '2020-12-01', 'paid', '2020-12-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (905, 2274, 1, 'rent', '2020-01', 18000.0, '2020-01-01', 'paid', '2020-01-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (905, 2274, 1, 'rent', '2021-01', 18000.0, '2021-01-01', 'paid', '2021-01-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (905, 2274, 1, 'rent', '2022-01', 18000.0, '2022-01-01', 'paid', '2022-01-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (905, 2274, 1, 'rent', '2023-01', 18000.0, '2023-01-01', 'paid', '2023-01-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (906, 2275, 1, 'rent', '2020-01', 12.0, '2020-01-01', 'paid', '2020-01-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (906, 2275, 1, 'rent', '2020-02', 12.0, '2020-02-01', 'paid', '2020-02-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (906, 2275, 1, 'rent', '2020-03', 12.0, '2020-03-01', 'paid', '2020-03-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (906, 2275, 1, 'rent', '2020-04', 12.0, '2020-04-01', 'paid', '2020-04-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (906, 2275, 1, 'rent', '2020-05', 12.0, '2020-05-01', 'paid', '2020-05-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (906, 2275, 1, 'rent', '2020-06', 12.0, '2020-06-01', 'paid', '2020-06-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (906, 2275, 1, 'rent', '2020-07', 12.0, '2020-07-01', 'paid', '2020-07-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (906, 2275, 1, 'rent', '2020-08', 12.0, '2020-08-01', 'paid', '2020-08-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (906, 2275, 1, 'rent', '2020-09', 12.0, '2020-09-01', 'paid', '2020-09-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (906, 2275, 1, 'rent', '2020-10', 12.0, '2020-10-01', 'paid', '2020-10-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (906, 2275, 1, 'rent', '2020-11', 12.0, '2020-11-01', 'paid', '2020-11-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (906, 2275, 1, 'rent', '2020-12', 12.0, '2020-12-01', 'paid', '2020-12-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (907, 2276, 1, 'rent', '2020-01', 3000.0, '2020-01-01', 'paid', '2020-01-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (907, 2276, 1, 'rent', '2020-02', 3000.0, '2020-02-01', 'paid', '2020-02-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (907, 2276, 1, 'rent', '2020-03', 3000.0, '2020-03-01', 'paid', '2020-03-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (907, 2276, 1, 'rent', '2020-04', 3000.0, '2020-04-01', 'paid', '2020-04-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (907, 2276, 1, 'rent', '2020-05', 3000.0, '2020-05-01', 'paid', '2020-05-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (907, 2276, 1, 'rent', '2020-06', 3000.0, '2020-06-01', 'paid', '2020-06-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (907, 2276, 1, 'rent', '2020-07', 3000.0, '2020-07-01', 'paid', '2020-07-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (907, 2276, 1, 'rent', '2020-08', 3000.0, '2020-08-01', 'paid', '2020-08-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (907, 2276, 1, 'rent', '2020-09', 3000.0, '2020-09-01', 'paid', '2020-09-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (907, 2276, 1, 'rent', '2020-10', 3000.0, '2020-10-01', 'paid', '2020-10-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (907, 2276, 1, 'rent', '2020-11', 3000.0, '2020-11-01', 'paid', '2020-11-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (907, 2276, 1, 'rent', '2020-12', 3000.0, '2020-12-01', 'paid', '2020-12-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (908, 2277, 1, 'rent', '2020-01', 3000.0, '2020-01-01', 'paid', '2020-01-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (908, 2277, 1, 'rent', '2020-02', 3000.0, '2020-02-01', 'paid', '2020-02-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (908, 2277, 1, 'rent', '2020-03', 3000.0, '2020-03-01', 'paid', '2020-03-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (908, 2277, 1, 'rent', '2020-04', 3000.0, '2020-04-01', 'paid', '2020-04-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (908, 2277, 1, 'rent', '2020-05', 3000.0, '2020-05-01', 'paid', '2020-05-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (908, 2277, 1, 'rent', '2020-06', 3000.0, '2020-06-01', 'paid', '2020-06-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (908, 2277, 1, 'rent', '2020-07', 3000.0, '2020-07-01', 'paid', '2020-07-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (908, 2277, 1, 'rent', '2020-08', 3000.0, '2020-08-01', 'paid', '2020-08-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (908, 2277, 1, 'rent', '2020-09', 3000.0, '2020-09-01', 'paid', '2020-09-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (908, 2277, 1, 'rent', '2020-10', 3000.0, '2020-10-01', 'paid', '2020-10-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (908, 2277, 1, 'rent', '2020-11', 3000.0, '2020-11-01', 'paid', '2020-11-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (908, 2277, 1, 'rent', '2020-12', 3000.0, '2020-12-01', 'paid', '2020-12-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (909, 2278, 1, 'rent', '2020-01', 1490.0, '2020-01-01', 'paid', '2020-01-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (909, 2278, 1, 'rent', '2020-02', 1490.0, '2020-02-01', 'paid', '2020-02-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (909, 2278, 1, 'rent', '2020-03', 1490.0, '2020-03-01', 'paid', '2020-03-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (909, 2278, 1, 'rent', '2020-04', 1490.0, '2020-04-01', 'paid', '2020-04-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (909, 2278, 1, 'rent', '2020-05', 1490.0, '2020-05-01', 'paid', '2020-05-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (909, 2278, 1, 'rent', '2020-06', 1490.0, '2020-06-01', 'paid', '2020-06-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (909, 2278, 1, 'rent', '2020-07', 1490.0, '2020-07-01', 'paid', '2020-07-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (909, 2278, 1, 'rent', '2020-08', 1490.0, '2020-08-01', 'paid', '2020-08-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (909, 2278, 1, 'rent', '2020-09', 1490.0, '2020-09-01', 'paid', '2020-09-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (909, 2278, 1, 'rent', '2020-10', 1490.0, '2020-10-01', 'paid', '2020-10-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (909, 2278, 1, 'rent', '2020-11', 1490.0, '2020-11-01', 'paid', '2020-11-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (909, 2278, 1, 'rent', '2020-12', 1490.0, '2020-12-01', 'paid', '2020-12-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (910, 2279, 1, 'rent', '2020-01', 1690.0, '2020-01-01', 'paid', '2020-01-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (910, 2279, 1, 'rent', '2020-02', 1690.0, '2020-02-01', 'paid', '2020-02-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (910, 2279, 1, 'rent', '2020-03', 1690.0, '2020-03-01', 'paid', '2020-03-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (910, 2279, 1, 'rent', '2020-04', 1690.0, '2020-04-01', 'paid', '2020-04-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (910, 2279, 1, 'rent', '2020-05', 1690.0, '2020-05-01', 'paid', '2020-05-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (910, 2279, 1, 'rent', '2020-06', 1690.0, '2020-06-01', 'paid', '2020-06-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (910, 2279, 1, 'rent', '2020-07', 1690.0, '2020-07-01', 'paid', '2020-07-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (910, 2279, 1, 'rent', '2020-08', 1690.0, '2020-08-01', 'paid', '2020-08-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (910, 2279, 1, 'rent', '2020-09', 1690.0, '2020-09-01', 'paid', '2020-09-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (910, 2279, 1, 'rent', '2020-10', 1690.0, '2020-10-01', 'paid', '2020-10-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (910, 2279, 1, 'rent', '2020-11', 1690.0, '2020-11-01', 'paid', '2020-11-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (910, 2279, 1, 'rent', '2020-12', 1690.0, '2020-12-01', 'paid', '2020-12-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (911, 2280, 1, 'rent', '2020-01', 21600.0, '2020-01-01', 'paid', '2020-01-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (911, 2280, 1, 'rent', '2021-01', 21600.0, '2021-01-01', 'paid', '2021-01-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (911, 2280, 1, 'rent', '2022-01', 21600.0, '2022-01-01', 'paid', '2022-01-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (911, 2280, 1, 'rent', '2023-01', 21600.0, '2023-01-01', 'paid', '2023-01-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (911, 2280, 1, 'rent', '2024-01', 21600.0, '2024-01-01', 'paid', '2024-01-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (912, 2281, 1, 'rent', '2020-01', 1500.0, '2020-01-01', 'paid', '2020-01-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (912, 2281, 1, 'rent', '2020-02', 1500.0, '2020-02-01', 'paid', '2020-02-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (912, 2281, 1, 'rent', '2020-03', 1500.0, '2020-03-01', 'paid', '2020-03-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (912, 2281, 1, 'rent', '2020-04', 1500.0, '2020-04-01', 'paid', '2020-04-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (912, 2281, 1, 'rent', '2020-05', 1500.0, '2020-05-01', 'paid', '2020-05-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (912, 2281, 1, 'rent', '2020-06', 1500.0, '2020-06-01', 'paid', '2020-06-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (912, 2281, 1, 'rent', '2020-07', 1500.0, '2020-07-01', 'paid', '2020-07-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (912, 2281, 1, 'rent', '2020-08', 1500.0, '2020-08-01', 'paid', '2020-08-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (912, 2281, 1, 'rent', '2020-09', 1500.0, '2020-09-01', 'paid', '2020-09-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (912, 2281, 1, 'rent', '2020-10', 1500.0, '2020-10-01', 'paid', '2020-10-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (912, 2281, 1, 'rent', '2020-11', 1500.0, '2020-11-01', 'paid', '2020-11-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (912, 2281, 1, 'rent', '2020-12', 1500.0, '2020-12-01', 'paid', '2020-12-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (913, 2282, 1, 'rent', '2020-01', 1490.0, '2020-01-01', 'paid', '2020-01-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (913, 2282, 1, 'rent', '2020-02', 1490.0, '2020-02-01', 'paid', '2020-02-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (913, 2282, 1, 'rent', '2020-03', 1490.0, '2020-03-01', 'paid', '2020-03-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (913, 2282, 1, 'rent', '2020-04', 1490.0, '2020-04-01', 'paid', '2020-04-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (913, 2282, 1, 'rent', '2020-05', 1490.0, '2020-05-01', 'paid', '2020-05-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (913, 2282, 1, 'rent', '2020-06', 1490.0, '2020-06-01', 'paid', '2020-06-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (913, 2282, 1, 'rent', '2020-07', 1490.0, '2020-07-01', 'paid', '2020-07-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (913, 2282, 1, 'rent', '2020-08', 1490.0, '2020-08-01', 'paid', '2020-08-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (913, 2282, 1, 'rent', '2020-09', 1490.0, '2020-09-01', 'paid', '2020-09-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (913, 2282, 1, 'rent', '2020-10', 1490.0, '2020-10-01', 'paid', '2020-10-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (913, 2282, 1, 'rent', '2020-11', 1490.0, '2020-11-01', 'paid', '2020-11-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (913, 2282, 1, 'rent', '2020-12', 1490.0, '2020-12-01', 'paid', '2020-12-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (914, 2283, 1, 'rent', '2020-01', 22000.0, '2020-01-01', 'paid', '2020-01-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (914, 2283, 1, 'rent', '2020-02', 22000.0, '2020-02-01', 'paid', '2020-02-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (914, 2283, 1, 'rent', '2020-03', 22000.0, '2020-03-01', 'paid', '2020-03-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (914, 2283, 1, 'rent', '2020-04', 22000.0, '2020-04-01', 'paid', '2020-04-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (914, 2283, 1, 'rent', '2020-05', 22000.0, '2020-05-01', 'paid', '2020-05-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (914, 2283, 1, 'rent', '2020-06', 22000.0, '2020-06-01', 'paid', '2020-06-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (914, 2283, 1, 'rent', '2020-07', 22000.0, '2020-07-01', 'paid', '2020-07-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (914, 2283, 1, 'rent', '2020-08', 22000.0, '2020-08-01', 'paid', '2020-08-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (914, 2283, 1, 'rent', '2020-09', 22000.0, '2020-09-01', 'paid', '2020-09-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (914, 2283, 1, 'rent', '2020-10', 22000.0, '2020-10-01', 'paid', '2020-10-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (914, 2283, 1, 'rent', '2020-11', 22000.0, '2020-11-01', 'paid', '2020-11-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (914, 2283, 1, 'rent', '2020-12', 22000.0, '2020-12-01', 'paid', '2020-12-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (915, 2284, 1, 'rent', '2020-01', 21600.0, '2020-01-01', 'paid', '2020-01-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (915, 2284, 1, 'rent', '2021-01', 21600.0, '2021-01-01', 'paid', '2021-01-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (915, 2284, 1, 'rent', '2022-01', 21600.0, '2022-01-01', 'paid', '2022-01-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (915, 2284, 1, 'rent', '2023-01', 21600.0, '2023-01-01', 'paid', '2023-01-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (916, 2285, 1, 'rent', '2020-01', 1490.0, '2020-01-01', 'paid', '2020-01-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (916, 2285, 1, 'rent', '2020-02', 1490.0, '2020-02-01', 'paid', '2020-02-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (916, 2285, 1, 'rent', '2020-03', 1490.0, '2020-03-01', 'paid', '2020-03-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (916, 2285, 1, 'rent', '2020-04', 1490.0, '2020-04-01', 'paid', '2020-04-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (916, 2285, 1, 'rent', '2020-05', 1490.0, '2020-05-01', 'paid', '2020-05-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (916, 2285, 1, 'rent', '2020-06', 1490.0, '2020-06-01', 'paid', '2020-06-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (916, 2285, 1, 'rent', '2020-07', 1490.0, '2020-07-01', 'paid', '2020-07-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (916, 2285, 1, 'rent', '2020-08', 1490.0, '2020-08-01', 'paid', '2020-08-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (916, 2285, 1, 'rent', '2020-09', 1490.0, '2020-09-01', 'paid', '2020-09-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (916, 2285, 1, 'rent', '2020-10', 1490.0, '2020-10-01', 'paid', '2020-10-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (916, 2285, 1, 'rent', '2020-11', 1490.0, '2020-11-01', 'paid', '2020-11-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (916, 2285, 1, 'rent', '2020-12', 1490.0, '2020-12-01', 'paid', '2020-12-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (917, 2236, 1, 'rent', '2021-09', 1490.0, '2021-09-01', 'paid', '2021-09-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (917, 2236, 1, 'rent', '2021-10', 1490.0, '2021-10-01', 'paid', '2021-10-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (917, 2236, 1, 'rent', '2021-11', 1490.0, '2021-11-01', 'paid', '2021-11-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (917, 2236, 1, 'rent', '2021-12', 1490.0, '2021-12-01', 'paid', '2021-12-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (917, 2236, 1, 'rent', '2022-01', 1490.0, '2022-01-01', 'paid', '2022-01-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (917, 2236, 1, 'rent', '2022-02', 1490.0, '2022-02-01', 'paid', '2022-02-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (917, 2236, 1, 'rent', '2022-03', 1490.0, '2022-03-01', 'paid', '2022-03-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (917, 2236, 1, 'rent', '2022-04', 1490.0, '2022-04-01', 'paid', '2022-04-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (917, 2236, 1, 'rent', '2022-05', 1490.0, '2022-05-01', 'paid', '2022-05-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (917, 2236, 1, 'rent', '2022-06', 1490.0, '2022-06-01', 'paid', '2022-06-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (917, 2236, 1, 'rent', '2022-07', 1490.0, '2022-07-01', 'paid', '2022-07-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (917, 2236, 1, 'rent', '2022-08', 1490.0, '2022-08-01', 'paid', '2022-08-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (918, 2287, 1, 'rent', '2020-01', 17880.0, '2020-01-01', 'paid', '2020-01-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (918, 2287, 1, 'rent', '2021-01', 17880.0, '2021-01-01', 'paid', '2021-01-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (918, 2287, 1, 'rent', '2022-01', 17880.0, '2022-01-01', 'paid', '2022-01-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (918, 2287, 1, 'rent', '2023-01', 17880.0, '2023-01-01', 'paid', '2023-01-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (919, 2288, 1, 'rent', '2020-01', 1490.0, '2020-01-01', 'paid', '2020-01-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (919, 2288, 1, 'rent', '2020-02', 1490.0, '2020-02-01', 'paid', '2020-02-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (919, 2288, 1, 'rent', '2020-03', 1490.0, '2020-03-01', 'paid', '2020-03-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (919, 2288, 1, 'rent', '2020-04', 1490.0, '2020-04-01', 'paid', '2020-04-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (919, 2288, 1, 'rent', '2020-05', 1490.0, '2020-05-01', 'paid', '2020-05-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (919, 2288, 1, 'rent', '2020-06', 1490.0, '2020-06-01', 'paid', '2020-06-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (919, 2288, 1, 'rent', '2020-07', 1490.0, '2020-07-01', 'paid', '2020-07-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (919, 2288, 1, 'rent', '2020-08', 1490.0, '2020-08-01', 'paid', '2020-08-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (919, 2288, 1, 'rent', '2020-09', 1490.0, '2020-09-01', 'paid', '2020-09-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (919, 2288, 1, 'rent', '2020-10', 1490.0, '2020-10-01', 'paid', '2020-10-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (919, 2288, 1, 'rent', '2020-11', 1490.0, '2020-11-01', 'paid', '2020-11-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (919, 2288, 1, 'rent', '2020-12', 1490.0, '2020-12-01', 'paid', '2020-12-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (920, 2289, 1, 'rent', '2020-01', 1490.0, '2020-01-01', 'paid', '2020-01-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (920, 2289, 1, 'rent', '2020-02', 1490.0, '2020-02-01', 'paid', '2020-02-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (920, 2289, 1, 'rent', '2020-03', 1490.0, '2020-03-01', 'paid', '2020-03-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (920, 2289, 1, 'rent', '2020-04', 1490.0, '2020-04-01', 'paid', '2020-04-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (920, 2289, 1, 'rent', '2020-05', 1490.0, '2020-05-01', 'paid', '2020-05-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (920, 2289, 1, 'rent', '2020-06', 1490.0, '2020-06-01', 'paid', '2020-06-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (920, 2289, 1, 'rent', '2020-07', 1490.0, '2020-07-01', 'paid', '2020-07-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (920, 2289, 1, 'rent', '2020-08', 1490.0, '2020-08-01', 'paid', '2020-08-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (920, 2289, 1, 'rent', '2020-09', 1490.0, '2020-09-01', 'paid', '2020-09-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (920, 2289, 1, 'rent', '2020-10', 1490.0, '2020-10-01', 'paid', '2020-10-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (920, 2289, 1, 'rent', '2020-11', 1490.0, '2020-11-01', 'paid', '2020-11-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (920, 2289, 1, 'rent', '2020-12', 1490.0, '2020-12-01', 'paid', '2020-12-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (921, 2290, 1, 'rent', '2020-01', 21600.0, '2020-01-01', 'paid', '2020-01-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (921, 2290, 1, 'rent', '2021-01', 21600.0, '2021-01-01', 'paid', '2021-01-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (921, 2290, 1, 'rent', '2022-01', 21600.0, '2022-01-01', 'paid', '2022-01-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (921, 2290, 1, 'rent', '2023-01', 21600.0, '2023-01-01', 'paid', '2023-01-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (922, 2291, 1, 'rent', '2020-01', 1490.0, '2020-01-01', 'paid', '2020-01-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (922, 2291, 1, 'rent', '2020-02', 1490.0, '2020-02-01', 'paid', '2020-02-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (922, 2291, 1, 'rent', '2020-03', 1490.0, '2020-03-01', 'paid', '2020-03-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (922, 2291, 1, 'rent', '2020-04', 1490.0, '2020-04-01', 'paid', '2020-04-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (922, 2291, 1, 'rent', '2020-05', 1490.0, '2020-05-01', 'paid', '2020-05-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (922, 2291, 1, 'rent', '2020-06', 1490.0, '2020-06-01', 'paid', '2020-06-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (922, 2291, 1, 'rent', '2020-07', 1490.0, '2020-07-01', 'paid', '2020-07-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (922, 2291, 1, 'rent', '2020-08', 1490.0, '2020-08-01', 'paid', '2020-08-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (922, 2291, 1, 'rent', '2020-09', 1490.0, '2020-09-01', 'paid', '2020-09-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (922, 2291, 1, 'rent', '2020-10', 1490.0, '2020-10-01', 'paid', '2020-10-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (922, 2291, 1, 'rent', '2020-11', 1490.0, '2020-11-01', 'paid', '2020-11-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (922, 2291, 1, 'rent', '2020-12', 1490.0, '2020-12-01', 'paid', '2020-12-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (923, 2292, 1, 'rent', '2020-01', 1800.0, '2020-01-01', 'paid', '2020-01-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (923, 2292, 1, 'rent', '2020-02', 1800.0, '2020-02-01', 'paid', '2020-02-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (923, 2292, 1, 'rent', '2020-03', 1800.0, '2020-03-01', 'paid', '2020-03-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (923, 2292, 1, 'rent', '2020-04', 1800.0, '2020-04-01', 'paid', '2020-04-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (923, 2292, 1, 'rent', '2020-05', 1800.0, '2020-05-01', 'paid', '2020-05-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (923, 2292, 1, 'rent', '2020-06', 1800.0, '2020-06-01', 'paid', '2020-06-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (923, 2292, 1, 'rent', '2020-07', 1800.0, '2020-07-01', 'paid', '2020-07-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (923, 2292, 1, 'rent', '2020-08', 1800.0, '2020-08-01', 'paid', '2020-08-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (923, 2292, 1, 'rent', '2020-09', 1800.0, '2020-09-01', 'paid', '2020-09-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (923, 2292, 1, 'rent', '2020-10', 1800.0, '2020-10-01', 'paid', '2020-10-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (923, 2292, 1, 'rent', '2020-11', 1800.0, '2020-11-01', 'paid', '2020-11-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (923, 2292, 1, 'rent', '2020-12', 1800.0, '2020-12-01', 'paid', '2020-12-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (924, 2293, 1, 'rent', '2020-01', 21600.0, '2020-01-01', 'paid', '2020-01-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (924, 2293, 1, 'rent', '2021-01', 21600.0, '2021-01-01', 'paid', '2021-01-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (924, 2293, 1, 'rent', '2022-01', 21600.0, '2022-01-01', 'paid', '2022-01-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (924, 2293, 1, 'rent', '2023-01', 21600.0, '2023-01-01', 'paid', '2023-01-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (924, 2293, 1, 'rent', '2024-01', 21600.0, '2024-01-01', 'paid', '2024-01-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (925, 2294, 1, 'rent', '2020-01', 21600.0, '2020-01-01', 'paid', '2020-01-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (925, 2294, 1, 'rent', '2021-01', 21600.0, '2021-01-01', 'paid', '2021-01-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (925, 2294, 1, 'rent', '2022-01', 21600.0, '2022-01-01', 'paid', '2022-01-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (925, 2294, 1, 'rent', '2023-01', 21600.0, '2023-01-01', 'paid', '2023-01-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (925, 2294, 1, 'rent', '2024-01', 21600.0, '2024-01-01', 'paid', '2024-01-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (926, 2295, 1, 'rent', '2020-01', 1690.0, '2020-01-01', 'paid', '2020-01-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (926, 2295, 1, 'rent', '2020-02', 1690.0, '2020-02-01', 'paid', '2020-02-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (926, 2295, 1, 'rent', '2020-03', 1690.0, '2020-03-01', 'paid', '2020-03-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (926, 2295, 1, 'rent', '2020-04', 1690.0, '2020-04-01', 'paid', '2020-04-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (926, 2295, 1, 'rent', '2020-05', 1690.0, '2020-05-01', 'paid', '2020-05-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (926, 2295, 1, 'rent', '2020-06', 1690.0, '2020-06-01', 'paid', '2020-06-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (926, 2295, 1, 'rent', '2020-07', 1690.0, '2020-07-01', 'paid', '2020-07-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (926, 2295, 1, 'rent', '2020-08', 1690.0, '2020-08-01', 'paid', '2020-08-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (926, 2295, 1, 'rent', '2020-09', 1690.0, '2020-09-01', 'paid', '2020-09-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (926, 2295, 1, 'rent', '2020-10', 1690.0, '2020-10-01', 'paid', '2020-10-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (926, 2295, 1, 'rent', '2020-11', 1690.0, '2020-11-01', 'paid', '2020-11-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (926, 2295, 1, 'rent', '2020-12', 1690.0, '2020-12-01', 'paid', '2020-12-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (927, 2296, 1, 'rent', '2020-01', 1690.0, '2020-01-01', 'paid', '2020-01-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (927, 2296, 1, 'rent', '2020-02', 1690.0, '2020-02-01', 'paid', '2020-02-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (927, 2296, 1, 'rent', '2020-03', 1690.0, '2020-03-01', 'paid', '2020-03-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (927, 2296, 1, 'rent', '2020-04', 1690.0, '2020-04-01', 'paid', '2020-04-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (927, 2296, 1, 'rent', '2020-05', 1690.0, '2020-05-01', 'paid', '2020-05-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (927, 2296, 1, 'rent', '2020-06', 1690.0, '2020-06-01', 'paid', '2020-06-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (927, 2296, 1, 'rent', '2020-07', 1690.0, '2020-07-01', 'paid', '2020-07-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (927, 2296, 1, 'rent', '2020-08', 1690.0, '2020-08-01', 'paid', '2020-08-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (927, 2296, 1, 'rent', '2020-09', 1690.0, '2020-09-01', 'paid', '2020-09-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (927, 2296, 1, 'rent', '2020-10', 1690.0, '2020-10-01', 'paid', '2020-10-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (927, 2296, 1, 'rent', '2020-11', 1690.0, '2020-11-01', 'paid', '2020-11-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (927, 2296, 1, 'rent', '2020-12', 1690.0, '2020-12-01', 'paid', '2020-12-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (928, 2297, 1, 'rent', '2020-01', 1690.0, '2020-01-01', 'paid', '2020-01-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (928, 2297, 1, 'rent', '2020-02', 1690.0, '2020-02-01', 'paid', '2020-02-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (928, 2297, 1, 'rent', '2020-03', 1690.0, '2020-03-01', 'paid', '2020-03-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (928, 2297, 1, 'rent', '2020-04', 1690.0, '2020-04-01', 'paid', '2020-04-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (928, 2297, 1, 'rent', '2020-05', 1690.0, '2020-05-01', 'paid', '2020-05-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (928, 2297, 1, 'rent', '2020-06', 1690.0, '2020-06-01', 'paid', '2020-06-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (928, 2297, 1, 'rent', '2020-07', 1690.0, '2020-07-01', 'paid', '2020-07-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (928, 2297, 1, 'rent', '2020-08', 1690.0, '2020-08-01', 'paid', '2020-08-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (928, 2297, 1, 'rent', '2020-09', 1690.0, '2020-09-01', 'paid', '2020-09-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (928, 2297, 1, 'rent', '2020-10', 1690.0, '2020-10-01', 'paid', '2020-10-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (928, 2297, 1, 'rent', '2020-11', 1690.0, '2020-11-01', 'paid', '2020-11-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (928, 2297, 1, 'rent', '2020-12', 1690.0, '2020-12-01', 'paid', '2020-12-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (929, 2298, 1, 'rent', '2020-01', 1490.0, '2020-01-01', 'paid', '2020-01-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (929, 2298, 1, 'rent', '2020-02', 1490.0, '2020-02-01', 'paid', '2020-02-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (929, 2298, 1, 'rent', '2020-03', 1490.0, '2020-03-01', 'paid', '2020-03-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (929, 2298, 1, 'rent', '2020-04', 1490.0, '2020-04-01', 'paid', '2020-04-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (929, 2298, 1, 'rent', '2020-05', 1490.0, '2020-05-01', 'paid', '2020-05-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (929, 2298, 1, 'rent', '2020-06', 1490.0, '2020-06-01', 'paid', '2020-06-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (929, 2298, 1, 'rent', '2020-07', 1490.0, '2020-07-01', 'paid', '2020-07-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (929, 2298, 1, 'rent', '2020-08', 1490.0, '2020-08-01', 'paid', '2020-08-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (929, 2298, 1, 'rent', '2020-09', 1490.0, '2020-09-01', 'paid', '2020-09-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (929, 2298, 1, 'rent', '2020-10', 1490.0, '2020-10-01', 'paid', '2020-10-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (929, 2298, 1, 'rent', '2020-11', 1490.0, '2020-11-01', 'paid', '2020-11-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (929, 2298, 1, 'rent', '2020-12', 1490.0, '2020-12-01', 'paid', '2020-12-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (930, 2299, 1, 'rent', '2020-01', 1490.0, '2020-01-01', 'paid', '2020-01-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (930, 2299, 1, 'rent', '2020-02', 1490.0, '2020-02-01', 'paid', '2020-02-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (930, 2299, 1, 'rent', '2020-03', 1490.0, '2020-03-01', 'paid', '2020-03-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (930, 2299, 1, 'rent', '2020-04', 1490.0, '2020-04-01', 'paid', '2020-04-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (930, 2299, 1, 'rent', '2020-05', 1490.0, '2020-05-01', 'paid', '2020-05-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (930, 2299, 1, 'rent', '2020-06', 1490.0, '2020-06-01', 'paid', '2020-06-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (930, 2299, 1, 'rent', '2020-07', 1490.0, '2020-07-01', 'paid', '2020-07-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (930, 2299, 1, 'rent', '2020-08', 1490.0, '2020-08-01', 'paid', '2020-08-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (930, 2299, 1, 'rent', '2020-09', 1490.0, '2020-09-01', 'paid', '2020-09-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (930, 2299, 1, 'rent', '2020-10', 1490.0, '2020-10-01', 'paid', '2020-10-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (930, 2299, 1, 'rent', '2020-11', 1490.0, '2020-11-01', 'paid', '2020-11-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (930, 2299, 1, 'rent', '2020-12', 1490.0, '2020-12-01', 'paid', '2020-12-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (931, 2300, 1, 'rent', '2020-01', 1490.0, '2020-01-01', 'paid', '2020-01-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (931, 2300, 1, 'rent', '2020-02', 1490.0, '2020-02-01', 'paid', '2020-02-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (931, 2300, 1, 'rent', '2020-03', 1490.0, '2020-03-01', 'paid', '2020-03-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (931, 2300, 1, 'rent', '2020-04', 1490.0, '2020-04-01', 'paid', '2020-04-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (931, 2300, 1, 'rent', '2020-05', 1490.0, '2020-05-01', 'paid', '2020-05-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (931, 2300, 1, 'rent', '2020-06', 1490.0, '2020-06-01', 'paid', '2020-06-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (931, 2300, 1, 'rent', '2020-07', 1490.0, '2020-07-01', 'paid', '2020-07-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (931, 2300, 1, 'rent', '2020-08', 1490.0, '2020-08-01', 'paid', '2020-08-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (931, 2300, 1, 'rent', '2020-09', 1490.0, '2020-09-01', 'paid', '2020-09-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (931, 2300, 1, 'rent', '2020-10', 1490.0, '2020-10-01', 'paid', '2020-10-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (931, 2300, 1, 'rent', '2020-11', 1490.0, '2020-11-01', 'paid', '2020-11-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (931, 2300, 1, 'rent', '2020-12', 1490.0, '2020-12-01', 'paid', '2020-12-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (932, 2301, 1, 'rent', '2020-01', 1490.0, '2020-01-01', 'paid', '2020-01-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (932, 2301, 1, 'rent', '2020-02', 1490.0, '2020-02-01', 'paid', '2020-02-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (932, 2301, 1, 'rent', '2020-03', 1490.0, '2020-03-01', 'paid', '2020-03-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (932, 2301, 1, 'rent', '2020-04', 1490.0, '2020-04-01', 'paid', '2020-04-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (932, 2301, 1, 'rent', '2020-05', 1490.0, '2020-05-01', 'paid', '2020-05-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (932, 2301, 1, 'rent', '2020-06', 1490.0, '2020-06-01', 'paid', '2020-06-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (932, 2301, 1, 'rent', '2020-07', 1490.0, '2020-07-01', 'paid', '2020-07-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (932, 2301, 1, 'rent', '2020-08', 1490.0, '2020-08-01', 'paid', '2020-08-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (932, 2301, 1, 'rent', '2020-09', 1490.0, '2020-09-01', 'paid', '2020-09-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (932, 2301, 1, 'rent', '2020-10', 1490.0, '2020-10-01', 'paid', '2020-10-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (932, 2301, 1, 'rent', '2020-11', 1490.0, '2020-11-01', 'paid', '2020-11-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (932, 2301, 1, 'rent', '2020-12', 1490.0, '2020-12-01', 'paid', '2020-12-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (933, 2302, 1, 'rent', '2020-01', 21600.0, '2020-01-01', 'paid', '2020-01-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (933, 2302, 1, 'rent', '2021-01', 21600.0, '2021-01-01', 'paid', '2021-01-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (933, 2302, 1, 'rent', '2022-01', 21600.0, '2022-01-01', 'paid', '2022-01-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (933, 2302, 1, 'rent', '2023-01', 21600.0, '2023-01-01', 'paid', '2023-01-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (933, 2302, 1, 'rent', '2024-01', 21600.0, '2024-01-01', 'paid', '2024-01-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (934, 2303, 1, 'rent', '2020-01', 21600.0, '2020-01-01', 'paid', '2020-01-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (934, 2303, 1, 'rent', '2021-01', 21600.0, '2021-01-01', 'paid', '2021-01-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (934, 2303, 1, 'rent', '2022-01', 21600.0, '2022-01-01', 'paid', '2022-01-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (934, 2303, 1, 'rent', '2023-01', 21600.0, '2023-01-01', 'paid', '2023-01-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (934, 2303, 1, 'rent', '2024-01', 21600.0, '2024-01-01', 'paid', '2024-01-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (935, 2304, 1, 'rent', '2020-01', 17880.0, '2020-01-01', 'paid', '2020-01-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (935, 2304, 1, 'rent', '2021-01', 17880.0, '2021-01-01', 'paid', '2021-01-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (935, 2304, 1, 'rent', '2022-01', 17880.0, '2022-01-01', 'paid', '2022-01-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (935, 2304, 1, 'rent', '2023-01', 17880.0, '2023-01-01', 'paid', '2023-01-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (935, 2304, 1, 'rent', '2024-01', 17880.0, '2024-01-01', 'paid', '2024-01-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (936, 2305, 1, 'rent', '2020-01', 1490.0, '2020-01-01', 'paid', '2020-01-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (936, 2305, 1, 'rent', '2020-02', 1490.0, '2020-02-01', 'paid', '2020-02-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (936, 2305, 1, 'rent', '2020-03', 1490.0, '2020-03-01', 'paid', '2020-03-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (936, 2305, 1, 'rent', '2020-04', 1490.0, '2020-04-01', 'paid', '2020-04-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (936, 2305, 1, 'rent', '2020-05', 1490.0, '2020-05-01', 'paid', '2020-05-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (936, 2305, 1, 'rent', '2020-06', 1490.0, '2020-06-01', 'paid', '2020-06-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (936, 2305, 1, 'rent', '2020-07', 1490.0, '2020-07-01', 'paid', '2020-07-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (936, 2305, 1, 'rent', '2020-08', 1490.0, '2020-08-01', 'paid', '2020-08-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (936, 2305, 1, 'rent', '2020-09', 1490.0, '2020-09-01', 'paid', '2020-09-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (936, 2305, 1, 'rent', '2020-10', 1490.0, '2020-10-01', 'paid', '2020-10-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (936, 2305, 1, 'rent', '2020-11', 1490.0, '2020-11-01', 'paid', '2020-11-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (936, 2305, 1, 'rent', '2020-12', 1490.0, '2020-12-01', 'paid', '2020-12-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (937, 2306, 1, 'rent', '2020-01', 21600.0, '2020-01-01', 'paid', '2020-01-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (937, 2306, 1, 'rent', '2021-01', 21600.0, '2021-01-01', 'paid', '2021-01-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (937, 2306, 1, 'rent', '2022-01', 21600.0, '2022-01-01', 'paid', '2022-01-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (937, 2306, 1, 'rent', '2023-01', 21600.0, '2023-01-01', 'paid', '2023-01-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (937, 2306, 1, 'rent', '2024-01', 21600.0, '2024-01-01', 'paid', '2024-01-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (938, 2307, 1, 'rent', '2020-01', 2000.0, '2020-01-01', 'paid', '2020-01-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (938, 2307, 1, 'rent', '2020-02', 2000.0, '2020-02-01', 'paid', '2020-02-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (938, 2307, 1, 'rent', '2020-03', 2000.0, '2020-03-01', 'paid', '2020-03-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (938, 2307, 1, 'rent', '2020-04', 2000.0, '2020-04-01', 'paid', '2020-04-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (938, 2307, 1, 'rent', '2020-05', 2000.0, '2020-05-01', 'paid', '2020-05-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (938, 2307, 1, 'rent', '2020-06', 2000.0, '2020-06-01', 'paid', '2020-06-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (938, 2307, 1, 'rent', '2020-07', 2000.0, '2020-07-01', 'paid', '2020-07-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (938, 2307, 1, 'rent', '2020-08', 2000.0, '2020-08-01', 'paid', '2020-08-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (938, 2307, 1, 'rent', '2020-09', 2000.0, '2020-09-01', 'paid', '2020-09-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (938, 2307, 1, 'rent', '2020-10', 2000.0, '2020-10-01', 'paid', '2020-10-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (938, 2307, 1, 'rent', '2020-11', 2000.0, '2020-11-01', 'paid', '2020-11-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (938, 2307, 1, 'rent', '2020-12', 2000.0, '2020-12-01', 'paid', '2020-12-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (939, 2308, 1, 'rent', '2020-01', 1490.0, '2020-01-01', 'paid', '2020-01-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (939, 2308, 1, 'rent', '2020-02', 1490.0, '2020-02-01', 'paid', '2020-02-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (939, 2308, 1, 'rent', '2020-03', 1490.0, '2020-03-01', 'paid', '2020-03-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (939, 2308, 1, 'rent', '2020-04', 1490.0, '2020-04-01', 'paid', '2020-04-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (939, 2308, 1, 'rent', '2020-05', 1490.0, '2020-05-01', 'paid', '2020-05-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (939, 2308, 1, 'rent', '2020-06', 1490.0, '2020-06-01', 'paid', '2020-06-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (939, 2308, 1, 'rent', '2020-07', 1490.0, '2020-07-01', 'paid', '2020-07-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (939, 2308, 1, 'rent', '2020-08', 1490.0, '2020-08-01', 'paid', '2020-08-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (939, 2308, 1, 'rent', '2020-09', 1490.0, '2020-09-01', 'paid', '2020-09-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (939, 2308, 1, 'rent', '2020-10', 1490.0, '2020-10-01', 'paid', '2020-10-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (939, 2308, 1, 'rent', '2020-11', 1490.0, '2020-11-01', 'paid', '2020-11-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (939, 2308, 1, 'rent', '2020-12', 1490.0, '2020-12-01', 'paid', '2020-12-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (940, 2309, 1, 'rent', '2020-01', 20280.0, '2020-01-01', 'paid', '2020-01-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (940, 2309, 1, 'rent', '2021-01', 20280.0, '2021-01-01', 'paid', '2021-01-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (940, 2309, 1, 'rent', '2022-01', 20280.0, '2022-01-01', 'paid', '2022-01-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (940, 2309, 1, 'rent', '2023-01', 20280.0, '2023-01-01', 'paid', '2023-01-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (940, 2309, 1, 'rent', '2024-01', 20280.0, '2024-01-01', 'paid', '2024-01-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (941, 2310, 1, 'rent', '2020-01', 21600.0, '2020-01-01', 'paid', '2020-01-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (941, 2310, 1, 'rent', '2021-01', 21600.0, '2021-01-01', 'paid', '2021-01-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (941, 2310, 1, 'rent', '2022-01', 21600.0, '2022-01-01', 'paid', '2022-01-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (941, 2310, 1, 'rent', '2023-01', 21600.0, '2023-01-01', 'paid', '2023-01-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (941, 2310, 1, 'rent', '2024-01', 21600.0, '2024-01-01', 'paid', '2024-01-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (942, 2311, 1, 'rent', '2020-01', 1490.0, '2020-01-01', 'paid', '2020-01-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (942, 2311, 1, 'rent', '2020-02', 1490.0, '2020-02-01', 'paid', '2020-02-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (942, 2311, 1, 'rent', '2020-03', 1490.0, '2020-03-01', 'paid', '2020-03-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (942, 2311, 1, 'rent', '2020-04', 1490.0, '2020-04-01', 'paid', '2020-04-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (942, 2311, 1, 'rent', '2020-05', 1490.0, '2020-05-01', 'paid', '2020-05-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (942, 2311, 1, 'rent', '2020-06', 1490.0, '2020-06-01', 'paid', '2020-06-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (942, 2311, 1, 'rent', '2020-07', 1490.0, '2020-07-01', 'paid', '2020-07-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (942, 2311, 1, 'rent', '2020-08', 1490.0, '2020-08-01', 'paid', '2020-08-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (942, 2311, 1, 'rent', '2020-09', 1490.0, '2020-09-01', 'paid', '2020-09-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (942, 2311, 1, 'rent', '2020-10', 1490.0, '2020-10-01', 'paid', '2020-10-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (942, 2311, 1, 'rent', '2020-11', 1490.0, '2020-11-01', 'paid', '2020-11-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (942, 2311, 1, 'rent', '2020-12', 1490.0, '2020-12-01', 'paid', '2020-12-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (943, 2312, 1, 'rent', '2020-01', 1490.0, '2020-01-01', 'paid', '2020-01-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (943, 2312, 1, 'rent', '2020-02', 1490.0, '2020-02-01', 'paid', '2020-02-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (943, 2312, 1, 'rent', '2020-03', 1490.0, '2020-03-01', 'paid', '2020-03-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (943, 2312, 1, 'rent', '2020-04', 1490.0, '2020-04-01', 'paid', '2020-04-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (943, 2312, 1, 'rent', '2020-05', 1490.0, '2020-05-01', 'paid', '2020-05-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (943, 2312, 1, 'rent', '2020-06', 1490.0, '2020-06-01', 'paid', '2020-06-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (943, 2312, 1, 'rent', '2020-07', 1490.0, '2020-07-01', 'paid', '2020-07-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (943, 2312, 1, 'rent', '2020-08', 1490.0, '2020-08-01', 'paid', '2020-08-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (943, 2312, 1, 'rent', '2020-09', 1490.0, '2020-09-01', 'paid', '2020-09-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (943, 2312, 1, 'rent', '2020-10', 1490.0, '2020-10-01', 'paid', '2020-10-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (943, 2312, 1, 'rent', '2020-11', 1490.0, '2020-11-01', 'paid', '2020-11-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (943, 2312, 1, 'rent', '2020-12', 1490.0, '2020-12-01', 'paid', '2020-12-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (944, 2313, 1, 'rent', '2020-01', 1690.0, '2020-01-01', 'paid', '2020-01-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (944, 2313, 1, 'rent', '2020-02', 1690.0, '2020-02-01', 'paid', '2020-02-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (944, 2313, 1, 'rent', '2020-03', 1690.0, '2020-03-01', 'paid', '2020-03-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (944, 2313, 1, 'rent', '2020-04', 1690.0, '2020-04-01', 'paid', '2020-04-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (944, 2313, 1, 'rent', '2020-05', 1690.0, '2020-05-01', 'paid', '2020-05-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (944, 2313, 1, 'rent', '2020-06', 1690.0, '2020-06-01', 'paid', '2020-06-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (944, 2313, 1, 'rent', '2020-07', 1690.0, '2020-07-01', 'paid', '2020-07-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (944, 2313, 1, 'rent', '2020-08', 1690.0, '2020-08-01', 'paid', '2020-08-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (944, 2313, 1, 'rent', '2020-09', 1690.0, '2020-09-01', 'paid', '2020-09-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (944, 2313, 1, 'rent', '2020-10', 1690.0, '2020-10-01', 'paid', '2020-10-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (944, 2313, 1, 'rent', '2020-11', 1690.0, '2020-11-01', 'paid', '2020-11-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (944, 2313, 1, 'rent', '2020-12', 1690.0, '2020-12-01', 'paid', '2020-12-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (945, 2314, 1, 'rent', '2020-01', 21600.0, '2020-01-01', 'paid', '2020-01-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (945, 2314, 1, 'rent', '2021-01', 21600.0, '2021-01-01', 'paid', '2021-01-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (945, 2314, 1, 'rent', '2022-01', 21600.0, '2022-01-01', 'paid', '2022-01-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (945, 2314, 1, 'rent', '2023-01', 21600.0, '2023-01-01', 'paid', '2023-01-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (945, 2314, 1, 'rent', '2024-01', 21600.0, '2024-01-01', 'paid', '2024-01-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (946, 2315, 2, 'rent', '2025-04', 10800.0, '2025-04-01', 'paid', '2025-04-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (946, 2315, 2, 'rent', '2025-10', 10800.0, '2025-10-01', 'paid', '2025-10-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (946, 2315, 2, 'rent', '2026-04', 10800.0, '2026-04-01', 'paid', '2026-04-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (946, 2315, 2, 'rent', '2026-10', 10800.0, '2026-10-01', 'paid', '2026-10-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (946, 2315, 2, 'rent', '2027-04', 10800.0, '2027-04-01', 'paid', '2027-04-03', 'transfer');

-- 統計: 已繳 1578 筆 ($7,013,928), 逾期 38 筆 ($438,155), 待繳 280 筆 ($2,352,905)

-- 驗證
SELECT payment_status, COUNT(*) as count, SUM(amount) as total FROM payments GROUP BY payment_status ORDER BY payment_status;