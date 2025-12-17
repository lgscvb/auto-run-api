-- ============================================================================
-- Hour Jungle CRM - 真實繳費記錄匯入
-- 生成時間: 2025-12-07 14:19:17
-- ============================================================================

-- 清空現有繳費記錄
TRUNCATE payments RESTART IDENTITY;

INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (618, 1968, 1, 'rent', '2025-01', 21600, '2025-01-01', 'paid', '2025-01-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (641, 1991, 1, 'rent', '2025-01', 30000, '2025-01-01', 'paid', '2025-01-20', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (579, 1929, 1, 'rent', '2025-01', 10800, '2025-01-01', 'paid', '2025-01-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (557, 1907, 1, 'rent', '2025-01', 8940, '2025-01-01', 'paid', '2025-01-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (576, 1926, 1, 'rent', '2025-01', 8940, '2025-01-01', 'paid', '2024-12-30', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (947, 2316, 1, 'rent', '2025-01', 8940, '2025-01-01', 'paid', '2024-08-14', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (600, 1950, 1, 'rent', '2025-01', 8940, '2025-01-01', 'paid', '2025-01-20', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (609, 1959, 1, 'rent', '2025-01', 8940, '2025-01-01', 'paid', '2025-01-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (616, 1966, 1, 'rent', '2025-01', 8940, '2025-01-01', 'paid', '2025-01-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (617, 1967, 1, 'rent', '2025-01', 8949, '2025-01-01', 'paid', '2025-01-13', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (959, 2337, 1, 'rent', '2025-01', 12000, '2025-01-01', 'paid', '2025-01-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (630, 1980, 1, 'rent', '2025-01', 10140, '2025-01-01', 'paid', '2025-01-21', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (629, 1979, 1, 'rent', '2025-01', 9880, '2025-01-01', 'paid', '2025-01-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (546, 1896, 1, 'rent', '2025-01', 11000, '2025-01-01', 'paid', '2025-01-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (603, 1953, 1, 'rent', '2025-01', 3000, '2025-01-01', 'paid', '2025-01-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (640, 1990, 1, 'rent', '2025-01', 16800, '2025-01-01', 'paid', '2025-01-13', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (577, 1927, 1, 'rent', '2025-02', 21600, '2025-02-01', 'paid', '2025-01-20', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (581, 1931, 1, 'rent', '2025-02', 21800, '2025-02-01', 'paid', '2025-02-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (558, 1908, 1, 'rent', '2025-02', 24000, '2025-02-01', 'overdue', NULL, 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (645, 1995, 1, 'rent', '2025-02', 30000, '2025-02-01', 'paid', '2025-02-20', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (578, 1928, 1, 'rent', '2025-02', 10140, '2025-02-01', 'paid', '2025-02-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (583, 1933, 1, 'rent', '2025-02', 10800, '2025-02-01', 'paid', '2025-02-19', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (549, 1899, 1, 'rent', '2025-02', 8940, '2025-02-01', 'paid', '2025-02-12', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (631, 1981, 1, 'rent', '2025-02', 12000, '2025-02-01', 'paid', '2025-02-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (632, 1982, 1, 'rent', '2025-02', 10140, '2025-02-01', 'paid', '2025-02-23', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (629, 1979, 1, 'rent', '2025-02', 9880, '2025-02-01', 'paid', '2025-02-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (546, 1896, 1, 'rent', '2025-02', 11000, '2025-02-01', 'paid', '2025-02-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (603, 1953, 1, 'rent', '2025-02', 3000, '2025-02-01', 'paid', '2025-02-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (624, 1974, 1, 'rent', '2025-02', 36000, '2025-02-01', 'paid', '2025-02-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (619, 1969, 1, 'rent', '2025-02', 36000, '2025-02-01', 'paid', '2025-02-13', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (642, 1992, 1, 'rent', '2025-02', 16800, '2025-02-01', 'paid', '2025-02-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (644, 1994, 1, 'rent', '2025-02', 16800, '2025-02-01', 'paid', '2025-02-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (643, 1993, 1, 'rent', '2025-02', 16800, '2025-02-01', 'paid', '2025-02-12', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (560, 1910, 1, 'rent', '2025-03', 21600, '2025-03-01', 'paid', '2025-03-12', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (582, 1932, 1, 'rent', '2025-03', 12000, '2025-03-01', 'paid', '2025-02-25', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (585, 1935, 1, 'rent', '2025-03', 21600, '2025-03-01', 'paid', '2025-02-13', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (586, 1936, 1, 'rent', '2025-03', 24000, '2025-03-01', 'paid', '2025-02-26', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (590, 1940, 1, 'rent', '2025-03', 10800, '2025-03-01', 'paid', '2025-03-20', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (591, 1941, 1, 'rent', '2025-03', 10800, '2025-03-01', 'paid', '2025-02-26', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (621, 1971, 1, 'rent', '2025-03', 10800, '2025-03-01', 'paid', '2025-03-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (647, 1997, 1, 'rent', '2025-03', 30000, '2025-03-01', 'paid', '2025-03-24', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (563, 1913, 1, 'rent', '2025-03', 6000, '2025-03-01', 'paid', '2025-03-12', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (559, 1909, 1, 'rent', '2025-03', 9900, '2025-03-01', 'paid', '2025-03-14', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (580, 1930, 1, 'rent', '2025-03', 12000, '2025-03-01', 'paid', '2025-02-25', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (584, 1934, 1, 'rent', '2025-03', 10800, '2025-03-01', 'paid', '2025-03-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (587, 1937, 1, 'rent', '2025-03', 10800, '2025-03-01', 'paid', '2025-03-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (588, 1938, 1, 'rent', '2025-03', 10800, '2025-03-01', 'paid', '2025-03-21', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (589, 1939, 1, 'rent', '2025-03', 12000, '2025-03-01', 'paid', '2025-03-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (605, 1955, 1, 'rent', '2025-03', 8940, '2025-03-01', 'paid', '2025-03-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (620, 1970, 1, 'rent', '2025-03', 9900, '2025-03-01', 'overdue', NULL, 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (633, 1983, 1, 'rent', '2025-03', 10140, '2025-03-01', 'paid', '2025-03-26', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (629, 1979, 1, 'rent', '2025-03', 9880, '2025-03-01', 'paid', '2025-03-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (546, 1896, 1, 'rent', '2025-03', 11000, '2025-03-01', 'paid', '2025-03-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (603, 1953, 1, 'rent', '2025-03', 3000, '2025-03-01', 'paid', '2025-03-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (646, 1996, 1, 'rent', '2025-03', 16800, '2025-03-01', 'paid', '2025-03-24', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (561, 1911, 1, 'rent', '2025-04', 10800, '2025-04-01', 'paid', '2025-03-27', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (562, 1912, 1, 'rent', '2025-04', 21600, '2025-04-01', 'paid', '2025-04-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (548, 1898, 1, 'rent', '2025-04', 8940, '2025-04-01', 'paid', '2025-04-29', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (570, 1920, 1, 'rent', '2025-04', 12000, '2025-04-01', 'paid', '2025-04-20', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (553, 1903, 1, 'rent', '2025-04', 8940, '2025-04-01', 'paid', '2025-04-30', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (592, 1942, 1, 'rent', '2025-04', 10800, '2025-04-01', 'paid', '2025-04-01', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (593, 1943, 1, 'rent', '2025-04', 8940, '2025-04-01', 'paid', '2025-04-20', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (594, 1944, 1, 'rent', '2025-04', 10800, '2025-04-01', 'paid', '2025-04-21', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (623, 1973, 1, 'rent', '2025-04', 9900, '2025-04-01', 'paid', '2025-04-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (607, 1957, 1, 'rent', '2025-04', 8940, '2025-04-01', 'paid', '2025-04-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (634, 1984, 1, 'rent', '2025-04', 10140, '2025-04-01', 'paid', '2025-04-21', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (952, 2324, 1, 'rent', '2025-04', 10140, '2025-04-01', 'paid', '2025-04-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (629, 1979, 1, 'rent', '2025-04', 9880, '2025-04-01', 'paid', '2025-04-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (546, 1896, 1, 'rent', '2025-04', 11000, '2025-04-01', 'paid', '2025-04-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (603, 1953, 1, 'rent', '2025-04', 3000, '2025-04-01', 'paid', '2025-04-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (648, 1998, 1, 'rent', '2025-04', 16800, '2025-04-01', 'paid', '2025-03-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (967, 2325, 1, 'rent', '2025-04', 3000, '2025-04-01', 'paid', '2025-04-25', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (565, 1915, 1, 'rent', '2025-05', 20280, '2025-05-01', 'paid', '2025-06-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (595, 1945, 1, 'rent', '2025-05', 24000, '2025-05-01', 'paid', '2025-05-01', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (625, 1975, 1, 'rent', '2025-05', 10800, '2025-05-01', 'paid', '2025-04-29', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (546, 1896, 1, 'rent', '2025-05', 11000, '2025-05-01', 'paid', '2025-05-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (629, 1979, 1, 'rent', '2025-05', 9880, '2025-05-01', 'paid', '2025-05-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (619, 1969, 1, 'rent', '2025-05', 36000, '2025-05-01', 'paid', '2025-05-13', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (564, 1914, 1, 'rent', '2025-05', 10140, '2025-05-01', 'paid', '2025-05-23', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (551, 1901, 1, 'rent', '2025-05', 8940, '2025-05-01', 'paid', '2025-05-19', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (610, 1960, 1, 'rent', '2025-05', 8940, '2025-05-01', 'paid', '2025-05-15', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (612, 1962, 1, 'rent', '2025-05', 8940, '2025-05-01', 'paid', '2025-05-24', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (613, 1963, 1, 'rent', '2025-05', 8940, '2025-05-01', 'overdue', NULL, 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (626, 1976, 1, 'rent', '2025-05', 10140, '2025-05-01', 'paid', '2025-05-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (627, 1977, 1, 'rent', '2025-05', 10140, '2025-05-01', 'paid', '2025-05-23', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (628, 1978, 1, 'rent', '2025-05', 10140, '2025-05-01', 'paid', '2025-05-29', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (967, 2325, 1, 'rent', '2025-05', 3000, '2025-05-01', 'paid', '2025-05-30', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (649, 1999, 1, 'rent', '2025-05', 30000, '2025-05-01', 'paid', '2025-05-23', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (547, 1897, 1, 'rent', '2025-06', 18000, '2025-06-01', 'paid', '2025-05-23', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (552, 1902, 1, 'rent', '2025-06', 21600, '2025-06-01', 'paid', '2025-06-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (566, 1916, 1, 'rent', '2025-06', 10140, '2025-06-01', 'paid', '2025-06-27', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (596, 1946, 1, 'rent', '2025-06', 21600, '2025-06-01', 'paid', '2025-06-18', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (546, 1896, 1, 'rent', '2025-06', 12000, '2025-06-01', 'paid', '2025-06-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (629, 1979, 1, 'rent', '2025-06', 10880, '2025-06-01', 'paid', '2025-06-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (563, 1913, 1, 'rent', '2025-06', 12000, '2025-06-01', 'paid', '2025-06-18', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (556, 1906, 1, 'rent', '2025-06', 8940, '2025-06-01', 'paid', '2025-06-16', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (574, 1924, 1, 'rent', '2025-06', 8940, '2025-06-01', 'paid', '2025-06-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (597, 1947, 1, 'rent', '2025-06', 8940, '2025-06-01', 'paid', '2025-06-16', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (598, 1948, 1, 'rent', '2025-06', 10800, '2025-06-01', 'paid', '2025-05-13', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (614, 1964, 1, 'rent', '2025-06', 8940, '2025-06-01', 'paid', '2025-06-24', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (554, 1904, 1, 'rent', '2025-06', 10140, '2025-06-01', 'paid', '2025-06-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (639, 1989, 1, 'rent', '2025-06', 10140, '2025-06-01', 'overdue', NULL, 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (555, 1905, 1, 'rent', '2025-06', 8940, '2025-06-01', 'paid', '2025-08-21', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (650, 2000, 1, 'rent', '2025-06', 16800, '2025-06-01', 'paid', '2025-06-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (651, 2001, 1, 'rent', '2025-06', 18000, '2025-06-01', 'paid', '2025-06-19', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (652, 2002, 1, 'rent', '2025-06', 20400, '2025-06-01', 'paid', '2025-06-25', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (797, 2003, 1, 'rent', '2025-06', 20400, '2025-06-01', 'paid', '2025-06-27', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (567, 1917, 1, 'rent', '2025-07', 21600, '2025-07-01', 'paid', '2025-06-17', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (599, 1949, 1, 'rent', '2025-07', 21600, '2025-07-01', 'paid', '2025-07-15', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (601, 1951, 1, 'rent', '2025-07', 21600, '2025-07-01', 'paid', '2025-07-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (629, 1979, 1, 'rent', '2025-07', 10880, '2025-07-01', 'paid', '2025-07-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (546, 1896, 1, 'rent', '2025-07', 12000, '2025-07-01', 'paid', '2025-07-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (579, 1929, 1, 'rent', '2025-07', 10800, '2025-07-01', 'paid', '2025-07-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (557, 1907, 1, 'rent', '2025-07', 8940, '2025-07-01', 'paid', '2025-07-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (576, 1926, 1, 'rent', '2025-07', 8940, '2025-07-01', 'paid', '2025-06-28', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (600, 1950, 1, 'rent', '2025-07', 8940, '2025-07-01', 'paid', '2025-07-20', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (609, 1959, 1, 'rent', '2025-07', 8940, '2025-07-01', 'paid', '2025-07-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (616, 1966, 1, 'rent', '2025-07', 8940, '2025-07-01', 'paid', '2025-07-01', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (617, 1967, 1, 'rent', '2025-07', 8940, '2025-07-01', 'paid', '2025-07-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (630, 1980, 1, 'rent', '2025-07', 10140, '2025-07-01', 'paid', '2025-07-21', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (643, 1993, 1, 'rent', '2025-07', 10800, '2025-07-01', 'paid', '2025-07-14', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (640, 1990, 1, 'rent', '2025-07', 10800, '2025-07-01', 'paid', '2025-06-25', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (654, 2004, 1, 'rent', '2025-07', 20400, '2025-07-01', 'paid', '2025-07-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (603, 1953, 1, 'rent', '2025-07', 3000, '2025-07-01', 'paid', '2025-07-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (655, 2005, 1, 'rent', '2025-07', 16140, '2025-07-01', 'paid', '2025-07-15', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (656, 2006, 1, 'rent', '2025-07', 25080, '2025-07-01', 'paid', '2025-07-23', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (568, 1918, 1, 'rent', '2025-08', 10140, '2025-08-01', 'paid', '2025-08-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (569, 1919, 1, 'rent', '2025-08', 21600, '2025-08-01', 'paid', '2025-08-26', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (602, 1952, 1, 'rent', '2025-08', 21600, '2025-08-01', 'paid', '2025-08-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (657, 2007, 1, 'rent', '2025-08', 27600, '2025-08-01', 'paid', '2025-08-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (603, 1953, 1, 'rent', '2025-08', 3000, '2025-08-01', 'paid', '2025-08-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (619, 1969, 1, 'rent', '2025-08', 36000, '2025-08-01', 'paid', '2025-08-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (546, 1896, 1, 'rent', '2025-08', 12000, '2025-08-01', 'paid', '2025-08-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (629, 1979, 1, 'rent', '2025-08', 10880, '2025-08-01', 'paid', '2025-08-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (578, 1928, 1, 'rent', '2025-08', 10140, '2025-08-01', 'paid', '2025-08-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (583, 1933, 1, 'rent', '2025-08', 10800, '2025-08-01', 'paid', '2025-08-22', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (549, 1899, 1, 'rent', '2025-08', 8940, '2025-08-01', 'paid', '2025-09-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (631, 1981, 1, 'rent', '2025-08', 10800, '2025-08-01', 'paid', '2025-08-22', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (632, 1982, 1, 'rent', '2025-08', 10140, '2025-08-01', 'paid', '2025-09-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (642, 1992, 1, 'rent', '2025-08', 10800, '2025-08-01', 'paid', '2025-07-22', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (644, 1994, 1, 'rent', '2025-08', 10800, '2025-08-01', 'paid', '2025-08-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (604, 1954, 1, 'rent', '2025-09', 153900, '2025-09-01', 'paid', '2025-09-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (606, 1956, 1, 'rent', '2025-09', 21600, '2025-09-01', 'paid', '2025-09-14', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (608, 1958, 1, 'rent', '2025-09', 20280, '2025-09-01', 'paid', '2025-09-30', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (658, 2008, 1, 'rent', '2025-09', 27600, '2025-09-01', 'paid', '2025-09-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (660, 2010, 1, 'rent', '2025-09', 27600, '2025-09-01', 'paid', '2025-09-16', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (629, 1979, 1, 'rent', '2025-09', 10880, '2025-09-01', 'paid', '2025-09-01', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (546, 1896, 1, 'rent', '2025-09', 12000, '2025-09-01', 'paid', '2025-09-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (580, 1930, 1, 'rent', '2025-09', 12000, '2025-09-01', 'paid', '2025-08-28', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (584, 1934, 1, 'rent', '2025-09', 10800, '2025-09-01', 'paid', '2025-08-31', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (587, 1937, 1, 'rent', '2025-09', 10800, '2025-09-01', 'paid', '2025-09-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (588, 1938, 1, 'rent', '2025-09', 10800, '2025-09-01', 'paid', '2025-09-17', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (589, 1939, 1, 'rent', '2025-09', 12000, '2025-09-01', 'paid', '2025-09-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (605, 1955, 1, 'rent', '2025-09', 8940, '2025-09-01', 'paid', '2025-09-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (620, 1970, 1, 'rent', '2025-09', 9900, '2025-09-01', 'paid', '2025-09-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (622, 1972, 1, 'rent', '2025-09', 9900, '2025-09-01', 'paid', '2025-09-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (633, 1983, 1, 'rent', '2025-09', 10140, '2025-09-01', 'paid', '2025-09-24', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (646, 1996, 1, 'rent', '2025-09', 10800, '2025-09-01', 'paid', '2025-09-17', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (621, 1971, 1, 'rent', '2025-09', 10800, '2025-09-01', 'paid', '2025-09-16', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (590, 1940, 1, 'rent', '2025-09', 10800, '2025-09-01', 'paid', '2025-09-17', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (591, 1941, 1, 'rent', '2025-09', 10800, '2025-09-01', 'paid', '2025-08-28', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (582, 1932, 1, 'rent', '2025-09', 12000, '2025-09-01', 'paid', '2025-09-01', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (659, 2009, 1, 'rent', '2025-09', 3000, '2025-09-01', 'paid', '2025-09-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (550, 1900, 1, 'rent', '2025-10', 18000, '2025-10-01', 'paid', '2025-10-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (661, 2011, 1, 'rent', '2025-10', 30000, '2025-10-01', 'paid', '2025-10-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (546, 1896, 1, 'rent', '2025-10', 12000, '2025-10-01', 'paid', '2025-10-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (629, 1979, 1, 'rent', '2025-10', 10880, '2025-10-01', 'paid', '2025-10-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (548, 1898, 1, 'rent', '2025-10', 8940, '2025-10-01', 'paid', '2025-10-31', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (561, 1911, 1, 'rent', '2025-10', 10800, '2025-10-01', 'paid', '2025-09-25', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (570, 1920, 1, 'rent', '2025-10', 12000, '2025-10-01', 'paid', '2025-10-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (553, 1903, 1, 'rent', '2025-10', 8940, '2025-10-01', 'paid', '2025-11-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (592, 1942, 1, 'rent', '2025-10', 10800, '2025-10-01', 'paid', '2025-09-30', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (593, 1943, 1, 'rent', '2025-10', 10800, '2025-10-01', 'paid', '2025-10-28', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (594, 1944, 1, 'rent', '2025-10', 10800, '2025-10-01', 'paid', '2025-10-20', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (623, 1973, 1, 'rent', '2025-10', 9900, '2025-10-01', 'paid', '2025-09-24', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (607, 1957, 1, 'rent', '2025-10', 8940, '2025-10-01', 'paid', '2025-10-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (634, 1984, 1, 'rent', '2025-10', 10140, '2025-10-01', 'paid', '1902-10-17', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (648, 1998, 1, 'rent', '2025-10', 10800, '2025-10-01', 'paid', '1902-10-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (659, 2009, 1, 'rent', '2025-10', 3000, '2025-10-01', 'paid', '2025-10-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (635, 1985, 1, 'rent', '2025-11', 21600, '2025-11-01', 'paid', '2025-11-13', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (637, 1987, 1, 'rent', '2025-11', 20280, '2025-11-01', 'paid', '2025-11-17', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (571, 1921, 1, 'rent', '2025-11', 21600, '2025-11-01', 'paid', '2025-11-19', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (572, 1922, 1, 'rent', '2025-11', 21600, '2025-11-01', 'paid', '2025-11-19', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (662, 2012, 1, 'rent', '2025-11', 27600, '2025-11-01', 'paid', '2025-11-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (546, 1896, 1, 'rent', '2025-11', 12000, '2025-11-01', 'paid', '2025-11-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (619, 1969, 1, 'rent', '2025-11', 36000, '2025-11-01', 'paid', '2025-11-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (629, 1979, 1, 'rent', '2025-11', 10880, '2025-11-01', 'paid', '2025-11-04', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (551, 1901, 1, 'rent', '2025-11', 20280, '2025-11-01', 'paid', '2025-11-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (610, 1960, 1, 'rent', '2025-11', 21600, '2025-11-01', 'paid', '2025-11-17', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (612, 1962, 1, 'rent', '2025-11', 20280, '2025-11-01', 'paid', '2025-11-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (613, 1963, 1, 'rent', '2025-11', 12000, '2025-11-01', 'paid', '2025-11-18', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (625, 1975, 1, 'rent', '2025-11', 12000, '2025-11-01', 'paid', '2025-10-27', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (626, 1976, 1, 'rent', '2025-11', 10140, '2025-11-01', 'paid', '2025-11-06', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (627, 1977, 1, 'rent', '2025-11', 10140, '2025-11-01', 'paid', '2025-11-24', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (628, 1978, 1, 'rent', '2025-11', 10140, '2025-11-01', 'paid', '2025-11-28', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (638, 1988, 1, 'rent', '2025-11', 10140, '2025-11-01', 'paid', '2025-10-31', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (564, 1914, 1, 'rent', '2025-11', 10140, '2025-11-01', 'paid', '2025-11-07', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (659, 2009, 1, 'rent', '2025-11', 3000, '2025-11-01', 'paid', '2025-11-05', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (663, 2013, 1, 'rent', '2025-11', 161200, '2025-11-01', 'paid', '2025-11-27', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (664, 2014, 1, 'rent', '2025-11', 3000, '2025-11-01', 'paid', '2025-11-17', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (573, 1923, 1, 'rent', '2025-12', 20280, '2025-12-01', 'paid', '2025-11-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (615, 1965, 1, 'rent', '2025-12', 21600, '2025-12-01', 'paid', '2025-11-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (629, 1979, 1, 'rent', '2025-12', 10880, '2025-12-01', 'paid', '2025-12-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (597, 1947, 1, 'rent', '2025-12', 8940, '2025-12-01', 'paid', '2025-12-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (614, 1964, 1, 'rent', '2025-12', 21600, '2025-12-01', 'paid', '2025-11-19', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (650, 2000, 1, 'rent', '2025-12', 10800, '2025-12-01', 'paid', '2025-12-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (624, 1974, 2, 'rent', '2025-03', 77600, '2025-03-01', 'paid', '2025-03-25', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (665, 2015, 2, 'rent', '2025-04', 30000, '2025-04-01', 'paid', '2025-04-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (666, 2016, 2, 'rent', '2025-04', 30000, '2025-04-01', 'paid', '2025-04-08', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (977, 2355, 2, 'rent', '2025-04', 16800, '2025-04-01', 'paid', '2025-04-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (667, 2017, 2, 'rent', '2025-04', 16800, '2025-04-01', 'paid', '2025-04-11', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (624, 1974, 2, 'rent', '2025-06', 96000, '2025-06-01', 'paid', '2025-06-02', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (668, 2018, 2, 'rent', '2025-06', 16800, '2025-06-01', 'paid', '2025-06-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (669, 2019, 2, 'rent', '2025-06', 20400, '2025-06-01', 'paid', '2025-06-17', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (670, 2020, 2, 'rent', '2025-06', 18000, '2025-06-01', 'paid', '2025-06-10', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (671, 2021, 2, 'rent', '2025-06', 18000, '2025-06-01', 'overdue', NULL, 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (672, 2022, 2, 'rent', '2025-06', 18000, '2025-06-01', 'overdue', NULL, 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (673, 2023, 2, 'rent', '2025-06', 18000, '2025-06-01', 'overdue', NULL, 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (674, 2024, 2, 'rent', '2025-06', 30000, '2025-06-01', 'paid', '2025-06-20', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (675, 2025, 2, 'rent', '2025-06', 20400, '2025-06-01', 'paid', '2025-06-30', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (676, 2026, 2, 'rent', '2025-07', 25080, '2025-07-01', 'paid', '2025-07-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (677, 2027, 2, 'rent', '2025-07', 16140, '2025-07-01', 'paid', '2025-07-15', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (678, 2028, 2, 'rent', '2025-07', 20400, '2025-07-01', 'paid', '2025-07-17', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (679, 2029, 2, 'rent', '2025-07', 25080, '2025-07-01', 'paid', '2025-07-22', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (680, 2030, 2, 'rent', '2025-08', 25080, '2025-08-01', 'paid', '2025-07-31', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (681, 2031, 2, 'rent', '2025-08', 26280, '2025-08-01', 'paid', '2025-08-21', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (624, 1974, 2, 'rent', '2025-09', 112000, '2025-09-01', 'paid', '2025-09-15', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (683, 2033, 2, 'rent', '2025-09', 25080, '2025-09-01', 'paid', '2025-09-01', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (684, 2034, 2, 'rent', '2025-09', 27600, '2025-09-01', 'paid', '2025-09-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (685, 2035, 2, 'rent', '2025-09', 27600, '2025-09-01', 'paid', '2025-09-13', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (686, 2036, 2, 'rent', '2025-09', 27600, '2025-09-01', 'paid', '2025-09-18', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (667, 2017, 2, 'rent', '2025-10', 10800, '2025-10-01', 'paid', '2025-10-09', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (687, 2037, 2, 'rent', '2025-10', 27600, '2025-10-01', 'paid', '2025-10-03', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (688, 2038, 2, 'rent', '2025-10', 27600, '2025-10-01', 'paid', '2025-10-29', 'transfer');
INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES (689, 2039, 2, 'rent', '2025-12', 27600, '2025-12-01', 'paid', '2025-12-02', 'transfer');

-- 統計:
-- 已繳 (paid):     228 筆  $   3,840,449
-- 待繳 (pending):    0 筆  $           0
-- 逾期 (overdue):    7 筆  $     106,980

-- 驗證
SELECT payment_status, COUNT(*) as count, SUM(amount) as total FROM payments GROUP BY payment_status ORDER BY payment_status;