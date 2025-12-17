-- ============================================================================
-- Hour Jungle CRM - 已結束客戶匯入
-- 生成時間: 2025-12-07 11:51:23
-- ============================================================================

-- === 客戶資料 (status = 'churned') ===
INSERT INTO customers (legacy_id, branch_id, name, company_name, customer_type, phone, company_tax_id, status)
VALUES ('DZ-115', 1, '賴宗政', '天府不動產仲介有限公司', 'company', '0975398610', '89162294', 'churned')
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO customers (legacy_id, branch_id, name, company_name, customer_type, phone, company_tax_id, status)
VALUES ('DZ-218', 1, '包奇艷', '昀績企業社', 'sole_proprietorship', '0900139313', '94539405', 'churned')
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO customers (legacy_id, branch_id, name, company_name, customer_type, phone, company_tax_id, status)
VALUES ('DZ-066', 1, '李沁澐', '沁享美學坊', 'sole_proprietorship', '0912605983', '88533945', 'churned')
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO customers (legacy_id, branch_id, name, company_name, customer_type, phone, company_tax_id, status)
VALUES ('DZ-132', 1, '陳建誠', '銳克科技有限公司', 'company', '0963567321', '83016865', 'churned')
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO customers (legacy_id, branch_id, name, company_name, customer_type, phone, company_tax_id, status)
VALUES ('DZ-174', 1, '賴彥廷', '喜特室內裝修有限公司', 'company', NULL, '94227022', 'churned')
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO customers (legacy_id, branch_id, name, company_name, customer_type, phone, company_tax_id, status)
VALUES ('DZ-207', 1, '鐘韋程', '嘉功工程行', 'sole_proprietorship', '0902263822', '94533036', 'churned')
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO customers (legacy_id, branch_id, name, company_name, customer_type, phone, company_tax_id, status)
VALUES ('DZ-187', 1, '張祐寧', '薩摩亞商青盈國際有限公司', 'company', '04-23506088', '90513585', 'churned')
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO customers (legacy_id, branch_id, name, company_name, customer_type, phone, company_tax_id, status)
VALUES ('DZ-186', 1, '曾湘稘', '曾德很棒有限公司', 'company', '0918124726', '93793245', 'churned')
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO customers (legacy_id, branch_id, name, company_name, customer_type, phone, company_tax_id, status)
VALUES ('DZ-229', 1, '朱栢逸', '影識科技有限公司', 'company', '0978094610', '00076671', 'churned')
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO customers (legacy_id, branch_id, name, company_name, customer_type, phone, company_tax_id, status)
VALUES ('DZ-246', 1, '吳世多', NULL, 'individual', '0910752244', NULL, 'churned')
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO customers (legacy_id, branch_id, name, company_name, customer_type, phone, company_tax_id, status)
VALUES ('DZ-210', 1, '吳振領', '錦鑫工程行', 'sole_proprietorship', '0909681760', '94535718', 'churned')
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO customers (legacy_id, branch_id, name, company_name, customer_type, phone, company_tax_id, status)
VALUES ('DZ-155', 1, '吳怡姍', '吾一家商行', 'sole_proprietorship', '0922995613', '93356531', 'churned')
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO customers (legacy_id, branch_id, name, company_name, customer_type, phone, company_tax_id, status)
VALUES ('DZ-134', 1, '胡良輝', '穩賀工程行', 'sole_proprietorship', '0931050843', '92314083', 'churned')
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO customers (legacy_id, branch_id, name, company_name, customer_type, phone, company_tax_id, status)
VALUES ('DZ-203', 1, '蔣榮宗', '妙博士數位文創有限公司', 'company', '0987661988', '93576866', 'churned')
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO customers (legacy_id, branch_id, name, company_name, customer_type, phone, company_tax_id, status)
VALUES ('DZ-157', 1, '巫成妍', '成泰豐有限公司', 'company', '0934068015', '94112247', 'churned')
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO customers (legacy_id, branch_id, name, company_name, customer_type, phone, company_tax_id, status)
VALUES ('DZ-159', 1, '劉真如', '鯉選物有限公司', 'company', '0919531495', '94082237', 'churned')
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO customers (legacy_id, branch_id, name, company_name, customer_type, phone, company_tax_id, status)
VALUES ('DZ-161', 1, '顏珮羽', '粲朔企業社', 'sole_proprietorship', '0908108557', '93362816', 'churned')
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO customers (legacy_id, branch_id, name, company_name, customer_type, phone, company_tax_id, status)
VALUES ('DZ-113', 1, '李昕妮(李寶妮)', '悠陽實業社', 'sole_proprietorship', '0921973512', '92289248', 'churned')
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO customers (legacy_id, branch_id, name, company_name, customer_type, phone, company_tax_id, status)
VALUES ('DZ-223', 1, '賴宗政', '天府不動產有限公司', 'company', '0975398610', '94183072', 'churned')
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO customers (legacy_id, branch_id, name, company_name, customer_type, phone, company_tax_id, status)
VALUES ('HR-003', 2, '沈孟輝', '孟盟企業社', 'sole_proprietorship', NULL, '95338691', 'churned')
ON CONFLICT (legacy_id) DO NOTHING;

-- === 合約資料 (status = 'terminated') ===
INSERT INTO contracts (customer_id, branch_id, contract_number, start_date, end_date, monthly_rent, deposit, deposit_status, status)
SELECT id, 1, 'DZ-115-TERM', '2023-01-06', '2024-01-06', 1490, 6000, 'held', 'terminated'
FROM customers WHERE legacy_id = 'DZ-115'
ON CONFLICT DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, start_date, end_date, monthly_rent, deposit, deposit_status, status)
SELECT id, 1, 'DZ-218-TERM', '2024-07-04', '2025-07-04', 2000, 6000, 'held', 'terminated'
FROM customers WHERE legacy_id = 'DZ-218'
ON CONFLICT DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, start_date, end_date, monthly_rent, deposit, deposit_status, status)
SELECT id, 1, 'DZ-066-TERM', '2024-01-03', '2025-01-03', 1800, 3000, 'held', 'terminated'
FROM customers WHERE legacy_id = 'DZ-066'
ON CONFLICT DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, start_date, end_date, monthly_rent, deposit, deposit_status, status)
SELECT id, 1, 'DZ-132-TERM', '2024-03-02', '2025-03-02', 1800, 6000, 'held', 'terminated'
FROM customers WHERE legacy_id = 'DZ-132'
ON CONFLICT DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, start_date, end_date, monthly_rent, deposit, deposit_status, status)
SELECT id, 1, 'DZ-174-TERM', '2023-09-11', '2024-09-11', 1490, 6000, 'held', 'terminated'
FROM customers WHERE legacy_id = 'DZ-174'
ON CONFLICT DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, start_date, end_date, monthly_rent, deposit, deposit_status, status)
SELECT id, 1, 'DZ-207-TERM', '2024-03-28', '2025-03-28', 1800, 6000, 'held', 'terminated'
FROM customers WHERE legacy_id = 'DZ-207'
ON CONFLICT DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, start_date, end_date, monthly_rent, deposit, deposit_status, status)
SELECT id, 1, 'DZ-187-TERM', '2024-08-03', '2025-08-03', 1800, 3000, 'held', 'terminated'
FROM customers WHERE legacy_id = 'DZ-187'
ON CONFLICT DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, start_date, end_date, monthly_rent, deposit, deposit_status, status)
SELECT id, 1, 'DZ-186-TERM', '2023-06-02', '2024-06-02', 1490, 6000, 'held', 'terminated'
FROM customers WHERE legacy_id = 'DZ-186'
ON CONFLICT DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, start_date, end_date, monthly_rent, deposit, deposit_status, status)
SELECT id, 1, 'DZ-229-TERM', '2024-10-11', '2025-10-11', 1690, 6000, 'held', 'terminated'
FROM customers WHERE legacy_id = 'DZ-229'
ON CONFLICT DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, start_date, end_date, monthly_rent, deposit, deposit_status, status)
SELECT id, 1, 'DZ-246-TERM', '2025-04-25', '2026-04-25', 3000, 0, 'held', 'terminated'
FROM customers WHERE legacy_id = 'DZ-246'
ON CONFLICT DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, start_date, end_date, monthly_rent, deposit, deposit_status, status)
SELECT id, 1, 'DZ-210-TERM', '2024-04-15', '2025-04-15', 1800, 6000, 'held', 'terminated'
FROM customers WHERE legacy_id = 'DZ-210'
ON CONFLICT DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, start_date, end_date, monthly_rent, deposit, deposit_status, status)
SELECT id, 1, 'DZ-155-TERM', '2024-04-25', '2025-04-25', 1800, 6000, 'held', 'terminated'
FROM customers WHERE legacy_id = 'DZ-155'
ON CONFLICT DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, start_date, end_date, monthly_rent, deposit, deposit_status, status)
SELECT id, 1, 'DZ-134-TERM', '2023-04-12', '2024-04-12', 1490, 6000, 'held', 'terminated'
FROM customers WHERE legacy_id = 'DZ-134'
ON CONFLICT DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, start_date, end_date, monthly_rent, deposit, deposit_status, status)
SELECT id, 1, 'DZ-203-TERM', '2024-03-11', '2025-03-11', 1650, 6000, 'held', 'terminated'
FROM customers WHERE legacy_id = 'DZ-203'
ON CONFLICT DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, start_date, end_date, monthly_rent, deposit, deposit_status, status)
SELECT id, 1, 'DZ-157-TERM', '2024-05-10', '2025-05-10', 1490, 6000, 'held', 'terminated'
FROM customers WHERE legacy_id = 'DZ-157'
ON CONFLICT DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, start_date, end_date, monthly_rent, deposit, deposit_status, status)
SELECT id, 1, 'DZ-159-TERM', '2023-05-12', '2024-05-12', 1490, 6000, 'held', 'terminated'
FROM customers WHERE legacy_id = 'DZ-159'
ON CONFLICT DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, start_date, end_date, monthly_rent, deposit, deposit_status, status)
SELECT id, 1, 'DZ-161-TERM', '2024-05-22', '2025-05-22', 1800, 6000, 'held', 'terminated'
FROM customers WHERE legacy_id = 'DZ-161'
ON CONFLICT DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, start_date, end_date, monthly_rent, deposit, deposit_status, status)
SELECT id, 1, 'DZ-113-TERM', '2023-12-29', '2024-12-29', 1490, 6000, 'held', 'terminated'
FROM customers WHERE legacy_id = 'DZ-113'
ON CONFLICT DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, start_date, end_date, monthly_rent, deposit, deposit_status, status)
SELECT id, 1, 'DZ-223-TERM', '2024-08-14', '2025-08-14', 1690, 6000, 'held', 'terminated'
FROM customers WHERE legacy_id = 'DZ-223'
ON CONFLICT DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, start_date, end_date, monthly_rent, deposit, deposit_status, status)
SELECT id, 2, 'HR-003-TERM', '2025-04-09', '2027-04-09', 1800, 6000, 'held', 'terminated'
FROM customers WHERE legacy_id = 'HR-003'
ON CONFLICT DO NOTHING;

-- === 驗證 ===
SELECT 'customers' as table_name, status, COUNT(*) FROM customers GROUP BY status ORDER BY status;
SELECT 'contracts' as table_name, status, deposit_status, COUNT(*) FROM contracts GROUP BY status, deposit_status ORDER BY status;