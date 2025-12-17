-- 匯入已結束客戶
-- 生成時間: 2025-12-07 00:21:55

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E001', 1, 'individual', '陳慕？（陳老師）', NULL, '63/05/27', NULL, NULL, '月繳繳清', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E001-END', 'virtual_office', '2020-01-01', '1921-07-08', 10, 0, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E001'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E002', 1, 'individual', '陳嘉偉', NULL, '1981-05-09 00:00:00', NULL, NULL, '月繳', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E002-END', 'virtual_office', '2020-01-01', '1921-01-11', 109, 0, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E002'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E003', 1, 'individual', '劉耀文', NULL, '1969-03-24 00:00:00', NULL, NULL, 'M', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E003-END', 'virtual_office', '2020-01-01', '1921-05-13', 110, 0, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E003'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E005', 1, 'individual', '詹雅淳', NULL, '1973-02-13 00:00:00', NULL, NULL, 'm', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E005-END', 'virtual_office', '2020-01-01', '2021-06-20', 110, 0, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E005'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E006', 1, 'individual', '王姿文', NULL, '1978-06-27 00:00:00', NULL, NULL, 'M', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E007', 1, 'individual', '賴柏瑞', '訊特科技有限公司', '1982-07-07 00:00:00', NULL, '91124851', '3m', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E007-END', 'virtual_office', '2020-01-01', '2021-08-18', 110, 0, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E007'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E008', 1, 'individual', '吳念蓁', 'Ｎ', '1974-08-06 00:00:00', NULL, NULL, 'M', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E009', 1, 'individual', '吳秉原', '弘鉅輪胎有限公司', '1975-01-04 00:00:00', NULL, '24585978', 'm', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E009-END', 'virtual_office', '2020-01-01', '2021-10-26', 110, 0, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E009'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E010', 1, 'individual', '李思漢', '待補', '1967-07-16 00:00:00', NULL, '待確認', 'M', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E010-END', 'virtual_office', '2020-01-01', '2021-06-01', 110, 0, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E010'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E011', 1, 'individual', '日益能源科技股份有限公司', '日益能源科技股份有限公司', NULL, NULL, '24881813', 'M', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E011-END', 'virtual_office', '2020-01-01', '2021-04-30', 110, 0, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E011'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E012', 1, 'individual', '日學行旅股份有限公司', '日學行旅股份有限公司', '1979-08-29 00:00:00', NULL, '61202008', NULL, 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E012-END', 'virtual_office', '2020-01-01', '2021-06-10', 109, 0, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E012'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E013', 1, 'individual', '張微', '博弈業ＨＲ', '1980-11-25 00:00:00', NULL, NULL, 'm', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E015', 1, 'individual', '楊淳晏', 'Ｎ', '1989-09-16 00:00:00', NULL, NULL, 'm', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E015-END', 'virtual_office', '2020-01-01', '2021-06-28', 110, 0, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E015'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E016', 1, 'individual', '湯詠為', '創禧科技有限公司', '1977-03-19 00:00:00', NULL, '61913204', 'M', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E016-END', 'virtual_office', '2020-01-01', '2021-08-20', 110, 0, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E016'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E017', 1, 'individual', '吳宗霖', '星鴻股份有限公司', '1976-08-02 00:00:00', NULL, '85103034', 'M', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E017-END', 'virtual_office', '2020-01-01', '2021-11-30', 110, 0, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E017'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E019', 1, 'individual', '李奕德', '艾弗工作室
', '1980-10-09 00:00:00', NULL, '82607758', '?', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E019-END', 'virtual_office', '2020-01-01', '2022-01-01', 10912, 0, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E019'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E021', 1, 'individual', '陳侑希', '軒轅社', '1983-08-09 00:00:00', NULL, '82650298', 'Y', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E021-END', 'virtual_office', '2020-01-01', '2022-05-03', 110, 0, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E021'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E022', 1, 'individual', '廖佑泓', '廖氏商行', '1975-03-19 00:00:00', NULL, '87262304', '季繳', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E022-END', 'virtual_office', '2020-01-01', '2023-04-24', 110, 0, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E022'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E024', 1, 'individual', '黃裕煇', '海派網路科技有限公司', '1978-05-22 00:00:00', NULL, '91050470', 'Y', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E024-END', 'virtual_office', '2020-01-01', '2022-04-26', 110, 3000, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E024'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E025', 1, 'individual', '李勇德', '天河工程行', '1953-09-20 00:00:00', NULL, '87256892', 'Y/2', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E025-END', 'virtual_office', '2020-01-01', '2022-04-08', 110, 3000, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E025'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E027', 1, 'individual', '楊仁豪', '藝途科技股份有限公司', '1979-08-29 00:00:00', NULL, '90841095', 'Y', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E027-END', 'virtual_office', '2020-01-01', '2022-03-25', 110, 0, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E027'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E029', 1, 'individual', 'Or.yu.chung', '香港商宇科創意有限公司', '504524279', '1972-05-30', '0938973561', NULL, 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E029-END', 'virtual_office', '2021-04-08', '2020-11-13', 1500, 3000, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E029'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E034', 1, 'individual', '陳泓睿', '賽奧有限公司', '1980-03-22 00:00:00', '2000-01-01', NULL, '3m', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E034-END', 'virtual_office', '2020-01-01', '2022-10-07', 110, 3000, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E034'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E035', 1, 'individual', '王智傭', '詠翔開發顧問有限公司', '1971-06-20 00:00:00', NULL, NULL, 'Y', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E035-END', 'virtual_office', '2020-01-01', '2022-07-10', 110, 3000, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E035'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E036', 1, 'individual', '楊仁豪', '日學行旅股份有限公司', '1976-08-29 00:00:00', NULL, '61202008', 'Y', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E036-END', 'virtual_office', '2020-01-01', '2022-07-16', 110, 0, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E036'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E037', 1, 'individual', '林俊希', '希氏設計工作室', '1982-01-20 00:00:00', NULL, '87116464', 'Y', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E037-END', 'virtual_office', '2020-01-01', '2022-07-20', 110, 0, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E037'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E038', 1, 'individual', '梁凱棋 
', '樹山鳥藝術有限公司 
', NULL, NULL, NULL, 'Ｙ', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E038-END', 'virtual_office', '2020-01-01', '2022-07-25', 110, 0, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E038'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E039', 1, 'individual', '吳秉原', '弘鉅輪胎有限公司', '1975-01-04 00:00:00', NULL, '24585978', 'm', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E040', 1, 'individual', '林志賢', NULL, '1977-04-19 00:00:00', NULL, NULL, 'M', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E040-END', 'virtual_office', '2020-01-01', '2022-07-23', 110, 3000, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E040'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E041', 1, 'individual', '吳秉原', '弘鉅輪胎有限公司', '1975-01-04 00:00:00', NULL, '24585978', 'm', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E043', 1, 'individual', '王琍芸', NULL, '1975-08-15 00:00:00', NULL, NULL, 'Ｙ', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E043-END', 'virtual_office', '2020-01-01', '2022-08-11', 110, 0, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E043'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E046', 1, 'individual', '劉宜沛', '堃耀工程行', NULL, NULL, NULL, 'y', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E046-END', 'virtual_office', '2020-01-01', '2022-08-27', 110, 3000, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E046'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E052', 1, 'individual', '歐乃禎', '歐乃禎個人髮型', '1972-04-01 00:00:00', '1800-01-01', NULL, 'Y', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E052-END', 'virtual_office', '2020-01-01', '2022-10-20', 110, 0, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E052'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E056', 1, 'individual', '段逸凡', '浚宇數位有限公司', '1970-08-26 00:00:00', NULL, '90892235', '無', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E056-END', 'virtual_office', '2020-01-01', '2022-05-22', 110, 3000, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E056'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E059', 1, 'individual', '林傳家', '九牧林土地開發有限公司', '38/05/28', NULL, NULL, NULL, 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E059-END', 'virtual_office', '2020-01-01', '2022-12-09', 110, 0, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E059'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E061', 1, 'individual', '賴聖凱', NULL, '1989-06-19 00:00:00', NULL, NULL, 'Ｙ', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E061-END', 'virtual_office', '2020-01-01', '2022-12-20', 110, 0, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E061'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E064', 1, 'individual', '劉桂翔', NULL, '1984-09-05 00:00:00', NULL, NULL, 'M', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E064-END', 'virtual_office', '2020-01-01', '2023-01-01', 110, 0, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E064'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E068', 1, 'individual', '陳彤威', '新楠星企業有限公司', '1975-11-18 00:00:00', NULL, '84397574', 'M', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E068-END', 'virtual_office', '2020-01-01', '2023-02-13', 111, 0, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E068'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E072', 1, 'individual', '蘇芮萱', NULL, '1979-07-02 00:00:00', NULL, NULL, 'M', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E072-END', 'virtual_office', '2020-01-01', '2023-03-23', 111, 0, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E072'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E074', 1, 'individual', '謝毓慈', '精品銅器股份有限公司', '1973-06-12 00:00:00', NULL, '86986365', 'Y', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E074-END', 'virtual_office', '2020-01-01', '2023-01-31', 111, 0, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E074'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E075', 1, 'individual', '林偉彥', '天赫有限公司', '1983-08-15 00:00:00', NULL, NULL, 'm', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E076', 1, 'individual', '王昀雅', '誠霂公關行銷工作室', '1977-02-20 00:00:00', '1800-01-01', NULL, 'y', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E077', 1, 'individual', '黃羽庭', '梅森漫活影像工作室', '1958-02-28 00:00:00', '1800-01-01', '87285572', 'y', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E014', 1, 'individual', '朱建勳', '七分之二的探索有限公司', '1978-03-14 00:00:00', NULL, '83082766', '3500/m', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E014-END', 'virtual_office', '2021-06-04', '2021-06-04', 2500, 3000, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E014'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E065', 1, 'individual', '杜宏霖', '玩美科創有限公司', '84/11/18', NULL, NULL, '1800/m', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E065-END', 'virtual_office', '2021-12-30', '2022-01-05', 1, 3000, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E065'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E062', 1, 'individual', '林子嫈', '米子設計工作室', '1982-11-06 00:00:00', NULL, NULL, '1800/m', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E062-END', 'virtual_office', '2021-12-22', '2021-12-22', 111, 3000, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E062'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E079', 1, 'individual', '葉宥鑫', '甲赫有限公司', '1972-01-12 00:00:00', NULL, NULL, '1800/m', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E081', 1, 'individual', '叢尚濬', '冠好文創事業有限公司', '1976-10-30 00:00:00', NULL, '90817546', '1800/m', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E083', 1, 'individual', '李悅慈', '陳茉設計工作室', '1984-02-28 00:00:00', NULL, NULL, '1800/m', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E149', 1, 'individual', '鍾亮毅', '鋯潕吉企業社', NULL, NULL, NULL, '1800/m', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E150', 1, 'individual', '呂易承', '金吉吉企業社', NULL, NULL, NULL, '1800/m', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E151', 1, 'individual', '洪吟馨', '甜馨企業社', NULL, NULL, NULL, '1800/m', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E165', 1, 'individual', '楊岱芬', '藍新', '1977-10-05 00:00:00', NULL, NULL, '2400/m', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E028', 1, 'individual', '陳佳纓', '感玩文化企業社', '1981-07-04 00:00:00', NULL, '87255021', '1500/m', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E028-END', 'virtual_office', '2021-03-03', '2022-03-03', 2500, 3000, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E028'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E030', 1, 'individual', '劉顓瑋', '禮禮創意有限公司', '1975-06-10 00:00:00', NULL, '83081767', '1500/m', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E030-END', 'virtual_office', '2022-09-15', '2022-08-26', 112, 3000, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E030'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E018', 1, 'individual', '李易昇', '營管家行銷企劃有限公司', '1974-12-27 00:00:00', NULL, '83207576', '14500/m', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E018-END', 'virtual_office', '2020-01-31', '2024-01-01', 2500, 3000, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E018'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E089', 1, 'individual', '謝毓慈', '好室一妝國際有限公司', '1973-06-12 00:00:00', NULL, NULL, '3500/m', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E090', 1, 'individual', '林依玉', '樂牧工作室', '1978-06-15 00:00:00', NULL, '87283862', '3000/m', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E097', 1, 'individual', '陳美珠', '發現諮詢企業社', '1944-02-10 00:00:00', NULL, NULL, '1200/m', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E103', 1, 'individual', '呂明憲', '握客床墊小舖', NULL, NULL, NULL, NULL, 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E033', 1, 'individual', '賴幸秀', '京旺國際娛樂製作有限公司

', NULL, NULL, '83150715', NULL, 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E033-END', 'virtual_office', '2020-01-01', '2022-07-06', 1500, 3000, 'refunded', 'annual', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E033'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E096', 1, 'individual', '陳沛淇', '淇翎總店', '1977-07-10 00:00:00', NULL, '88681001', NULL, 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E096-END', 'virtual_office', '2020-01-01', '2022-08-25', 1000, 3000, 'refunded', 'annual', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E096'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E156', 1, 'individual', '陳介于', '營響未來股份有限公司', '1974-01-13 00:00:00', NULL, NULL, NULL, 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E156-END', 'virtual_office', '2020-01-01', '2023-05-08', 1490, 3000, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E156'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E060', 1, 'individual', '王勇澤', '興澤車體膜料有限公司', '76/09/13', NULL, '90605867', NULL, 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E060-END', 'virtual_office', '2020-01-01', '2022-12-09', 1800, 3000, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E060'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E107', 1, 'individual', '石婉葶', '晨瑞國際代購', '1978-09-24 00:00:00', NULL, NULL, NULL, 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E107-END', 'virtual_office', '2020-01-01', '2022-11-15', 1800, 3000, 'refunded', 'annual', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E107'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E176', 1, 'individual', '許富傑', '台灣泛亞零售管理顧問股份有限公司', '1974-02-24 00:00:00', NULL, '83723869', NULL, 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E176-END', 'virtual_office', '2020-01-01', '2023-09-15', 15000, 3000, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E176'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E070', 1, 'individual', '陳彤威', '新楠星企業有限公司', '1975-11-18 00:00:00', NULL, '84397574', NULL, 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E070-END', 'virtual_office', '2020-01-01', '2023-02-07', 13500, 3000, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E070'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E108', 1, 'individual', '姚星民', '張星誠商行', '1979-04-05 00:00:00', NULL, NULL, NULL, 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E108-END', 'virtual_office', '2020-01-01', '2022-11-17', 1800, 3000, 'refunded', 'annual', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E108'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E048', 1, 'individual', '陳韋甫', NULL, '1983-04-10 00:00:00', NULL, '54068195', NULL, 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E048-END', 'virtual_office', '2020-01-01', '2022-11-14', 3000, 3000, 'refunded', 'annual', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E048'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E084', 1, 'individual', '陳柏偉', '鑫芯國際貿易有限公司', '1977-03-19 00:00:00', NULL, '61901050', NULL, 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E084-END', 'virtual_office', '2020-01-01', '2023-06-01', 2000, 3000, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E084'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E121', 1, 'individual', '趙宏遠', '柚見商行', '1980-06-05 00:00:00', NULL, NULL, NULL, 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E121-END', 'virtual_office', '2020-01-01', '2023-02-01', 1490, 3000, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E121'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E104', 1, 'individual', '張菀庭', '乎葛商行', '1985-08-11 00:00:00', NULL, NULL, NULL, 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E104-END', 'virtual_office', '2020-01-01', '2022-11-04', 1800, 3000, 'refunded', 'annual', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E104'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E120', 1, 'individual', '林祐瑄', '瑄祐工作室', '台中市南區美村南路51巷8號', NULL, '1985081900:00:00', NULL, 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E120-END', 'virtual_office', '2020-01-01', '2023-01-16', 1800, 3000, 'refunded', 'annual', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E120'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E100', 1, 'individual', '溫珮琪', '鉦陽能源科技股份有限公司', '台中市南區樹義路268號9樓-3', NULL, '1977012600:00:00', NULL, 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E100-END', 'virtual_office', '2020-01-01', '2023-09-26', 1800, 3000, 'refunded', 'annual', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E100'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E098', 1, 'individual', '朱新以', '台灣企評聯合鑑定中心股份有限公司', '高雄市鼓山區富農路146號7F', '2000-01-01', '19841122', '59116533', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E098-END', 'virtual_office', '2020-01-01', '2023-01-01', 1000, 3000, 'refunded', 'annual', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E098'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E123', 1, 'individual', '劉宗賢', '智勳管理諮詢股份有限公司', '台北市中正區富水里12鄰永春街163巷6號', NULL, '1965080900:00:00', NULL, 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E123-END', 'virtual_office', '2020-01-01', '2023-03-08', 1800, 3000, 'refunded', 'annual', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E123'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E142', 1, 'individual', '洪志勳', '幾時有甜點工作室', '台中市福聯街22巷12號5F32', NULL, '1954071700:00:00', NULL, 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E142-END', 'virtual_office', '2020-01-01', '2023-03-06', 1800, 3000, 'refunded', 'annual', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E142'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E101', 1, 'individual', '林明泰', '君泰商舖', '桃園市桃園區中埔一街330號5樓', NULL, '1977091600:00:00', '88700366', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E101-END', 'virtual_office', '2020-01-01', '2022-10-07', 1800, 3000, 'refunded', 'annual', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E101'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E051', 1, 'individual', '陳世杰', '桀初廣告股份有限公司
', '1971-01-12 00:00:00', '2021-10-04', NULL, '6000/未退', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E051-END', 'virtual_office', '2020-01-01', '2022-10-04', 2500, 0, 'refunded', 'annual', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E051'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E182', 1, 'individual', '吳秉原', '弘鉅輪胎有限公司', '台中市西屯區文心路三段40號', NULL, '1975010400:00:00', '24585978', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E182-END', 'virtual_office', '2020-01-01', '2023-08-07', 1490, 3000, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E182'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E162', 1, 'individual', '王亮祁', '汎星企業社', '臺中市西區吉龍里14鄰美村路一段609號', NULL, '1981051900:00:00', NULL, 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E162-END', 'virtual_office', '2020-01-01', '2023-05-29', 1800, 3000, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E162'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E069', 1, 'individual', '林達雄', '龍達金興業有限公司', '台中市后里區太平里四月路五哩巷106號', NULL, '1960062700:00:00', NULL, 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E069-END', 'virtual_office', '2020-01-01', '2023-01-13', 1800, 3000, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E069'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E188', 1, 'individual', '張伯任', '智谷系統有限公司', '臺中市北屯區北屯里8鄰興安路一段30號二樓之5', NULL, '1967080100:00:00', '85080813', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E188-END', 'virtual_office', '2020-01-01', '2023-11-01', 7500, 3000, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E188'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E200', 1, 'individual', '馬賢慧', '儷莫恩源氏企業社', NULL, NULL, NULL, '94514299', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E200-END', 'virtual_office', '2020-01-01', '2024-01-16', 1490, 3000, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E200'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E092', 1, 'individual', '史汶尉', '浮嶼立槳有限公司', '南投縣信義鄉人和村66號', NULL, '870316', NULL, 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E092-END', 'virtual_office', '2020-01-01', '2023-08-03', 1490, 3000, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E092'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E119', 1, 'individual', '張郢丰(黃孟煒)', '嵵廴制作室內裝修有限公司', '台中市烏日區高鐵三路33號5F-5', NULL, '1982060500:00:00', NULL, 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E119-END', 'virtual_office', '2020-01-01', '2023-02-01', 1500, 3000, 'refunded', 'annual', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E119'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E191', 1, 'individual', '張荷里', '花里有限公司 ', '臺中市潭子區新田里11鄰豐興路二段622號', NULL, '1985032700:00:00', '93541059', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E191-END', 'virtual_office', '2020-01-01', '2023-11-27', 1490, 3000, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E191'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E026', 1, 'individual', '曾冠豪', '豪思空間設計', '台南市北區育德路99號', NULL, '1966090800:00:00', '87259407', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E026-END', 'virtual_office', '2020-01-01', '2023-04-16', 1500, 3000, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E026'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E099', 1, 'individual', '陳勝寶', '集思室內裝修企業社', '台中市南屯區大墩一街241-2號', NULL, '1972022000:00:00', '82614272', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E099-END', 'virtual_office', '2020-01-01', '2022-09-26', 12500, 3000, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E099'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E146', 1, 'individual', '王美芬', '俊潔企業社', NULL, NULL, '1949081500:00:00', NULL, 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E146-END', 'virtual_office', '2020-01-01', '2023-03-10', 1490, 3000, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E146'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E152', 1, 'individual', '林伯宇', '聚全有限公司', NULL, NULL, '1983082700:00:00', NULL, 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E152-END', 'virtual_office', '2020-01-01', '2023-04-06', 1800, 3000, 'refunded', 'annual', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E152'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E130', 1, 'individual', '張耀仁', '橘貓本舖', '台中市大里區工業5路35號', NULL, '1982081300:00:00', '92305367', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E130-END', 'virtual_office', '2020-01-01', '2023-03-02', 1800, 3000, 'refunded', 'annual', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E130'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E175', 1, 'individual', '張詠翔', '智穎智能股份有限公司', '新竹縣竹北市 東興里26鄰自強北路338號十九樓之 3', NULL, '76/9/25', '83603490', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E175-END', 'virtual_office', '2020-01-01', '2023-09-08', 2000, 3000, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E175'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E213', 1, 'individual', '蕭智翔', NULL, '台中市中區民族路152之4之1號 ', NULL, NULL, NULL, 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E213-END', 'virtual_office', '2020-01-01', '2024-05-03', 3000, 3000, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E213'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E042', 1, 'individual', '蔡依紜', '青履客社創有限公司', '苗栗縣公館鄉石墻村10鄰203號', NULL, NULL, '91048138', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E042-END', 'virtual_office', '2020-01-01', '2023-06-17', 1500, 3000, 'refunded', 'annual', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E042'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E208', 1, 'individual', '吳詩芸', '娜拉葆商店', '臺中市北區陝西八街 2 號八樓之 5', NULL, '1987061900:00:00', '94520074', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E208-END', 'virtual_office', '2020-01-01', '2024-02-19', 12, 3000, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E208'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E222', 1, 'individual', '陳彥伯', NULL, '台中市北屯區天津路四段253號', NULL, NULL, NULL, 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E222-END', 'virtual_office', '2020-01-01', '2024-08-01', 3000, 3000, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E222'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E219', 1, 'individual', '趙珮宇', '斜作股份有限公司', '台中市南屯區大墩四街10號8F', NULL, '1983112200:00:00', '90820922', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E219-END', 'virtual_office', '2020-01-01', '2024-07-15', 3000, 3000, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E219'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E204', 1, 'individual', '洪晟瑋', '銀好運工作室', '彰化縣彰化市民生路294號', NULL, '1986103100:00:00', NULL, 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E204-END', 'virtual_office', '2020-01-01', '2024-03-20', 1490, 3000, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E204'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E221', 1, 'individual', '秦鶴梅', '明宸企業社', '高雄市楠梓區監理街49號9樓', NULL, '1970100500:00:00', NULL, 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E221-END', 'virtual_office', '2020-01-01', '2024-07-23', 1690, 3000, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E221'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E135', 1, 'individual', '許瑞伶', '富秤子實業', '台中市烏日區健型南一路20號', NULL, NULL, '92309075', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E135-END', 'virtual_office', '2020-01-01', '2024-03-08', 1800, 3000, 'refunded', 'annual', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E135'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E023', 1, 'individual', '楊淳淳', '日淳插畫工作室', '台中市豐原區朴子街376巷15號', NULL, '1974033000:00:00', '87265626', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E023-END', 'virtual_office', '2020-01-01', '2023-11-07', 1500, 3000, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E023'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E185', 1, 'individual', '李易昇', '營管家行銷企劃有限公司', '台中市清水區民治五街146號', NULL, '1974122700:00:00', '83207576', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E185-END', 'virtual_office', '2020-01-01', '2023-07-11', 1490, 3000, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E185'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E233', 1, 'individual', '楊明勳', '愛迪生創意有限公司', '台中市西屯區重慶路99號5樓之3', NULL, '1983031700:00:00', '83015296', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E233-END', 'virtual_office', '2020-01-01', '2024-11-06', 22000, 3000, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E233'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E184', 1, 'individual', '劉宛姍 ', '雪嫩代購', '台北市南港區同德路38號7F-1', NULL, '1977101800:00:00', '93432215', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E184-END', 'virtual_office', '2020-01-01', '2023-07-17', 1800, 3000, 'refunded', 'annual', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E184'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E178', 1, 'individual', '洪筱璇', '羽蛇科技有限公司', '台中市西屯區文心路三段181-1號', NULL, '1984033000:00:00', NULL, 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E178-END', 'virtual_office', '2020-01-01', '2023-09-11', 1490, 3000, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E178'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E165', 1, 'individual', '張惠如', '沐樂創意設計', '台中市西區進化北路365號7樓-2', NULL, '1976121000:00:00', '83462029', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E165-END', 'virtual_office', '2021-09-10', '2023-06-19', 1490, 3000, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E165'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E045', 1, 'individual', '胡金泉', '廣揚開發工程行', NULL, NULL, NULL, '87285664', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E045-END', 'virtual_office', '2020-01-01', '2023-08-05', 1490, 3000, 'refunded', 'annual', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E045'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E148', 1, 'individual', '鄭仁華', '源夢有限公司', '彰化縣埤頭鄉斗苑西路273號', NULL, '1965070400:00:00', '94119473', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E148-END', 'virtual_office', '2020-01-01', '2023-03-27', 1490, 3000, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E148'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E125', 1, 'individual', '蘇德忠', '德忠企業有限公司', NULL, NULL, '1954102900:00:00', '83718731', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E125-END', 'virtual_office', '2020-01-01', '2023-03-01', 1490, 3000, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E125'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E049', 1, 'individual', '羅玉萍', '紘翔餐飲顧問', '台中市西區公館里33鄰五權六街1號三樓之2', NULL, '1967021400:00:00', '83121249', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E049-END', 'virtual_office', '2020-01-01', '2023-10-20', 1800, 3000, 'refunded', 'annual', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E049'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E094', 1, 'individual', '華采榆', '崇昕工作室', '彰化縣員林市臨崙雅里12鄰崙雅巷49號', NULL, '1990070700:00:00', '88670665', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E094-END', 'virtual_office', '2020-01-01', '2023-10-12', 1800, 3000, 'refunded', 'annual', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E094'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E189', 1, 'individual', '徐辰侑', '派特里克國際有限公司', '台中市西屯區至善路77號15樓-2', NULL, '1990071100:00:00', '93580622', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E189-END', 'virtual_office', '2020-01-01', '2023-11-14', 1490, 3000, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E189'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E066', 1, 'individual', '李沁澐', '沁享美學坊', '台中市太平區新平路一段71號', NULL, '1974082700:00:00', '88533945', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E066-END', 'virtual_office', '2020-01-01', '2024-01-03', 1800, 3000, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E066'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E187', 1, 'individual', '張祐寧', '薩摩亞商青盈國際有限公司', NULL, NULL, '1985111500:00:00', '90513585', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E187-END', 'virtual_office', '2020-01-01', '2024-08-03', 1800, 3000, 'refunded', 'annual', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E187'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E210', 1, 'individual', '吳振領', '錦鑫工程行', '新竹縣竹北市聯興里18鄰新興路465號', NULL, '1982122500:00:00', '94535718', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E210-END', 'virtual_office', '2020-01-01', '2024-04-15', 1800, 3000, 'refunded', 'annual', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E210'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E225', 1, 'individual', '王基銘', '鏵沅國際有限公司', '台中市霧峰區暗坑巷17號', NULL, '1966070300:00:00', '90402526', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E225-END', 'virtual_office', '2020-01-01', '2024-09-23', 1690, 3000, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E225'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E226', 1, 'individual', '王基銘', '晉謙股份有限公司', '台中市霧峰區暗坑巷17號', NULL, '1966070300:00:00', '54311017', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E226-END', 'virtual_office', '2020-01-01', '2024-09-12', 1690, 3000, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E226'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E227', 1, 'individual', '謝維倫', '景曜企業有限公司', '雲林縣林內鄉林北村中正東路94巷10號', NULL, '1983110200:00:00', '89779967', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E227-END', 'virtual_office', '2020-01-01', '2024-09-12', 1690, 3000, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E227'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E137', 1, 'individual', '曾華田', '旭振貿易有限公司', NULL, NULL, NULL, '83003003', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E137-END', 'virtual_office', '2020-01-01', '2023-03-03', 1490, 3000, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E137'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E138', 1, 'individual', '曾華田', '集利貿易有限公司', NULL, NULL, NULL, '82997469', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E138-END', 'virtual_office', '2020-01-01', '2023-03-03', 1490, 3000, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E138'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E139', 1, 'individual', '曾華田', '鑫盛贏貿易有限公司', NULL, NULL, NULL, '83003242', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E139-END', 'virtual_office', '2020-01-01', '2023-03-03', 1490, 3000, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E139'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E140', 1, 'individual', '曾華田', '榮盛隆貿易有限公司', NULL, NULL, NULL, NULL, 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E140-END', 'virtual_office', '2020-01-01', '2023-03-03', 1490, 3000, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E140'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E186', 1, 'individual', '曾湘稘', '曾德很棒有限公司', '臺中市太平區新興里5鄰新光路新生五十巷22弄6號', NULL, '1976060600:00:00', '93793245', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E186-END', 'virtual_office', '2020-01-01', '2023-06-02', 1490, 3000, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E186'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E207', 1, 'individual', '鐘韋程', '嘉功工程行', '彰化縣溪州鄉東州村永安路158號', NULL, '1992082200:00:00', '94533036', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E207-END', 'virtual_office', '2020-01-01', '2024-03-28', 1800, 3000, 'refunded', 'annual', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E207'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E134', 1, 'individual', '胡良輝', '穩賀工程行', '台中市太平區新城里樹孝路115巷6之12號九樓', NULL, '1959122600:00:00', '92314083', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E134-END', 'virtual_office', '2020-01-01', '2023-04-12', 1490, 3000, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E134'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E155', 1, 'individual', '吳怡姍', '吾一家商行', '台中市北區健行路443號12樓之37', NULL, '1977061300:00:00', '93356531', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E155-END', 'virtual_office', '2020-01-01', '2024-04-25', 1800, 3000, 'refunded', 'annual', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E155'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E157', 1, 'individual', '巫成妍', '成泰豐有限公司', '南投縣國姓鄉國姓路278號', NULL, '1987012700:00:00', '94112247', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E157-END', 'virtual_office', '2020-01-01', '2024-05-10', 1490, 3000, 'refunded', 'annual', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E157'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E203', 1, 'individual', '蔣榮宗', '妙博士數位文創有限公司', '臺中市西區大忠里1鄰大忠南街55號7樓之5 ', NULL, NULL, '93576866', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E203-END', 'virtual_office', '2020-01-01', '2024-03-11', 1650, 3000, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E203'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E161', 1, 'individual', '顏珮羽', '粲朔企業社', '桃園市八德區興仁里13鄰中正一路140號四樓', NULL, '1981010500:00:00', '93362816', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E161-END', 'virtual_office', '2020-01-01', '2024-05-22', 1800, 3000, 'refunded', 'annual', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E161'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E174', 1, 'individual', '賴彥廷', '喜特室內裝修有限公司', '台中市北屯區軍和街489之38號', NULL, '1978012100:00:00', '94227022', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E174-END', 'virtual_office', '2020-01-01', '2023-09-11', 1490, 3000, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E174'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E132', 1, 'individual', '陳建誠', '銳克科技有限公司', '台中市北屯區雷中街40-8', NULL, '1986101800:00:00', '83016865', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E132-END', 'virtual_office', '2020-01-01', '2024-03-02', 1800, 3000, 'refunded', 'annual', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E132'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E159', 1, 'individual', '劉真如', '鯉選物有限公司', '臺北市信義區安康里11鄰虎林街164巷15-2號一樓', NULL, '1975030200:00:00', '94082237', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E159-END', 'virtual_office', '2020-01-01', '2023-05-12', 1490, 3000, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E159'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E218', 1, 'individual', '包奇艷 ', '昀績企業社', '台中市南屯區東興路三段257號11樓之3', NULL, '1969102500:00:00', '94539405', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E218-END', 'virtual_office', '2020-01-01', '2024-07-04', 2000, 3000, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E218'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E115', 1, 'individual', '賴宗政', '天府不動產仲介有限公司', '台中市西屯區台灣大道888號6F-6', NULL, '1980030300:00:00', '89162294', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E115-END', 'virtual_office', '2020-01-01', '2023-01-06', 1490, 3000, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E115'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E223', 1, 'individual', '賴宗政', '天府不動產有限公司', '台中市西屯區台灣大道888號6F-6', NULL, '1980030300:00:00', '94183072', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E223-END', 'virtual_office', '2020-01-01', '2024-08-14', 1690, 3000, 'refunded', 'annual', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E223'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E143', 1, 'individual', '夏宏義', '虹瑞行', '金門縣金門鎮汶沙里25鄰忠孝新邨20號', NULL, '1937020400:00:00', '92306611', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E143-END', 'virtual_office', '2020-01-01', '2024-03-16', 1800, 3000, 'refunded', 'annual', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E143'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E110', 1, 'individual', '王育祺', '光緯企業社', '苗栗縣苑裡鎮社苓里社苓１３之２號', NULL, NULL, '92252235', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E110-END', 'virtual_office', '2020-01-01', '2022-12-05', 1490, 3000, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E110'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E113', 1, 'individual', '李昕妮(李寶妮)', '悠陽實業社', '台中市大雅區建興路155號9f-5', NULL, '1977122900:00:00', '92289248', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E113-END', 'virtual_office', '2020-01-01', '2023-12-29', 1490, 3000, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E113'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E246', 1, 'individual', '吳世多', NULL, '苗栗縣竹南鎮新南里7鄰110巷6號4樓之7', NULL, '1997111800:00:00', NULL, 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E246-END', 'virtual_office', '2020-01-01', '2025-04-25', 3000, 3000, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E246'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E229', 1, 'individual', '朱栢逸', '影識科技有限公司', '台中市大雅區二和里學府路120巷39號', NULL, '1984120700:00:00', '00076671', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E229-END', 'virtual_office', '2020-01-01', '2024-10-11', 1690, 3000, 'refunded', 'monthly', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E229'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ('DZ-E234', 1, 'individual', '江雅宣', '星晨堂', '台中市東區東福路37號6F之2', NULL, '1982021200:00:00', '95259648', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-E234-END', 'virtual_office', '2020-01-01', '2024-11-18', 1800, 3000, 'refunded', 'annual', 'expired'
FROM customers c WHERE c.legacy_id = 'DZ-E234'
ON CONFLICT (contract_number) DO NOTHING;

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, phone, address, source_channel, status, metadata)
VALUES ('HR-EV03', 2, 'sole_proprietorship', '沈孟輝', '孟盟企業社', '95338691', 'M121312364', NULL, '南投縣仁愛鄉精英村富貴路58號', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 2, 'HR-EV03-END', 'virtual_office', '2025-04-09', '2027-04-09', 1800, 6000, 'held', 'semi_annual', 'expired'
FROM customers c WHERE c.legacy_id = 'HR-EV03'
ON CONFLICT (contract_number) DO NOTHING;


-- 驗證
SELECT 'churned_customers' as item, COUNT(*) FROM customers WHERE status = 'churned';
SELECT 'expired_contracts' as item, COUNT(*) FROM contracts WHERE status = 'expired';