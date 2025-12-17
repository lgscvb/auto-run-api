-- ============================================================================
-- Hour Jungle CRM - 資料匯入腳本
-- 生成時間: 2025-12-07 00:14:20
-- ============================================================================

-- 逐筆執行（不使用交易）

-- 清空現有資料（如需保留請註解掉）
-- TRUNCATE customers CASCADE;
-- TRUNCATE contracts CASCADE;

-- ============================================================================
-- 客戶資料
-- ============================================================================
INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-004', 1, 'individual', '陳孟暄', '明昶法律事務所', NULL, 'S125208599', NULL, '910606816', NULL, '台中市南區忠明南路582-23號13樓', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-020', 1, 'sole_proprietorship', '劉怡廷', '吉爾哈登工作室', '87279184', 'N225231461', NULL, '912347735', NULL, '彰化縣員林市林森路298號5F-1', NULL, 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-022', 1, 'sole_proprietorship', '廖佑泓', '廖氏商行', '87262304', 'B122005592', NULL, '905350658', NULL, '台中市中區成功路186號', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-031', 1, 'company', '謝居助', '洛酷科技有限公司', '90827024', 'N121779691', NULL, '986919616', NULL, '台中市大肚區沙田路一段710巷18-10號', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-032', 1, 'sole_proprietorship', '張文溥', '季節東京媄睫專業坊', '72350716', 'E223713810', NULL, '978579905', NULL, '台北市信義區中坡南路233-2號2樓', NULL, 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-047', 1, 'company', '陳苡端', '立湟有限公司', '90519589', 'A126593136', NULL, '912359211', NULL, '彰化縣田中進北路里18鄰中山街410號', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-053', 1, 'sole_proprietorship', '蔡淑華', '超省購生活用品企業社', '87287065', 'N222808835', NULL, '938238305', NULL, '彰化縣大村鄉貢旗村11鄰貢旗巷8號', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-055', 1, 'sole_proprietorship', '郭嘉玲', '一貝兒美容工作室', '82621674', 'L222286140', NULL, '978752611', NULL, '台中市大甲區蔣公路115號', NULL, 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-057', 1, 'company', '李冠葳', '鑫秝喨國際有限公司', '90666511', 'B122655547', NULL, '978522319', NULL, '台中市南屯區大墩十一街275號8樓', 'U71862c932a9db6cc864ba23e8e388b43', 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-058', 1, 'sole_proprietorship', '洪庭琦', '明偉水產行', '88527351', 'N225164778', NULL, '977357582', NULL, '彰化縣芳苑鄉新生村草崙路20號', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-063', 1, 'company', '呂育豪', '恩梯科技股份有限公司', '90627530', 'N125370318', '1995-01-23', '985858814', NULL, '彰化縣秀水鄉莊雅村7鄰寶溪巷23號', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-067', 1, 'sole_proprietorship', '陳景泰', '景泰批發實業社', '88535330', 'A129127634', NULL, '920118756', NULL, '台中市南屯區文昌街226巷85號', 'U30b64492180ce3a065d1dffe7fe2560e', 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-071', 1, 'sole_proprietorship', '陳羿綾', '利奇商行', '88542178', 'L224151819', NULL, '931106214', NULL, '台中市霧峰區甲寅里18鄰德泰街96號', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-073', 1, 'individual', '黃琬茹', '萊益國際股份有限公司台中分公司', '89154626', 'M222518424', NULL, '919503209', NULL, '南投縣國姓鄉長流村大長路541號', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-078', 1, 'individual', '郭靜萍', '貽順有限公司', '90238194', 'C220226612', NULL, '915306358', NULL, '新北市汐止區康寧75巷27號20樓-5', 'U50a9a392a916fb7006817704a63cc296', 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-080', 1, 'sole_proprietorship', '王聖堯', '小熊零件行', '88628251', 'L125648551', NULL, '916614121', NULL, '台中市北區育樂街62號17樓-8', 'Ufb8d81d002107f177ac5b93bd9cf2c9c', 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-082', 1, 'sole_proprietorship', '黃奕穎', '商贏企業', '87264561', 'B900114436', NULL, '902289190', NULL, '台中市太平區中平路1巷23號3F', 'U80ee7a4f8addaaeea6ce528ad11a3dbb', 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-085', 1, 'company', '湯詠為', '新遞國際開發有限公司', '90296031', 'B122655547', NULL, '978522319', NULL, '台中市南屯區大墩十一街275號8樓', 'U71862c932a9db6cc864ba23e8e388b43', 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-086', 1, 'company', '廖裕雄', '新大科技有限公司', '53400856', 'V1211161114', NULL, '912700355', NULL, '台中市西區大忠南街55號', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-087', 1, 'sole_proprietorship', '顏依庭', '福樂寵工作室', '88643176', 'B222445052', '1990-11-16', '958989820', NULL, '台中市東區二聖街141號', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-088', 1, 'company', '陳為彤', '由非室內裝修設計有限公司', '28341943', 'Q120027565', NULL, '426313952', NULL, '台中市龍井區藝術北街13號', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-091', 1, 'sole_proprietorship', '莊湘婷', '緁作工作室', '88653693', 'N225150041', NULL, '919574209', NULL, '彰化縣二林鎮大永里9鄰永安路12號', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-093', 1, 'sole_proprietorship', '洪鐿慈', '頌芝承工作室', '88666711', 'K222725277', NULL, '978361958', NULL, '台中市大甲區經國路2542巷10弄46號', 'Ub2045eb749e0732aa0e20312eb62723c', 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-095', 1, 'sole_proprietorship', '高弘哲', '原食工坊', '88669983', 'M122979792', NULL, '984362392', NULL, '南投縣埔里鎮八德路12號', 'U0919d0da2a66b5e5694a1dd2bf9b725e', 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-102', 1, 'sole_proprietorship', '尤仁聖', '樂沐金商行', '88451438', 'N123726858', NULL, '923236623', NULL, '台中市西區美村路一段270巷22號2樓', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-105', 1, 'individual', '陳進國', '照鴻貿易股份有限公司', '52854204', 'B120360394', NULL, '423023311', NULL, '台中市西區公益路155巷102號', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-106', 1, 'individual', '管世勤', '仁徠貿易股份有限公司', '13051163', 'F120237241', NULL, '423023311', NULL, '台中市西區公益路155巷102號', 'U352c5eb27f47976a6b9b0d43b9b58adf', 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-109', 1, 'sole_proprietorship', '張倩怡', '小倩媽咪行銷工作室', '92252191', 'N225091270', NULL, '911489389', NULL, '台中市南區樹義里3鄰大廈二段6之16巷45號6F之2', 'U01ec627387f6d649f58e6bd9372c00eb', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-111', 1, 'individual', '湯詠為', '華為秝喨國際有限公司', '89189940', 'B122655547', NULL, '978522319', NULL, '台中市南屯區大墩十一街275號8樓', 'U71862c932a9db6cc864ba23e8e388b43', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-112', 1, 'sole_proprietorship', '黃仲瑛(green)', '短腿基商鋪', '92286798', 'K220754341', NULL, '922283073', NULL, '南投市國姓鄉大長路611號', 'Ua74e6e49afada52033be0f809aa921b8', 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-114', 1, 'individual', '楊儒俊(楊滷蛋)', '順映影像有限公司', '89166685', 'N123470459', NULL, '920580835', NULL, '台中市南區學府路137號7F-1', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-116', 1, 'individual', '林倇如', '植光圈友善坊', '92296867', 'D222148247', NULL, '987098310', NULL, '台中市西區模範街18巷5號', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-117', 1, 'individual', '邱昱頵', '優翼科技工程有限公司', '82981086', 'H223992387', NULL, '973499197', NULL, '台中市清水區港埠路三段209號4樓之5', 'Uc0092ede60c8e32be7122fea123e0b27', 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-118', 1, 'sole_proprietorship', '傅裕田', '旺玖企業社', '92291618', 'N124859507', NULL, '968318469', NULL, '員林市林厝里山腳路一段坡姜巷30弄69號', 'U7eb7153773664d030d5001ecd0f79733', 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-122', 1, 'sole_proprietorship', '蔡承恩', '盛豐新流量商業社', '92301904', 'L124640828', NULL, '983256044', NULL, '台中市潭子區頭家東里大成街1巷111號4樓', 'U6de3e697009985ce48b6f6306a5e2676', 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-124', 1, 'sole_proprietorship', '張軒銘', '竺墨文創企業社', '88634275', 'B122988934', NULL, '902196881', NULL, '台中市南屯區同心里10鄰文心路一段182號十四樓之3', 'Ua4ceb223e5f74b3e0e25383fd16bd358', 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-126', 1, 'sole_proprietorship', '薛峻宇', '富丞裕國際商行', '92301539', 'S123823267', NULL, '975930960', NULL, '台中市沙鹿區興安路35-8號', 'U59e32e01dc9c2f47b440ccda4a49db4a', 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-127', 1, 'individual', '詹睿華', '步臻低碳策略有限公司', '24431726', 'Ｑ221373666', NULL, NULL, NULL, '高雄市小港區濟南里14鄰清山街123巷8號', 'Ue07cc2ad8efb2199bd1fea0593351c8b', 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-128', 1, 'sole_proprietorship', '徐繼涵', '范特希雅時光旅行小舖', '92307490', 'H222890857', NULL, '922346590', NULL, '桃園市桃園區三民路一段68巷7弄1號', 'U2f38c9befcf37902be6a4c9960f96800', 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-129', 1, 'sole_proprietorship', '楊鴻興', '大心沉香', '92302220', 'N125434817', NULL, '925308629', NULL, '南投縣仁愛鄉大同村高峰巷68號', 'U46cf41bba5a0df131f1e769e03a9a36a', 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-131', 1, 'sole_proprietorship', '李為承', '好日來商行', '92304938', 'L121409656', NULL, '988030656', NULL, '台中市太平區旱溪西路二段213號', 'U283ee794e3b586920a60fbb5b03872ab', 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-133', 1, 'individual', '蒲世派', '鎧將金屬開發有限公司', '84694173', 'N122014322', NULL, '928183556', NULL, '台中市烏日區溪南路一段745巷106-2號', 'U97d893c959c8d06a5fb6c47e0bf3ec39', 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-136', 1, 'individual', '曾華田', '宏川貿易有限公司', '83015269', 'Ｄ121780383', NULL, '988667577', NULL, '台南市北區賢北里13鄰賢北街39號', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-141', 1, 'individual', '廖宗恩', '天原興業有限公司', '90665386', 'B123645983', NULL, '907016816', NULL, '台中市太平區大興路16號12F-1', 'U04fdafa03cf04d7bb7947046dae6afa0', 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-144', 1, 'sole_proprietorship', '魏珮妤', '喂喂四聲喂工作室', '92308738', 'G221773178', NULL, '908885119', NULL, '台中市南區德富路282號9F', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-145', 1, 'sole_proprietorship', '蔡雅涵', '四春企業社', '92310559', 'E224139425', NULL, '937627963', NULL, '台中市北區五常街218號12F之5', 'Ud545357bef6ef3b88f244a837ef6b814', 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-147', 1, 'individual', '李春蓉', '承新文創有限公司', '83026890', 'L1221387899', NULL, '933702461', NULL, '台中市大墩二街46號13F之2', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-153', 1, 'individual', '楊凱程', '樸裕國際顧問有限公司', '82983015', 'F124132676', NULL, '977367515', NULL, '台中市清水區港埠路三段213號7F之2', 'U2f9578965b787c08434b6ba5e3e631ab', 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-154', 1, 'sole_proprietorship', '鄒采薇', '浩萊國際企業社', '93354230', 'B222002048', NULL, '925043978', NULL, '台中市潭子區榮興街21巷1號6F', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-158', 1, 'sole_proprietorship', '林聖淮', '知寬植行', '93357757', 'D123088159', NULL, '961210655', NULL, '台南市中西區西湖里湖美街70巷6號7F-12', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-160', 1, 'sole_proprietorship', '陳品彣', '顧寶藝工作室', '93419515', 'C221565387', NULL, '913047633', NULL, '台中市西區精誠五街33號6F-6', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-163', 1, 'company', '劉基寅', '劉基寅建築師事務所', '93456567', 'N124254986', NULL, '912638602', NULL, '彰化縣永靖鄉永北村北勢巷17弄43號', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-164', 1, 'individual', '曾宥榆', '川榆室所有限公司', '94020964', 'L223774176', NULL, '928116042', NULL, '台中市大里區新平街22巷22號4F', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-166', 1, 'sole_proprietorship', '林明宏', '小胖芭樂水果行', '93432714', 'B123221072', NULL, '976029328', NULL, '臺中市大雅區員林里18鄰
大林路164巷33號', 'Uea29ef0deb4c1ab6b9939c77e43f00e3', 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-167', 1, 'company', '王維翊', '帛珅有限公司', '94260605', 'L223745399', NULL, '928941685', NULL, '台中市大里區仁化路384號', 'U010ebbf5c87ba4751545caacd527e9ac', 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-168', 1, 'company', 'Sandra Boesiger', '艾瑟烘焙坊', '93426369', 'A900063022', NULL, '423741654', NULL, '台中市南屯區精科五路五號', 'Ud05f2e82ec54f2d15143f1ee841a6c36', 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-170', 1, 'sole_proprietorship', '楊志偉', '楊董企業社', '93437513', 'H121289345', NULL, '986179000', NULL, '台中市潭子區豐興路一段1巷1號', 'U3a4a85a4bfaf127dfe3f35d477972b49', 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-171', 1, 'individual', '蕭家如', NULL, NULL, 'M221749229', NULL, '921325738', NULL, '台中市烏日區日光路388號', 'U1849885058b2d3081bbb642473c4b2ab', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-172', 1, 'sole_proprietorship', '劉怡廷', '吉爾哈登工作室', '87279184', 'N225231461', NULL, '912347735', NULL, '彰化縣員林市林森路298號5F-1', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-173', 1, 'company', '張伯任', '智谷系統有限公司', '85080813', 'Н122470764', NULL, '988085328', NULL, '臺中市北屯區北屯里8鄰興安路一段30號二樓之5', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-179', 1, 'sole_proprietorship', '林可恬', '隱士播放清單商店', '93446230', 'A225389318', NULL, '976109705', NULL, '台中市西屯區大隆路169號4F-2', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-180', 1, 'company', '陳俊吉', '台灣心零售股份有限公司', '82779061', 'L121721479', NULL, '919049458', NULL, '台中市霧峰區丁台路822-2號', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-181', 1, 'company', '朱建勳', '七分之二的探索有限公司', '83082766', 'L123494966', NULL, '937245693', NULL, '台北市大安區復興南路一段219號4F', NULL, 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-183', 1, 'company', '楊舒如', '淬矩闢梯有限公司', '93772017', 'S124147051', NULL, '910884025', NULL, '臺中市烏日區烏日里5鄰公園路107號十二樓之1', 'U166a89a270dcdd2f3662711be05d9a1c', 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-190', 1, 'company', '程永富', '上永富科技股份有限公司', '53783798', 'P123234230', NULL, '229015606', NULL, '新北市新莊區中正路新生巷5號1F', 'U8e35ce2183c592b7710192984a6831e6', 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-192', 1, 'company', '李沛潔', '微笑玩家國際貿易有限公司', '90814626', 'A22563733', NULL, '986280780', NULL, '台中市南屯區大墩六街492-3號', NULL, 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-193', 1, 'sole_proprietorship', '劉韋萱', '究鮮商行', '94475681', 'S223664233', NULL, '918932838', NULL, '臺中縣大里市國光里14鄰大勝街15號', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-194', 1, 'sole_proprietorship', '楊珮茹', '鼎盛行銷', '94475697', 'L223975557', NULL, '981372357', NULL, '台中市環中路八段1513號', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-195', 1, 'company', '陳卉珊', '滿單有限公司', '93661526', 'D221495292', NULL, '955805511', NULL, '105 台北市光復北路11巷97號12樓之3', 'U43fd467642532dbefba8ba85d0be1638', 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-196', 1, 'company', '林鈺樺', '程晧事業有限公司', '93607235', 'B222594032', NULL, '982322578', NULL, '臺中市北屯區中清路二段312號', 'U5021b0a7b0ad95eeb1e89835446509d5', 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-197', 1, 'sole_proprietorship', '邱鏡泉', '泉佳鑫企業社', '94512530', 'S123202204', NULL, '987890098', NULL, '高雄市苓雅區輔仁路176巷31號3F', 'U898f4af1cb2615f1e6810a38d756864b', 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-198', 1, 'sole_proprietorship', '許翊萱', '世燁環境清潔企業社', '94513263', 'N221416879', NULL, '977453675', NULL, '台中市南屯區永春東七路800號9樓-2', 'U861b9d823dc0f1e4e82d34bb5cda2519', 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-199', 1, 'sole_proprietorship', '姚孟綺', '鼠適圈工作室', '94513783', 'D222667265', NULL, '983865832', NULL, '臺南市東區和平里仁和路7 9號之1 二樓之1', 'U26c052d7f3c506745e35660709ebe602', 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-201', 1, 'individual', '邱笠辛', NULL, NULL, NULL, NULL, '981682755', NULL, NULL, NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-202', 1, 'company', '林長佑', '農益富股份有限公司', '93701739', 'C120312522', NULL, '937514575', NULL, '新北市三峽區龍恩里11鄰大學路37之3號八樓', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-205', 1, 'sole_proprietorship', '劉姵君', '明采文創工作室', '94530050', NULL, NULL, '981218558', NULL, '臺中市太平區新福里中山路四段139巷11號', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-206', 1, 'sole_proprietorship', '江怡霈', '江小咪商行', '87059766', NULL, NULL, '936830112', NULL, '苗栗縣苗栗市維祥里22鄰維勝街32巷16弄23號', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-209', 1, 'company', '黃啟能', '搖滾山姆有限公司', '95490294', 'E122791756', NULL, '915948020', NULL, '彰化縣員林鎮大仁南街125巷5號', 'U61cd0ce7319e69a2eaf799fe5f691fba', 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-211', 1, 'company', '吳三奇', '吉品智慧科技有限公司', '93785266', 'E121500402', NULL, '73870185', NULL, '高雄市三民區九如一路61號6F', 'U56fb839af135bceaad15166f5943b99b', 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-212', 1, 'sole_proprietorship', '卓家宇', '晨甯水產行', '94539664', 'B122546490', NULL, '986491781', NULL, '台中市西屯路二段17號', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-214', 1, 'company', '林碩彥', '磐星能源科技有限公司', '42970268', 'N125331008', NULL, '932605045', NULL, '台北市大同區延平北路一段92號4樓之1', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-215', 1, 'sole_proprietorship', '余非', '哺哺行', '80062621', 'L225183753', NULL, '952030702', NULL, '南投縣埔里鎮清新里英六街2之2號', 'U9f3be8cdbb3d6343e533031df676bd0c', 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-216', 1, 'sole_proprietorship', '曾采琳', '機車俠機車行', '72327820', 'R220625117', NULL, '917998240', NULL, '台中市美德街203-2號', 'Ua09c938eef7d697236147f9f59af693a', 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-217', 1, 'company', '劉基寅', '劉基寅建築師事務所', '93456567', 'N124254986', NULL, '912638602', NULL, '彰化縣永靖鄉永北村北勢巷17弄43號', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-220', 1, 'company', '廖彩瑩', '沃土謙植有限公司', '90067451', 'M222417806', NULL, '968007628', NULL, '台中市大里區大智路565巷5之6號', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-224', 1, 'sole_proprietorship', '盧富粲', '伯樂商行', '92246847', 'L123138918', NULL, '976686277', NULL, '台中市太平區廣三街59號5樓', 'U194f44687f415c316a00081c075b8891', 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-228', 1, 'company', '黎文興', '興盛行銷管理顧問有限公司', NULL, 'Q00462418', NULL, '913212317', NULL, '台中市霧峰區樹仁路一街7號4樓', NULL, 'migration', 'active', '{"is_foreigner": true}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-230', 1, 'company', '郭氏花', '花芙辰寶國際行銷管理顧問有限公司', NULL, 'C2229545', NULL, '913212317', NULL, '台中市東區互助街138號2樓', NULL, 'migration', 'active', '{"is_foreigner": true}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-231', 1, 'sole_proprietorship', '李菊', '昇瑪商行', '95254432', 'B290161652', NULL, '911675218', NULL, '台中市梧棲區永興路一段630巷57號', 'U85dfcb8ea5b5d94a17fa3a16e5128c9c', 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-232', 1, 'company', '陳姵伶', '旭營興業有限公司', '80247075', 'A229501972', NULL, '424968588', NULL, '台中市大里區大峰路565-5號', 'Ufabe6b41968a955015253e73a22b1ed4', 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-234', 1, 'sole_proprietorship', '江雅宣', '星晨堂', '95259648', 'M222530537', NULL, '970134212', NULL, '台中市東區東福路37號6F之2', NULL, 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-235', 1, 'sole_proprietorship', '邱麗潔', '日安家商行', '91713034', 'K221644424', NULL, '986773601', NULL, '台北市大安區羅斯福路三段261之1號2樓之1', 'U7c1c917e16bab75a1f9bbad24bc72785', 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-236', 1, 'company', '陳印泰', '金如泰股份有限公司', NULL, NULL, NULL, '923190767', NULL, '南投縣草屯鎮博愛路423巷1弄10號', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-237', 1, 'company', '洪瑋辰', '辰緻國際有限公司', '90363642', 'B123734636', NULL, '912896333', NULL, '台北市中正區羅斯福路3段100之52號1樓', NULL, 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-238', 1, 'sole_proprietorship', '許淯珊', '弎弎審美在線工作室', '95320566', 'I200244463', NULL, '975752819', NULL, '南投縣魚池鄉大林村嵩山巷9號', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-239', 1, 'sole_proprietorship', '謝孟廷', '起床打單有限公司', NULL, 'H124682173', NULL, '983468787', NULL, '桃園市龍潭區梅龍三街189巷2之1號', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-240', 1, 'company', '鄭黃梅蘭', '益群團購顧問有限公司', '96759190', 'L200686591', NULL, '911659420', NULL, '台中市南屯區文心南三路281號5樓', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-241', 1, 'company', '鍾孟迪', '流星有限公司', NULL, 'L123575984', NULL, NULL, NULL, '雲林縣西螺鎮漢光里吉興街30號', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-242', 1, 'sole_proprietorship', '徐翊寧', '球球歐瑞歐工作室', '95325648', 'K222668820', NULL, '988359098', NULL, '苗栗縣三義鄉西湖村伯公坑40-16號', 'Ub8489a025c85394d728a962332b71a51', 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-243', 1, 'sole_proprietorship', '葉永清', '悠然餘閒手作坊', '95325627', 'L121349557', NULL, '98869363', NULL, '台中市豐原區北陽里18鄰南陽路92巷7弄11號', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-244', 1, 'sole_proprietorship', '謝詔寧', '御林軍御藝美妝', '95336244', 'H224490597', NULL, '968858717', NULL, '台中市太平區東平路1-8號', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-245', 1, 'sole_proprietorship', '林祥慶', '曜森生活工作室', '95333627', 'L124327884', NULL, '912858509', NULL, '台中市太平區新高里12鄰樹德路70巷10號', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-247', 1, 'company', '林昭勲', '兩兩空間製作所有限公司', '60694959', NULL, NULL, NULL, NULL, NULL, NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-248', 1, 'company', '蔡馥詠', '馥諦健康事業有限公司', '60574336', 'A229203293', NULL, '973035810', 'uong9408@gmail.com', '台北市萬華區和平西路3段382巷2弄35號4樓', 'U8ed09c8044e3309a7234f71840210e39', 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-249', 1, 'company', '廖昱軒', '協通實業有限公司', '60584117', 'L124019118', NULL, '955077346', NULL, '台中市大雅區月祥路838號', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-250', 1, 'company', '羅子翔', '子昇有限公司', '60620205', 'L125055369', NULL, '915299085', NULL, '台中市霧峰區舊正里2鄰象鼻路1之25號', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-251', 1, 'company', '林子堯', '中盛建維有限公司', '60622604', 'P120407106', NULL, '986328585', NULL, '台中市南區忠明南路582-8號11樓', 'U9aec05bf19d095831e9c065c4a18e6d2', 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-252', 1, 'company', '鄭凱謙', '溪流雲創意整合有限公司', '60621577', 'S124831165', NULL, '975112863', 'fcc510612@gmail.com', '高雄市大寮區民光街62巷2號6樓', NULL, 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-253', 1, 'company', '張奉琦', '仁琦科技有限公司', '60653438', 'B120765988', NULL, '931670862', NULL, '台中市西屯區福順路333號8樓之8', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-254', 1, 'sole_proprietorship', '阮氏海厚', '金海小舖', NULL, 'C5582139', NULL, '978097605', NULL, '台中市霧峰區吉峰東路76號之3', NULL, 'migration', 'active', '{"is_foreigner": true}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-255', 1, 'company', '張皓暐', '至溢營造有限公司', '89478110', 'S123343988', NULL, '424628999', NULL, '台中市西屯區福安里13鄰福科路343號', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-256', 1, 'company', '謝昇諺', '和和國際有限公司', '89127516', 'L123022219', NULL, '931094168', NULL, '台中市潭子區圓通南路194巷1弄5號', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-257', 1, 'company', '黃信樺', '証信法律事務所', NULL, 'N125138918', NULL, '938726399', NULL, '台中市南區建國南路2段158號7樓之1', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-258', 1, 'individual', '潘玫雯', NULL, NULL, 'M221354159', NULL, '930770307', NULL, '南投縣埔里鎮籃城里籃城路5號', NULL, 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-259', 1, 'company', '洪曉樺', '獨自紅有限公司', '60539985', 'L224205670', NULL, '953795503', NULL, '台中市太平區坪林里12鄰中山路2段177巷9號', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-260', 1, 'company', '沈邦文', '文瀛營造有限公司', '60495109', 'K121471983', NULL, '909588512', NULL, '台中市北屯區柳陽西街93巷7號5樓', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-261', 1, 'sole_proprietorship', '潘憲德', '捌伍設計', '60875902', 'B122172758', NULL, '987280985', NULL, '台中市西區民生北路97號4樓', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-262', 1, 'company', 'Ravin Wadhawan', '台灣科尼起重機設備有限公司', '24517420', NULL, NULL, '79708929', 'eva.lee@konecranes.com', NULL, NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('DZ-263', 1, 'individual', '劉翁昌', NULL, '86813152', 'L123160456', NULL, '921379482', NULL, '台中市南屯區文心南六路50號', NULL, 'migration', 'churned', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('HR-001', 2, 'sole_proprietorship', '吳瑋翎', '米恩恩商行', NULL, 'P223313309', NULL, '9331891630', NULL, '台中市豐原區南陽里29鄰保康路52號７F-5', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('HR-002', 2, 'sole_proprietorship', '朱芸靜', '朱芸工作室', NULL, 'V220875746', NULL, '9095249710', NULL, '台中市大里區西榮里12鄰東明路52號', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('HR-004', 2, 'sole_proprietorship', '陳枳岇', '善容塗裝工程行', NULL, NULL, NULL, '9727797370', NULL, '台中市梧棲區自強一街38號', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('HR-005', 2, 'sole_proprietorship', '陳玉美', '衫野人戶外用品', NULL, 'L202703159', NULL, '9859866890', NULL, '台中市沙鹿區正德路201巷36弄10號', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('HR-006', 2, 'company', '黃文貞', '傑文記帳士事務所', NULL, 'B221450666', NULL, '9213304860', NULL, '台中市南屯區大墩四街12號4樓', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('HR-007', 2, 'company', '羅子庭', '庭信有限公司', NULL, 'L125157126', NULL, '9854417780', NULL, '台中市霧峰區舊正里2鄰象鼻路1之25號', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('HR-008', 2, 'company', '羅子捷', '宏捷有限公司', NULL, 'L125370432', NULL, '9769055360', NULL, '台中市北屯區軍福十三路106號9樓之6', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('HR-009', 2, 'sole_proprietorship', '田秀華', '秀旺工程行', NULL, 'V220572044', NULL, '9238586650', NULL, '屏東縣瑪家鄉北葉村4鄰風景33之2號', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('HR-010', 2, 'sole_proprietorship', '羅瑞寶', '寶信工程行', NULL, 'H120171017', NULL, '9559228760', NULL, '桃園縣桃園市大有里11鄰民有十五街10號8樓', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('HR-011', 2, 'sole_proprietorship', '鍾耀霆', '物潤實業社', NULL, 'T124306015', NULL, NULL, NULL, '屏東縣九如鄉維新街106巷15號', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('HR-012', 2, 'sole_proprietorship', '陳詩宜', '暮斯迪歐工作室', NULL, 'A229118913', NULL, '9320394900', NULL, '台北市信義區黎順里崇德街19號5樓', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('HR-013', 2, 'company', '張呈如', '台灣海協有限公司', NULL, 'L224041023', NULL, '9091991970', NULL, '台中市清水區大勇路128號9樓之7', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('HR-014', 2, 'sole_proprietorship', '黎小貞', '可愛企業社', NULL, 'N260029758', NULL, '9731098550', 'minhkhang170213@gmail.com', '彰化縣社頭鄉社頭村13鄰員集路2段420巷1號', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('HR-015', 2, 'sole_proprietorship', '劉思辰', NULL, NULL, 'F228082964', NULL, '9772719270', NULL, '台中市南屯區黎明路2段96號14樓', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('HR-016', 2, 'sole_proprietorship', '朱冠霖', '佑富裕地產企業社', NULL, 'Q122921262', NULL, '9743993990', NULL, '台中市太平區新興一街1號14樓之5', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('HR-017', 2, 'sole_proprietorship', '賈千儀', '石在晶閃工作室', NULL, 'B222886624', NULL, '9702917130', NULL, '台中市西屯區何仁里永昌二街68之1號2樓', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('HR-018', 2, 'sole_proprietorship', '林基賢', '緒瑕工作室', NULL, 'P102780782', NULL, '9128585090', NULL, '臺中市太平區宜昌里16鄰環中東路四段67號十三樓之2', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('HR-019', 2, 'company', '洪莉晴', '鎧鋐企業股份有限公司', NULL, 'L221634375', NULL, NULL, NULL, '彰化縣溪湖鎮西寮里員鹿路4段８７巷１４號', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('HR-020', 2, 'sole_proprietorship', '彭郁斐', '斐樂晶礦企業社', NULL, 'H223183600', NULL, '9032922290', NULL, '桃園市楊梅區裕成里光裕南街97巷9號', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('HR-021', 2, 'company', '歐玫君', '大穀達人股份有限公司', NULL, 'N125285323', NULL, '9114939210', NULL, '台中市烏日區光日路406號', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('HR-022', 2, 'sole_proprietorship', '陳柏諺', '禾頤商貿行', NULL, 'E124878470', NULL, '9058130580', NULL, '苗栗縣銅鑼鄉朝陽村朝東35之15號7樓之2', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('HR-023', 2, 'sole_proprietorship', '廖含珮', '翊棠商行', NULL, 'B221416333', NULL, '9283128580', NULL, '台中市西區後龍里20鄰民族路331號', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('HR-024', 2, 'sole_proprietorship', '黃羽衫', '亮京京工程清潔企業社', NULL, 'H2244671994', NULL, '9150609090', NULL, '台中市南區工學里14鄰大慶街一段130巷7號3樓之二', 'Ufbbba31c18ae0748ad726ba922437882', 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('HR-025', 2, 'sole_proprietorship', '王盈甄', '甄選良品生活百貨行', NULL, 'L223141328', NULL, '9552288930', NULL, '台中市太平區德隆里19鄰德明路396巷43號', NULL, 'migration', 'active', '{"is_foreigner": false}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ('HR-026', 2, 'company', '黃宥勝', '羅摩工程貿易有限公司', NULL, 'N80006740', NULL, '9071529800', NULL, '彰化縣伸港鄉新港村水尾路16號3樓之3', NULL, 'migration', 'active', '{"is_foreigner": true}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();


-- ============================================================================
-- 合約資料
-- ============================================================================
INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-004-2025', 'virtual_office', '2025-05-10', '2026-05-10', 12000, 22000.0, 'held', 'monthly',
    CASE WHEN '2026-05-10' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-004'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-020-2025', 'virtual_office', '2024-06-05', '2025-06-05', 1500, 3000.0, 'held', 'annual',
    CASE WHEN '2025-06-05' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-020'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-022-2025', 'virtual_office', '2024-04-25', '2026-04-25', 1490, 3000.0, 'held', 'monthly',
    CASE WHEN '2026-04-25' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-022'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-031-2025', 'virtual_office', '2024-02-15', '2026-02-15', 1490, 3000.0, 'held', 'monthly',
    CASE WHEN '2026-02-15' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-031'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-032-2025', 'virtual_office', '2024-10-31', '2025-10-31', 1500, 3000.0, 'held', 'annual',
    CASE WHEN '2025-10-31' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-032'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-047-2025', 'virtual_office', '2025-11-18', '2027-11-18', 1690, 3000.0, 'held', 'biennial',
    CASE WHEN '2027-11-18' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-047'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-053-2025', 'virtual_office', '2025-06-30', '2026-06-30', 1800, 3000.0, 'held', 'annual',
    CASE WHEN '2026-06-30' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-053'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-055-2025', 'virtual_office', '2023-10-01', '2025-10-01', 1490, 3000.0, 'held', 'monthly',
    CASE WHEN '2025-10-01' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-055'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-057-2025', 'virtual_office', '2024-12-08', '2026-12-08', 1690, 3000.0, 'held', 'monthly',
    CASE WHEN '2026-12-08' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-057'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-058-2025', 'virtual_office', '2023-12-07', '2025-12-07', 1490, 3000.0, 'held', 'monthly',
    CASE WHEN '2025-12-07' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-058'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-063-2025', 'virtual_office', '2023-12-22', '2025-12-22', 1490, 3000.0, 'held', 'monthly',
    CASE WHEN '2025-12-22' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-063'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-067-2025', 'virtual_office', '2024-01-05', '2026-01-05', 1490, 6000.0, 'held', 'monthly',
    CASE WHEN '2026-01-05' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-067'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-071-2025', 'virtual_office', '2025-02-10', '2026-02-10', 2000, 3000.0, 'held', 'annual',
    CASE WHEN '2026-02-10' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-071'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-073-2025', 'virtual_office', '2024-03-01', '2026-03-01', 1650, 3000.0, 'held', 'monthly',
    CASE WHEN '2026-03-01' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-073'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-078-2025', 'virtual_office', '2025-03-24', '2027-03-24', 1800, 6000.0, 'held', 'biennial',
    CASE WHEN '2027-03-24' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-078'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-080-2025', 'virtual_office', '2025-04-01', '2027-04-01', 1800, 6000.0, 'held', 'monthly',
    CASE WHEN '2027-04-01' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-080'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-082-2025', 'virtual_office', '2025-04-11', '2027-04-11', 1800, 6000.0, 'held', 'biennial',
    CASE WHEN '2027-04-11' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-082'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-085-2025', 'virtual_office', '2025-06-01', '2027-06-01', 2000, 6000.0, 'held', 'monthly',
    CASE WHEN '2027-06-01' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-085'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-086-2025', 'virtual_office', '2024-11-05', '2026-11-04', 1690, 2980.0, 'held', 'monthly',
    CASE WHEN '2026-11-04' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-086'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-087-2025', 'virtual_office', '2025-05-30', '2027-05-30', 1690, 6000.0, 'held', 'biennial',
    CASE WHEN '2027-05-30' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-087'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-088-2025', 'virtual_office', '2025-06-28', '2027-06-28', 1690, 6000.0, 'held', 'monthly',
    CASE WHEN '2027-06-28' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-088'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-091-2025', 'virtual_office', '2025-07-18', '2026-07-18', 1800, 6000.0, 'held', 'monthly',
    CASE WHEN '2026-07-18' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-091'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-093-2025', 'virtual_office', '2025-08-08', '2027-08-08', 1690, 6000.0, 'held', 'monthly',
    CASE WHEN '2027-08-08' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-093'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-095-2025', 'virtual_office', '2025-08-17', '2026-08-17', 1800, 6000.0, 'held', 'monthly',
    CASE WHEN '2026-08-17' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-095'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-102-2025', 'virtual_office', '2025-04-18', '2026-10-18', 2000, 6000.0, 'held', 'monthly',
    CASE WHEN '2026-10-18' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-102'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-105-2025', 'virtual_office', '2025-11-30', '2026-11-30', 1800, 6000.0, 'held', 'monthly',
    CASE WHEN '2026-11-30' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-105'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-106-2025', 'virtual_office', '2025-11-30', '2026-11-30', 1800, 6000.0, 'held', 'monthly',
    CASE WHEN '2026-11-30' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-106'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-109-2025', 'virtual_office', '2024-12-02', '2025-12-02', 1800, 6000.0, 'held', 'monthly',
    CASE WHEN '2025-12-02' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-109'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-111-2025', 'virtual_office', '2022-12-10', '2024-12-10', 1490, 6000.0, 'held', 'monthly',
    CASE WHEN '2024-12-10' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-111'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-112-2025', 'virtual_office', '2024-12-13', '2025-12-13', 1800, 6000.0, 'held', 'monthly',
    CASE WHEN '2025-12-13' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-112'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-114-2025', 'virtual_office', '2025-01-03', '2026-01-03', 1490, 6000.0, 'held', 'monthly',
    CASE WHEN '2026-01-03' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-114'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-116-2025', 'virtual_office', '2025-02-01', '2026-02-01', 1800, 6000.0, 'held', 'monthly',
    CASE WHEN '2026-02-01' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-116'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-117-2025', 'virtual_office', '2025-02-21', '2027-02-20', 1690, 6000.0, 'held', 'monthly',
    CASE WHEN '2027-02-20' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-117'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-118-2025', 'virtual_office', '2025-01-12', '2027-01-12', 1800, 6000.0, 'held', 'monthly',
    CASE WHEN '2027-01-12' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-118'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-122-2025', 'virtual_office', '2025-03-01', '2027-03-01', 2000, 6000.0, 'held', 'monthly',
    CASE WHEN '2027-03-01' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-122'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-124-2025', 'virtual_office', '2025-02-16', '2026-02-16', 1800, 6000.0, 'held', 'monthly',
    CASE WHEN '2026-02-16' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-124'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-126-2025', 'virtual_office', '2025-03-01', '2027-03-01', 2000, 6000.0, 'held', 'monthly',
    CASE WHEN '2027-03-01' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-126'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-127-2025', 'virtual_office', '2025-02-24', '2027-02-24', 1800, 6000.0, 'held', 'monthly',
    CASE WHEN '2027-02-24' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-127'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-128-2025', 'virtual_office', '2025-03-01', '2027-03-01', 1800, 6000.0, 'held', 'monthly',
    CASE WHEN '2027-03-01' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-128'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-129-2025', 'virtual_office', '2025-03-01', '2027-03-01', 1800, 6000.0, 'held', 'biennial',
    CASE WHEN '2027-03-01' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-129'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-131-2025', 'virtual_office', '2025-03-02', '2026-03-02', 1800, 6000.0, 'held', 'monthly',
    CASE WHEN '2026-03-02' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-131'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-133-2025', 'virtual_office', '2025-03-06', '2027-03-06', 1800, 6000.0, 'held', 'monthly',
    CASE WHEN '2027-03-06' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-133'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-136-2025', 'virtual_office', '2025-03-03', '2027-03-03', 1800, 6000.0, 'held', 'monthly',
    CASE WHEN '2027-03-03' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-136'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-141-2025', 'virtual_office', '2025-03-06', '2027-03-06', 2000, 6000.0, 'held', 'monthly',
    CASE WHEN '2027-03-06' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-141'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-144-2025', 'virtual_office', '2025-03-17', '2027-03-17', 1800, 6000.0, 'held', 'monthly',
    CASE WHEN '2027-03-17' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-144'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-145-2025', 'virtual_office', '2025-03-01', '2027-03-01', 1800, 6000.0, 'held', 'monthly',
    CASE WHEN '2027-03-01' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-145'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-147-2025', 'virtual_office', '2025-04-01', '2027-04-01', 1800, 6000.0, 'held', 'monthly',
    CASE WHEN '2027-04-01' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-147'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-153-2025', 'virtual_office', '2025-04-25', '2027-04-25', 1800, 6000.0, 'held', 'monthly',
    CASE WHEN '2027-04-25' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-153'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-154-2025', 'virtual_office', '2025-04-19', '2027-04-19', 1800, 6000.0, 'held', 'monthly',
    CASE WHEN '2027-04-19' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-154'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-158-2025', 'virtual_office', '2025-05-10', '2026-05-10', 2000, 6000.0, 'held', 'monthly',
    CASE WHEN '2026-05-10' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-158'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-160-2025', 'virtual_office', '2025-06-21', '2026-06-21', 1800, 6000.0, 'held', 'monthly',
    CASE WHEN '2026-06-21' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-160'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-163-2025', 'virtual_office', '2025-06-15', '2030-06-15', 1490, 6000.0, 'held', 'monthly',
    CASE WHEN '2030-06-15' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-163'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-164-2025', 'virtual_office', '2025-06-14', '2027-06-14', 1800, 6000.0, 'held', 'monthly',
    CASE WHEN '2027-06-14' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-164'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-166-2025', 'virtual_office', '2025-07-17', '2026-07-17', 1800, 6000.0, 'held', 'monthly',
    CASE WHEN '2026-07-17' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-166'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-167-2025', 'virtual_office', '2025-07-25', '2027-07-25', 1490, 6000.0, 'held', 'monthly',
    CASE WHEN '2027-07-25' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-167'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-168-2025', 'virtual_office', '2025-07-14', '2026-07-14', 1800, 6000.0, 'held', 'monthly',
    CASE WHEN '2026-07-14' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-168'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-170-2025', 'virtual_office', '2025-08-14', '2026-08-14', 1800, 6000.0, 'held', 'monthly',
    CASE WHEN '2026-08-14' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-170'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-171-2025', 'virtual_office', '2023-08-07', '2024-08-07', 3000, 0, 'held', 'monthly',
    CASE WHEN '2024-08-07' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-171'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-172-2025', 'virtual_office', '2025-09-01', '2026-09-01', 12825, 13500.0, 'held', 'monthly',
    CASE WHEN '2026-09-01' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-172'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-173-2025', 'virtual_office', '2025-09-01', '2027-09-01', 1490, 6000.0, 'held', 'monthly',
    CASE WHEN '2027-09-01' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-173'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-179-2025', 'virtual_office', '2025-09-14', '2026-09-14', 1800, 6000.0, 'held', 'monthly',
    CASE WHEN '2026-09-14' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-179'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-180-2025', 'virtual_office', '2025-10-01', '2027-10-01', 1490, 6000.0, 'held', 'monthly',
    CASE WHEN '2027-10-01' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-180'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-181-2025', 'virtual_office', '2023-09-21', '2025-09-21', 1490, 6000.0, 'held', 'monthly',
    CASE WHEN '2025-09-21' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-181'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-183-2025', 'virtual_office', '2025-07-11', '2027-07-11', 1490, 6000.0, 'held', 'monthly',
    CASE WHEN '2027-07-11' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-183'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-190-2025', 'virtual_office', '2025-11-15', '2026-11-15', 1800, 6000.0, 'held', 'semi_annual',
    CASE WHEN '2026-11-15' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-190'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-192-2025', 'virtual_office', '2024-11-27', '2025-11-27', 1800, 6000.0, 'held', 'monthly',
    CASE WHEN '2025-11-27' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-192'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-193-2025', 'virtual_office', '2025-11-24', '2027-11-24', 1690, 6000.0, 'held', 'monthly',
    CASE WHEN '2027-11-24' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-193'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-194-2025', 'virtual_office', '2025-11-17', '2026-11-17', 1000, 6000.0, 'held', 'semi_annual',
    CASE WHEN '2026-11-17' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-194'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-195-2025', 'virtual_office', '2023-12-14', '2025-12-14', 1490, 6000.0, 'held', 'semi_annual',
    CASE WHEN '2025-12-14' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-195'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-196-2025', 'virtual_office', '2023-12-27', '2025-12-27', 1490, 6000.0, 'held', 'monthly',
    CASE WHEN '2025-12-27' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-196'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-197-2025', 'virtual_office', '2024-01-12', '2026-01-12', 1490, 6000.0, 'held', 'semi_annual',
    CASE WHEN '2026-01-12' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-197'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-198-2025', 'virtual_office', '2024-01-08', '2026-01-08', 1490, 6000.0, 'held', 'semi_annual',
    CASE WHEN '2026-01-08' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-198'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-199-2025', 'virtual_office', '2025-01-10', '2026-01-10', 1800, 6000.0, 'held', 'monthly',
    CASE WHEN '2026-01-10' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-199'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-201-2025', 'virtual_office', '2025-02-16', '2026-02-16', 12000, 0, 'held', 'monthly',
    CASE WHEN '2026-02-16' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-201'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-202-2025', 'virtual_office', '2024-03-08', '2026-03-08', 1650, 6000.0, 'held', 'semi_annual',
    CASE WHEN '2026-03-08' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-202'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-205-2025', 'virtual_office', '2025-03-18', '2027-03-18', 1800, 6000.0, 'held', 'semi_annual',
    CASE WHEN '2027-03-18' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-205'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-206-2025', 'virtual_office', '2024-03-13', '2026-03-13', 1650, 6000.0, 'held', 'semi_annual',
    CASE WHEN '2026-03-13' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-206'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-209-2025', 'virtual_office', '2024-04-09', '2026-04-09', 1650, 6000.0, 'held', 'semi_annual',
    CASE WHEN '2026-04-09' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-209'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-211-2025', 'virtual_office', '2024-05-01', '2025-05-01', 12000, 12000.0, 'held', 'monthly',
    CASE WHEN '2025-05-01' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-211'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-212-2025', 'virtual_office', '2025-05-02', '2027-05-02', 1800, 6000.0, 'held', 'semi_annual',
    CASE WHEN '2027-05-02' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-212'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-214-2025', 'virtual_office', '2024-05-14', '2026-05-14', 1690, 6000.0, 'held', 'semi_annual',
    CASE WHEN '2026-05-14' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-214'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-215-2025', 'virtual_office', '2024-05-30', '2026-05-30', 1690, 6000.0, 'held', 'semi_annual',
    CASE WHEN '2026-05-30' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-215'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-216-2025', 'virtual_office', '2024-05-31', '2026-05-31', 1690, 6000.0, 'held', 'semi_annual',
    CASE WHEN '2026-05-31' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-216'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-217-2025', 'virtual_office', '2025-06-01', '2030-06-01', 10880, 9880.0, 'held', 'monthly',
    CASE WHEN '2030-06-01' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-217'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-220-2025', 'virtual_office', '2024-07-22', '2026-07-22', 1690, 6000.0, 'held', 'semi_annual',
    CASE WHEN '2026-07-22' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-220'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-224-2025', 'virtual_office', '2025-08-28', '2027-08-28', 2000, 6000.0, 'held', 'semi_annual',
    CASE WHEN '2027-08-28' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-224'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-228-2025', 'virtual_office', '2024-08-23', '2026-08-23', 1690, 6000.0, 'held', 'semi_annual',
    CASE WHEN '2026-08-23' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-228'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-230-2025', 'virtual_office', '2024-09-25', '2026-09-25', 1690, 6000.0, 'held', 'semi_annual',
    CASE WHEN '2026-09-25' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-230'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-231-2025', 'virtual_office', '2024-10-25', '2026-10-25', 1690, 6000.0, 'held', 'semi_annual',
    CASE WHEN '2026-10-25' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-231'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-232-2025', 'virtual_office', '2024-11-20', '2027-11-19', 1800, 6000.0, 'held', 'monthly',
    CASE WHEN '2027-11-19' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-232'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-234-2025', 'virtual_office', '2024-11-18', '2025-11-18', 1800, 6000.0, 'held', 'annual',
    CASE WHEN '2025-11-18' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-234'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-235-2025', 'virtual_office', '2025-11-21', '2027-11-21', 1690, 6000.0, 'held', 'biennial',
    CASE WHEN '2027-11-21' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-235'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-236-2025', 'virtual_office', '2024-11-10', '2026-11-10', 1690, 6000.0, 'held', 'semi_annual',
    CASE WHEN '2026-11-10' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-236'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-237-2025', 'virtual_office', '2024-12-01', '2025-12-01', 1690, 6000.0, 'held', 'semi_annual',
    CASE WHEN '2025-12-01' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-237'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-238-2025', 'virtual_office', '2025-01-13', '2027-01-13', 1800, 6000.0, 'held', 'semi_annual',
    CASE WHEN '2027-01-13' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-238'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-239-2025', 'virtual_office', '2025-01-20', '2026-01-20', 2000, 6000.0, 'held', 'annual',
    CASE WHEN '2026-01-20' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-239'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-240-2025', 'virtual_office', '2025-02-03', '2027-02-03', 1800, 6000.0, 'held', 'semi_annual',
    CASE WHEN '2027-02-03' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-240'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-241-2025', 'virtual_office', '2025-01-06', '2027-01-06', 1800, 6000.0, 'held', 'semi_annual',
    CASE WHEN '2027-01-06' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-241'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-242-2025', 'virtual_office', '2025-02-15', '2027-02-15', 1800, 6000.0, 'held', 'semi_annual',
    CASE WHEN '2027-02-15' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-242'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-243-2025', 'virtual_office', '2025-02-20', '2026-02-19', 2000, 6000.0, 'held', 'annual',
    CASE WHEN '2026-02-19' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-243'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-244-2025', 'virtual_office', '2025-03-24', '2027-03-24', 1800, 6000.0, 'held', 'semi_annual',
    CASE WHEN '2027-03-24' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-244'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-245-2025', 'virtual_office', '2025-03-24', '2026-03-24', 2000, 6000.0, 'held', 'annual',
    CASE WHEN '2026-03-24' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-245'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-247-2025', 'virtual_office', '2025-04-29', '2027-04-29', 1800, 6000.0, 'held', 'semi_annual',
    CASE WHEN '2027-04-29' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-247'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-248-2025', 'virtual_office', '2025-05-23', '2026-05-23', 2000, 6000.0, 'held', 'annual',
    CASE WHEN '2026-05-23' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-248'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-249-2025', 'virtual_office', '2025-06-01', '2027-06-11', 1800, 6000.0, 'held', 'semi_annual',
    CASE WHEN '2027-06-11' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-249'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-250-2025', 'virtual_office', '2025-06-19', '2027-06-19', 1000, 6000.0, 'held', 'biennial',
    CASE WHEN '2027-06-19' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-250'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-251-2025', 'virtual_office', '2025-07-01', '2027-07-01', 1200, 6000.0, 'held', 'biennial',
    CASE WHEN '2027-07-01' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-251'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-252-2025', 'virtual_office', '2025-06-27', '2027-06-27', 1200, 6000.0, 'held', 'biennial',
    CASE WHEN '2027-06-27' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-252'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-253-2025', 'virtual_office', '2025-07-10', '2027-07-10', 1200, 6000.0, 'held', 'biennial',
    CASE WHEN '2027-07-10' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-253'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-254-2025', 'virtual_office', '2025-07-10', '2027-07-10', 1690, 6000.0, 'held', 'semi_annual',
    CASE WHEN '2027-07-10' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-254'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-255-2025', 'virtual_office', '2025-07-24', '2026-07-24', 1590, 6000.0, 'held', 'annual',
    CASE WHEN '2026-07-24' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-255'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-256-2025', 'virtual_office', '2025-08-11', '2026-08-11', 2000, 6000.0, 'held', 'annual',
    CASE WHEN '2026-08-11' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-256'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-257-2025', 'virtual_office', '2025-09-08', '2026-09-08', 1800, 6000.0, 'held', 'annual',
    CASE WHEN '2026-09-08' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-257'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-258-2025', 'virtual_office', '2025-09-10', '2026-09-10', 3000, 0, 'held', 'monthly',
    CASE WHEN '2026-09-10' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-258'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-259-2025', 'virtual_office', '2025-09-16', '2026-09-16', 1800, 6000.0, 'held', 'annual',
    CASE WHEN '2026-09-16' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-259'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-260-2025', 'virtual_office', '2025-10-13', '2026-10-13', 2000, 6000.0, 'held', 'annual',
    CASE WHEN '2026-10-13' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-260'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-261-2025', 'virtual_office', '2025-11-07', '2026-11-07', 1800, 6000.0, 'held', 'annual',
    CASE WHEN '2026-11-07' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-261'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-262-2025', 'virtual_office', '2025-12-01', '2026-11-30', 12350, 13000.0, 'held', 'annual',
    CASE WHEN '2026-11-30' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-262'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, 'DZ-263-2025', 'virtual_office', '2025-11-11', '2026-11-11', 3000, 0, 'held', 'monthly',
    CASE WHEN '2026-11-11' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'DZ-263'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 2, 'HR-001-2025', 'virtual_office', '2025-04-08', '2026-04-08', 2000, 6000.0, 'held', 'annual',
    CASE WHEN '2026-04-08' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'HR-001'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 2, 'HR-002-2025', 'virtual_office', '2025-04-08', '2026-04-08', 2000, 6000.0, 'held', 'annual',
    CASE WHEN '2026-04-08' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'HR-002'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 2, 'HR-004-2025', 'virtual_office', '2025-04-11', '2027-04-11', 1800, 6000.0, 'held', 'semi_annual',
    CASE WHEN '2027-04-11' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'HR-004'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 2, 'HR-005-2025', 'virtual_office', '2025-06-10', '2027-06-10', 1800, 6000.0, 'held', 'semi_annual',
    CASE WHEN '2027-06-10' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'HR-005'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 2, 'HR-006-2025', 'virtual_office', '2025-06-18', '2027-06-18', 1200, 6000.0, 'held', 'biennial',
    CASE WHEN '2027-06-18' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'HR-006'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 2, 'HR-007-2025', 'virtual_office', '2025-06-19', '2027-06-19', 1000, 6000.0, 'held', 'biennial',
    CASE WHEN '2027-06-19' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'HR-007'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 2, 'HR-008-2025', 'virtual_office', '2025-06-19', '2027-06-19', 1000, 6000.0, 'held', 'monthly',
    CASE WHEN '2027-06-19' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'HR-008'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 2, 'HR-009-2025', 'virtual_office', '2025-06-19', '2027-06-19', 1000, 6000.0, 'held', 'monthly',
    CASE WHEN '2027-06-19' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'HR-009'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 2, 'HR-010-2025', 'virtual_office', '2025-06-19', '2027-06-19', 1000, 6000.0, 'held', 'monthly',
    CASE WHEN '2027-06-19' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'HR-010'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 2, 'HR-011-2025', 'virtual_office', '2025-06-20', '2026-06-20', 2000, 6000.0, 'held', 'annual',
    CASE WHEN '2026-06-20' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'HR-011'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 2, 'HR-012-2025', 'virtual_office', '2025-06-30', '2027-06-30', 1200, 6000.0, 'held', 'biennial',
    CASE WHEN '2027-06-30' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'HR-012'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 2, 'HR-013-2025', 'virtual_office', '2025-07-09', '2026-07-09', 1590, 6000.0, 'held', 'annual',
    CASE WHEN '2026-07-09' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'HR-013'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 2, 'HR-014-2025', 'virtual_office', '2025-07-14', '2027-07-14', 1690, 6000.0, 'held', 'semi_annual',
    CASE WHEN '2027-07-14' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'HR-014'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 2, 'HR-015-2025', 'virtual_office', '2025-07-16', '2027-07-16', 1200, 6000.0, 'held', 'biennial',
    CASE WHEN '2027-07-16' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'HR-015'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 2, 'HR-016-2025', 'virtual_office', '2025-07-22', '2026-07-22', 1590, 6000.0, 'held', 'annual',
    CASE WHEN '2026-07-22' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'HR-016'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 2, 'HR-017-2025', 'virtual_office', '2025-08-01', '2026-08-01', 1590, 6000.0, 'held', 'annual',
    CASE WHEN '2026-08-01' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'HR-017'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 2, 'HR-018-2025', 'virtual_office', '2025-08-21', '2027-08-21', 1690, 6000.0, 'held', 'biennial',
    CASE WHEN '2027-08-21' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'HR-018'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 2, 'HR-019-2025', 'virtual_office', '2025-08-01', '2026-08-01', 1690, 6000.0, 'held', 'annual',
    CASE WHEN '2026-08-01' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'HR-019'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 2, 'HR-020-2025', 'virtual_office', '2025-09-01', '2026-09-01', 1590, 6000.0, 'held', 'annual',
    CASE WHEN '2026-09-01' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'HR-020'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 2, 'HR-021-2025', 'virtual_office', '2025-09-01', '2026-09-01', 1800, 6000.0, 'held', 'annual',
    CASE WHEN '2026-09-01' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'HR-021'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 2, 'HR-022-2025', 'virtual_office', '2025-09-15', '2026-09-15', 1800, 6000.0, 'held', 'annual',
    CASE WHEN '2026-09-15' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'HR-022'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 2, 'HR-023-2025', 'virtual_office', '2025-09-18', '2026-09-18', 1800, 6000.0, 'held', 'annual',
    CASE WHEN '2026-09-18' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'HR-023'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 2, 'HR-024-2025', 'virtual_office', '2025-10-04', '2026-10-04', 1800, 6000.0, 'held', 'annual',
    CASE WHEN '2026-10-04' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'HR-024'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 2, 'HR-025-2025', 'virtual_office', '2025-10-29', '2026-10-29', 1800, 6000.0, 'held', 'annual',
    CASE WHEN '2026-10-29' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'HR-025'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();

INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 2, 'HR-026-2025', 'virtual_office', '2025-12-02', '2026-12-02', 1800, 6000.0, 'held', 'annual',
    CASE WHEN '2026-12-02' >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = 'HR-026'
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();


-- 匯入完成

-- 驗證匯入結果
SELECT 'customers' as table_name, COUNT(*) as count FROM customers
UNION ALL
SELECT 'contracts', COUNT(*) FROM contracts;