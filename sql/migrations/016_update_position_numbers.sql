-- ============================================================================
-- Hour Jungle CRM - 更新合約位置編號
-- Migration: 016_update_position_numbers.sql
-- 根據 PPT 提取的公司名稱，匹配現有合約並設定 position_number
-- ============================================================================

-- 建立臨時表存放 PPT 數據
CREATE TEMP TABLE temp_ppt_positions (
    position_number INTEGER PRIMARY KEY,
    company_name VARCHAR(200)
);

-- 插入 PPT 提取的 107 個位置數據
INSERT INTO temp_ppt_positions (position_number, company_name) VALUES
(1, '廖氏商行'),
(2, '江小咪商行'),
(3, '洛酷科技有限公司'),
(4, '鑫秝喨國際有限公司'),
(5, '一貝兒美容工作室'),
(6, '吉爾哈登工作室'),
(7, '緁作工作室'),
(8, '流星有限公司'),
(9, '晨甯水產行'),
(10, '子昇有限公司'),
(11, '優翼科技工程有限公司'),
(12, '季節東京媄睫專業坊'),
(13, '楊董企業社'),
(14, '程晧事業有限公司'),
(15, '昇瑪商行'),
(16, '機車俠機車行'),
(17, '兩兩空間製作所有限公司'),
(18, '辰緻國際股份有限公司'),
(19, '頌芝承工作室'),
(20, '立湟有限公司'),
(21, '旭營興業有限公司'),
(22, '台灣心零售股份有限公司'),
(23, '超省購生活用品企業社'),
(24, '明偉水產行'),
(25, '隱士播放清單商店'),
(26, '起床打單有限公司'),
(27, '恩梯科技股份有限公司'),
(28, '獨自紅有限公司'),
(29, '益群團購顧問有限公司'),
(30, '景泰批發實業社'),
(31, '泉家鑫企業社'),
(32, '利奇商行'),
(33, '至溢營造有限公司'),
(34, '萊益國際股份有限公司台中分公司'),
(35, '花芙辰寶國際行銷管理顧問有限公司'),
(36, '明采文創工作室'),
(37, '貽順有限公司'),
(38, '知寬植行'),
(39, '小熊零件行'),
(40, '商贏企業'),
(41, '中盛建維有限公司'),
(42, '朱芸工作室'),
(43, '竺墨文創企業社'),
(44, '究鮮商行'),
(45, '新大科技有限公司'),
(46, '新遞國際物流有限公司'),
(47, '福樂寵工作室'),
(48, '由非室內裝修設計有限公司'),
(49, '農益富股份有限公司'),
(50, '原食工坊'),
(51, '帛珅有限公司'),
(52, '搖滾山姆有限公司'),
(53, '樂沐金商行'),
(54, '鼎盛行銷'),
(55, '微笑玩家國際貿易有限公司'),
(56, '仁徠貿易股份有限公司'),
(57, '照鴻貿易股份有限公司'),
(58, '日安家商行'),
(59, '上永富科技股份有限公司'),
(60, '光緯企業社'),
(61, '華為秝喨國際有限公司'),
(62, '短腿基商舖'),
(63, '金海小舖'),
(64, '順映影像有限公司'),
(65, '植光圈友善坊'),
(66, '旺玖企業社'),
(67, '鼠適圈工作室'),
(68, '滿單有限公司'),
(69, '七分之二的探索有限公司'),
(70, '步臻有限公司'),
(71, '范特希雅時光旅行小舖'),
(72, '大心沉香'),
(73, '鎧將金屬開發有限公司'),
(74, '文瀛營造有限公司'),
(75, '協通實業有限公司'),
(76, '天原興業有限公司'),
(77, '金如泰股份有限公司'),
(78, '好日來商行'),
(79, '伯樂商行'),
(80, '宏川貿易有限公司'),
(81, '興盛行銷管理顧問有限公司'),
(82, '富丞裕國際商行'),
(83, '盛豐新流量商業社'),
(84, '喂喂四聲喂工作室'),
(85, '磐星能源科技有限公司'),
(86, '承新文創有限公司'),
(87, '捌伍設計'),
(88, '溪流雲創意整合有限公司'),
(89, '智谷系統有限公司'),
(90, '顧寶藝工作室'),
(91, '仁琦科技有限公司'),
(92, '浩萊國際企業社'),
(93, '小倩媽咪行銷工作室'),
(94, '四春企業社'),
(95, '樸裕國際顧問有限公司'),
(96, '御林軍御藝美妝'),
(97, '馥諦健康事業有限公司'),
(98, '世燁環境清潔企業社'),
(99, '和和國際有限公司'),
(100, '曜森生活工作室'),
(101, '川榆室所有限公司'),
(102, '小胖芭樂水果行'),
(103, '淬矩闢梯有限公司'),
(104, '沃土謙植有限公司'),
(105, '艾瑟烘焙坊'),
(106, '球球歐瑞歐工作室'),
(107, '弎弎審美在線工作室');

-- 先檢查匹配情況（預覽）
SELECT
    ppt.position_number,
    ppt.company_name AS ppt_company,
    c.id AS customer_id,
    c.company_name AS db_company,
    c.name AS customer_name,
    ct.id AS contract_id,
    ct.status AS contract_status
FROM temp_ppt_positions ppt
LEFT JOIN customers c ON (
    -- 完全匹配
    c.company_name = ppt.company_name
    OR
    -- 模糊匹配（去除空格和特殊字符）
    REPLACE(REPLACE(c.company_name, ' ', ''), '　', '') = REPLACE(REPLACE(ppt.company_name, ' ', ''), '　', '')
    OR
    -- 包含匹配
    c.company_name LIKE '%' || ppt.company_name || '%'
    OR
    ppt.company_name LIKE '%' || c.company_name || '%'
)
LEFT JOIN contracts ct ON (
    ct.customer_id = c.id
    AND ct.status = 'active'
    AND ct.branch_id = 1  -- 大忠本館
)
WHERE c.id IS NOT NULL
ORDER BY ppt.position_number;

-- 執行更新：設定合約的 position_number
-- 只更新 branch_id=1（大忠本館）且 status='active' 的合約
UPDATE contracts ct
SET position_number = matched.position_number
FROM (
    SELECT DISTINCT ON (ppt.position_number)
        ppt.position_number,
        ct.id AS contract_id
    FROM temp_ppt_positions ppt
    JOIN customers c ON (
        c.company_name = ppt.company_name
        OR REPLACE(REPLACE(c.company_name, ' ', ''), '　', '') = REPLACE(REPLACE(ppt.company_name, ' ', ''), '　', '')
        OR c.company_name LIKE '%' || ppt.company_name || '%'
        OR ppt.company_name LIKE '%' || c.company_name || '%'
    )
    JOIN contracts ct ON (
        ct.customer_id = c.id
        AND ct.status = 'active'
        AND ct.branch_id = 1
    )
    ORDER BY ppt.position_number, ct.id
) matched
WHERE ct.id = matched.contract_id;

-- 顯示更新結果
SELECT
    ct.id AS contract_id,
    ct.contract_number,
    ct.position_number,
    c.company_name,
    ct.status
FROM contracts ct
JOIN customers c ON ct.customer_id = c.id
WHERE ct.position_number IS NOT NULL
  AND ct.branch_id = 1
ORDER BY ct.position_number;

-- 顯示未匹配的位置（PPT 有但資料庫沒有的公司）
SELECT
    ppt.position_number,
    ppt.company_name AS '未匹配公司'
FROM temp_ppt_positions ppt
LEFT JOIN customers c ON (
    c.company_name = ppt.company_name
    OR REPLACE(REPLACE(c.company_name, ' ', ''), '　', '') = REPLACE(REPLACE(ppt.company_name, ' ', ''), '　', '')
)
WHERE c.id IS NULL
ORDER BY ppt.position_number;

-- 清理臨時表
DROP TABLE temp_ppt_positions;
