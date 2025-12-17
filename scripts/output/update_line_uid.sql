-- ============================================================================
-- Hour Jungle CRM - 更新 LINE User ID
-- 生成時間: 2025-12-07 13:58:07
-- 共 7 筆需要更新
-- ============================================================================

-- DZ-246 吳世多
UPDATE customers SET line_user_id = 'U75c10e78ed82d6d38444c909a72713e7' WHERE id = 2325;
-- DZ-E219 趙珮宇
UPDATE customers SET line_user_id = 'U18ca81636491b60b0e32787cf97046cf' WHERE id = 2277;
-- DZ-114 楊儒俊(楊滷蛋)
UPDATE customers SET line_user_id = 'U6480d5b910d8cdcf571b93fbc70992f6' WHERE id = 1926;
-- DZ-214 林碩彥
UPDATE customers SET line_user_id = 'Uf37084ea30da255395e8075b6e139361' WHERE id = 1976;
-- DZ-E189 徐辰侑
UPDATE customers SET line_user_id = 'U2b1d7c6b9526287459aa7ff6861b0d84' WHERE id = 2291;
-- DZ-159 劉真如
UPDATE customers SET line_user_id = 'U50ef0d966b44e60db7abeefe8b61e183' WHERE id = 2331;
-- DZ-161 顏珮羽
UPDATE customers SET line_user_id = 'Ub16b1e69828841fb8b48062decaf0777' WHERE id = 2352;

-- 驗證
SELECT COUNT(*) as total, COUNT(line_user_id) as has_line_uid FROM customers WHERE status = 'active';