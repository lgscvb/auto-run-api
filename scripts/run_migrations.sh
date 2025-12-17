#!/bin/bash
# 執行 CRM 資料庫 Migration
# 使用方式: ./run_migrations.sh

set -e

DB_CONTAINER="hourjungle-crm-postgres-1"
DB_USER="postgres"
DB_NAME="crm"

echo "=========================================="
echo "Hour Jungle CRM - Database Migrations"
echo "=========================================="

# 1. 執行 015_floor_plan.sql（新增 position_number 欄位）
echo ""
echo "[1/3] 執行 015_floor_plan.sql..."
docker exec -i $DB_CONTAINER psql -U $DB_USER -d $DB_NAME < /root/hourjungle-crm/sql/migrations/015_floor_plan.sql
echo "✅ floor_plan 表結構建立完成"

# 2. 執行 016_update_position_numbers.sql（自動匹配）
echo ""
echo "[2/3] 執行 016_update_position_numbers.sql..."
docker exec -i $DB_CONTAINER psql -U $DB_USER -d $DB_NAME < /root/hourjungle-crm/sql/migrations/016_update_position_numbers.sql
echo "✅ 位置自動匹配完成"

# 3. 執行手動修正
echo ""
echo "[3/3] 執行手動修正..."
docker exec -i $DB_CONTAINER psql -U $DB_USER -d $DB_NAME << 'EOF'
-- 名稱差異手動修正
UPDATE contracts SET position_number = 1 WHERE id = 548;   -- 廖氏商行
UPDATE contracts SET position_number = 31 WHERE id = 616;  -- 泉佳鑫
UPDATE contracts SET position_number = 46 WHERE id = 563;  -- 新遞國際開發
UPDATE contracts SET position_number = 62 WHERE id = 575;  -- 短腿基商鋪
UPDATE contracts SET position_number = 70 WHERE id = 583;  -- 步臻低碳策略

-- 續約客戶位置
UPDATE contracts SET position_number = 5 WHERE id = 553;   -- 一貝兒美容工作室
UPDATE contracts SET position_number = 6 WHERE id = 547;   -- 吉爾哈登工作室
UPDATE contracts SET position_number = 69 WHERE id = 608;  -- 七分之二的探索
UPDATE contracts SET position_number = 93 WHERE id = 573;  -- 小倩媽咪
EOF
echo "✅ 手動修正完成"

# 4. 重新載入 PostgREST schema cache
echo ""
echo "[4/4] 重新載入 PostgREST schema..."
docker exec $DB_CONTAINER psql -U $DB_USER -d $DB_NAME -c "NOTIFY pgrst, 'reload schema';"
echo "✅ Schema 已重新載入"

# 5. 驗證結果
echo ""
echo "=========================================="
echo "驗證結果"
echo "=========================================="
docker exec -i $DB_CONTAINER psql -U $DB_USER -d $DB_NAME << 'EOF'
SELECT
    '已設定位置的合約數' as info,
    COUNT(*) as count
FROM contracts
WHERE position_number IS NOT NULL;

SELECT
    position_number,
    id as contract_id,
    status
FROM contracts
WHERE position_number IS NOT NULL
ORDER BY position_number
LIMIT 10;
EOF

echo ""
echo "=========================================="
echo "Migration 執行完成！"
echo "=========================================="
