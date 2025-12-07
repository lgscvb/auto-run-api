#!/usr/bin/env python3
"""
生成 SQL 匯入腳本
直接在 GCP PostgreSQL 執行
"""

import pandas as pd
from pathlib import Path
from datetime import datetime

# 路徑設定
SCRIPT_DIR = Path(__file__).parent
OUTPUT_DIR = SCRIPT_DIR / "output"

# 讀取清洗後的資料
print("=== 讀取資料 ===")
customers_df = pd.read_csv(OUTPUT_DIR / "customers_final.csv")
contracts_df = pd.read_csv(OUTPUT_DIR / "contracts_cleaned.csv")

print(f"客戶: {len(customers_df)} 筆")
print(f"合約: {len(contracts_df)} 筆")

# SQL 輸出
sql_lines = []

# Header
sql_lines.append("-- ============================================================================")
sql_lines.append("-- Hour Jungle CRM - 資料匯入腳本")
sql_lines.append(f"-- 生成時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
sql_lines.append("-- ============================================================================")
sql_lines.append("")
sql_lines.append("-- 逐筆執行（不使用交易）")
sql_lines.append("")

# 清空現有資料（可選）
sql_lines.append("-- 清空現有資料（如需保留請註解掉）")
sql_lines.append("-- TRUNCATE customers CASCADE;")
sql_lines.append("-- TRUNCATE contracts CASCADE;")
sql_lines.append("")

# 匯入客戶
sql_lines.append("-- ============================================================================")
sql_lines.append("-- 客戶資料")
sql_lines.append("-- ============================================================================")

def escape_sql(val):
    """轉義 SQL 字串"""
    if pd.isna(val) or val == '':
        return "NULL"
    val = str(val).replace("'", "''")
    return f"'{val}'"

def escape_number(val):
    """處理數字"""
    if pd.isna(val) or val == '':
        return "NULL"
    # 處理浮點數格式的電話號碼
    val_str = str(val)
    if val_str.endswith('.0'):
        val_str = val_str[:-2]
    return escape_sql(val_str)

for idx, row in customers_df.iterrows():
    # 取得欄位值
    legacy_id = escape_sql(row.get('legacy_id'))
    branch_id = 1 if str(row.get('branch_code', '')).startswith('DZ') else 2
    customer_type = escape_sql(row.get('customer_type', 'individual'))
    name = escape_sql(row.get('name'))
    company_name = escape_sql(row.get('company_name'))
    company_tax_id = escape_number(row.get('company_tax_id'))
    id_number = escape_sql(row.get('id_number'))
    birthday = escape_sql(row.get('birthday')) if pd.notna(row.get('birthday')) else "NULL"
    phone = escape_number(row.get('phone'))
    email = escape_sql(row.get('email'))
    address = escape_sql(row.get('address'))
    line_user_id = escape_sql(row.get('line_user_id'))
    source_channel = escape_sql(row.get('source_channel', 'migration'))
    status = escape_sql(row.get('status', 'active'))
    is_foreigner = 'TRUE' if row.get('is_foreigner') == True else 'FALSE'
    notes = escape_sql(row.get('notes'))
    industry_notes = escape_sql(row.get('industry_notes'))

    sql = f"""INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, birthday, phone, email, address, line_user_id, source_channel, status, metadata)
VALUES ({legacy_id}, {branch_id}, {customer_type}, {name}, {company_name}, {company_tax_id}, {id_number}, {birthday}, {phone}, {email}, {address}, {line_user_id}, {source_channel}, {status}, '{{"is_foreigner": {is_foreigner.lower()}}}'::jsonb)
ON CONFLICT (legacy_id) DO UPDATE SET
    name = EXCLUDED.name,
    company_name = EXCLUDED.company_name,
    phone = EXCLUDED.phone,
    line_user_id = COALESCE(EXCLUDED.line_user_id, customers.line_user_id),
    status = EXCLUDED.status,
    updated_at = NOW();"""

    sql_lines.append(sql)
    sql_lines.append("")

# 匯入合約
sql_lines.append("")
sql_lines.append("-- ============================================================================")
sql_lines.append("-- 合約資料")
sql_lines.append("-- ============================================================================")

from dateutil.relativedelta import relativedelta

for idx, row in contracts_df.iterrows():
    customer_legacy_id = row.get('customer_legacy_id')
    branch_id = 1 if str(customer_legacy_id).startswith('DZ') else 2
    contract_type = escape_sql(row.get('contract_type', 'virtual_office'))

    # 處理缺失或錯誤的日期
    raw_start = row.get('start_date')
    raw_end = row.get('end_date')
    payment_method = str(row.get('payment_method', 'M'))

    # 根據付款週期決定合約長度
    contract_duration = {
        'M': relativedelta(years=1),      # 月付 = 1年
        'Y': relativedelta(years=1),      # 年付 = 1年
        '6M': relativedelta(months=6),    # 半年付 = 6個月
        '2Y': relativedelta(years=2)      # 兩年付 = 2年
    }.get(payment_method, relativedelta(years=1))

    # 缺 start_date → 從 end_date 往前推
    if pd.isna(raw_start) or raw_start == '':
        if pd.notna(raw_end) and raw_end != '':
            end_dt = pd.to_datetime(raw_end)
            start_dt = end_dt - contract_duration
            raw_start = start_dt.strftime('%Y-%m-%d')
        else:
            raw_start = datetime.now().strftime('%Y-%m-%d')

    # 缺 end_date 或 end_date <= start_date → 重新計算
    if pd.isna(raw_end) or raw_end == '' or raw_end <= raw_start:
        start_dt = pd.to_datetime(raw_start)
        end_dt = start_dt + contract_duration
        raw_end = end_dt.strftime('%Y-%m-%d')

    start_date = escape_sql(raw_start)
    end_date = escape_sql(raw_end)
    monthly_rent = row.get('monthly_rent', 0)
    if pd.isna(monthly_rent):
        monthly_rent = 0
    deposit = row.get('deposit', 0)
    if pd.isna(deposit):
        deposit = 0
    # 轉換押金狀態
    deposit_status_map = {
        'not_returned': 'held',
        'returned': 'refunded',
        'forfeited': 'forfeited'
    }
    raw_deposit_status = row.get('deposit_status', 'held')
    if pd.isna(raw_deposit_status):
        raw_deposit_status = 'held'
    deposit_status = escape_sql(deposit_status_map.get(str(raw_deposit_status), 'held'))
    payment_method = row.get('payment_method', 'M')

    # 轉換付款週期
    payment_cycle_map = {
        'M': 'monthly',
        'Y': 'annual',
        '6M': 'semi_annual',
        '2Y': 'biennial'
    }
    payment_cycle = escape_sql(payment_cycle_map.get(str(payment_method), 'monthly'))

    # 生成合約編號
    contract_number = f"{customer_legacy_id}-{datetime.now().year}"

    sql = f"""INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, {branch_id}, {escape_sql(contract_number)}, {contract_type}, {start_date}, {end_date}, {monthly_rent}, {deposit}, {deposit_status}, {payment_cycle},
    CASE WHEN {end_date} >= CURRENT_DATE THEN 'active' ELSE 'expired' END
FROM customers c WHERE c.legacy_id = {escape_sql(customer_legacy_id)}
ON CONFLICT (contract_number) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    monthly_rent = EXCLUDED.monthly_rent,
    status = EXCLUDED.status,
    updated_at = NOW();"""

    sql_lines.append(sql)
    sql_lines.append("")

# 完成
sql_lines.append("")
sql_lines.append("-- 匯入完成")
sql_lines.append("")
sql_lines.append("-- 驗證匯入結果")
sql_lines.append("SELECT 'customers' as table_name, COUNT(*) as count FROM customers")
sql_lines.append("UNION ALL")
sql_lines.append("SELECT 'contracts', COUNT(*) FROM contracts;")

# 寫入檔案
output_file = OUTPUT_DIR / "import_data.sql"
with open(output_file, 'w', encoding='utf-8') as f:
    f.write('\n'.join(sql_lines))

print(f"\n=== SQL 腳本已生成 ===")
print(f"檔案: {output_file}")
print(f"客戶 INSERT: {len(customers_df)} 筆")
print(f"合約 INSERT: {len(contracts_df)} 筆")
print("\n執行方式:")
print("  psql -h <GCP_HOST> -U hjadmin -d hourjungle -f import_data.sql")
