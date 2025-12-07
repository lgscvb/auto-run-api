#!/usr/bin/env python3
"""
匯入「已結束」客戶資料
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
import re

# 路徑設定
SCRIPT_DIR = Path(__file__).parent
OUTPUT_DIR = SCRIPT_DIR / "output"
RAW_DIR = Path("/Users/daihaoting_1/Downloads/原始資料")

def parse_date(val):
    """解析各種日期格式"""
    if pd.isna(val) or val == '' or val == 'N' or val == 'n':
        return None

    val = str(val).strip()

    # 民國年格式: 110/6/8
    match = re.match(r'(\d{2,3})/(\d{1,2})/(\d{1,2})', val)
    if match:
        year = int(match.group(1)) + 1911
        month = int(match.group(2))
        day = int(match.group(3))
        try:
            return f"{year}-{month:02d}-{day:02d}"
        except:
            return None

    # ISO 格式
    try:
        dt = pd.to_datetime(val)
        return dt.strftime('%Y-%m-%d')
    except:
        return None

def clean_phone(val):
    """清理電話號碼"""
    if pd.isna(val) or val == '' or val == 'N' or val == 'n':
        return None
    val = str(val).replace('-', '').replace(' ', '').strip()
    if val.startswith('09') and len(val) == 10:
        return val
    return val if val else None

def escape_sql(val):
    """轉義 SQL 字串"""
    if pd.isna(val) or val == '' or val == 'N' or val == 'n':
        return "NULL"
    val = str(val).replace("'", "''")
    return f"'{val}'"

# 讀取已結束資料
print("=== 讀取已結束資料 ===")
dz_ended = pd.read_excel(RAW_DIR / "客戶資料表crm.xlsx", sheet_name='已結束')
hr_ended = pd.read_excel(RAW_DIR / "環瑞客戶資料表crm.xlsx", sheet_name='已結束')

print(f"大忠館已結束: {len(dz_ended)} 筆")
print(f"環瑞館已結束: {len(hr_ended)} 筆")

# 處理大忠館已結束
sql_lines = []
sql_lines.append("-- 匯入已結束客戶")
sql_lines.append(f"-- 生成時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
sql_lines.append("")

customer_count = 0
contract_count = 0

for idx, row in dz_ended.iterrows():
    # 客戶編號
    raw_id = row.get('Unnamed: 0')
    if pd.isna(raw_id):
        continue
    legacy_id = f"DZ-E{int(raw_id):03d}"  # DZ-E001 格式（E = Ended）

    # 基本資料
    name = escape_sql(row.get('姓名'))
    company_name = row.get('公司')
    if pd.isna(company_name) or company_name == 'N':
        company_name = None
    company_name = escape_sql(company_name)

    phone = clean_phone(row.get('聯絡電話'))
    phone = escape_sql(phone)

    id_number = row.get('Id number')
    id_number = escape_sql(id_number) if pd.notna(id_number) and id_number != 'N' else "NULL"

    address = row.get('Add')
    address = escape_sql(address) if pd.notna(address) and address != 'N' else "NULL"

    birthday = parse_date(row.get('生日'))
    birthday = escape_sql(birthday) if birthday else "NULL"

    # 合約資料
    contract_date = row.get('合約起迄日期')
    start_date = parse_date(row.get('簽約日期'))
    end_date = parse_date(contract_date) if contract_date else None

    # 費用
    fee = row.get('費用')
    monthly_rent = 2500  # 預設
    if pd.notna(fee):
        fee_str = str(fee)
        match = re.search(r'(\d+)', fee_str)
        if match:
            monthly_rent = int(match.group(1))

    # 押金
    deposit = row.get('押金')
    deposit_val = 0
    if pd.notna(deposit) and deposit != 'N' and deposit != 'n':
        try:
            deposit_val = int(float(str(deposit).replace('Y', '').strip() or 0))
        except:
            deposit_val = 3000  # 預設

    # 付款方式
    payment = row.get('繳費方式')
    payment_cycle = 'monthly'
    if pd.notna(payment):
        payment_str = str(payment)
        if '月' in payment_str or 'M' in payment_str.upper():
            payment_cycle = 'monthly'
        elif '年' in payment_str or 'Y' in payment_str.upper():
            payment_cycle = 'annual'

    # 客戶 INSERT
    sql = f"""INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, id_number, birthday, phone, address, source_channel, status, metadata)
VALUES ({escape_sql(legacy_id)}, 1, 'individual', {name}, {company_name}, {id_number}, {birthday}, {phone}, {address}, 'migration', 'churned', '{{"is_foreigner": false}}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;"""
    sql_lines.append(sql)
    customer_count += 1

    # 合約 INSERT（如果有日期）
    if start_date or end_date:
        if not start_date:
            start_date = '2020-01-01'  # 預設
        if not end_date:
            end_date = '2024-01-01'  # 預設過期日

        contract_number = f"{legacy_id}-END"
        sql = f"""INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 1, {escape_sql(contract_number)}, 'virtual_office', {escape_sql(start_date)}, {escape_sql(end_date)}, {monthly_rent}, {deposit_val}, 'refunded', {escape_sql(payment_cycle)}, 'expired'
FROM customers c WHERE c.legacy_id = {escape_sql(legacy_id)}
ON CONFLICT (contract_number) DO NOTHING;"""
        sql_lines.append(sql)
        contract_count += 1

    sql_lines.append("")

# 處理環瑞館已結束
for idx, row in hr_ended.iterrows():
    raw_id = row.get('編號')
    if pd.isna(raw_id):
        continue
    legacy_id = f"HR-E{raw_id}"

    name = escape_sql(row.get('姓名'))
    company_name = escape_sql(row.get('公司名稱'))
    phone = clean_phone(row.get('聯絡電話'))
    phone = escape_sql(phone)
    id_number = escape_sql(row.get('Id number')) if pd.notna(row.get('Id number')) else "NULL"
    address = escape_sql(row.get('Add')) if pd.notna(row.get('Add')) else "NULL"
    tax_id = escape_sql(row.get('統編')) if pd.notna(row.get('統編')) else "NULL"

    start_date = parse_date(row.get('起始日期'))
    end_date = parse_date(row.get('合約到期日'))

    fee = row.get('金額')
    monthly_rent = 1800
    if pd.notna(fee):
        match = re.search(r'(\d+)', str(fee))
        if match:
            monthly_rent = int(match.group(1))

    deposit_str = str(row.get('押金', '0'))
    deposit_val = 0
    match = re.search(r'(\d+)', deposit_str)
    if match:
        deposit_val = int(match.group(1))

    deposit_status = 'held'
    if '未退' in deposit_str:
        deposit_status = 'held'
    elif '退' in deposit_str:
        deposit_status = 'refunded'

    # 客戶
    sql = f"""INSERT INTO customers (legacy_id, branch_id, customer_type, name, company_name, company_tax_id, id_number, phone, address, source_channel, status, metadata)
VALUES ({escape_sql(legacy_id)}, 2, 'sole_proprietorship', {name}, {company_name}, {tax_id}, {id_number}, {phone}, {address}, 'migration', 'churned', '{{"is_foreigner": false}}'::jsonb)
ON CONFLICT (legacy_id) DO NOTHING;"""
    sql_lines.append(sql)
    customer_count += 1

    # 合約
    if start_date or end_date:
        if not start_date:
            start_date = '2024-01-01'
        if not end_date:
            end_date = '2025-01-01'

        contract_number = f"{legacy_id}-END"
        sql = f"""INSERT INTO contracts (customer_id, branch_id, contract_number, contract_type, start_date, end_date, monthly_rent, deposit, deposit_status, payment_cycle, status)
SELECT c.id, 2, {escape_sql(contract_number)}, 'virtual_office', {escape_sql(start_date)}, {escape_sql(end_date)}, {monthly_rent}, {deposit_val}, {escape_sql(deposit_status)}, 'semi_annual', 'expired'
FROM customers c WHERE c.legacy_id = {escape_sql(legacy_id)}
ON CONFLICT (contract_number) DO NOTHING;"""
        sql_lines.append(sql)
        contract_count += 1

    sql_lines.append("")

# 驗證
sql_lines.append("")
sql_lines.append("-- 驗證")
sql_lines.append("SELECT 'churned_customers' as item, COUNT(*) FROM customers WHERE status = 'churned';")
sql_lines.append("SELECT 'expired_contracts' as item, COUNT(*) FROM contracts WHERE status = 'expired';")

# 輸出
output_file = OUTPUT_DIR / "import_ended.sql"
with open(output_file, 'w', encoding='utf-8') as f:
    f.write('\n'.join(sql_lines))

print(f"\n=== 完成 ===")
print(f"客戶: {customer_count} 筆")
print(f"合約: {contract_count} 筆")
print(f"輸出: {output_file}")
