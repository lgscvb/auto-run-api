#!/usr/bin/env python3
"""
匯入已結束客戶 (churned) 及其合約和繳費記錄
用於追蹤押金退款狀態
"""

import pandas as pd
import subprocess
import json
import re
from datetime import datetime, date
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
OUTPUT_DIR = SCRIPT_DIR / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

# API 端點
API_BASE = "https://auto.yourspce.org/api/db"

# Excel 檔案路徑
DZ_FILE = '/Users/daihaoting_1/Downloads/原始資料/客戶資料表crm.xlsx'
HR_FILE = '/Users/daihaoting_1/Downloads/原始資料/環瑞客戶資料表crm.xlsx'

# 需要匯入的已結束客戶編號
TERMINATED_DZ_IDS = [
    115, 218, 66, 132, 174, 207, 187, 186, 229, 246,
    210, 155, 134, 203, 157, 159, 161, 113, 223
]
TERMINATED_HR_IDS = ['V03']

# 大忠館欄位對應（欄位名稱有錯位，根據實際資料內容對應）
DZ_COLUMN_MAP = {
    'excel_id': 'Unnamed: 0',
    'name': '姓名',
    'company': '公司',
    'company_type': '公司型態',
    'seat_type': '種類',
    'payment_cycle': '繳費方式',
    'contract_start': '合約起日期',
    'monthly_rent': '簽約日期',      # 實際存放月租金
    'deposit_info': '費用',          # 實際存放押金資訊
    'tax_id': 'Add',                 # 實際存放統編
    'birthday': 'Co number',         # 實際存放生日
    'address': '生日',               # 實際存放地址
    'phone': 'Add.1',                # 實際存放電話
    'id_number': '聯絡電話',         # 實際存放身分證
}

# 環瑞館欄位對應（正常）
HR_COLUMN_MAP = {
    'excel_id': '編號',
    'name': '姓名',
    'company': '公司名稱',
    'company_type': '類別',
    'seat_type': '項目',
    'payment_cycle': '繳費方式',
    'contract_start': '起始日期',
    'contract_end': '合約到期日',
    'monthly_rent': '金額',
    'deposit_info': '押金',
    'tax_id': '統編',
    'address': 'Add',
    'phone': '聯絡電話',
    'id_number': 'Id number',
}


def parse_deposit_info(deposit_str):
    """解析押金資訊，如 '6000/未退' 或 '3000/已退'"""
    if pd.isna(deposit_str) or not deposit_str:
        return None, 'held'

    deposit_str = str(deposit_str).strip()

    # 解析 "6000/未退" 或 "3000/已退" 格式
    match = re.match(r'(\d+)/(未退|已退)', deposit_str)
    if match:
        amount = int(match.group(1))
        status = 'refunded' if '已退' in deposit_str else 'held'
        return amount, status

    # 純數字
    try:
        return int(float(deposit_str)), 'held'
    except:
        return None, 'held'


def parse_monthly_rent(rent_str):
    """解析月租金，如 '1490/m' 或 '1800'"""
    if pd.isna(rent_str) or not rent_str:
        return None

    rent_str = str(rent_str).strip()
    match = re.match(r'(\d+)/m', rent_str)
    if match:
        return int(match.group(1))

    try:
        return int(float(rent_str))
    except:
        return None


def parse_roc_date(date_str):
    """解析民國年日期，如 '112/01/06' 或 '0114/04/09'"""
    if pd.isna(date_str) or not date_str:
        return None

    date_str = str(date_str).strip().replace('0114', '114').replace('0113', '113').replace('0112', '112')

    if '/' in date_str:
        parts = date_str.split('/')
        if len(parts) == 3:
            try:
                year = int(parts[0])
                if year < 200:  # 民國年
                    year += 1911
                month = int(parts[1])
                day = int(parts[2])
                return date(year, month, day)
            except:
                pass
    return None


def clean_phone(phone):
    """清理電話號碼"""
    if pd.isna(phone) or not phone:
        return None

    phone = str(phone).strip()
    # 移除非數字字元（保留 - 和開頭的 0）
    if phone.startswith('0') or phone.startswith('04'):
        return phone
    return None


def clean_tax_id(tax_id):
    """清理統一編號"""
    if pd.isna(tax_id) or not tax_id:
        return None

    tax_id = str(tax_id).strip()
    if len(tax_id) == 8 and tax_id.isdigit():
        return tax_id
    return None


def get_customer_type(company_type, company):
    """判斷客戶類型"""
    if pd.isna(company_type) or not company_type:
        if company and pd.notna(company):
            return 'company'
        return 'individual'

    company_type = str(company_type).strip()
    if company_type in ['公司', '有限公司', '股份有限公司']:
        return 'company'
    elif company_type in ['行號', '商行', '工作室', '企業社']:
        return 'sole_proprietorship'  # 資料庫使用 sole_proprietorship
    return 'individual'


def parse_dz_customers(df):
    """解析大忠館已結束客戶"""
    customers = []

    for excel_id in TERMINATED_DZ_IDS:
        rows = df[df[DZ_COLUMN_MAP['excel_id']] == excel_id]
        if len(rows) == 0:
            print(f"  未找到: DZ-{excel_id:03d}")
            continue

        row = rows.iloc[0]
        legacy_id = f"DZ-{excel_id:03d}"

        deposit_amount, deposit_status = parse_deposit_info(row.get(DZ_COLUMN_MAP['deposit_info']))
        monthly_rent = parse_monthly_rent(row.get(DZ_COLUMN_MAP['monthly_rent']))
        contract_start = parse_roc_date(row.get(DZ_COLUMN_MAP['contract_start']))

        customer = {
            'legacy_id': legacy_id,
            'branch_id': 1,  # 大忠館
            'name': str(row[DZ_COLUMN_MAP['name']]).strip() if pd.notna(row[DZ_COLUMN_MAP['name']]) else None,
            'company_name': str(row[DZ_COLUMN_MAP['company']]).strip() if pd.notna(row[DZ_COLUMN_MAP['company']]) and row[DZ_COLUMN_MAP['company']] != 'N' else None,
            'company_type': get_customer_type(row.get(DZ_COLUMN_MAP['company_type']), row.get(DZ_COLUMN_MAP['company'])),
            'phone': clean_phone(row.get(DZ_COLUMN_MAP['phone'])),
            'company_tax_id': clean_tax_id(row.get(DZ_COLUMN_MAP['tax_id'])),
            'deposit_amount': deposit_amount,
            'deposit_status': deposit_status,
            'monthly_rent': monthly_rent,
            'contract_start': contract_start,
            'seat_type': str(row[DZ_COLUMN_MAP['seat_type']]).strip() if pd.notna(row.get(DZ_COLUMN_MAP['seat_type'])) else None,
        }
        customers.append(customer)

    return customers


def parse_hr_customers(df):
    """解析環瑞館已結束客戶"""
    customers = []

    for excel_id in TERMINATED_HR_IDS:
        rows = df[df[HR_COLUMN_MAP['excel_id']] == excel_id]
        if len(rows) == 0:
            print(f"  未找到: HR-{excel_id}")
            continue

        row = rows.iloc[0]
        # V03 → HR-003
        num = int(excel_id[1:]) if excel_id.startswith('V') else int(excel_id)
        legacy_id = f"HR-{num:03d}"

        deposit_amount, deposit_status = parse_deposit_info(row.get(HR_COLUMN_MAP['deposit_info']))
        monthly_rent = parse_monthly_rent(row.get(HR_COLUMN_MAP['monthly_rent']))
        contract_start = parse_roc_date(row.get(HR_COLUMN_MAP['contract_start']))
        contract_end = parse_roc_date(row.get(HR_COLUMN_MAP['contract_end']))

        customer = {
            'legacy_id': legacy_id,
            'branch_id': 2,  # 環瑞館
            'name': str(row[HR_COLUMN_MAP['name']]).strip() if pd.notna(row[HR_COLUMN_MAP['name']]) else None,
            'company_name': str(row[HR_COLUMN_MAP['company']]).strip() if pd.notna(row[HR_COLUMN_MAP['company']]) else None,
            'company_type': get_customer_type(row.get(HR_COLUMN_MAP['company_type']), row.get(HR_COLUMN_MAP['company'])),
            'phone': clean_phone(row.get(HR_COLUMN_MAP['phone'])),
            'company_tax_id': clean_tax_id(row.get(HR_COLUMN_MAP['tax_id'])),
            'deposit_amount': deposit_amount,
            'deposit_status': deposit_status,
            'monthly_rent': monthly_rent,
            'contract_start': contract_start,
            'contract_end': contract_end,
            'seat_type': str(row[HR_COLUMN_MAP['seat_type']]).strip() if pd.notna(row.get(HR_COLUMN_MAP['seat_type'])) else None,
        }
        customers.append(customer)

    return customers


def generate_sql(customers):
    """生成 SQL 匯入腳本"""
    sql_lines = []
    sql_lines.append("-- ============================================================================")
    sql_lines.append("-- Hour Jungle CRM - 已結束客戶匯入")
    sql_lines.append(f"-- 生成時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    sql_lines.append("-- ============================================================================")
    sql_lines.append("")

    # 客戶 INSERT
    sql_lines.append("-- === 客戶資料 (status = 'churned') ===")
    for c in customers:
        name_sql = f"'{c['name']}'" if c['name'] else "NULL"
        company_sql = f"'{c['company_name'].replace(chr(39), chr(39)+chr(39))}'" if c['company_name'] else "NULL"
        phone_sql = f"'{c['phone']}'" if c['phone'] else "NULL"
        tax_id_sql = f"'{c['company_tax_id']}'" if c['company_tax_id'] else "NULL"

        sql = f"""INSERT INTO customers (legacy_id, branch_id, name, company_name, customer_type, phone, company_tax_id, status)
VALUES ('{c['legacy_id']}', {c['branch_id']}, {name_sql}, {company_sql}, '{c['company_type']}', {phone_sql}, {tax_id_sql}, 'churned')
ON CONFLICT (legacy_id) DO NOTHING;"""
        sql_lines.append(sql)

    sql_lines.append("")
    sql_lines.append("-- === 合約資料 (status = 'terminated') ===")

    # 合約 INSERT
    for c in customers:
        if not c['monthly_rent']:
            continue

        start_date = c['contract_start'].isoformat() if c['contract_start'] else '2024-01-01'
        end_date = c.get('contract_end')
        if end_date:
            end_date = end_date.isoformat()
        else:
            # 預設合約期：開始日期後一年，確保 end_date > start_date
            if c['contract_start']:
                from dateutil.relativedelta import relativedelta
                end_date = (c['contract_start'] + relativedelta(years=1)).isoformat()
            else:
                end_date = '2024-12-31'

        deposit_sql = c['deposit_amount'] if c['deposit_amount'] else 0
        deposit_status = c['deposit_status']

        # 生成合約編號
        contract_num = f"{c['legacy_id']}-TERM"

        sql = f"""INSERT INTO contracts (customer_id, branch_id, contract_number, start_date, end_date, monthly_rent, deposit, deposit_status, status)
SELECT id, {c['branch_id']}, '{contract_num}', '{start_date}', '{end_date}', {c['monthly_rent']}, {deposit_sql}, '{deposit_status}', 'terminated'
FROM customers WHERE legacy_id = '{c['legacy_id']}'
ON CONFLICT DO NOTHING;"""
        sql_lines.append(sql)

    sql_lines.append("")
    sql_lines.append("-- === 驗證 ===")
    sql_lines.append("SELECT 'customers' as table_name, status, COUNT(*) FROM customers GROUP BY status ORDER BY status;")
    sql_lines.append("SELECT 'contracts' as table_name, status, deposit_status, COUNT(*) FROM contracts GROUP BY status, deposit_status ORDER BY status;")

    return sql_lines


def main():
    print("=" * 60)
    print("匯入已結束客戶 (churned)")
    print("=" * 60)

    all_customers = []

    # 解析大忠館
    print("\n=== 處理大忠館 ===")
    dz_df = pd.read_excel(DZ_FILE, sheet_name='已結束')
    dz_customers = parse_dz_customers(dz_df)
    print(f"  找到 {len(dz_customers)} 位客戶")
    all_customers.extend(dz_customers)

    # 解析環瑞館
    print("\n=== 處理環瑞館 ===")
    hr_df = pd.read_excel(HR_FILE, sheet_name='已結束')
    hr_customers = parse_hr_customers(hr_df)
    print(f"  找到 {len(hr_customers)} 位客戶")
    all_customers.extend(hr_customers)

    # 統計押金狀態
    held_count = sum(1 for c in all_customers if c['deposit_status'] == 'held' and c['deposit_amount'])
    refunded_count = sum(1 for c in all_customers if c['deposit_status'] == 'refunded')
    held_amount = sum(c['deposit_amount'] or 0 for c in all_customers if c['deposit_status'] == 'held')

    print(f"\n=== 押金狀態統計 ===")
    print(f"  未退還 (held):    {held_count} 筆, ${held_amount:,}")
    print(f"  已退還 (refunded): {refunded_count} 筆")

    # 生成 SQL
    sql_lines = generate_sql(all_customers)
    output_file = OUTPUT_DIR / "import_terminated_customers.sql"

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(sql_lines))

    print(f"\n=== SQL 檔案已生成 ===")
    print(f"檔案: {output_file}")
    print(f"客戶數: {len(all_customers)}")

    print(f"\n=== 執行匯入 ===")
    print(f"gcloud compute scp {output_file} instance-20251204-075237:/tmp/ --zone=us-west1-a")
    print(f"gcloud compute ssh instance-20251204-075237 --zone=us-west1-a --command='docker exec -i hj-postgres psql -U hjadmin -d hourjungle < /tmp/import_terminated_customers.sql'")


if __name__ == "__main__":
    main()
