#!/usr/bin/env python3
"""
匯入 2026 年繳費記錄
從 Excel 繳費表匯入到 PostgreSQL payments 表
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
PAYMENT_FILE = "/Users/daihaoting_1/Downloads/原始資料/2026客戶繳費.xlsx"

# 月份對應
MONTH_MAP = {
    "1月": 1, "2月": 2, "3月": 3, "4月": 4, "5月": 5, "6月": 6,
    "7月": 7, "8月": 8, "9月": 9, "10月": 10, "11月": 11, "12月": 12
}


def get_legacy_id(excel_id):
    """將 Excel 編號轉換為資料庫 legacy_id"""
    if pd.isna(excel_id):
        return None

    excel_id_str = str(excel_id).strip()
    if not excel_id_str or not excel_id_str.replace('.', '').isdigit():
        return None

    try:
        num = int(float(excel_id_str))
        return f"DZ-{num:03d}"
    except (ValueError, TypeError):
        return None


def parse_amount(amount_str):
    """解析繳費金額"""
    if pd.isna(amount_str):
        return None

    amount_str = str(amount_str).strip()

    # 純數字
    try:
        return int(float(amount_str))
    except (ValueError, TypeError):
        return None


def parse_date(date_val):
    """解析日期"""
    if pd.isna(date_val):
        return None

    # 已經是 datetime
    if isinstance(date_val, (datetime, date)):
        if isinstance(date_val, datetime):
            return date_val.date()
        return date_val

    # Timestamp
    if hasattr(date_val, 'date'):
        return date_val.date()

    # 字串格式 (民國年)
    date_str = str(date_val).strip()
    if "/" in date_str:
        parts = date_str.replace("0115", "115").replace("0114", "114").split("/")
        if len(parts) == 3:
            try:
                year = int(parts[0])
                if year < 200:  # 民國年
                    year += 1911
                month = int(parts[1])
                day = int(parts[2])
                return date(year, month, day)
            except ValueError:
                pass

    return None


def fetch_customers():
    """從 API 取得客戶資料"""
    result = subprocess.run([
        'curl', '-s',
        f'{API_BASE}/customers?select=id,legacy_id,name,branch_id&limit=1000'
    ], capture_output=True, text=True)

    customers = json.loads(result.stdout)
    return {c['legacy_id']: c for c in customers if c.get('legacy_id')}


def fetch_contracts():
    """從 API 取得合約資料"""
    result = subprocess.run([
        'curl', '-s',
        f'{API_BASE}/contracts?select=id,customer_id,branch_id,status&limit=1000'
    ], capture_output=True, text=True)

    contracts = json.loads(result.stdout)
    # customer_id → contract (優先取 active)
    contract_map = {}
    for c in contracts:
        customer_id = c['customer_id']
        if customer_id not in contract_map or c['status'] == 'active':
            contract_map[customer_id] = c
    return contract_map


def process_excel():
    """處理 Excel 檔案"""
    print(f"=== 處理 2026 繳費資料 ===")
    print(f"檔案: {PAYMENT_FILE}")

    customer_map = fetch_customers()
    print(f"客戶: {len(customer_map)} 筆")

    contract_map = fetch_contracts()
    print(f"合約: {len(contract_map)} 筆")

    xl = pd.ExcelFile(PAYMENT_FILE)
    print(f"工作表: {xl.sheet_names}")

    payments = []
    skipped = []
    unmatched = []

    for sheet_name in xl.sheet_names:
        if sheet_name not in MONTH_MAP:
            print(f"  跳過非月份工作表: {sheet_name}")
            continue

        month = MONTH_MAP[sheet_name]
        df = pd.read_excel(PAYMENT_FILE, sheet_name=sheet_name)

        for idx, row in df.iterrows():
            excel_id = row.get('Unnamed: 0')
            name = row.get('Unnamed: 1')
            company = row.get('Unnamed: 2')
            payment_date_raw = row.get('繳費日')
            amount_raw = row.get('繳費金額')

            legacy_id = get_legacy_id(excel_id)
            if not legacy_id:
                continue

            customer = customer_map.get(legacy_id)
            if not customer:
                unmatched.append({
                    'legacy_id': legacy_id,
                    'name': name,
                    'company': company,
                    'month': sheet_name
                })
                continue

            customer_id = customer['id']
            contract = contract_map.get(customer_id)
            if not contract:
                unmatched.append({
                    'legacy_id': legacy_id,
                    'name': name,
                    'month': sheet_name,
                    'reason': '無合約'
                })
                continue

            contract_id = contract['id']
            branch_id = contract['branch_id']

            # 解析金額和日期
            amount = parse_amount(amount_raw)
            payment_date = parse_date(payment_date_raw)

            payment_period = f"2026-{month:02d}"

            # 2026 年資料大多是待繳
            if payment_date and amount:
                payment_status = 'paid'
            else:
                payment_status = 'pending'
                # 對於待繳，需要從合約取得月租金
                if not amount:
                    # 跳過無金額記錄
                    skipped.append({
                        'legacy_id': legacy_id,
                        'name': name,
                        'month': sheet_name,
                        'reason': '無金額(待繳記錄)'
                    })
                    continue

            payments.append({
                'contract_id': contract_id,
                'customer_id': customer_id,
                'branch_id': branch_id,
                'payment_type': 'rent',
                'payment_period': payment_period,
                'amount': amount,
                'due_date': f"2026-{month:02d}-01",
                'payment_status': payment_status,
                'paid_at': payment_date.isoformat() if payment_date else None,
                'payment_method': 'transfer'
            })

    print(f"\n統計:")
    print(f"  有效繳費: {len(payments)} 筆")
    print(f"  跳過記錄: {len(skipped)} 筆")
    print(f"  無法匹配: {len(unmatched)} 筆")

    return payments, skipped, unmatched


def generate_sql(payments):
    """生成 SQL"""
    sql_lines = []
    sql_lines.append("-- ============================================================================")
    sql_lines.append("-- Hour Jungle CRM - 2026 繳費記錄匯入")
    sql_lines.append(f"-- 生成時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    sql_lines.append("-- ============================================================================")
    sql_lines.append("")

    stats = {'paid': 0, 'pending': 0}
    total_paid = 0
    total_pending = 0

    for p in payments:
        paid_at_sql = f"'{p['paid_at']}'" if p['paid_at'] else "NULL"

        sql = f"""INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES ({p['contract_id']}, {p['customer_id']}, {p['branch_id']}, '{p['payment_type']}', '{p['payment_period']}', {p['amount']}, '{p['due_date']}', '{p['payment_status']}', {paid_at_sql}, '{p['payment_method']}')
ON CONFLICT DO NOTHING;"""
        sql_lines.append(sql)

        stats[p['payment_status']] += 1
        if p['payment_status'] == 'paid':
            total_paid += p['amount']
        else:
            total_pending += p['amount']

    sql_lines.append("")
    sql_lines.append(f"-- 統計:")
    sql_lines.append(f"-- 已繳 (paid):    {stats['paid']:>4} 筆  ${total_paid:>12,.0f}")
    sql_lines.append(f"-- 待繳 (pending): {stats['pending']:>4} 筆  ${total_pending:>12,.0f}")
    sql_lines.append("")
    sql_lines.append("-- 驗證")
    sql_lines.append("SELECT payment_period, payment_status, COUNT(*) as count, SUM(amount) as total FROM payments WHERE payment_period LIKE '2026-%' GROUP BY payment_period, payment_status ORDER BY payment_period;")

    return sql_lines, stats, total_paid, total_pending


def main():
    print("=" * 60)
    print("匯入 2026 年繳費記錄")
    print("=" * 60)

    payments, skipped, unmatched = process_excel()

    if not payments:
        print("\n無有效繳費記錄可匯入")
        return

    sql_lines, stats, total_paid, total_pending = generate_sql(payments)

    output_file = OUTPUT_DIR / "import_2026_payments.sql"
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(sql_lines))

    print(f"\n=== SQL 檔案已生成 ===")
    print(f"檔案: {output_file}")
    print(f"已繳: {stats['paid']} 筆, ${total_paid:,.0f}")
    print(f"待繳: {stats['pending']} 筆, ${total_pending:,.0f}")

    if unmatched:
        unmatched_file = OUTPUT_DIR / "unmatched_2026_payments.csv"
        pd.DataFrame(unmatched).to_csv(unmatched_file, index=False, encoding='utf-8-sig')
        print(f"無法匹配: {unmatched_file}")

    print(f"\n=== 執行匯入 ===")
    print(f"gcloud compute scp {output_file} instance-20251204-075237:/tmp/ --zone=us-west1-a")
    print(f"gcloud compute ssh instance-20251204-075237 --zone=us-west1-a --command='docker exec -i hj-postgres psql -U hjadmin -d hourjungle < /tmp/import_2026_payments.sql'")


if __name__ == "__main__":
    main()
