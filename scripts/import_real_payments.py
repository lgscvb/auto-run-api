#!/usr/bin/env python3
"""
匯入真實繳費記錄
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
DZ_PAYMENT_FILE = "/Users/daihaoting_1/Downloads/原始資料/2025 客戶繳費.xlsx"
HR_PAYMENT_FILE = "/Users/daihaoting_1/Downloads/原始資料/環2025客戶繳費.xlsx"

# 月份對應
MONTH_MAP = {
    "1月": 1, "2月": 2, "3月": 3, "4月": 4, "5月": 5, "6月": 6,
    "7月": 7, "8月": 8, "9月": 9, "10月": 10, "11月": 11, "12月": 12
}


def get_legacy_id(excel_id, source_branch):
    """將 Excel 編號轉換為資料庫 legacy_id"""
    if pd.isna(excel_id):
        return None

    excel_id_str = str(excel_id).strip()

    # 跳過標題行
    if "待遷出" in excel_id_str or not excel_id_str:
        return None

    try:
        if source_branch == '大忠館':
            # 純數字 → DZ-XXX（三位數補零）
            num = int(float(excel_id_str))
            return f"DZ-{num:03d}"

        elif source_branch == '環瑞館':
            if excel_id_str.upper().startswith('V'):
                # V01 → HR-001
                num = int(excel_id_str[1:])
                return f"HR-{num:03d}"
            else:
                # 純數字 → 可能是跨館的大忠館客戶
                num = int(float(excel_id_str))
                return f"DZ-{num:03d}"
    except (ValueError, TypeError):
        return None

    return None


def parse_amount(amount_str):
    """解析繳費金額，處理特殊格式"""
    if pd.isna(amount_str):
        return None, None, None

    amount_str = str(amount_str).strip()

    # 退款記錄
    if "已退" in amount_str or "退款" in amount_str or "提前退" in amount_str:
        # 嘗試提取退款金額
        numbers = re.findall(r'\d+', amount_str)
        if numbers:
            return None, 'refund', int(numbers[0])
        return None, 'refund', None

    # 加總格式: 6000+4140
    if "+" in amount_str:
        parts = amount_str.split("+")
        try:
            total = sum(int(p.strip()) for p in parts if p.strip().isdigit())
            return total, 'paid', None
        except ValueError:
            pass

    # 現金備註: 6000 現金
    if "現金" in amount_str:
        numbers = re.findall(r'\d+', amount_str)
        if numbers:
            return int(numbers[0]), 'paid', None

    # 刷卡備註: 台南刷卡8940
    if "刷卡" in amount_str:
        numbers = re.findall(r'\d+', amount_str)
        if numbers:
            return int(numbers[0]), 'paid', None

    # 純數字
    try:
        return int(float(amount_str)), 'paid', None
    except (ValueError, TypeError):
        return None, None, None


def parse_date(date_val):
    """解析日期，處理多種格式"""
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

    # 字串格式
    date_str = str(date_val).strip()

    # 民國年格式: 114/01/20 或 0114/1/3
    if "/" in date_str:
        parts = date_str.replace("0114", "114").split("/")
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

    # Excel 數字日期
    try:
        excel_date = int(float(date_str))
        # Excel 日期起始於 1900-01-01
        from datetime import timedelta
        base = date(1899, 12, 30)
        return base + timedelta(days=excel_date)
    except (ValueError, TypeError):
        pass

    return None


def fetch_customers():
    """從 API 取得客戶資料"""
    result = subprocess.run([
        'curl', '-s',
        f'{API_BASE}/customers?select=id,legacy_id,name,branch_id&limit=1000'
    ], capture_output=True, text=True)

    customers = json.loads(result.stdout)

    # 建立 legacy_id → customer 映射
    customer_map = {}
    for c in customers:
        if c.get('legacy_id'):
            customer_map[c['legacy_id']] = c

    return customer_map


def fetch_contracts():
    """從 API 取得合約資料"""
    result = subprocess.run([
        'curl', '-s',
        f'{API_BASE}/contracts?select=id,customer_id,branch_id,status&limit=1000'
    ], capture_output=True, text=True)

    contracts = json.loads(result.stdout)

    # 建立 customer_id → contract 映射（優先取 active 合約）
    contract_map = {}
    for c in contracts:
        customer_id = c['customer_id']
        if customer_id not in contract_map or c['status'] == 'active':
            contract_map[customer_id] = c

    return contract_map


def process_excel_file(file_path, branch_name, customer_map, contract_map):
    """處理單個 Excel 檔案"""
    print(f"\n=== 處理 {branch_name} 繳費資料 ===")
    print(f"檔案: {file_path}")

    xl = pd.ExcelFile(file_path)
    print(f"工作表: {xl.sheet_names}")

    payments = []
    skipped = []
    refunds = []
    unmatched = []

    for sheet_name in xl.sheet_names:
        if sheet_name not in MONTH_MAP:
            print(f"  跳過非月份工作表: {sheet_name}")
            continue

        month = MONTH_MAP[sheet_name]
        df = pd.read_excel(file_path, sheet_name=sheet_name)

        for idx, row in df.iterrows():
            excel_id = row.get('Unnamed: 0')
            name = row.get('Unnamed: 1')
            company = row.get('Unnamed: 2')
            payment_date_raw = row.get('繳費日')
            amount_raw = row.get('繳費金額')

            # 取得 legacy_id
            legacy_id = get_legacy_id(excel_id, branch_name)
            if not legacy_id:
                continue

            # 查找客戶
            customer = customer_map.get(legacy_id)
            if not customer:
                unmatched.append({
                    'legacy_id': legacy_id,
                    'name': name,
                    'company': company,
                    'month': sheet_name,
                    'amount': amount_raw
                })
                continue

            customer_id = customer['id']

            # 查找合約
            contract = contract_map.get(customer_id)
            if not contract:
                unmatched.append({
                    'legacy_id': legacy_id,
                    'name': name,
                    'company': company,
                    'month': sheet_name,
                    'amount': amount_raw,
                    'reason': '無合約'
                })
                continue

            contract_id = contract['id']
            # 環瑞館跨館客戶使用環瑞館 branch_id
            branch_id = 2 if branch_name == '環瑞館' else contract['branch_id']

            # 解析金額
            amount, payment_type, refund_amount = parse_amount(amount_raw)

            # 也檢查繳費日欄位是否有退款備註（如 "7/1已退款6000"）
            payment_date_str = str(payment_date_raw) if pd.notna(payment_date_raw) else ''
            if '已退' in payment_date_str or '退款' in payment_date_str:
                payment_type = 'refund'
                # 嘗試從繳費日欄位提取退款金額
                numbers = re.findall(r'已退款?(\d+)', payment_date_str)
                if numbers:
                    refund_amount = int(numbers[0])

            # 處理退款
            if payment_type == 'refund':
                refunds.append({
                    'legacy_id': legacy_id,
                    'name': name,
                    'month': sheet_name,
                    'raw_amount': amount_raw,
                    'raw_date': payment_date_raw,
                    'refund_amount': refund_amount
                })
                continue

            # 解析日期
            payment_date = parse_date(payment_date_raw)

            # 無金額或無效記錄
            if not amount:
                skipped.append({
                    'legacy_id': legacy_id,
                    'name': name,
                    'month': sheet_name,
                    'reason': '無金額',
                    'raw_amount': amount_raw
                })
                continue

            # 判斷狀態
            payment_period = f"2025-{month:02d}"
            if payment_date:
                payment_status = 'paid'
            else:
                # 無繳費日期
                today = date.today()
                period_date = date(2025, month, 1)
                if period_date < today.replace(day=1):
                    payment_status = 'overdue'
                else:
                    payment_status = 'pending'

            payments.append({
                'contract_id': contract_id,
                'customer_id': customer_id,
                'branch_id': branch_id,
                'payment_type': 'rent',
                'payment_period': payment_period,
                'amount': amount,
                'due_date': f"2025-{month:02d}-01",
                'payment_status': payment_status,
                'paid_at': payment_date.isoformat() if payment_date else None,
                'payment_method': 'transfer'
            })

    print(f"\n{branch_name} 統計:")
    print(f"  有效繳費: {len(payments)} 筆")
    print(f"  退款記錄: {len(refunds)} 筆")
    print(f"  跳過記錄: {len(skipped)} 筆")
    print(f"  無法匹配: {len(unmatched)} 筆")

    return payments, refunds, skipped, unmatched


def generate_sql(all_payments, output_file):
    """生成 SQL 匯入腳本"""
    sql_lines = []
    sql_lines.append("-- ============================================================================")
    sql_lines.append("-- Hour Jungle CRM - 真實繳費記錄匯入")
    sql_lines.append(f"-- 生成時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    sql_lines.append("-- ============================================================================")
    sql_lines.append("")
    sql_lines.append("-- 清空現有繳費記錄")
    sql_lines.append("TRUNCATE payments RESTART IDENTITY;")
    sql_lines.append("")

    stats = {'paid': 0, 'pending': 0, 'overdue': 0}
    total_paid = 0
    total_pending = 0
    total_overdue = 0

    for p in all_payments:
        paid_at_sql = f"'{p['paid_at']}'" if p['paid_at'] else "NULL"

        sql = f"""INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES ({p['contract_id']}, {p['customer_id']}, {p['branch_id']}, '{p['payment_type']}', '{p['payment_period']}', {p['amount']}, '{p['due_date']}', '{p['payment_status']}', {paid_at_sql}, '{p['payment_method']}');"""
        sql_lines.append(sql)

        stats[p['payment_status']] += 1
        if p['payment_status'] == 'paid':
            total_paid += p['amount']
        elif p['payment_status'] == 'pending':
            total_pending += p['amount']
        else:
            total_overdue += p['amount']

    sql_lines.append("")
    sql_lines.append(f"-- 統計:")
    sql_lines.append(f"-- 已繳 (paid):    {stats['paid']:>4} 筆  ${total_paid:>12,.0f}")
    sql_lines.append(f"-- 待繳 (pending): {stats['pending']:>4} 筆  ${total_pending:>12,.0f}")
    sql_lines.append(f"-- 逾期 (overdue): {stats['overdue']:>4} 筆  ${total_overdue:>12,.0f}")
    sql_lines.append("")
    sql_lines.append("-- 驗證")
    sql_lines.append("SELECT payment_status, COUNT(*) as count, SUM(amount) as total FROM payments GROUP BY payment_status ORDER BY payment_status;")

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(sql_lines))

    print(f"\n=== SQL 檔案已生成 ===")
    print(f"檔案: {output_file}")
    print(f"\n統計:")
    print(f"  已繳 (paid):    {stats['paid']:>4} 筆  ${total_paid:>12,.0f}")
    print(f"  待繳 (pending): {stats['pending']:>4} 筆  ${total_pending:>12,.0f}")
    print(f"  逾期 (overdue): {stats['overdue']:>4} 筆  ${total_overdue:>12,.0f}")
    print(f"  總計:           {len(all_payments):>4} 筆")

    return stats


def main():
    print("=" * 60)
    print("匯入真實繳費記錄")
    print("=" * 60)

    # 取得客戶和合約資料
    print("\n取得資料庫資料...")
    customer_map = fetch_customers()
    print(f"  客戶: {len(customer_map)} 筆")

    contract_map = fetch_contracts()
    print(f"  合約: {len(contract_map)} 筆")

    all_payments = []
    all_refunds = []
    all_skipped = []
    all_unmatched = []

    # 處理大忠館
    payments, refunds, skipped, unmatched = process_excel_file(
        DZ_PAYMENT_FILE, '大忠館', customer_map, contract_map
    )
    all_payments.extend(payments)
    all_refunds.extend(refunds)
    all_skipped.extend(skipped)
    all_unmatched.extend(unmatched)

    # 處理環瑞館
    payments, refunds, skipped, unmatched = process_excel_file(
        HR_PAYMENT_FILE, '環瑞館', customer_map, contract_map
    )
    all_payments.extend(payments)
    all_refunds.extend(refunds)
    all_skipped.extend(skipped)
    all_unmatched.extend(unmatched)

    # 生成 SQL
    output_file = OUTPUT_DIR / "import_real_payments.sql"
    generate_sql(all_payments, output_file)

    # 輸出無法匹配的記錄
    if all_unmatched:
        unmatched_file = OUTPUT_DIR / "unmatched_payments.csv"
        pd.DataFrame(all_unmatched).to_csv(unmatched_file, index=False, encoding='utf-8-sig')
        print(f"\n無法匹配記錄: {unmatched_file}")

    # 輸出退款記錄
    if all_refunds:
        refunds_file = OUTPUT_DIR / "refund_payments.csv"
        pd.DataFrame(all_refunds).to_csv(refunds_file, index=False, encoding='utf-8-sig')
        print(f"退款記錄: {refunds_file}")

    print(f"\n=== 完成 ===")
    print(f"\n執行匯入:")
    print(f"  gcloud compute scp {output_file} instance-20251204-075237:/tmp/ --zone=us-west1-a")
    print(f"  gcloud compute ssh instance-20251204-075237 --zone=us-west1-a --command='docker exec -i hj-postgres psql -U hjadmin -d hourjungle < /tmp/import_real_payments.sql'")


if __name__ == "__main__":
    main()
