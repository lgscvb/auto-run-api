#!/usr/bin/env python3
"""
根據合約生成繳費記錄
每個有效合約每月生成一筆應收款
"""

import pandas as pd
from pathlib import Path
from datetime import datetime, date
from dateutil.relativedelta import relativedelta

# 路徑設定
SCRIPT_DIR = Path(__file__).parent
OUTPUT_DIR = SCRIPT_DIR / "output"

def escape_sql(val):
    """轉義 SQL 字串"""
    if pd.isna(val) or val == '':
        return "NULL"
    val = str(val).replace("'", "''")
    return f"'{val}'"

def main():
    print("=== 生成繳費記錄 ===")

    # 從 GCP 讀取合約資料
    # 這裡我們用 curl 取得資料
    import subprocess
    import json

    # 取得所有合約
    result = subprocess.run([
        'curl', '-s',
        'https://auto.yourspce.org/api/db/contracts?select=id,customer_id,branch_id,contract_number,start_date,end_date,monthly_rent,payment_cycle,status&limit=500'
    ], capture_output=True, text=True)

    contracts = json.loads(result.stdout)
    print(f"取得 {len(contracts)} 筆合約")

    # 生成 SQL
    sql_lines = []
    sql_lines.append("-- ============================================================================")
    sql_lines.append("-- Hour Jungle CRM - 繳費記錄生成腳本")
    sql_lines.append(f"-- 生成時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    sql_lines.append("-- ============================================================================")
    sql_lines.append("")

    today = date.today()
    current_month = today.replace(day=1)
    payment_count = 0

    for contract in contracts:
        # 只處理有效合約
        if contract['status'] != 'active':
            continue

        contract_id = contract['id']
        customer_id = contract['customer_id']
        branch_id = contract['branch_id']
        monthly_rent = contract['monthly_rent']
        start_date = datetime.strptime(contract['start_date'], '%Y-%m-%d').date()
        end_date = datetime.strptime(contract['end_date'], '%Y-%m-%d').date()

        # 決定付款週期
        payment_cycle = contract.get('payment_cycle', 'monthly')
        if payment_cycle == 'annual':
            months_per_payment = 12
        elif payment_cycle == 'semi_annual':
            months_per_payment = 6
        elif payment_cycle == 'biennial':
            months_per_payment = 24
        else:  # monthly
            months_per_payment = 1

        # 計算每期金額
        period_amount = monthly_rent * months_per_payment

        # 生成從當月開始往後 3 個月的繳費記錄
        # 這樣可以看到即將到期的應收款
        period_start = current_month
        for i in range(3):  # 生成 3 期
            due_date = period_start + relativedelta(months=i * months_per_payment)

            # 確保在合約期間內
            if due_date < start_date.replace(day=1):
                continue
            if due_date > end_date:
                continue

            # 生成繳費期間標籤
            period_label = due_date.strftime('%Y-%m')

            # 判斷狀態：過期的為 overdue，未來的為 pending
            if due_date < current_month:
                payment_status = 'overdue'
            else:
                payment_status = 'pending'

            sql = f"""INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES ({contract_id}, {customer_id}, {branch_id}, 'rent', '{period_label}', {period_amount}, '{due_date}', '{payment_status}')
ON CONFLICT DO NOTHING;"""

            sql_lines.append(sql)
            payment_count += 1

    sql_lines.append("")
    sql_lines.append(f"-- 生成了 {payment_count} 筆繳費記錄")
    sql_lines.append("")
    sql_lines.append("-- 驗證")
    sql_lines.append("SELECT payment_status, COUNT(*) as count, SUM(amount) as total FROM payments GROUP BY payment_status;")

    # 寫入檔案
    output_file = OUTPUT_DIR / "import_payments.sql"
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(sql_lines))

    print(f"\n=== 完成 ===")
    print(f"生成了 {payment_count} 筆繳費記錄")
    print(f"SQL 檔案: {output_file}")
    print(f"\n執行方式:")
    print(f"  gcloud compute ssh instance-20251204-075237 --zone=us-west1-a --command='docker exec -i hj-postgres psql -U hjadmin -d hourjungle < /tmp/import_payments.sql'")

if __name__ == "__main__":
    main()
