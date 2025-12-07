#!/usr/bin/env python3
"""
為活躍合約生成當月待繳記錄
解決 Dashboard 無法顯示待收款項的問題
"""

import subprocess
import json
from datetime import datetime, date
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
OUTPUT_DIR = SCRIPT_DIR / "output"

def main():
    print("=== 生成當月待繳記錄 ===")

    current_month = date.today().replace(day=1)
    payment_period = current_month.strftime('%Y-%m')
    print(f"當月: {payment_period}")

    # 取得所有活躍合約
    result = subprocess.run([
        'curl', '-s',
        'https://auto.yourspce.org/api/db/contracts?select=id,customer_id,branch_id,monthly_rent,payment_cycle,status&status=eq.active&limit=500'
    ], capture_output=True, text=True)
    contracts = json.loads(result.stdout)
    print(f"活躍合約: {len(contracts)} 筆")

    # 取得當月已有的繳費記錄
    result = subprocess.run([
        'curl', '-s',
        f'https://auto.yourspce.org/api/db/payments?select=contract_id,payment_status&payment_period=eq.{payment_period}'
    ], capture_output=True, text=True)
    existing_payments = json.loads(result.stdout)
    existing_contract_ids = {p['contract_id'] for p in existing_payments}
    print(f"當月已有記錄: {len(existing_contract_ids)} 筆")

    # 找出需要建立待繳記錄的合約
    pending_contracts = [c for c in contracts if c['id'] not in existing_contract_ids]
    print(f"需建立待繳: {len(pending_contracts)} 筆")

    if not pending_contracts:
        print("無需建立新記錄")
        return

    # 生成 SQL
    sql_lines = []
    sql_lines.append("-- ============================================================================")
    sql_lines.append("-- Hour Jungle CRM - 當月待繳記錄生成")
    sql_lines.append(f"-- 生成時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    sql_lines.append(f"-- 繳費期間: {payment_period}")
    sql_lines.append("-- ============================================================================")
    sql_lines.append("")

    total_pending = 0
    for c in pending_contracts:
        contract_id = c['id']
        customer_id = c['customer_id']
        branch_id = c['branch_id']
        monthly_rent = c['monthly_rent']

        # 根據繳費週期計算金額
        payment_cycle = c.get('payment_cycle', 'monthly')
        if payment_cycle == 'annual':
            amount = monthly_rent * 12
        elif payment_cycle == 'semi_annual':
            amount = monthly_rent * 6
        elif payment_cycle == 'biennial':
            amount = monthly_rent * 24
        else:
            amount = monthly_rent

        sql = f"""INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES ({contract_id}, {customer_id}, {branch_id}, 'rent', '{payment_period}', {amount}, '{current_month}', 'pending')
ON CONFLICT DO NOTHING;"""
        sql_lines.append(sql)
        total_pending += amount

    sql_lines.append("")
    sql_lines.append(f"-- 統計: {len(pending_contracts)} 筆待繳, 總金額 ${total_pending:,.0f}")
    sql_lines.append("")
    sql_lines.append("-- 驗證")
    sql_lines.append("SELECT payment_status, COUNT(*) as count, SUM(amount) as total FROM payments WHERE payment_period = '{}' GROUP BY payment_status;".format(payment_period))

    # 寫入檔案
    output_file = OUTPUT_DIR / "generate_pending_payments.sql"
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(sql_lines))

    print(f"\n=== SQL 檔案已生成 ===")
    print(f"檔案: {output_file}")
    print(f"待繳筆數: {len(pending_contracts)}")
    print(f"待繳金額: ${total_pending:,.0f}")

    print(f"\n=== 執行匯入 ===")
    print(f"gcloud compute scp {output_file} instance-20251204-075237:/tmp/ --zone=us-west1-a")
    print(f"gcloud compute ssh instance-20251204-075237 --zone=us-west1-a --command='docker exec -i hj-postgres psql -U hjadmin -d hourjungle < /tmp/generate_pending_payments.sql'")

if __name__ == "__main__":
    main()
