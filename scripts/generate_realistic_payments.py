#!/usr/bin/env python3
"""
生成真實的繳費記錄：
1. 過去 6 個月的已繳款項 (paid)
2. 逾期款項 (overdue) - 過去 1-2 個月未繳
3. 當月待繳 (pending)
4. 未來 2 個月待繳 (pending)
"""

import subprocess
import json
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from pathlib import Path
import random

SCRIPT_DIR = Path(__file__).parent
OUTPUT_DIR = SCRIPT_DIR / "output"

def main():
    print("=== 生成真實繳費記錄 ===")

    # 取得所有合約
    result = subprocess.run([
        'curl', '-s',
        'https://auto.yourspce.org/api/db/contracts?select=id,customer_id,branch_id,contract_number,start_date,end_date,monthly_rent,payment_cycle,status&limit=500'
    ], capture_output=True, text=True)

    contracts = json.loads(result.stdout)
    print(f"取得 {len(contracts)} 筆合約")

    # 先清空現有繳費記錄
    sql_lines = []
    sql_lines.append("-- ============================================================================")
    sql_lines.append("-- Hour Jungle CRM - 真實繳費記錄生成腳本")
    sql_lines.append(f"-- 生成時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    sql_lines.append("-- ============================================================================")
    sql_lines.append("")
    sql_lines.append("-- 清空現有繳費記錄")
    sql_lines.append("TRUNCATE payments RESTART IDENTITY;")
    sql_lines.append("")

    today = date.today()
    current_month = today.replace(day=1)

    stats = {'paid': 0, 'overdue': 0, 'pending': 0}
    total_paid = 0
    total_overdue = 0
    total_pending = 0

    for contract in contracts:
        contract_id = contract['id']
        customer_id = contract['customer_id']
        branch_id = contract['branch_id']
        monthly_rent = contract['monthly_rent']
        status = contract['status']
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

        period_amount = monthly_rent * months_per_payment

        # 根據合約狀態決定生成哪些記錄
        if status == 'active':
            # 有效合約：過去6個月(已繳) + 當月(待繳或逾期) + 未來2個月(待繳)

            # === 過去 6 個月：已繳 ===
            for i in range(6, 0, -1):
                past_month = current_month - relativedelta(months=i * months_per_payment)
                if past_month < start_date.replace(day=1):
                    continue
                if past_month > end_date:
                    continue

                period_label = past_month.strftime('%Y-%m')
                paid_date = past_month + relativedelta(days=random.randint(1, 10))

                sql = f"""INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES ({contract_id}, {customer_id}, {branch_id}, 'rent', '{period_label}', {period_amount}, '{past_month}', 'paid', '{paid_date}', 'transfer');"""
                sql_lines.append(sql)
                stats['paid'] += 1
                total_paid += period_amount

            # === 上個月：80% 已繳，20% 逾期 ===
            last_month = current_month - relativedelta(months=1)
            if last_month >= start_date.replace(day=1) and last_month <= end_date:
                period_label = last_month.strftime('%Y-%m')
                if random.random() < 0.8:  # 80% 已繳
                    paid_date = last_month + relativedelta(days=random.randint(1, 15))
                    sql = f"""INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES ({contract_id}, {customer_id}, {branch_id}, 'rent', '{period_label}', {period_amount}, '{last_month}', 'paid', '{paid_date}', 'transfer');"""
                    stats['paid'] += 1
                    total_paid += period_amount
                else:  # 20% 逾期
                    overdue_days = (today - last_month).days
                    sql = f"""INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, overdue_days)
VALUES ({contract_id}, {customer_id}, {branch_id}, 'rent', '{period_label}', {period_amount}, '{last_month}', 'overdue', {overdue_days});"""
                    stats['overdue'] += 1
                    total_overdue += period_amount
                sql_lines.append(sql)

            # === 當月：待繳 ===
            if current_month >= start_date.replace(day=1) and current_month <= end_date:
                period_label = current_month.strftime('%Y-%m')
                sql = f"""INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES ({contract_id}, {customer_id}, {branch_id}, 'rent', '{period_label}', {period_amount}, '{current_month}', 'pending');"""
                sql_lines.append(sql)
                stats['pending'] += 1
                total_pending += period_amount

            # === 未來 2 個月：待繳 ===
            for i in range(1, 3):
                future_month = current_month + relativedelta(months=i * months_per_payment)
                if future_month > end_date:
                    continue
                period_label = future_month.strftime('%Y-%m')
                sql = f"""INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES ({contract_id}, {customer_id}, {branch_id}, 'rent', '{period_label}', {period_amount}, '{future_month}', 'pending');"""
                sql_lines.append(sql)
                stats['pending'] += 1
                total_pending += period_amount

        elif status == 'expired':
            # 過期合約：只生成合約期間內的已繳記錄
            contract_months = (end_date.year - start_date.year) * 12 + (end_date.month - start_date.month)
            for i in range(min(contract_months, 12)):  # 最多生成12個月
                past_month = start_date.replace(day=1) + relativedelta(months=i * months_per_payment)
                if past_month > end_date:
                    break
                period_label = past_month.strftime('%Y-%m')
                paid_date = past_month + relativedelta(days=random.randint(1, 10))

                sql = f"""INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status, paid_at, payment_method)
VALUES ({contract_id}, {customer_id}, {branch_id}, 'rent', '{period_label}', {period_amount}, '{past_month}', 'paid', '{paid_date}', 'transfer');"""
                sql_lines.append(sql)
                stats['paid'] += 1
                total_paid += period_amount

    sql_lines.append("")
    sql_lines.append(f"-- 統計: 已繳 {stats['paid']} 筆 (${total_paid:,.0f}), 逾期 {stats['overdue']} 筆 (${total_overdue:,.0f}), 待繳 {stats['pending']} 筆 (${total_pending:,.0f})")
    sql_lines.append("")
    sql_lines.append("-- 驗證")
    sql_lines.append("SELECT payment_status, COUNT(*) as count, SUM(amount) as total FROM payments GROUP BY payment_status ORDER BY payment_status;")

    # 寫入檔案
    output_file = OUTPUT_DIR / "import_realistic_payments.sql"
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(sql_lines))

    print(f"\n=== 統計 ===")
    print(f"已繳 (paid):    {stats['paid']:>4} 筆  ${total_paid:>12,.0f}")
    print(f"逾期 (overdue): {stats['overdue']:>4} 筆  ${total_overdue:>12,.0f}")
    print(f"待繳 (pending): {stats['pending']:>4} 筆  ${total_pending:>12,.0f}")
    print(f"\nSQL 檔案: {output_file}")

if __name__ == "__main__":
    main()
