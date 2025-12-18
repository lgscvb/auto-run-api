#!/usr/bin/env python3
"""
為活躍合約生成正確的待繳記錄

繳費日期 (due_date) 計算邏輯：
1. 優先使用合約表的 payment_day 欄位
2. 若 payment_day 為空，fallback 到 start_date 的「日」
3. 繳費月份根據 payment_cycle（monthly, annual, semi_annual 等）決定
"""

import subprocess
import json
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
OUTPUT_DIR = SCRIPT_DIR / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

API_BASE = "https://auto.yourspce.org/api/db"


def get_payment_dates_in_period(start_date, payment_cycle, target_year, target_month, contract_payment_day=None):
    """
    計算合約在指定月份的繳費日期

    Args:
        start_date: 合約開始日期
        payment_cycle: 繳費週期 (monthly, annual, semi_annual, biennial, quarterly)
        target_year: 目標年份
        target_month: 目標月份
        contract_payment_day: 合約設定的繳費日（優先使用）

    Returns:
        list of (due_date, amount_multiplier) 或空列表
    """
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, '%Y-%m-%d').date()

    target_start = date(target_year, target_month, 1)
    target_end = (target_start + relativedelta(months=1)) - relativedelta(days=1)

    results = []

    # 優先使用合約設定的繳費日，否則用開始日期的「日」
    base_payment_day = contract_payment_day if contract_payment_day else start_date.day

    if payment_cycle == 'monthly':
        # 每月繳費，繳費日 = 合約設定的 payment_day
        # 使用當月最後一天作為上限（例：設定31號，2月用28號）
        payment_day = min(base_payment_day, target_end.day)
        try:
            due_date = date(target_year, target_month, payment_day)
        except ValueError:
            # 如果日期無效，用月底
            due_date = target_end

        # 確保合約在該日期前已開始
        if start_date <= due_date:
            results.append((due_date, 1))  # 1 個月租金

    elif payment_cycle == 'annual':
        # 每年繳費，繳費月份 = 合約開始月份，繳費日 = payment_day
        if start_date.month == target_month:
            payment_day = min(base_payment_day, target_end.day)
            try:
                due_date = date(target_year, target_month, payment_day)
            except ValueError:
                due_date = target_end

            if start_date <= due_date:
                results.append((due_date, 12))  # 12 個月租金

    elif payment_cycle == 'semi_annual':
        # 每半年繳費
        # 繳費月份 = 開始月份, 開始月份+6, 開始月份+12...
        start_month = start_date.month
        payment_months = [(start_month + i * 6 - 1) % 12 + 1 for i in range(2)]

        if target_month in payment_months:
            payment_day = min(base_payment_day, target_end.day)
            try:
                due_date = date(target_year, target_month, payment_day)
            except ValueError:
                due_date = target_end

            if start_date <= due_date:
                results.append((due_date, 6))  # 6 個月租金

    elif payment_cycle == 'quarterly':
        # 每季繳費
        start_month = start_date.month
        payment_months = [(start_month + i * 3 - 1) % 12 + 1 for i in range(4)]

        if target_month in payment_months:
            payment_day = min(base_payment_day, target_end.day)
            try:
                due_date = date(target_year, target_month, payment_day)
            except ValueError:
                due_date = target_end

            if start_date <= due_date:
                results.append((due_date, 3))  # 3 個月租金

    elif payment_cycle == 'biennial':
        # 每兩年繳費
        # 只在開始月份繳費，且要檢查年份
        if start_date.month == target_month:
            # 計算從開始日期到目標日期經過的月數
            months_diff = (target_year - start_date.year) * 12 + (target_month - start_date.month)
            if months_diff >= 0 and months_diff % 24 == 0:
                payment_day = min(base_payment_day, target_end.day)
                try:
                    due_date = date(target_year, target_month, payment_day)
                except ValueError:
                    due_date = target_end

                if start_date <= due_date:
                    results.append((due_date, 24))  # 24 個月租金

    elif payment_cycle == 'triennial':
        # 每三年繳費
        # 只在開始月份繳費，且要檢查年份
        if start_date.month == target_month:
            months_diff = (target_year - start_date.year) * 12 + (target_month - start_date.month)
            if months_diff >= 0 and months_diff % 36 == 0:
                payment_day = min(base_payment_day, target_end.day)
                try:
                    due_date = date(target_year, target_month, payment_day)
                except ValueError:
                    due_date = target_end

                if start_date <= due_date:
                    results.append((due_date, 36))  # 36 個月租金

    return results


def fetch_active_contracts():
    """取得所有活躍合約（包含 payment_day）"""
    result = subprocess.run([
        'curl', '-s',
        f'{API_BASE}/contracts?select=id,customer_id,branch_id,start_date,monthly_rent,payment_cycle,payment_day,status&status=eq.active&limit=500'
    ], capture_output=True, text=True)
    return json.loads(result.stdout)


def fetch_existing_payments(target_period):
    """取得目標期間已繳費（paid）的記錄，不包含 pending"""
    result = subprocess.run([
        'curl', '-s',
        f'{API_BASE}/payments?select=contract_id,payment_status&payment_period=eq.{target_period}&payment_status=eq.paid'
    ], capture_output=True, text=True)
    payments = json.loads(result.stdout)
    return {p['contract_id'] for p in payments}


def fetch_customers():
    """取得客戶資料"""
    result = subprocess.run([
        'curl', '-s',
        f'{API_BASE}/customers?select=id,name,legacy_id&limit=500'
    ], capture_output=True, text=True)
    customers = json.loads(result.stdout)
    return {c['id']: c for c in customers}


def main():
    # 目標月份
    target_year = 2025
    target_month = 12
    target_period = f"{target_year}-{target_month:02d}"

    print("=" * 60)
    print(f"生成 {target_period} 待繳記錄（正確版）")
    print("=" * 60)

    # 取得資料
    contracts = fetch_active_contracts()
    print(f"活躍合約: {len(contracts)} 筆")

    existing_contract_ids = fetch_existing_payments(target_period)
    print(f"已有繳費記錄: {len(existing_contract_ids)} 筆")

    customers = fetch_customers()

    # 統計各繳費週期
    cycle_stats = {}
    for c in contracts:
        cycle = c.get('payment_cycle', 'monthly')
        cycle_stats[cycle] = cycle_stats.get(cycle, 0) + 1

    print(f"\n繳費週期分布:")
    for cycle, count in sorted(cycle_stats.items()):
        print(f"  {cycle}: {count} 筆")

    # 計算每個合約是否需要在目標月份繳費
    pending_payments = []
    skipped_existing = []
    skipped_no_payment = []

    for contract in contracts:
        contract_id = contract['id']
        customer_id = contract['customer_id']
        branch_id = contract['branch_id']
        start_date = contract['start_date']
        monthly_rent = contract['monthly_rent']
        payment_cycle = contract.get('payment_cycle', 'monthly')
        contract_payment_day = contract.get('payment_day')  # 合約設定的繳費日

        customer = customers.get(customer_id, {})
        customer_name = customer.get('name', 'Unknown')
        legacy_id = customer.get('legacy_id', '')

        # 計算繳費日期（優先使用合約的 payment_day）
        payment_dates = get_payment_dates_in_period(
            start_date, payment_cycle, target_year, target_month, contract_payment_day
        )

        if not payment_dates:
            skipped_no_payment.append({
                'contract_id': contract_id,
                'customer_name': customer_name,
                'legacy_id': legacy_id,
                'payment_cycle': payment_cycle,
                'start_date': start_date,
                'reason': f'{payment_cycle} 在 {target_month}月 無繳費日'
            })
            continue

        # 檢查是否已有記錄
        if contract_id in existing_contract_ids:
            skipped_existing.append({
                'contract_id': contract_id,
                'customer_name': customer_name,
                'legacy_id': legacy_id
            })
            continue

        # 生成待繳記錄
        for due_date, multiplier in payment_dates:
            amount = int(monthly_rent * multiplier)
            pending_payments.append({
                'contract_id': contract_id,
                'customer_id': customer_id,
                'branch_id': branch_id,
                'payment_type': 'rent',
                'payment_period': target_period,
                'amount': amount,
                'due_date': due_date.isoformat(),
                'payment_status': 'pending',
                'customer_name': customer_name,
                'legacy_id': legacy_id,
                'payment_cycle': payment_cycle,
                'multiplier': multiplier,
                'payment_day': contract_payment_day or due_date.day  # 紀錄使用的繳費日
            })

    # 輸出統計
    print(f"\n=== 計算結果 ===")
    print(f"需生成待繳: {len(pending_payments)} 筆")
    print(f"已有記錄跳過: {len(skipped_existing)} 筆")
    print(f"本月無需繳費: {len(skipped_no_payment)} 筆")

    # 按繳費週期統計待繳
    print(f"\n待繳記錄明細:")
    cycle_pending = {}
    for p in pending_payments:
        cycle = p['payment_cycle']
        if cycle not in cycle_pending:
            cycle_pending[cycle] = {'count': 0, 'total': 0}
        cycle_pending[cycle]['count'] += 1
        cycle_pending[cycle]['total'] += p['amount']

    for cycle, data in sorted(cycle_pending.items()):
        print(f"  {cycle}: {data['count']} 筆, ${data['total']:,.0f}")

    # 顯示跳過的合約（本月無需繳費）
    if skipped_no_payment:
        print(f"\n本月無需繳費的合約（前 10 筆）:")
        for item in skipped_no_payment[:10]:
            print(f"  {item['legacy_id']} {item['customer_name']}: {item['reason']}")

    if not pending_payments:
        print("\n無需生成新記錄")
        return

    # 生成 SQL
    sql_lines = []
    sql_lines.append("-- ============================================================================")
    sql_lines.append(f"-- Hour Jungle CRM - {target_period} 待繳記錄生成（正確版）")
    sql_lines.append(f"-- 生成時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    sql_lines.append("-- 邏輯: 優先使用合約的 payment_day 欄位，否則用開始日期的「日」")
    sql_lines.append("-- ============================================================================")
    sql_lines.append("")

    # 先刪除錯誤的待繳記錄
    sql_lines.append(f"-- 刪除之前錯誤生成的 {target_period} pending 記錄")
    sql_lines.append(f"DELETE FROM payments WHERE payment_period = '{target_period}' AND payment_status = 'pending';")
    sql_lines.append("")

    total_amount = 0
    for p in pending_payments:
        sql = f"""INSERT INTO payments (contract_id, customer_id, branch_id, payment_type, payment_period, amount, due_date, payment_status)
VALUES ({p['contract_id']}, {p['customer_id']}, {p['branch_id']}, '{p['payment_type']}', '{p['payment_period']}', {p['amount']}, '{p['due_date']}', '{p['payment_status']}')
ON CONFLICT DO NOTHING;"""
        sql_lines.append(f"-- {p['legacy_id']} {p['customer_name']} ({p['payment_cycle']}, x{p['multiplier']}, 繳費日:{p['payment_day']}號)")
        sql_lines.append(sql)
        total_amount += p['amount']

    sql_lines.append("")
    sql_lines.append(f"-- 統計: {len(pending_payments)} 筆待繳, 總金額 ${total_amount:,.0f}")
    sql_lines.append("")
    sql_lines.append("-- 驗證")
    sql_lines.append(f"SELECT payment_status, COUNT(*) as count, SUM(amount) as total FROM payments WHERE payment_period = '{target_period}' GROUP BY payment_status;")

    # 寫入檔案
    output_file = OUTPUT_DIR / "generate_pending_payments_v2.sql"
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(sql_lines))

    print(f"\n=== SQL 檔案已生成 ===")
    print(f"檔案: {output_file}")
    print(f"待繳筆數: {len(pending_payments)}")
    print(f"待繳金額: ${total_amount:,.0f}")

    print(f"\n=== 執行指令 ===")
    print(f"gcloud compute scp {output_file} instance-20251204-075237:/tmp/ --zone=us-west1-a")
    print(f"gcloud compute ssh instance-20251204-075237 --zone=us-west1-a --command='docker exec -i hj-postgres psql -U hjadmin -d hourjungle < /tmp/generate_pending_payments_v2.sql'")


if __name__ == "__main__":
    main()
