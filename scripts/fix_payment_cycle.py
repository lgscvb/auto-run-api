#!/usr/bin/env python3
"""
修正合約的 payment_cycle 欄位
從客戶資料表和繳費表交叉驗證
"""

import pandas as pd
import subprocess
import json
from datetime import datetime
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
OUTPUT_DIR = SCRIPT_DIR / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

# 繳費週期對應
CYCLE_MAP = {
    'y': 'annual', 'Y': 'annual',
    '2y': 'biennial', '2Y': 'biennial',
    '3y': 'triennial', '3Y': 'triennial',  # 三年繳
    '6m': 'semi_annual', '6M': 'semi_annual',
    'm': 'monthly', 'M': 'monthly', 'Ｍ': 'monthly',
    '3m': 'quarterly', '3M': 'quarterly',
}


def extract_from_customer_table():
    """從客戶資料表讀取繳費方式"""
    # 大忠館
    dz_file = '/Users/daihaoting_1/Downloads/原始資料/客戶資料表crm.xlsx'
    df_dz = pd.read_excel(dz_file, sheet_name=0)

    cycles = {}
    for idx, row in df_dz.iterrows():
        excel_id = row.get('編號')
        cycle_raw = row.get('繳費方式')
        name = row.get('姓名')

        if pd.notna(excel_id) and pd.notna(cycle_raw):
            try:
                num = int(float(excel_id))
                legacy_id = f"DZ-{num:03d}"
                cycle = CYCLE_MAP.get(str(cycle_raw).strip())
                if cycle:
                    cycles[legacy_id] = {
                        'raw': str(cycle_raw).strip(),
                        'cycle': cycle,
                        'name': name,
                        'source': '客戶資料表'
                    }
            except:
                pass

    # 環瑞館
    hr_file = '/Users/daihaoting_1/Downloads/原始資料/環瑞客戶資料表crm.xlsx'
    if Path(hr_file).exists():
        df_hr = pd.read_excel(hr_file, sheet_name=0)
        for idx, row in df_hr.iterrows():
            excel_id = row.get('編號')
            cycle_raw = row.get('繳費方式')
            name = row.get('姓名')

            if pd.notna(excel_id) and pd.notna(cycle_raw):
                excel_id_str = str(excel_id).strip()
                try:
                    if excel_id_str.upper().startswith('V'):
                        num = int(excel_id_str[1:])
                        legacy_id = f"HR-{num:03d}"
                    else:
                        continue
                    cycle = CYCLE_MAP.get(str(cycle_raw).strip())
                    if cycle:
                        cycles[legacy_id] = {
                            'raw': str(cycle_raw).strip(),
                            'cycle': cycle,
                            'name': name,
                            'source': '環瑞客戶資料表'
                        }
                except:
                    pass

    return cycles


def extract_from_payment_table():
    """從繳費表讀取繳費週期"""
    cycles = {}

    # 大忠館繳費表
    dz_file = '/Users/daihaoting_1/Downloads/原始資料/2025 客戶繳費.xlsx'
    xl = pd.ExcelFile(dz_file)

    for sheet_name in xl.sheet_names:
        df = pd.read_excel(dz_file, sheet_name=sheet_name)
        for idx, row in df.iterrows():
            excel_id = row.get('Unnamed: 0')
            cycle_raw = row.get('Unnamed: 3')
            name = row.get('Unnamed: 1')

            if pd.notna(excel_id) and pd.notna(cycle_raw):
                try:
                    num = int(float(excel_id))
                    legacy_id = f"DZ-{num:03d}"
                    cycle = CYCLE_MAP.get(str(cycle_raw).strip())
                    if cycle and legacy_id not in cycles:
                        cycles[legacy_id] = {
                            'raw': str(cycle_raw).strip(),
                            'cycle': cycle,
                            'name': name,
                            'source': f'繳費表-{sheet_name}'
                        }
                except:
                    pass

    # 環瑞館繳費表
    hr_file = '/Users/daihaoting_1/Downloads/原始資料/環2025客戶繳費.xlsx'
    if Path(hr_file).exists():
        xl_hr = pd.ExcelFile(hr_file)
        for sheet_name in xl_hr.sheet_names:
            df = pd.read_excel(hr_file, sheet_name=sheet_name)
            for idx, row in df.iterrows():
                excel_id = row.get('Unnamed: 0')
                cycle_raw = row.get('Unnamed: 3')
                name = row.get('Unnamed: 1')

                if pd.notna(excel_id) and pd.notna(cycle_raw):
                    excel_id_str = str(excel_id).strip()
                    try:
                        if excel_id_str.upper().startswith('V'):
                            num = int(excel_id_str[1:])
                            legacy_id = f"HR-{num:03d}"
                        else:
                            num = int(float(excel_id_str))
                            legacy_id = f"DZ-{num:03d}"

                        cycle = CYCLE_MAP.get(str(cycle_raw).strip())
                        if cycle and legacy_id not in cycles:
                            cycles[legacy_id] = {
                                'raw': str(cycle_raw).strip(),
                                'cycle': cycle,
                                'name': name,
                                'source': f'環瑞繳費表-{sheet_name}'
                            }
                    except:
                        pass

    return cycles


def main():
    print("=" * 60)
    print("修正合約 payment_cycle（交叉驗證版）")
    print("=" * 60)

    # 從兩個來源讀取
    customer_cycles = extract_from_customer_table()
    payment_cycles = extract_from_payment_table()

    print(f"\n客戶資料表: {len(customer_cycles)} 筆")
    print(f"繳費表: {len(payment_cycles)} 筆")

    # 合併：以客戶資料表為主，繳費表補充
    all_cycles = {}
    for legacy_id, info in customer_cycles.items():
        all_cycles[legacy_id] = info

    for legacy_id, info in payment_cycles.items():
        if legacy_id not in all_cycles:
            all_cycles[legacy_id] = info

    print(f"合併後: {len(all_cycles)} 筆")

    # 取得資料庫資料
    result = subprocess.run([
        'curl', '-s',
        'https://auto.yourspce.org/api/db/contracts?select=id,customer_id,payment_cycle,status&limit=500'
    ], capture_output=True, text=True)
    contracts = json.loads(result.stdout)

    result = subprocess.run([
        'curl', '-s',
        'https://auto.yourspce.org/api/db/customers?select=id,legacy_id,name&limit=500'
    ], capture_output=True, text=True)
    customers = json.loads(result.stdout)
    customer_map = {c['id']: c for c in customers}

    print(f"資料庫合約: {len(contracts)} 筆")

    # 找出需要更新的
    updates = []
    correct = 0
    no_data = 0

    for contract in contracts:
        customer = customer_map.get(contract['customer_id'], {})
        legacy_id = customer.get('legacy_id')
        customer_name = customer.get('name', '')

        if not legacy_id:
            continue

        excel_info = all_cycles.get(legacy_id)
        if not excel_info:
            no_data += 1
            continue

        db_cycle = contract['payment_cycle']
        excel_cycle = excel_info['cycle']

        if db_cycle == excel_cycle:
            correct += 1
        else:
            updates.append({
                'contract_id': contract['id'],
                'legacy_id': legacy_id,
                'customer_name': customer_name,
                'db_cycle': db_cycle,
                'excel_cycle': excel_cycle,
                'excel_raw': excel_info['raw'],
                'source': excel_info['source']
            })

    print(f"\n=== 比對結果 ===")
    print(f"正確: {correct} 筆")
    print(f"需要更新: {len(updates)} 筆")
    print(f"無 Excel 資料: {no_data} 筆")

    if not updates:
        print("\n無需更新")
        return

    # 顯示需要更新的
    print(f"\n需要更新的合約:")
    for u in updates:
        print(f"  {u['legacy_id']:10} {u['customer_name']:12} {u['db_cycle']:12} → {u['excel_cycle']:12} (Excel: {u['excel_raw']}, {u['source']})")

    # 生成 SQL
    sql_lines = []
    sql_lines.append("-- ============================================================================")
    sql_lines.append("-- Hour Jungle CRM - 修正 payment_cycle")
    sql_lines.append(f"-- 生成時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    sql_lines.append(f"-- 共 {len(updates)} 筆需要更新")
    sql_lines.append("-- 來源: 客戶資料表 + 繳費表 交叉驗證")
    sql_lines.append("-- ============================================================================")
    sql_lines.append("")

    for u in updates:
        sql = f"UPDATE contracts SET payment_cycle = '{u['excel_cycle']}' WHERE id = {u['contract_id']};"
        sql_lines.append(f"-- {u['legacy_id']} {u['customer_name']}: {u['db_cycle']} → {u['excel_cycle']} (Excel: {u['excel_raw']})")
        sql_lines.append(sql)

    sql_lines.append("")
    sql_lines.append("-- 驗證")
    sql_lines.append("SELECT payment_cycle, COUNT(*) as count FROM contracts WHERE status = 'active' GROUP BY payment_cycle ORDER BY payment_cycle;")

    # 寫入檔案
    output_file = OUTPUT_DIR / "fix_payment_cycle.sql"
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(sql_lines))

    print(f"\n=== SQL 檔案已生成 ===")
    print(f"檔案: {output_file}")

    print(f"\n=== 執行指令 ===")
    print(f"gcloud compute scp {output_file} instance-20251204-075237:/tmp/ --zone=us-west1-a")
    print(f"gcloud compute ssh instance-20251204-075237 --zone=us-west1-a --command='docker exec -i hj-postgres psql -U hjadmin -d hourjungle < /tmp/fix_payment_cycle.sql'")


if __name__ == "__main__":
    main()
