#!/usr/bin/env python3
"""
建立爬蟲 customer_id → 資料庫 legacy_id 對應表
並將匹配成功的 LINE User ID 更新到資料庫
"""

import pandas as pd
import subprocess
import json
from datetime import datetime
from pathlib import Path
import re

SCRIPT_DIR = Path(__file__).parent
OUTPUT_DIR = SCRIPT_DIR / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

API_BASE = "https://auto.yourspce.org/api/db"


def fetch_customers():
    """從資料庫取得客戶資料"""
    result = subprocess.run([
        'curl', '-s',
        f'{API_BASE}/customers?select=id,legacy_id,name,company_name,company_tax_id,phone,line_user_id&limit=500'
    ], capture_output=True, text=True)
    return json.loads(result.stdout)


def normalize_phone(phone):
    """正規化電話號碼"""
    if pd.isna(phone) or not phone:
        return None
    phone_str = re.sub(r'\D', '', str(phone))
    if phone_str.startswith('886'):
        phone_str = '0' + phone_str[3:]
    if phone_str and not phone_str.startswith('0'):
        phone_str = '0' + phone_str
    return phone_str if len(phone_str) >= 9 else None


def normalize_company_name(name):
    """正規化公司名稱（用於模糊匹配）"""
    if pd.isna(name) or not name:
        return None
    name = str(name).strip()
    suffixes = ['有限公司', '股份有限公司', '商行', '企業社', '工作室', '行銷', '事業', '實業社']
    for suffix in suffixes:
        name = name.replace(suffix, '')
    return name.strip()


def normalize_name(name):
    """正規化姓名"""
    if pd.isna(name) or not name:
        return ""
    name = str(name).strip()
    name = re.sub(r'\([^)]*\)', '', name)
    return name.strip()


def main():
    print("=" * 60)
    print("建立 ID 對應表（爬蟲 → 資料庫）")
    print("=" * 60)

    # 1. 載入已匹配的資料
    final_file = OUTPUT_DIR / "line_uid_final.csv"
    if final_file.exists():
        df_matched = pd.read_csv(final_file)
        print(f"\n已匹配記錄 (line_uid_final.csv): {len(df_matched)} 筆")
    else:
        df_matched = pd.DataFrame()
        print("\n無已匹配記錄")

    # 2. 載入爬蟲資料
    crawler_file = OUTPUT_DIR / "line_uids.csv"
    df_crawler = pd.read_csv(crawler_file)
    print(f"爬蟲資料: {len(df_crawler)} 筆")

    # 3. 取得資料庫客戶
    customers = fetch_customers()
    print(f"資料庫客戶: {len(customers)} 筆")

    # 建立索引
    customer_by_legacy = {c['legacy_id']: c for c in customers if c.get('legacy_id')}
    customer_by_name = {}
    customer_by_company = {}

    for c in customers:
        if c.get('name'):
            norm = normalize_name(c['name'])
            customer_by_name.setdefault(norm, []).append(c)
        if c.get('company_name'):
            customer_by_company.setdefault(c['company_name'], []).append(c)
            norm = normalize_company_name(c['company_name'])
            if norm:
                customer_by_company.setdefault(norm, []).append(c)

    # 4. 使用已匹配資料
    all_matches = []
    if not df_matched.empty:
        for _, row in df_matched.iterrows():
            all_matches.append({
                'crm_id': row['crm_id'],
                'legacy_id': row['legacy_id'],
                'scraped_name': row['scraped_name'],
                'new_name': row['new_name'],
                'new_company': row.get('new_company', ''),
                'line_user_id': row['line_user_id'],
                'match_method': row['match_method']
            })

    matched_crm_ids = set(m['crm_id'] for m in all_matches)

    # 5. 嘗試匹配剩餘的
    unmatched_crawler = df_crawler[~df_crawler['customer_id'].isin(matched_crm_ids)]
    print(f"待匹配: {len(unmatched_crawler)} 筆")

    new_matches = []
    still_unmatched = []

    for _, row in unmatched_crawler.iterrows():
        crm_id = row['customer_id']
        scraped_name = str(row['name']).strip() if pd.notna(row['name']) else ''
        line_uid = row['line_user_id']

        matched = None
        match_method = None

        # 方法 1: 姓名精確匹配
        norm_name = normalize_name(scraped_name)
        if norm_name and norm_name in customer_by_name:
            candidates = customer_by_name[norm_name]
            if len(candidates) == 1:
                matched = candidates[0]
                match_method = '姓名精確'

        # 方法 2: 公司名精確匹配
        if not matched and scraped_name in customer_by_company:
            candidates = customer_by_company[scraped_name]
            if len(candidates) == 1:
                matched = candidates[0]
                match_method = '公司名精確'

        # 方法 3: 公司名正規化匹配
        if not matched:
            norm_company = normalize_company_name(scraped_name)
            if norm_company and norm_company in customer_by_company:
                candidates = customer_by_company[norm_company]
                if len(candidates) == 1:
                    matched = candidates[0]
                    match_method = '公司名模糊'

        if matched:
            new_matches.append({
                'crm_id': crm_id,
                'legacy_id': matched['legacy_id'],
                'scraped_name': scraped_name,
                'new_name': matched['name'],
                'new_company': matched.get('company_name', ''),
                'line_user_id': line_uid,
                'match_method': match_method
            })
        else:
            still_unmatched.append({
                'crm_id': crm_id,
                'scraped_name': scraped_name,
                'line_user_id': line_uid
            })

    print(f"\n新匹配成功: {len(new_matches)} 筆")
    print(f"仍無法匹配: {len(still_unmatched)} 筆")

    all_matches.extend(new_matches)
    print(f"總匹配數: {len(all_matches)} 筆")

    # 6. 輸出對應表
    df_all = pd.DataFrame(all_matches)
    mapping_file = OUTPUT_DIR / "id_mapping.csv"
    df_all.to_csv(mapping_file, index=False, encoding='utf-8-sig')
    print(f"\n對應表: {mapping_file}")

    if still_unmatched:
        df_unmatched = pd.DataFrame(still_unmatched)
        unmatched_file = OUTPUT_DIR / "unmatched.csv"
        df_unmatched.to_csv(unmatched_file, index=False, encoding='utf-8-sig')
        print(f"無法匹配: {unmatched_file}")

    # 7. 檢查資料庫中已有 LINE UID 的情況
    already_has_uid = [c for c in customers if c.get('line_user_id')]
    print(f"\n資料庫已有 LINE UID: {len(already_has_uid)} 筆")

    # 8. 生成更新 SQL
    updates = []
    for match in all_matches:
        legacy_id = match['legacy_id']
        line_uid = match['line_user_id']
        customer = customer_by_legacy.get(legacy_id)
        if customer and not customer.get('line_user_id'):
            updates.append({
                'customer_id': customer['id'],
                'legacy_id': legacy_id,
                'name': customer['name'],
                'line_user_id': line_uid
            })

    print(f"需要更新 LINE UID: {len(updates)} 筆")

    if updates:
        sql_lines = []
        sql_lines.append("-- ============================================================================")
        sql_lines.append("-- Hour Jungle CRM - 更新 LINE User ID")
        sql_lines.append(f"-- 生成時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        sql_lines.append(f"-- 共 {len(updates)} 筆需要更新")
        sql_lines.append("-- ============================================================================")
        sql_lines.append("")

        for u in updates:
            sql = f"UPDATE customers SET line_user_id = '{u['line_user_id']}' WHERE id = {u['customer_id']};"
            sql_lines.append(f"-- {u['legacy_id']} {u['name']}")
            sql_lines.append(sql)

        sql_lines.append("")
        sql_lines.append("-- 驗證")
        sql_lines.append("SELECT COUNT(*) as total, COUNT(line_user_id) as has_line_uid FROM customers WHERE status = 'active';")

        sql_file = OUTPUT_DIR / "update_line_uid.sql"
        with open(sql_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(sql_lines))

        print(f"\nSQL 檔案: {sql_file}")
        print(f"\n=== 執行指令 ===")
        print(f"gcloud compute scp {sql_file} instance-20251204-075237:/tmp/ --zone=us-west1-a")
        print(f"gcloud compute ssh instance-20251204-075237 --zone=us-west1-a --command='docker exec -i hj-postgres psql -U hjadmin -d hourjungle < /tmp/update_line_uid.sql'")

    # 9. 統計摘要
    print("\n" + "=" * 60)
    print("統計摘要")
    print("=" * 60)
    print(f"爬蟲資料總數: {len(df_crawler)}")
    print(f"成功匹配: {len(all_matches)} ({len(all_matches)/len(df_crawler)*100:.1f}%)")
    print(f"無法匹配: {len(still_unmatched)} ({len(still_unmatched)/len(df_crawler)*100:.1f}%)")
    print(f"資料庫已有 UID: {len(already_has_uid)}")
    print(f"本次需更新: {len(updates)}")

    # 匹配方法統計
    if all_matches:
        print("\n匹配方法分布:")
        methods = {}
        for m in all_matches:
            method = m['match_method']
            methods[method] = methods.get(method, 0) + 1
        for method, count in sorted(methods.items(), key=lambda x: -x[1]):
            print(f"  {method}: {count} 筆")


if __name__ == "__main__":
    main()
