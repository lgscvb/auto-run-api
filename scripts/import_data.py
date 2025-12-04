#!/usr/bin/env python3
"""
Hour Jungle CRM - Data Import Script
匯入已清洗的 CSV 資料到 PostgreSQL

使用方式:
    python import_data.py --data-dir /path/to/csv/files

CSV 檔案格式:
    - customers_transformed_*.csv
    - contracts_transformed_*.csv
    - payments_ALL_*.csv
    - commissions_transformed_*.csv
"""

import os
import sys
import csv
import argparse
import logging
from datetime import datetime
from decimal import Decimal
from typing import Optional, Dict, Any, List

import psycopg2
from psycopg2.extras import execute_values, RealDictCursor

# 設定日誌
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============================================================================
# 資料庫連接
# ============================================================================

def get_connection():
    """取得資料庫連接"""
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "hourjungle"),
        user=os.getenv("POSTGRES_USER", "hjadmin"),
        password=os.getenv("POSTGRES_PASSWORD", ""),
        cursor_factory=RealDictCursor
    )


# ============================================================================
# 資料轉換
# ============================================================================

def clean_value(value: str) -> Optional[str]:
    """清理欄位值"""
    if value is None:
        return None
    value = str(value).strip()
    if value.lower() in ('', 'nan', 'none', 'null', 'n/a', '-'):
        return None
    return value


def parse_date(value: str) -> Optional[str]:
    """解析日期"""
    value = clean_value(value)
    if not value:
        return None

    # 嘗試多種日期格式
    formats = [
        '%Y-%m-%d',
        '%Y/%m/%d',
        '%d/%m/%Y',
        '%m/%d/%Y',
        '%Y-%m-%d %H:%M:%S'
    ]

    for fmt in formats:
        try:
            return datetime.strptime(value, fmt).strftime('%Y-%m-%d')
        except ValueError:
            continue

    logger.warning(f"無法解析日期: {value}")
    return None


def parse_decimal(value: str) -> Optional[Decimal]:
    """解析金額"""
    value = clean_value(value)
    if not value:
        return None

    try:
        # 移除千分位逗號
        value = value.replace(',', '').replace('$', '').replace(' ', '')
        return Decimal(value)
    except:
        logger.warning(f"無法解析金額: {value}")
        return None


def parse_int(value: str) -> Optional[int]:
    """解析整數"""
    value = clean_value(value)
    if not value:
        return None

    try:
        return int(float(value))
    except:
        return None


# ============================================================================
# 資料匯入
# ============================================================================

def import_customers(conn, csv_path: str) -> int:
    """匯入客戶資料"""
    logger.info(f"匯入客戶資料: {csv_path}")

    imported = 0
    errors = []

    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)

        for row in reader:
            try:
                # 對應欄位
                data = {
                    'legacy_id': clean_value(row.get('legacy_id') or row.get('客戶編號')),
                    'branch_id': parse_int(row.get('branch_id')) or 1,
                    'customer_type': clean_value(row.get('customer_type')) or 'individual',
                    'name': clean_value(row.get('name') or row.get('姓名')),
                    'company_name': clean_value(row.get('company_name') or row.get('公司名稱')),
                    'company_tax_id': clean_value(row.get('company_tax_id') or row.get('統一編號')),
                    'phone': clean_value(row.get('phone') or row.get('電話')),
                    'email': clean_value(row.get('email') or row.get('Email')),
                    'address': clean_value(row.get('address') or row.get('地址')),
                    'source_channel': clean_value(row.get('source_channel') or row.get('來源')) or 'others',
                    'status': clean_value(row.get('status')) or 'active',
                    'notes': clean_value(row.get('notes') or row.get('備註'))
                }

                if not data['name']:
                    logger.warning(f"跳過無姓名的記錄: {row}")
                    continue

                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO customers (
                            legacy_id, branch_id, customer_type, name, company_name,
                            company_tax_id, phone, email, address, source_channel,
                            status, notes
                        ) VALUES (
                            %(legacy_id)s, %(branch_id)s, %(customer_type)s, %(name)s, %(company_name)s,
                            %(company_tax_id)s, %(phone)s, %(email)s, %(address)s, %(source_channel)s,
                            %(status)s, %(notes)s
                        )
                        ON CONFLICT (legacy_id) DO UPDATE SET
                            name = EXCLUDED.name,
                            phone = EXCLUDED.phone,
                            email = EXCLUDED.email,
                            updated_at = NOW()
                        RETURNING id
                    """, data)

                    imported += 1

            except Exception as e:
                errors.append(f"Row {imported + 1}: {e}")
                logger.error(f"匯入客戶失敗: {e}")

    conn.commit()
    logger.info(f"客戶匯入完成: {imported} 筆, {len(errors)} 錯誤")
    return imported


def import_contracts(conn, csv_path: str) -> int:
    """匯入合約資料"""
    logger.info(f"匯入合約資料: {csv_path}")

    imported = 0

    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)

        for row in reader:
            try:
                # 先找到客戶 ID
                legacy_id = clean_value(row.get('legacy_id') or row.get('customer_legacy_id') or row.get('客戶編號'))

                with conn.cursor() as cur:
                    cur.execute("SELECT id FROM customers WHERE legacy_id = %s", (legacy_id,))
                    result = cur.fetchone()

                    if not result:
                        logger.warning(f"找不到客戶 {legacy_id}, 跳過合約")
                        continue

                    customer_id = result['id']

                    data = {
                        'customer_id': customer_id,
                        'branch_id': parse_int(row.get('branch_id')) or 1,
                        'contract_type': clean_value(row.get('contract_type')) or 'virtual_office',
                        'plan_name': clean_value(row.get('plan_name') or row.get('方案')),
                        'start_date': parse_date(row.get('start_date') or row.get('開始日期')),
                        'end_date': parse_date(row.get('end_date') or row.get('結束日期')),
                        'monthly_rent': parse_decimal(row.get('monthly_rent') or row.get('月租金')) or 0,
                        'deposit': parse_decimal(row.get('deposit') or row.get('押金')) or 0,
                        'payment_cycle': clean_value(row.get('payment_cycle')) or 'monthly',
                        'payment_day': parse_int(row.get('payment_day')) or 5,
                        'status': clean_value(row.get('status')) or 'active',
                        'broker_name': clean_value(row.get('broker_name') or row.get('介紹人')),
                        'notes': clean_value(row.get('notes') or row.get('備註'))
                    }

                    if not data['start_date'] or not data['end_date']:
                        logger.warning(f"跳過缺少日期的合約: {row}")
                        continue

                    cur.execute("""
                        INSERT INTO contracts (
                            customer_id, branch_id, contract_type, plan_name,
                            start_date, end_date, monthly_rent, deposit,
                            payment_cycle, payment_day, status, broker_name, notes
                        ) VALUES (
                            %(customer_id)s, %(branch_id)s, %(contract_type)s, %(plan_name)s,
                            %(start_date)s, %(end_date)s, %(monthly_rent)s, %(deposit)s,
                            %(payment_cycle)s, %(payment_day)s, %(status)s, %(broker_name)s, %(notes)s
                        )
                        RETURNING id
                    """, data)

                    imported += 1

            except Exception as e:
                logger.error(f"匯入合約失敗: {e}")

    conn.commit()
    logger.info(f"合約匯入完成: {imported} 筆")
    return imported


def import_payments(conn, csv_path: str) -> int:
    """匯入繳費記錄"""
    logger.info(f"匯入繳費記錄: {csv_path}")

    imported = 0

    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)

        for row in reader:
            try:
                # 找客戶 ID
                legacy_id = clean_value(row.get('legacy_id') or row.get('customer_legacy_id') or row.get('客戶編號'))

                with conn.cursor() as cur:
                    cur.execute("SELECT id FROM customers WHERE legacy_id = %s", (legacy_id,))
                    result = cur.fetchone()

                    if not result:
                        logger.warning(f"找不到客戶 {legacy_id}, 跳過付款")
                        continue

                    customer_id = result['id']

                    # 找合約 ID (取最新的)
                    cur.execute("""
                        SELECT id, branch_id FROM contracts
                        WHERE customer_id = %s
                        ORDER BY start_date DESC
                        LIMIT 1
                    """, (customer_id,))
                    contract_result = cur.fetchone()

                    if not contract_result:
                        logger.warning(f"客戶 {legacy_id} 沒有合約, 跳過付款")
                        continue

                    contract_id = contract_result['id']
                    branch_id = contract_result['branch_id']

                    # 解析付款狀態
                    status_raw = clean_value(row.get('payment_status') or row.get('狀態') or row.get('status'))
                    if status_raw:
                        status_map = {
                            '已繳': 'paid',
                            '未繳': 'pending',
                            '逾期': 'overdue',
                            'paid': 'paid',
                            'pending': 'pending',
                            'overdue': 'overdue'
                        }
                        payment_status = status_map.get(status_raw, 'pending')
                    else:
                        payment_status = 'pending'

                    data = {
                        'contract_id': contract_id,
                        'customer_id': customer_id,
                        'branch_id': branch_id,
                        'payment_type': 'rent',
                        'payment_period': clean_value(row.get('payment_period') or row.get('期間') or row.get('月份')),
                        'amount': parse_decimal(row.get('amount') or row.get('金額')) or 0,
                        'payment_method': clean_value(row.get('payment_method') or row.get('付款方式')),
                        'payment_status': payment_status,
                        'due_date': parse_date(row.get('due_date') or row.get('到期日')),
                        'paid_at': parse_date(row.get('paid_at') or row.get('付款日期')) if payment_status == 'paid' else None,
                        'notes': clean_value(row.get('notes') or row.get('備註'))
                    }

                    if not data['due_date']:
                        logger.warning(f"跳過缺少到期日的付款: {row}")
                        continue

                    cur.execute("""
                        INSERT INTO payments (
                            contract_id, customer_id, branch_id, payment_type,
                            payment_period, amount, payment_method, payment_status,
                            due_date, paid_at, notes
                        ) VALUES (
                            %(contract_id)s, %(customer_id)s, %(branch_id)s, %(payment_type)s,
                            %(payment_period)s, %(amount)s, %(payment_method)s, %(payment_status)s,
                            %(due_date)s, %(paid_at)s, %(notes)s
                        )
                        RETURNING id
                    """, data)

                    imported += 1

            except Exception as e:
                logger.error(f"匯入付款失敗: {e}")

    conn.commit()
    logger.info(f"付款匯入完成: {imported} 筆")
    return imported


def import_commissions(conn, csv_path: str) -> int:
    """匯入佣金記錄"""
    logger.info(f"匯入佣金記錄: {csv_path}")

    imported = 0

    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)

        for row in reader:
            try:
                # 找客戶 ID
                legacy_id = clean_value(row.get('legacy_id') or row.get('customer_legacy_id') or row.get('客戶編號'))

                with conn.cursor() as cur:
                    cur.execute("SELECT id FROM customers WHERE legacy_id = %s", (legacy_id,))
                    result = cur.fetchone()

                    if not result:
                        logger.warning(f"找不到客戶 {legacy_id}, 跳過佣金")
                        continue

                    customer_id = result['id']

                    # 找合約 ID
                    cur.execute("""
                        SELECT id FROM contracts
                        WHERE customer_id = %s
                        ORDER BY start_date DESC
                        LIMIT 1
                    """, (customer_id,))
                    contract_result = cur.fetchone()

                    if not contract_result:
                        logger.warning(f"客戶 {legacy_id} 沒有合約, 跳過佣金")
                        continue

                    contract_id = contract_result['id']

                    # 解析狀態
                    status_raw = clean_value(row.get('status') or row.get('狀態'))
                    if status_raw:
                        status_map = {
                            '已付': 'paid',
                            '待付': 'pending',
                            '可付': 'eligible',
                            'paid': 'paid',
                            'pending': 'pending',
                            'eligible': 'eligible'
                        }
                        status = status_map.get(status_raw, 'pending')
                    else:
                        status = 'pending'

                    data = {
                        'customer_id': customer_id,
                        'contract_id': contract_id,
                        'amount': parse_decimal(row.get('amount') or row.get('佣金金額')) or 0,
                        'based_on_rent': parse_decimal(row.get('based_on_rent') or row.get('月租金')),
                        'contract_start': parse_date(row.get('contract_start') or row.get('合約開始日')),
                        'eligible_date': parse_date(row.get('eligible_date') or row.get('可付款日')),
                        'status': status,
                        'notes': clean_value(row.get('notes') or row.get('備註'))
                    }

                    cur.execute("""
                        INSERT INTO commissions (
                            customer_id, contract_id, amount, based_on_rent,
                            contract_start, eligible_date, status, notes
                        ) VALUES (
                            %(customer_id)s, %(contract_id)s, %(amount)s, %(based_on_rent)s,
                            %(contract_start)s, %(eligible_date)s, %(status)s, %(notes)s
                        )
                        RETURNING id
                    """, data)

                    imported += 1

            except Exception as e:
                logger.error(f"匯入佣金失敗: {e}")

    conn.commit()
    logger.info(f"佣金匯入完成: {imported} 筆")
    return imported


# ============================================================================
# 主程式
# ============================================================================

def find_csv_file(directory: str, pattern: str) -> Optional[str]:
    """在目錄中找符合 pattern 的 CSV 檔案"""
    import glob
    files = glob.glob(os.path.join(directory, f"*{pattern}*.csv"))
    if files:
        # 回傳最新的檔案
        return max(files, key=os.path.getmtime)
    return None


def main():
    parser = argparse.ArgumentParser(description='匯入 Hour Jungle CRM 資料')
    parser.add_argument('--data-dir', required=True, help='CSV 檔案目錄')
    parser.add_argument('--skip-customers', action='store_true', help='跳過客戶匯入')
    parser.add_argument('--skip-contracts', action='store_true', help='跳過合約匯入')
    parser.add_argument('--skip-payments', action='store_true', help='跳過付款匯入')
    parser.add_argument('--skip-commissions', action='store_true', help='跳過佣金匯入')

    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Hour Jungle CRM 資料匯入")
    logger.info("=" * 60)

    try:
        conn = get_connection()
        logger.info("資料庫連接成功")
    except Exception as e:
        logger.error(f"資料庫連接失敗: {e}")
        sys.exit(1)

    results = {}

    # 匯入順序很重要: 客戶 -> 合約 -> 付款 -> 佣金
    try:
        # 1. 客戶
        if not args.skip_customers:
            csv_file = find_csv_file(args.data_dir, 'customers_transformed')
            if csv_file:
                results['customers'] = import_customers(conn, csv_file)
            else:
                logger.warning("找不到客戶 CSV 檔案")

        # 2. 合約
        if not args.skip_contracts:
            csv_file = find_csv_file(args.data_dir, 'contracts_transformed')
            if csv_file:
                results['contracts'] = import_contracts(conn, csv_file)
            else:
                logger.warning("找不到合約 CSV 檔案")

        # 3. 付款
        if not args.skip_payments:
            csv_file = find_csv_file(args.data_dir, 'payments_ALL')
            if csv_file:
                results['payments'] = import_payments(conn, csv_file)
            else:
                logger.warning("找不到付款 CSV 檔案")

        # 4. 佣金
        if not args.skip_commissions:
            csv_file = find_csv_file(args.data_dir, 'commissions_transformed')
            if csv_file:
                results['commissions'] = import_commissions(conn, csv_file)
            else:
                logger.warning("找不到佣金 CSV 檔案")

    except Exception as e:
        logger.error(f"匯入過程發生錯誤: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

    # 輸出結果
    logger.info("=" * 60)
    logger.info("匯入完成!")
    logger.info("=" * 60)
    for table, count in results.items():
        logger.info(f"  {table}: {count} 筆")

    return results


if __name__ == '__main__':
    main()
