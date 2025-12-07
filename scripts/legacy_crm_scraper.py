#!/usr/bin/env python3
"""
Hour Jungle CRM - 舊系統 LINE UID 爬蟲
從舊 CRM 系統爬取客戶的 LINE User ID

舊 CRM 資訊:
- 網址: https://www.hourjungle.work/backend/taichung_customers
- 登入: admin / admin
- 目標: 爬取 LINE UID 對應關係
"""

import asyncio
import csv
import os
from datetime import datetime
from typing import List, Dict, Optional

# Playwright 需要安裝: pip install playwright && playwright install

OUTPUT_DIR = "/Users/daihaoting_1/Desktop/code/hourjungle-crm/scripts/output"

# 舊 CRM 設定
OLD_CRM_BASE_URL = "https://www.hourjungle.work/backend"
OLD_CRM_LOGIN_URL = f"{OLD_CRM_BASE_URL}/login"  # 假設登入頁面
OLD_CRM_CUSTOMERS_URL = f"{OLD_CRM_BASE_URL}/taichung_customers"
OLD_CRM_USERNAME = "admin"
OLD_CRM_PASSWORD = "123qwe"


async def scrape_line_uids():
    """
    使用 Playwright 爬取舊 CRM 系統的 LINE UID
    """
    from playwright.async_api import async_playwright

    print("Hour Jungle CRM - 舊系統 LINE UID 爬蟲")
    print("=" * 60)

    results: List[Dict] = []

    async with async_playwright() as p:
        # 使用 Chromium 瀏覽器
        browser = await p.chromium.launch(
            headless=False,  # 設為 True 可在背景執行
            slow_mo=500  # 放慢操作以便觀察
        )

        context = await browser.new_context(
            viewport={'width': 1280, 'height': 800},
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        )

        page = await context.new_page()

        try:
            # ============================================================
            # 步驟 1: 登入
            # ============================================================
            print("\n[1] 正在登入舊 CRM 系統...")

            # 直接訪問登入頁面
            await page.goto(OLD_CRM_LOGIN_URL, timeout=30000)
            await page.wait_for_load_state('domcontentloaded')

            current_url = page.url
            print(f"    當前頁面: {current_url}")

            # 檢查是否需要登入
            # 這部分需要根據實際頁面結構調整
            login_form = await page.query_selector('form[action*="login"], input[name="password"]')

            if login_form or 'login' in current_url.lower():
                print("    需要登入，正在輸入帳密...")

                # 等待表單載入完成
                await page.wait_for_selector('#login-username', state='visible', timeout=10000)

                # 填寫帳號 (使用精確 selector)
                await page.fill('#login-username', OLD_CRM_USERNAME)
                print(f"    已填寫帳號: {OLD_CRM_USERNAME}")

                # 填寫密碼
                await page.fill('#login-password', OLD_CRM_PASSWORD)
                print("    已填寫密碼")

                # 截圖確認填寫狀態
                await page.screenshot(path=os.path.join(OUTPUT_DIR, "before_login.png"))
                print("    已截圖 (登入前)")

                # 點擊登入按鈕並等待導航
                async with page.expect_navigation(timeout=30000):
                    await page.click('button[type="submit"]')
                    print("    已點擊登入按鈕")

                # 等待頁面載入完成
                await page.wait_for_load_state('networkidle')
                await asyncio.sleep(1)

                print(f"    登入後頁面: {page.url}")

                # 檢查是否登入成功
                if 'login' in page.url.lower():
                    # 可能登入失敗，截圖查看錯誤訊息
                    error_msg = await page.query_selector('.text-danger, .alert-danger, .error')
                    if error_msg:
                        error_text = await error_msg.inner_text()
                        print(f"    登入失敗: {error_text}")
                    else:
                        print("    警告: 仍在登入頁面，可能帳密錯誤")
            else:
                print("    無需登入，直接進入系統")

            # ============================================================
            # 步驟 2: 進入客戶列表
            # ============================================================
            print("\n[2] 正在載入客戶列表...")

            if OLD_CRM_CUSTOMERS_URL not in page.url:
                await page.goto(OLD_CRM_CUSTOMERS_URL, timeout=30000)
                await page.wait_for_load_state('networkidle')

            # 截圖以便 debug
            screenshot_path = os.path.join(OUTPUT_DIR, "scraper_screenshot.png")
            await page.screenshot(path=screenshot_path)
            print(f"    已截圖: {screenshot_path}")

            # 取得頁面 HTML 以便分析結構
            html_content = await page.content()
            html_path = os.path.join(OUTPUT_DIR, "scraper_page.html")
            with open(html_path, 'w', encoding='utf-8') as f:
                f.write(html_content)
            print(f"    已儲存 HTML: {html_path}")

            # ============================================================
            # 步驟 3: 收集所有客戶連結
            # ============================================================
            print("\n[3] 收集客戶連結...")

            # 修改分頁設定，顯示更多筆數
            page_size_select = await page.query_selector('select[name*="length"]')
            if page_size_select:
                # 選擇顯示 100 筆
                await page_size_select.select_option('100')
                await asyncio.sleep(2)
                print("    已設定每頁顯示 100 筆")

            # 收集所有「查看」連結（處理分頁）
            customer_ids = []
            page_num = 1

            while True:
                # 收集當前頁面的連結
                view_links = await page.query_selector_all('a.read.btn-primary')
                page_count = 0

                for link in view_links:
                    href = await link.get_attribute('href')
                    if href and '/taichung_customers/' in href:
                        customer_id = href.split('/')[-1]
                        if customer_id.isdigit() and customer_id not in customer_ids:
                            customer_ids.append(customer_id)
                            page_count += 1

                print(f"    第 {page_num} 頁: 新增 {page_count} 個客戶")

                # 檢查是否有下一頁
                next_btn = await page.query_selector('#data-table_next:not(.disabled)')
                if not next_btn:
                    break

                # 點擊下一頁
                await next_btn.click()
                await asyncio.sleep(1)  # 等待 AJAX 載入
                await page.wait_for_load_state('networkidle')
                page_num += 1

            print(f"    總共找到 {len(customer_ids)} 個客戶")

            # ============================================================
            # 步驟 4: 訪問每個客戶詳細頁面
            # ============================================================
            print("\n[4] 爬取客戶詳細資料...")

            import re
            for i, cid in enumerate(customer_ids):  # 爬取全部客戶
                try:
                    detail_url = f"https://www.hourjungle.work/backend/taichung_customers/{cid}"
                    await page.goto(detail_url, timeout=15000)
                    await page.wait_for_load_state('domcontentloaded')

                    # 取得頁面內容
                    content = await page.content()

                    # 提取客戶資訊
                    customer_data = {
                        'customer_id': cid,
                        'name': '',
                        'company_name': '',
                        'line_user_id': None
                    }

                    # 找客戶姓名
                    name_elem = await page.query_selector('input[name="name"], .customer-name, td:has-text("姓名") + td')
                    if name_elem:
                        customer_data['name'] = await name_elem.inner_text() if await name_elem.inner_text() else await name_elem.get_attribute('value')

                    # 找 LINE UID (U 開頭 33 字符)
                    line_uid_match = re.search(r'U[a-f0-9]{32}', content)
                    if line_uid_match:
                        customer_data['line_user_id'] = line_uid_match.group(0)
                        results.append(customer_data)
                        print(f"    [{i+1}/{len(customer_ids)}] 客戶 {cid}: LINE UID = {line_uid_match.group(0)[:20]}...")
                    else:
                        # 嘗試其他模式 (可能是 line_user_id 欄位)
                        line_input = await page.query_selector('input[name*="line"], input[id*="line"]')
                        if line_input:
                            line_val = await line_input.get_attribute('value')
                            if line_val and line_val.startswith('U'):
                                customer_data['line_user_id'] = line_val
                                results.append(customer_data)
                                print(f"    [{i+1}/{len(customer_ids)}] 客戶 {cid}: LINE UID = {line_val[:20]}...")
                            else:
                                print(f"    [{i+1}/{len(customer_ids)}] 客戶 {cid}: 無 LINE UID")
                        else:
                            print(f"    [{i+1}/{len(customer_ids)}] 客戶 {cid}: 無 LINE UID")

                    # 第一個詳細頁面截圖
                    if i == 0:
                        await page.screenshot(path=os.path.join(OUTPUT_DIR, "customer_detail.png"))
                        detail_html = await page.content()
                        with open(os.path.join(OUTPUT_DIR, "customer_detail.html"), 'w', encoding='utf-8') as f:
                            f.write(detail_html)
                        print("    已儲存第一個客戶詳細頁面截圖")

                except Exception as e:
                    print(f"    [{i+1}/{len(customer_ids)}] 客戶 {cid}: 錯誤 - {e}")
                    continue

        except Exception as e:
            print(f"\n錯誤: {e}")

            # 發生錯誤時截圖
            error_screenshot = os.path.join(OUTPUT_DIR, "scraper_error.png")
            await page.screenshot(path=error_screenshot)
            print(f"錯誤截圖: {error_screenshot}")

        finally:
            await browser.close()

    # ============================================================
    # 輸出結果
    # ============================================================
    print("\n" + "=" * 60)
    print("爬取結果")
    print("=" * 60)
    print(f"共找到 {len(results)} 筆 LINE UID")

    if results:
        output_file = os.path.join(OUTPUT_DIR, "line_uids.csv")
        with open(output_file, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=results[0].keys())
            writer.writeheader()
            writer.writerows(results)
        print(f"已輸出: {output_file}")
    else:
        print("未找到任何 LINE UID")
        print("\n請檢查:")
        print("  1. scraper_screenshot.png - 查看頁面截圖")
        print("  2. scraper_page.html - 分析頁面結構")
        print("  3. 根據實際結構調整爬蟲程式碼")

    return results


def main():
    """主程式"""
    # 確保輸出目錄存在
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # 執行爬蟲
    results = asyncio.run(scrape_line_uids())

    print("\n爬蟲完成！")
    return results


if __name__ == "__main__":
    main()
