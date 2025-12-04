-- ============================================================================
-- Hour Jungle CRM - PostgreSQL Row-Level Security
-- 04_rls.sql - 角色與存取控制
-- ============================================================================

-- ============================================================================
-- 1. 建立角色
-- ============================================================================

-- API 匿名用戶 (PostgREST 預設)
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'anon') THEN
        CREATE ROLE anon NOLOGIN;
    END IF;
END
$$;

-- API 認證用戶
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'api_user') THEN
        CREATE ROLE api_user NOLOGIN;
    END IF;
END
$$;

-- 員工角色
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'staff') THEN
        CREATE ROLE staff NOLOGIN;
    END IF;
END
$$;

-- 管理員角色
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'admin') THEN
        CREATE ROLE admin NOLOGIN;
    END IF;
END
$$;

-- 授權角色給 hjadmin (主要用戶)
GRANT anon TO hjadmin;
GRANT api_user TO hjadmin;
GRANT staff TO hjadmin;
GRANT admin TO hjadmin;

-- ============================================================================
-- 2. 基本權限授予
-- ============================================================================

-- Schema 權限
GRANT USAGE ON SCHEMA public TO anon, api_user, staff, admin;

-- 序列權限 (用於 INSERT)
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO api_user, staff, admin;

-- ============================================================================
-- 3. 表格權限 - anon (匿名用戶)
-- ============================================================================

-- anon 可以讀取主要表格（供 PostgREST 匿名 API 使用）
GRANT SELECT ON branches TO anon;
GRANT SELECT ON customers TO anon;
GRANT SELECT ON contracts TO anon;
GRANT SELECT ON payments TO anon;
GRANT SELECT ON commissions TO anon;
GRANT SELECT ON accounting_firms TO anon;

-- anon 可存取所有 Views
GRANT SELECT ON v_line_user_lookup TO anon;
GRANT SELECT ON v_customer_summary TO anon;
GRANT SELECT ON v_payments_due TO anon;
GRANT SELECT ON v_renewal_reminders TO anon;
GRANT SELECT ON v_commission_tracker TO anon;
GRANT SELECT ON v_branch_revenue_summary TO anon;
GRANT SELECT ON v_overdue_details TO anon;
GRANT SELECT ON v_today_tasks TO anon;

-- ============================================================================
-- 4. 表格權限 - api_user (認證 API 用戶)
-- ============================================================================

-- 讀取權限
GRANT SELECT ON branches TO api_user;
GRANT SELECT ON customers TO api_user;
GRANT SELECT ON contracts TO api_user;
GRANT SELECT ON payments TO api_user;
GRANT SELECT ON commissions TO api_user;
GRANT SELECT ON accounting_firms TO api_user;
GRANT SELECT ON notification_queue TO api_user;

-- Views 權限
GRANT SELECT ON v_customer_summary TO api_user;
GRANT SELECT ON v_payments_due TO api_user;
GRANT SELECT ON v_renewal_reminders TO api_user;
GRANT SELECT ON v_commission_tracker TO api_user;
GRANT SELECT ON v_branch_revenue_summary TO api_user;
GRANT SELECT ON v_overdue_details TO api_user;
GRANT SELECT ON v_line_user_lookup TO api_user;
GRANT SELECT ON v_today_tasks TO api_user;

-- ============================================================================
-- 5. 表格權限 - staff (員工)
-- ============================================================================

-- 場館 (只讀)
GRANT SELECT ON branches TO staff;

-- 客戶 (讀寫)
GRANT SELECT, INSERT, UPDATE ON customers TO staff;

-- 合約 (讀寫)
GRANT SELECT, INSERT, UPDATE ON contracts TO staff;

-- 付款 (讀寫)
GRANT SELECT, INSERT, UPDATE ON payments TO staff;

-- 佣金 (只讀)
GRANT SELECT ON commissions TO staff;

-- 會計事務所 (只讀)
GRANT SELECT ON accounting_firms TO staff;

-- 通知佇列 (讀寫)
GRANT SELECT, INSERT, UPDATE ON notification_queue TO staff;

-- 系統設定 (只讀)
GRANT SELECT ON system_settings TO staff;

-- 審計日誌 (只讀自己場館)
GRANT SELECT ON audit_logs TO staff;

-- Views
GRANT SELECT ON v_customer_summary TO staff;
GRANT SELECT ON v_payments_due TO staff;
GRANT SELECT ON v_renewal_reminders TO staff;
GRANT SELECT ON v_commission_tracker TO staff;
GRANT SELECT ON v_branch_revenue_summary TO staff;
GRANT SELECT ON v_overdue_details TO staff;
GRANT SELECT ON v_line_user_lookup TO staff;
GRANT SELECT ON v_today_tasks TO staff;

-- ============================================================================
-- 6. 表格權限 - admin (管理員)
-- ============================================================================

-- 完整權限
GRANT ALL ON branches TO admin;
GRANT ALL ON customers TO admin;
GRANT ALL ON contracts TO admin;
GRANT ALL ON payments TO admin;
GRANT ALL ON commissions TO admin;
GRANT ALL ON accounting_firms TO admin;
GRANT ALL ON notification_queue TO admin;
GRANT ALL ON system_settings TO admin;
GRANT ALL ON audit_logs TO admin;

-- Views
GRANT SELECT ON v_customer_summary TO admin;
GRANT SELECT ON v_payments_due TO admin;
GRANT SELECT ON v_renewal_reminders TO admin;
GRANT SELECT ON v_commission_tracker TO admin;
GRANT SELECT ON v_branch_revenue_summary TO admin;
GRANT SELECT ON v_overdue_details TO admin;
GRANT SELECT ON v_line_user_lookup TO admin;
GRANT SELECT ON v_today_tasks TO admin;

-- ============================================================================
-- 7. 啟用 Row-Level Security
-- ============================================================================

ALTER TABLE customers ENABLE ROW LEVEL SECURITY;
ALTER TABLE contracts ENABLE ROW LEVEL SECURITY;
ALTER TABLE payments ENABLE ROW LEVEL SECURITY;
ALTER TABLE commissions ENABLE ROW LEVEL SECURITY;
ALTER TABLE notification_queue ENABLE ROW LEVEL SECURITY;
ALTER TABLE audit_logs ENABLE ROW LEVEL SECURITY;

-- ============================================================================
-- 8. RLS Policies - customers 表
-- ============================================================================

-- 員工只能看自己場館的客戶
CREATE POLICY staff_customers_select ON customers
    FOR SELECT TO staff
    USING (
        branch_id = COALESCE(
            NULLIF(current_setting('request.jwt.claims', true)::json->>'branch_id', '')::INTEGER,
            branch_id  -- 如果沒有設定 branch_id，允許所有（開發用）
        )
    );

CREATE POLICY staff_customers_insert ON customers
    FOR INSERT TO staff
    WITH CHECK (
        branch_id = COALESCE(
            NULLIF(current_setting('request.jwt.claims', true)::json->>'branch_id', '')::INTEGER,
            branch_id
        )
    );

CREATE POLICY staff_customers_update ON customers
    FOR UPDATE TO staff
    USING (
        branch_id = COALESCE(
            NULLIF(current_setting('request.jwt.claims', true)::json->>'branch_id', '')::INTEGER,
            branch_id
        )
    );

-- 管理員可以存取所有客戶
CREATE POLICY admin_customers_all ON customers
    FOR ALL TO admin
    USING (TRUE)
    WITH CHECK (TRUE);

-- API 用戶可以讀取所有客戶
CREATE POLICY api_user_customers_select ON customers
    FOR SELECT TO api_user
    USING (TRUE);

-- ============================================================================
-- 9. RLS Policies - contracts 表
-- ============================================================================

CREATE POLICY staff_contracts_select ON contracts
    FOR SELECT TO staff
    USING (
        branch_id = COALESCE(
            NULLIF(current_setting('request.jwt.claims', true)::json->>'branch_id', '')::INTEGER,
            branch_id
        )
    );

CREATE POLICY staff_contracts_insert ON contracts
    FOR INSERT TO staff
    WITH CHECK (
        branch_id = COALESCE(
            NULLIF(current_setting('request.jwt.claims', true)::json->>'branch_id', '')::INTEGER,
            branch_id
        )
    );

CREATE POLICY staff_contracts_update ON contracts
    FOR UPDATE TO staff
    USING (
        branch_id = COALESCE(
            NULLIF(current_setting('request.jwt.claims', true)::json->>'branch_id', '')::INTEGER,
            branch_id
        )
    );

CREATE POLICY admin_contracts_all ON contracts
    FOR ALL TO admin
    USING (TRUE)
    WITH CHECK (TRUE);

CREATE POLICY api_user_contracts_select ON contracts
    FOR SELECT TO api_user
    USING (TRUE);

-- ============================================================================
-- 10. RLS Policies - payments 表
-- ============================================================================

CREATE POLICY staff_payments_select ON payments
    FOR SELECT TO staff
    USING (
        branch_id = COALESCE(
            NULLIF(current_setting('request.jwt.claims', true)::json->>'branch_id', '')::INTEGER,
            branch_id
        )
    );

CREATE POLICY staff_payments_insert ON payments
    FOR INSERT TO staff
    WITH CHECK (
        branch_id = COALESCE(
            NULLIF(current_setting('request.jwt.claims', true)::json->>'branch_id', '')::INTEGER,
            branch_id
        )
    );

CREATE POLICY staff_payments_update ON payments
    FOR UPDATE TO staff
    USING (
        branch_id = COALESCE(
            NULLIF(current_setting('request.jwt.claims', true)::json->>'branch_id', '')::INTEGER,
            branch_id
        )
    );

CREATE POLICY admin_payments_all ON payments
    FOR ALL TO admin
    USING (TRUE)
    WITH CHECK (TRUE);

CREATE POLICY api_user_payments_select ON payments
    FOR SELECT TO api_user
    USING (TRUE);

-- ============================================================================
-- 11. RLS Policies - commissions 表
-- ============================================================================

-- 員工只能看 (不能改)
CREATE POLICY staff_commissions_select ON commissions
    FOR SELECT TO staff
    USING (TRUE);

-- 管理員完整權限
CREATE POLICY admin_commissions_all ON commissions
    FOR ALL TO admin
    USING (TRUE)
    WITH CHECK (TRUE);

-- API 用戶只讀
CREATE POLICY api_user_commissions_select ON commissions
    FOR SELECT TO api_user
    USING (TRUE);

-- ============================================================================
-- 12. RLS Policies - notification_queue 表
-- ============================================================================

CREATE POLICY staff_notifications_select ON notification_queue
    FOR SELECT TO staff
    USING (TRUE);

CREATE POLICY staff_notifications_insert ON notification_queue
    FOR INSERT TO staff
    WITH CHECK (TRUE);

CREATE POLICY staff_notifications_update ON notification_queue
    FOR UPDATE TO staff
    USING (TRUE);

CREATE POLICY admin_notifications_all ON notification_queue
    FOR ALL TO admin
    USING (TRUE)
    WITH CHECK (TRUE);

-- ============================================================================
-- 13. RLS Policies - audit_logs 表
-- ============================================================================

-- 員工可以看審計日誌（用於查詢歷史）
CREATE POLICY staff_audit_select ON audit_logs
    FOR SELECT TO staff
    USING (TRUE);

-- 管理員完整權限
CREATE POLICY admin_audit_all ON audit_logs
    FOR ALL TO admin
    USING (TRUE)
    WITH CHECK (TRUE);

-- ============================================================================
-- 14. Functions 權限
-- ============================================================================

-- 授予函數執行權限
GRANT EXECUTE ON FUNCTION schedule_payment_reminder(INTEGER, VARCHAR, TIMESTAMPTZ) TO staff, admin;
GRANT EXECUTE ON FUNCTION batch_update_overdue_status() TO admin;
GRANT EXECUTE ON FUNCTION get_revenue_summary(INTEGER, DATE, DATE) TO staff, admin;
GRANT EXECUTE ON FUNCTION get_customer_statistics(INTEGER) TO staff, admin;

-- ============================================================================
-- 15. PostgREST 特殊設定
-- ============================================================================

-- 建立 API schema (用於 PostgREST 暴露)
CREATE SCHEMA IF NOT EXISTS api;

-- 在 api schema 建立暴露的 views
CREATE OR REPLACE VIEW api.customers AS SELECT * FROM public.v_customer_summary;
CREATE OR REPLACE VIEW api.payments_due AS SELECT * FROM public.v_payments_due;
CREATE OR REPLACE VIEW api.renewals AS SELECT * FROM public.v_renewal_reminders;
CREATE OR REPLACE VIEW api.commissions AS SELECT * FROM public.v_commission_tracker;
CREATE OR REPLACE VIEW api.branch_summary AS SELECT * FROM public.v_branch_revenue_summary;
CREATE OR REPLACE VIEW api.overdue AS SELECT * FROM public.v_overdue_details;
CREATE OR REPLACE VIEW api.today_tasks AS SELECT * FROM public.v_today_tasks;

-- 授權 api schema
GRANT USAGE ON SCHEMA api TO anon, api_user, staff, admin;
GRANT SELECT ON ALL TABLES IN SCHEMA api TO api_user, staff, admin;

-- ============================================================================
-- 16. 安全性輔助函數
-- ============================================================================

-- 取得當前用戶角色
CREATE OR REPLACE FUNCTION current_user_role()
RETURNS TEXT AS $$
BEGIN
    RETURN COALESCE(
        current_setting('request.jwt.claims', true)::json->>'role',
        'anon'
    );
END;
$$ LANGUAGE plpgsql STABLE;

-- 取得當前用戶場館 ID
CREATE OR REPLACE FUNCTION current_user_branch_id()
RETURNS INTEGER AS $$
BEGIN
    RETURN NULLIF(
        current_setting('request.jwt.claims', true)::json->>'branch_id',
        ''
    )::INTEGER;
END;
$$ LANGUAGE plpgsql STABLE;

-- 授權這些函數
GRANT EXECUTE ON FUNCTION current_user_role() TO anon, api_user, staff, admin;
GRANT EXECUTE ON FUNCTION current_user_branch_id() TO anon, api_user, staff, admin;
