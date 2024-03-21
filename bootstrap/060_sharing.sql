
-- Access to the SUNDECK database is given to application_package in deploy.py
-- SHARING.GLOBAL_QUERY_HISTORY is already filtered on SNOWFLAKE_ACCOUNT_LOCATOR = CURRENT_ACCOUNT()
CREATE OR REPLACE VIEW REPORTING.SUNDECK_QUERY_HISTORY
    AS SELECT
        SNOWFLAKE_QUERY_ID,
        SUNDECK_QUERY_ID,
        FLOW_NAME,
        QUERY_TEXT_RECEIVED,
        QUERY_TEXT_FINAL,
        ALT_WAREHOUSE_ROUTE,
        SUNDECK_STATUS,
        SUNDECK_ERROR_CODE,
        SUNDECK_ERROR_MESSAGE,
        USER_NAME,
        ROLE_NAME,
        SUNDECK_START_TIME,
        SNOWFLAKE_SUBMISSION_TIME,
        SNOWFLAKE_END_TIME,
        ACTIONS_EXECUTED,
        SCHEMA_ONLY_REQUEST,
        STARTING_WAREHOUSE,
        WORKLOAD_TAG,
        SNOWFLAKE_SESSION_ID
    from SHARING.GLOBAL_QUERY_HISTORY WHERE
        UPPER(SUNDECK_ACCOUNT_ID) = (select UPPER(value) from internal.config where key = 'tenant_id');
