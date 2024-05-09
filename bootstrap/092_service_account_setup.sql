
DROP PROCEDURE IF EXISTS admin.finalize_setup_from_service_account(varchar, varchar, varchar);
CREATE OR REPLACE PROCEDURE admin.finalize_setup_from_service_account(api_integration_ref_id varchar, url varchar, web_url varchar, token varchar default null)
RETURNS object
LANGUAGE sql
COMMENT = 'Deprecated: unused after parallelism improvements, to be removed'
as
begin
    -- Create the task so we can run finalize_setup asynchronously (duplicated in finalize_setup)
    -- Does not start the task -- the first time the task runs, finalize_setup() will start the task.
    CREATE OR REPLACE TASK TASKS.UPGRADE_CHECK
        SCHEDULE = '1440 minute'
        ALLOW_OVERLAPPING_EXECUTION = FALSE
        USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = "XSMALL"
        AS
        CALL ADMIN.UPGRADE_CHECK();
    grant MONITOR, OPERATE on TASK TASKS.UPGRADE_CHECK to APPLICATION ROLE ADMIN;

    -- Merge all three properties into the config table in one merge statement.
    MERGE INTO internal.config AS target
    USING (
        SELECT $1 as key, $2 as value from VALUES
            ('tenant_url', :web_url), ('url', :url), ('tenant_id', split_part(:web_url, '/', -1))
    ) AS source
    ON target.key = source.key
    WHEN MATCHED THEN
        UPDATE SET value = source.value
    WHEN NOT MATCHED THEN
        INSERT (key, value) VALUES (source.key, source.value);

    -- Bind the given reference ID to the 'OPSCENTER_API_INTEGRATION' reference. Must match the reference in manifest.yml
    call admin.update_reference('OPSCENTER_API_INTEGRATION', 'ADD', :api_integration_ref_id);

    -- Save the token if provided
    let ret object;
    if (token is not null) then
        -- Create the scalar UDF for the Sundeck auth token (EF URL set up by the app in permissions.py)
        execute immediate 'create or replace function internal.get_ef_token() returns string as \'\\\'' || token || '\\\'\';';
        CALL admin.setup_external_functions('opscenter_api_integration');
    end if;
    return :ret;
end;

CREATE OR REPLACE PROCEDURE admin.create_upgrade_check_task()
RETURNS TEXT
LANGUAGE SQL
AS
BEGIN
    -- Create the task so we can run finalize_setup asynchronously (duplicated in finalize_setup)
    -- Does not start the task -- the first time the task runs, finalize_setup() will start the task.
    CREATE OR REPLACE TASK TASKS.UPGRADE_CHECK
        SCHEDULE = '1440 minute'
        ALLOW_OVERLAPPING_EXECUTION = FALSE
        USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = "XSMALL"
        AS
    declare
        setup_version varchar default internal.get_version();
        task_name text default 'UPGRADE_CHECK';
        root_task_id text default (select INTERNAL.ROOT_TASK_ID());
        task_run_id text default (select INTERNAL.TASK_RUN_ID());
    begin
        let query_id text := (select query_id from table(information_schema.task_history(TASK_NAME => :task_name, ROOT_TASK_ID => :root_task_id)) WHERE GRAPH_RUN_GROUP_ID = :task_run_id  AND DATABASE_NAME = current_database() limit 1);
        let input object;
        CALL INTERNAL.START_TASK(:task_name, :setup_version, :task_run_id, :query_id) into :input;

        let output object;
        CALL ADMIN.UPGRADE_CHECK() into :output;

        CALL INTERNAL.FINISH_TASK(:task_name, :setup_version, :task_run_id, :output);
    END;

    grant MONITOR, OPERATE on TASK TASKS.UPGRADE_CHECK to APPLICATION ROLE ADMIN;
    return '';
END;

-- Merges tenant_url, url, and tenant_id into the config table. Conditionally updates the internal.get_ef_token UDF.
CREATE OR REPLACE PROCEDURE admin.update_sundeck_configuration(url varchar, web_url varchar, tenant_id varchar, token varchar default null)
RETURNS TEXT
LANGUAGE SQL
AS
BEGIN
    -- Merge all three properties into the config table in one merge statement.
    MERGE INTO internal.config AS target
    USING (
        SELECT $1 as key, $2 as value from VALUES
            ('tenant_url', :web_url), ('url', :url), ('tenant_id', :tenant_id)
    ) AS source
    ON target.key = source.key
    WHEN MATCHED THEN
        UPDATE SET value = source.value
    WHEN NOT MATCHED THEN
        INSERT (key, value) VALUES (source.key, source.value);

    if (token is not null) then
        -- Create the scalar UDF for the Sundeck auth token, if provided.
        execute immediate 'create or replace function internal.get_ef_token() returns string as \'\\\'' || token || '\\\'\';';
    end if;

    return '';
END;



CREATE OR REPLACE PROCEDURE admin.upgrade_check()
returns object
language sql
as
declare
    old_version varchar default NULL;
    setup_version varchar default internal.get_version();
begin
    let output object;
    -- It is important that we always call this procedure. If the Sundeck token is regenerated, we need to also recreate
    -- the external functions. ADMIN.FINALIZE_SETUP does also call setup_external_functions, but this task only runs
    -- finalize_setup once per native app version.
    CALL admin.setup_external_functions('opscenter_api_integration');

    call internal.get_config('post_setup') into :old_version;
    let ran_finalize_setup boolean := false;
    if (old_version is null or old_version <> setup_version) then
        call admin.finalize_setup();
        ran_finalize_setup := true;
    end if;

    return OBJECT_CONSTRUCT('old_version', :old_version, 'new_version', :setup_version, 'finalize_setup', :ran_finalize_setup);
EXCEPTION
    WHEN OTHER THEN
        SYSTEM$LOG_ERROR(OBJECT_CONSTRUCT('error', 'Unhandled exception occurred during UPGRADE_CHECK.', 'SQLCODE', :sqlcode,
            'SQLERRM', :sqlerrm, 'SQLSTATE', :sqlstate));
        return OBJECT_CONSTRUCT('error', 'Unhandled exception occurred during UPGRADE_CHECK.', 'SQLCODE', :sqlcode, 'SQLSTATE', :sqlstate, 'SQLERRM', :sqlerrm);
END;
