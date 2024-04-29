
create or replace function internal.task_run_id()
returns text
AS
$$
     SYSTEM$TASK_RUNTIME_INFO('CURRENT_TASK_GRAPH_RUN_GROUP_ID')
$$;

create or replace function internal.task_query_id(task_name text, task_run_id text)
returns text
AS
$$
    select query_id from table(information_schema.task_history(TASK_NAME => task_name))
        WHERE GRAPH_RUN_GROUP_ID = task_run_id  AND DATABASE_NAME = current_database()
        limit 1
$$;

-- Generic table that tasks should record their execution into.
CREATE TABLE INTERNAL.TASK_LOG IF NOT EXISTS (task_start timestamp_ltz, success boolean, object_type varchar, object_name varchar,
    input variant, output variant, task_finish timestamp_ltz, task_run_id text, query_id text);

-- Create specific-purpose views that we had before.
CREATE OR REPLACE VIEW REPORTING.SIMPLE_DATA_EVENTS_TASK_HISTORY AS SELECT * exclude (object_name) rename (object_type as table_name)
    FROM INTERNAL.TASK_LOG WHERE object_type = 'SIMPLE_DATA_EVENT';
CREATE OR REPLACE VIEW REPORTING.WAREHOUSE_LOAD_EVENTS_TASK_HISTORY AS SELECT * exclude (object_name) rename (object_type as warehouse_name)
    FROM INTERNAL.TASK_LOG where object_type = 'WAREHOUSE_LOAD_EVENT';
CREATE OR REPLACE VIEW REPORTING.UPGRADE_HISTORY AS SELECT task_start, success, output['old_version'] as old_version, output['new_version'] as new_version, task_finish, task_run_id, query_id,
    FROM INTERNAL.TASK_LOG where object_type = 'UPGRADE';

-- Create a generic view for all task executions.
CREATE OR REPLACE VIEW REPORTING.TASK_LOG_HISTORY AS SELECT * FROM INTERNAL.TASK_LOG;
