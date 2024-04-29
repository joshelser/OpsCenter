
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

CREATE TABLE INTERNAL.TASK_LOG IF NOT EXISTS (task_start timestamp_ltz, success boolean, object_type varchar, object_name varchar, input variant, output variant, task_finish timestamp_ltz, task_run_id text, query_id text);
