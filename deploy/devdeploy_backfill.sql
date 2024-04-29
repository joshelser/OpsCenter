begin
    let dt timestamp := (select dateadd(day, -1, current_timestamp()));
    insert into "{DATABASE}".INTERNAL.TASK_WAREHOUSE_EVENTS SELECT current_timestamp(), true, null, OBJECT_CONSTRUCT('oldest_running', :dt, 'newest_completed', :dt)::VARIANT, :dt, NULL, NULL, 'WAREHOUSE_EVENTS';
    insert into "{DATABASE}".INTERNAL.TASK_QUERY_HISTORY SELECT current_timestamp(), true, null, OBJECT_CONSTRUCT('oldest_running', :dt, 'newest_completed', :dt)::VARIANT, :dt, NULL, NULL, 'QUERY_HISTORY';

    insert into "{DATABASE}".INTERNAL.TASK_SIMPLE_DATA_EVENTS SELECT current_timestamp(), true, 'SERVERLESS_TASK_HISTORY', null, OBJECT_CONSTRUCT('oldest_running', :dt)::VARIANT, :dt, NULL, NULL;
    insert into "{DATABASE}".INTERNAL.TASK_SIMPLE_DATA_EVENTS SELECT current_timestamp(), true, 'TASK_HISTORY', null, OBJECT_CONSTRUCT('oldest_running', :dt)::VARIANT, :dt, NULL, NULL;
    insert into "{DATABASE}".INTERNAL.TASK_SIMPLE_DATA_EVENTS SELECT current_timestamp(), true, 'SESSIONS', null, OBJECT_CONSTRUCT('oldest_running', :dt)::VARIANT, :dt, NULL, NULL;
    insert into "{DATABASE}".INTERNAL.TASK_SIMPLE_DATA_EVENTS SELECT current_timestamp(), true, 'WAREHOUSE_METERING_HISTORY', null, OBJECT_CONSTRUCT('oldest_running', :dt)::VARIANT, :dt, NULL, NULL;
    insert into "{DATABASE}".INTERNAL.TASK_SIMPLE_DATA_EVENTS SELECT current_timestamp(), true, 'LOGIN_HISTORY', null, OBJECT_CONSTRUCT('oldest_running', :dt)::VARIANT, :dt, NULL, NULL;
    insert into "{DATABASE}".INTERNAL.TASK_SIMPLE_DATA_EVENTS SELECT current_timestamp(), true, 'HYBRID_TABLE_USAGE_HISTORY', null, OBJECT_CONSTRUCT('oldest_running', :dt)::VARIANT, :dt, NULL, NULL;
    insert into "{DATABASE}".INTERNAL.TASK_SIMPLE_DATA_EVENTS SELECT current_timestamp(), true, 'MATERIALIZED_VIEW_REFRESH_HISTORY', null, OBJECT_CONSTRUCT('oldest_running', :dt)::VARIANT, :dt, NULL, NULL;

    show warehouses;
    let res resultset := (select "name" as n from table(result_scan(last_query_id())));
    let cur cursor for res;
    for r in cur do
        let wh_name string := r.N;
        insert into "{DATABASE}".INTERNAL.TASK_WAREHOUSE_LOAD_EVENTS SELECT current_timestamp(), true, :wh_name, null, OBJECT_CONSTRUCT('oldest_running', :dt)::VARIANT, :dt, NULL, NULL;
    end for;
end;
