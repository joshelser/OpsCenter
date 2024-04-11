
CREATE sequence if not exists analysis.autorouting_seq START = 1 INCREMENT = 1;

create or replace function analysis.choose_warehouse(warehouse_name varchar,
                            warehouse_size varchar,
                            query_type varchar,
                            parameterized_query_hash varchar,
                            model_runtime_score varchar,
                            cost number(38,12),
                            query_text varchar,
                            database_name varchar,
                            schema_name varchar,
                            hint object
                        )
returns table (next_warehouse_size varchar, warehouse_size varchar, query_text varchar, database_name varchar, schema_name varchar)
language python
runtime_version=3.10
handler='run'
imports = ('{{stage}}/python/ml.zip')
packages=('pandas', 'numpy')
as $$
import pandas
from _snowflake import vectorized
import ml

class run:
    @vectorized(input=pandas.DataFrame)
    def end_partition(self, df):
        return ml.end_partition(df)
$$;

create table if not exists analysis.autorouting_history (query_signature varchar, query_text varchar, database_name varchar, schema_name varchar, target_warehouse varchar, run_id number, input variant);

create or replace procedure analysis.refresh_autorouting(label_name varchar, warehouse_prefix varchar, hint object, initial_warehouse varchar, lookback_period varchar)
returns table(query_signature varchar, query_text varchar, database_name varchar, schema_name varchar, target_warehouse varchar)
language sql
as 
begin
let nextrun number := (select analysis.autorouting_seq.nextval);
let input variant := (select {'label_name': :label_name, 'warehouse_prefix': :warehouse_prefix, 'hint': :hint, 'initial_warehouse': :initial_warehouse, 'lookback_period': :lookback_period}::variant);
let sql varchar := 'insert into analysis.autorouting_history(query_signature, query_text, database_name, schema_name, target_warehouse, run_id, input)
with raw as (
select
       warehouse_name,
       cost,
       warehouse_size,
       query_type,
       query_parameterized_hash,
       start_time,
       query_text,
       database_name,
       schema_name,
       tools.model_run_time(total_elapsed_time) as run_time
from reporting.labeled_query_history where {label_name}
and start_time > current_timestamp - interval \'{lookback_period}\' -- only consider recent queries
and execution_status = \'SUCCESS\' -- dont consider failed queries
and query_parameterized_hash is not null -- filter out queries w/o a hash
and warehouse_size is not null -- no warehouse means a metadata only query and we can ignore it (cant route anyways)
)
select 
query_parameterized_hash as query_signature,
t.query_text as query_text,
t.database_name as database_name,
t.schema_name as schema_name,
concat(?, t.next_warehouse_size) as target_warehouse,
? as run_id,
parse_json(?) as input
from raw, table(analysis.choose_warehouse(
		warehouse_name,
		warehouse_size,
		query_type,
		query_parameterized_hash,
		run_time,
		cost,
        query_text,
        database_name,
        schema_name,
        parse_json(?)::object) over (partition by query_parameterized_hash order by start_time)) t';
let tmpl_sql varchar := (select tools.templatejs(:sql, {'label_name': :label_name, 'lookback_period': :lookback_period}));
let res resultset := (execute immediate :tmpl_sql using (warehouse_prefix, nextrun, input, hint));
let rettbl resultset := (select query_signature, query_text, database_name, schema_name, target_warehouse from analysis.autorouting_history where run_id = :nextrun);
return table(rettbl);
end;
