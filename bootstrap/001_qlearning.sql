
create or replace function analysis.choose_warehouse(warehouse_name varchar,
                            warehouse_size varchar,
                            query_type varchar,
                            parameterized_query_hash varchar,
                            model_runtime_score varchar,
                            cost number(38,12))
returns table (next_warehouse_size varchar, warehouse_size varchar)
language python
runtime_version=3.10
handler='run'
imports = ('{{stage}}/qlearning.zip')
packages=('pandas', 'numpy')
as $$
import pandas
from _snowflake import vectorized
import qlearning

class run:
    @vectorized(input=pandas.DataFrame)
    def end_partition(self, df):
        return qlearning.end_partition(df)
$$;
