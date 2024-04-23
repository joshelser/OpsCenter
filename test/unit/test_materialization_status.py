import json


def recreate_task_history(cur):
    try:
        cur.execute("DROP VIEW IF EXISTS internal.all_task_history")
    except Exception as e:
        print(f"Ignoring exception during setup: {e}")
        pass
    cur.execute(
        "create or replace table internal.all_task_history(run timestamp, success boolean, "
        + "input object, output object, table_name text)"
    )


def insert_row(
    cur, run: str, success: bool, input: dict, output: dict, table_name: str
):
    input_json = "NULL"
    if input:
        input_json = f"parse_json('{json.dumps(input)}')"
    output_json = "NULL"
    if output:
        output_json = f"parse_json('{json.dumps(output)}')"
    cur.execute(
        f"""insert into internal.all_task_history select '{run}'::TIMESTAMP, {success}, \
        {input_json}, {output_json}, '{table_name}'"""
    )


def test_materialization_status(conn):
    with conn() as cnx:
        cur = cnx.cursor()
        recreate_task_history(cur)

        # Query History
        input = None
        output = {
            "new_INCOMPLETE": 100,
            "new_closed": 1000,
            "new_records": 1100,
            "newest_completed": "2022-04-01 00:45:00",
            "oldest_running": "2022-04-01 00:59:00",
        }
        insert_row(cur, "2022-04-01 04:00:00", True, input, output, "QUERY_HISTORY")

        input = output
        output = {
            "new_INCOMPLETE": 100,
            "new_closed": 1000,
            "new_records": 1100,
            "newest_completed": "2022-04-01 01:45:00",
            "oldest_running": "2022-04-01 01:59:00",
        }
        insert_row(cur, "2022-04-01 05:00:00", True, input, output, "QUERY_HISTORY")

        # Warehouse Events
        input = None
        output = {
            "new_INCOMPLETE": 5,
            "new_closed": 100,
            "new_records": 105,
            "newest_completed": "2022-04-01 00:55:00",
            "oldest_running": "2022-04-01 00:55:00",
        }
        insert_row(
            cur, "2022-04-01 04:00:00", True, input, output, "WAREHOUSE_EVENTS_HISTORY"
        )

        input = output
        output = {
            "new_INCOMPLETE": 5,
            "new_closed": 100,
            "new_records": 105,
            "newest_completed": "2022-04-01 01:55:00",
            "oldest_running": "2022-04-01 01:55:00",
        }
        insert_row(
            cur, "2022-04-01 05:00:00", True, input, output, "WAREHOUSE_EVENTS_HISTORY"
        )

        rows = cur.execute(
            """select table_name, full_materialization_complete, last_execution, current_execution, next_execution from
            table(admin.materialization_status()) order by table_name"""
        ).fetchall()
        assert len(rows) == 2

        row = rows[0]
        assert row[0] == "QUERY_HISTORY"
        assert row[1] is True
        last_execution = json.loads(row[2])
        assert last_execution == {
            "kind": "INCREMENTAL",
            "success": True,
            "start": "2022-04-01 05:00:00.000",
        }
        assert row[3] is None
        next_execution = json.loads(row[4])
        assert next_execution == {
            "estimated_start": "2022-04-01 06:00:00.000",
            "kind": "INCREMENTAL",
        }

        row = rows[1]
        assert row[0] == "WAREHOUSE_EVENTS_HISTORY"
        assert row[1] is True
        last_execution = json.loads(row[2])
        assert last_execution == {
            "kind": "INCREMENTAL",
            "success": True,
            "start": "2022-04-01 05:00:00.000",
        }
        assert row[3] is None
        next_execution = json.loads(row[4])
        assert next_execution == {
            "estimated_start": "2022-04-01 06:00:00.000",
            "kind": "INCREMENTAL",
        }
