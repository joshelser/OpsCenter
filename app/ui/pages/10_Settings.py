import streamlit as st
import connection
import config
import sthelp
import setup
from telemetry import action


sthelp.chrome("Settings")


def invalid_number(string):
    try:
        float(string)  # Convert the string to float
        return False
    except ValueError:
        return True


def get_task_status(dbname):
    sql = f"""call admin.run_as_app('describe task TASKS."{dbname}";');"""
    df = connection.execute(sql)
    if df is None or len(df) == 0:
        return None
    return df["state"][0] == "started"


def get_task_state(status: bool):
    if status:
        return "RESUME"
    else:
        return "SUSPEND"


def task_listing(
    task_display_name: str,
    task_internal_name: str,
    frequency,
):
    try:
        status = get_task_status(task_internal_name)
    except Exception:
        return None, None

    return (
        st.checkbox(
            f"Enable {task_display_name} ({frequency})",
            value=status,
            key=f"enable_{task_internal_name}",
        ),
        status,
    )


tasks, config_tab, setup_tab, reset = st.tabs(
    ["Tasks", "Config", "Initial Setup", "Reset"]
)


with config_tab:
    form = st.form("Configuration")
    with form:
        compute_credit_cost, serverless_credit_cost, tbcost = config.get_costs()
        if compute_credit_cost is None:
            compute_credit_cost = 2.00
        if serverless_credit_cost is None:
            serverless_credit_cost = 3.00
        if tbcost is None:
            tbcost = 40.00
        compute = st.text_input(
            "Compute Credit Cost", value=compute_credit_cost, key="compute_credit_cost"
        )
        serverless = st.text_input(
            "Serverless Credit Cost",
            value=serverless_credit_cost,
            key="serverless_credit_cost",
        )
        storage = st.text_input("Storage Cost (/tb)", value=tbcost, key="storage_cost")
        telemetry = st.checkbox("Usage Telemetry", value=config.is_telemetry_enabled(), key="telemetry")

        if st.form_submit_button("Save"):
            action("Update Configuration")

            if (
                invalid_number(compute)
                or invalid_number(serverless)
                or invalid_number(storage)
            ):
                st.error("Please enter a valid number for all costs.")

            config.set_costs(compute, serverless, storage)
            connection.execute(
                f"""
            BEGIN
                CREATE OR REPLACE FUNCTION INTERNAL.GET_CREDIT_COST()
                    RETURNS NUMBER AS
                    $${compute}$$;

                CREATE OR REPLACE FUNCTION INTERNAL.GET_SERVERLESS_CREDIT_COST()
                    RETURNS NUMBER AS
                    $${serverless}$$;

                CREATE OR REPLACE FUNCTION INTERNAL.GET_STORAGE_COST()
                    RETURNS NUMBER AS
                    $${storage}$$;
            END;
            """
            )
            config.set_telemetry(telemetry)
            st.success("Saved")


with setup_tab:
    setup.setup_block()


def save_tasks(container, wem, qhm, pm):
    action("Save Tasks")
    with container:
        with st.spinner("Saving changes to task settings."):
            sql = f"""
            call admin.run_as_app($$
            begin
                alter task TASKS.WAREHOUSE_EVENTS_MAINTENANCE {get_task_state(wem)};
                alter task TASKS.QUERY_HISTORY_MAINTENANCE {get_task_state(qhm)};
                alter task TASKS.SFUSER_MAINTENANCE {get_task_state(pm)};
            end;
            $$);
            """
            connection.execute(sql)


with tasks:
    st.title("Tasks")

    checkboxes_container = st.empty()
    form = checkboxes_container.container()
    with form:
        wem, wems = task_listing(
            "Warehouse Events Maintenance",
            "WAREHOUSE_EVENTS_MAINTENANCE",
            "every hour",
        )
        qhm, qhms = task_listing(
            "Query History Maintenance",
            "QUERY_HISTORY_MAINTENANCE",
            "every hour",
        )
        pm, pms = task_listing(
            "Snowflake User Replication", "SFUSER_MAINTENANCE", "every day"
        )
        # Only enable the button once the page has been reloaded and the checkbox is inconsistent with the task state. This is because streamlit
        # state is ugly and we don't want to record state here since it is already managed in Snowflake. Note this still has a bug if users
        # click multiple times quickly as the save button could be clicked before the last checkbox selection is recorded/refreshed.
        st.button(
            "Save Changes",
            on_click=save_tasks,
            args=[form, wem, qhm, pm],
            disabled=(wems == wem and qhms == qhm and pms == pm),
        )

    if wem is None or qhm is None or pm is None:
        checkboxes_container.warning(
            "Unable to load task information. Make sure to run post-setup scripts."
        )


with reset:
    st.title("Reset/Reload")
    do_reset = st.button("Reset and reload query history and warehouse events.")
    if do_reset:
        action("Reset")
        bar = st.progress(0, text="Cleaning and refreshing query and warehouse events.")
        msg = st.empty()
        msg.warning("Resetting. Please do not navigate away from this page.")
        connection.execute(
            """
        begin
            truncate table internal.task_query_history;
            truncate table internal.task_warehouse_events;
            truncate table internal_reporting_mv.cluster_and_warehouse_sessions_complete_and_daily;
            truncate table internal_reporting_mv.query_history_complete_and_daily;
        end;
        """
        )
        bar.progress(
            10,
            text="Old activity removed, refreshing warehouse events. This may take a bit.",
        )
        connection.execute("call internal.refresh_warehouse_events(true);")
        bar.progress(
            30,
            text="Warehouse events refreshed, refreshing queries. This may take a bit.",
        )
        connection.execute("call internal.refresh_queries(true);")
        bar.progress(100, text="All events refreshed.")
        msg.info("Reset Complete.")
