import getopt
import time
import helpers
import re
import shutil
import sys


def _setup_database(cur, database: str, schema: str, stage: str):
    print(f"Setting up database '{database}' for local development.")
    cur.execute(
        f"""
    BEGIN
        CREATE DATABASE IF NOT EXISTS {database};
        USE DATABASE {database};
        CREATE SCHEMA IF NOT EXISTS {schema};
        USE SCHEMA {schema};
        CREATE STAGE IF NOT EXISTS {database}.{schema}.{stage}
            file_format = (type = 'CSV' field_delimiter = None record_delimiter = None);
    END;
    """
    )


def _copy_dependencies(cur, schema: str, stage: str):
    print(f"Copying dependencies to @{schema}.{stage}.")
    # Writes all dependencies into `@stage/python`
    for file in ["sqlglot.zip", "crud.zip"]:
        local_file_path = f"app/python/{file}"
        stage_file_path = f"@{schema}.{stage}/python"
        put_cmd = f"PUT 'file://{local_file_path}' '{stage_file_path}' overwrite=true auto_compress=false"
        print(put_cmd)
        cur.execute(put_cmd)

    # Also copy the CRUD zip into @stage/ui because the streamlit app cannot reach "out" of the `ui` directory
    # to dynamically load this from elsewhere in the stage.
    put_cmd = f"PUT 'file://app/python/crud.zip' '@{schema}.{stage}/ui' overwrite=true auto_compress=false"
    print(put_cmd)
    cur.execute(put_cmd)

    # Copy the CRUD zip from app/python to app/ui to keep devdeploy and deploy streamlit consistent with each other.
    shutil.copy2("app/python/crud.zip", "app/ui/crud.zip")


def _copy_opscenter_files(cur, schema: str, stage: str, deployment: str):
    print(f"Copying OpsCenter files to @{schema}.{stage}.")
    scripts = helpers.generate_body(False, stage_name=f"@{schema}.{stage}")
    scripts += helpers.generate_qtag()
    scripts += helpers.generate_get_sundeck_deployment_function(deployment)
    body = helpers.generate_setup_script(scripts)
    regex = re.compile("APPLICATION\\s+ROLE", re.IGNORECASE)
    body = regex.sub("DATABASE ROLE", body)
    regex = re.compile("OR\\s+ALTER\\s+VERSIONED\\s+SCHEMA", re.IGNORECASE)
    body = regex.sub("SCHEMA IF NOT EXISTS", body)
    cur.execute(body)


def _fake_app_package_objects(cur, database: str):
    # In the "local" mode, we don't have a real application package and the sharing model does not apply.
    # Create a fake table so the setup script can be run normally.
    cur.execute(
        f"""BEGIN
    CREATE SCHEMA IF NOT EXISTS "{database}".SHARING;
    CREATE TABLE IF NOT EXISTS "{database}".SHARING.GLOBAL_QUERY_HISTORY(
        SNOWFLAKE_ACCOUNT_LOCATOR text,
        SNOWFLAKE_QUERY_ID text,
        SUNDECK_QUERY_ID text,
        FLOW_NAME text,
        QUERY_TEXT_RECEIVED text,
        QUERY_TEXT_FINAL text,
        SNOWFLAKE_SUBMISSION_TIME timestamp_ntz,
        ALT_WAREHOUSE_ROUTE text,
        SUNDECK_STATUS text,
        SUNDECK_ERROR_CODE text,
        SUNDECK_ERROR_MESSAGE text,
        SNOWFLAKE_REGION text,
        SNOWFLAKE_CLOUD text,
        SUNDECK_START_TIME timestamp_ntz,
        SUNDECK_ACCOUNT_ID text,
        ACTIONS_EXECUTED variant,
        SCHEMA_ONLY_REQUEST boolean);
    END;"""
    )


def _finish_local_setup(cur, database: str, schema: str):
    print("Setting up internal state to mimic a set-up app.")

    # Call FINALIZE_SETUP first to perform any migrations. This implicitly triggers the tasks.
    cur.execute(f"call {database}.ADMIN.FINALIZE_SETUP();")

    start_time = time.time()

    # Then, wait for the tasks to report that they have run.
    while True:
        # Execute a query to fetch data from the table
        cur.execute(
            "SELECT * FROM internal.config where key in ('WAREHOUSE_EVENTS_MAINTENANCE', 'QUERY_HISTORY_MAINTENANCE', 'SNOWFLAKE_USER_MAINTENANCE') and value is not null;"
        )

        rows = cur.fetchall()

        # if we have two rows, means materialization is complete
        if len(rows) == 3:
            print("OpsCenter setup complete.")
            break

        elapsed_time = time.time() - start_time
        # bail after 3 minutes
        if elapsed_time >= 300:
            print("Aborting OpsCenter setup as it did not complete in 3 minutes!")
            sys.exit(1)

        # check every 20 seconds
        time.sleep(20)


def devdeploy(
    profile: str, schema: str, stage: str, deployment: str, finishSetup: bool
):
    """
    Create the app package to enable local development
    :param profile: the Snowsql configuration profile to use.
    """
    conn = helpers.connect_to_snowflake(profile=profile, schema=schema)
    cur = conn.cursor()
    cur.execute("SET DEPLOYENV='DEV';")

    # Create the database (and stage) if not already present
    _setup_database(cur, conn.database, conn.schema, stage)

    # Build a new zip file with the CRUD python project.
    helpers.zip_python_module("crud", "app/crud", "app/python/crud.zip")

    # Copy dependencies into the stage
    _copy_dependencies(cur, conn.schema, stage)

    # The setup script relies on objects in the app package, but this mode of deploy does not use an app package.
    # Create those resources by hand with dummy data.
    _fake_app_package_objects(cur, conn.database)

    # Deploy the OpsCenter code into the stage.
    _copy_opscenter_files(cur, conn.schema, stage, deployment)

    # Finish local setup by setting internal state to mimic a set-up app
    # (e.g. materializes data, starts tasks)
    if finishSetup:
        _finish_local_setup(cur, conn.database, conn.schema)

    conn.close()


def usage():
    print("devdeploy.py -p <snowsql_profile_name> -d <sundeck_deployment> -s")


def main(argv):
    """
    Parse command line arguments and call devdeploy.
    """
    profile = "local_opscenter"
    schema = "PUBLIC"
    stage = "OC_STAGE"
    deployment = "dev"
    finishSetup = True
    opts, args = getopt.getopt(
        argv, "d:hp:s", ["deployment=", "profile=", "skip-finish-setup"]
    )
    for opt, arg in opts:
        if opt == "-h":
            usage()
            sys.exit()
        elif opt in ("-p", "--profile"):
            profile = arg
        elif opt in ("-d", "--deployment"):
            deployment = arg
        elif opt in ("-s", "--skip-finish-setup"):
            finishSetup = False

    if profile is None or stage is None:
        usage()
        sys.exit()

    devdeploy(profile, schema, stage, deployment, finishSetup)


if __name__ == "__main__":
    main(sys.argv[1:])
