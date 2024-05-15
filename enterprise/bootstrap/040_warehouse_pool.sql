
CREATE TABLE INTERNAL.WH_POOLS IF NOT EXISTS(
    name text,
    warehouses array
);

CREATE OR REPLACE PROCEDURE INTERNAL.CREATE_WAREHOUSE(wh_name string, wh_sz string, autoscale_min int, autoscale_max int, snowflake_tag object)
RETURNS object
AS
BEGIN
 -- TODO: ADD TAG
   let query string := 'CREATE WAREHOUSE IF NOT EXISTS ' || wh_name || ' WITH WAREHOUSE_SIZE =' || '\'' || wh_sz || '\'' || ' AUTO_SUSPEND = 1' || ' MIN_CLUSTER_COUNT =' || autoscale_min || ' MAX_CLUSTER_COUNT =' || autoscale_max || '
 INITIALLY_SUSPENDED = TRUE ';
   EXECUTE IMMEDIATE :query;
   return null;
EXCEPTION
    WHEN OTHER THEN
    RAISE;
END;
