
create or replace procedure tools.warehouse_size_as_int(v VARCHAR)
    returns int
    language sql
as
declare
    INVALID_WAREHOUSE_SIZE EXCEPTION(-20511, 'No such warehouse size, valid sizes are \'X-Small\', \'Small\', \'Medium\',  \'Large\',  \'X-Large\', \'2X-Large\', \'3X-Large\', \'4X-Large\', \'5X-Large\', \'6X-Large\' ');
begin
    case (v)
      when 'X-Small'  then return 1;
      when 'Small'    then return 2;
      when 'Medium'   then return 3;
      when 'Large'    then return 4;
      when 'X-Large'  then return 5;
      when '2X-Large' then return 6;
      when '3X-Large' then return 7;
      when '4X-Large' then return 8;
      when '5X-Large' then return 9;
      when '6X-Large' then return 10;
      else raise INVALID_WAREHOUSE_SIZE;
    end;
end;

create or replace procedure tools.create_warehouses_with_sizes(prefix string, min_size string, max_size string)
    returns string
    language sql
as
declare
    INVALID_WAREHOUSE_SIZE_PARAMS EXCEPTION(-20512, 'Warehouse min_size should be less than max_size');
    INVALID_WAREHOUSE_PREFIX EXCEPTION(-20513, 'Warehouse prefix cannot be empty');
begin
    let min int := 0;
    let max int := 0;
    call warehouse_size_as_int(:min_size) INTO min;
    call warehouse_size_as_int(:max_size) INTO max;
    if (min > max) then
        raise INVALID_WAREHOUSE_SIZE_PARAMS;
    end if;
    if (length(:prefix) = 0) then
        raise INVALID_WAREHOUSE_PREFIX;
    end if;

    let sql string := 'select column1 as size, column2 as size_numeric
            from values (\'XSMALL\', 1), (\'SMALL\', 2), (\'MEDIUM\', 3), (\'LARGE\', 4), (\'XLARGE\', 5),
            (\'X2LARGE\', 6), (\'X3LARGE\', 7), (\'X4LARGE\', 8), (\'X5LARGE\', 9), (\'X6LARGE\', 10)
            WHERE size_numeric >= ? AND size_numeric <= ?';
    let rs resultset := (execute immediate sql using (min, max));

    let wh_names string := '';
    let c1 cursor for rs;
    for record in c1 DO
        let wh_name string := :prefix || '_' || record.size;
        let cmd string := 'CREATE WAREHOUSE IF NOT EXISTS "' || wh_name || '" WITH WAREHOUSE_SIZE =\'' || record.size || '\' AUTO_SUSPEND = 1 INITIALLY_SUSPENDED = TRUE';
        execute immediate :cmd;
        wh_names := wh_names || ', "' || wh_name || '"';
    end for;
    return 'Successfully created warehouses' || wh_names;
end;


create or replace procedure tools.drop_warehouses_with_sizes(prefix string, min_size string, max_size string)
    returns string
    language sql
as
declare
    INVALID_WAREHOUSE_SIZE_PARAMS EXCEPTION(-20512, 'Warehouse min_size should be less than max_size');
    INVALID_WAREHOUSE_PREFIX EXCEPTION(-20513, 'Warehouse prefix cannot be empty');
begin
    let min int := 0;
    let max int := 0;
    call warehouse_size_as_int(:min_size) INTO min;
    call warehouse_size_as_int(:max_size) INTO max;
    if (min > max) then
        raise INVALID_WAREHOUSE_SIZE_PARAMS;
    end if;
    if (length(:prefix) = 0) then
        raise INVALID_WAREHOUSE_PREFIX;
    end if;

    let sql string := 'select column1 as size, column2 as size_numeric
            from values (\'XSMALL\', 1), (\'SMALL\', 2), (\'MEDIUM\', 3), (\'LARGE\', 4), (\'XLARGE\', 5),
            (\'X2LARGE\', 6), (\'X3LARGE\', 7), (\'X4LARGE\', 8), (\'X5LARGE\', 9), (\'X6LARGE\', 10)
            WHERE size_numeric >= ? AND size_numeric <= ?';
    let rs resultset := (execute immediate sql using (min, max));

    let wh_names string := '';
    let c1 cursor for rs;
    for record in c1 DO
        let wh_name string := :prefix || '_' || record.size;
        let cmd string := 'DROP WAREHOUSE IF EXISTS "' || wh_name || '"';
        execute immediate :cmd;
        wh_names := wh_names || ', "' || wh_name || '"';
    end for;
    return 'Successfully dropped warehouses' || wh_names;
end;
