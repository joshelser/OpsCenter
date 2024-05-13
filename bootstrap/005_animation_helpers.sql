CREATE OR REPLACE FUNCTION tools.wh_fill(decayTickCount float, ordinalCount float, positions ARRAY, sizes ARRAY)  
RETURNS STRING
LANGUAGE JAVASCRIPT
COMMENT='Collects a list of query executions by tick and combines them into a single string indiciating executions and size status.'
AS $$
  lastKnownChar = '-';
  ordinalCount = parseInt(ORDINALCOUNT, 10);
  lastPos = POSITIONS[POSITIONS.length - 1];
  sizeMap = {};
  for (i = 0; i < POSITIONS.length; i++) {
    sizeMap[POSITIONS[i]] = SIZES[i];
  }
  result = Array(ordinalCount);

  lastUpdate = -1;
  for (var i = 0; i <= ordinalCount; i++) {
    if (sizeMap.hasOwnProperty(i)) {
      lastKnownChar = sizeMap[i];
      lastUpdate = i;
      result[i] = lastKnownChar.toUpperCase();
    } else {
      result[i] = lastKnownChar;
      
    }
    if((lastUpdate+DECAYTICKCOUNT) < i && i > lastPos) {
        lastKnownChar = '-';
    }
    
  }
  return result.join('');
$$;


CREATE OR REPLACE FUNCTION TOOLS.wh_size(warehouse_size varchar)
RETURNS STRING
COMMENT='Converts Snowflake warehouse size (e.g. X-Small) to a letter between a and j with a being smallest and j being biggest.'
AS
$$
case
    when warehouse_size = 'X-Small' then 'a'
    when warehouse_size = 'Small' then 'b'
    when warehouse_size = 'Medium' then 'c'
    when warehouse_size = 'Large' then 'd'
    when warehouse_size = 'X-Large' then 'e'
    when warehouse_size = '2X-Large' then 'f'
    when warehouse_size = '3X-Large' then 'g'
    when warehouse_size = '4X-Large' then 'h'
    when warehouse_size = '5X-Large' then 'i'
    when warehouse_size = '6X-Large' then 'j'
    else null
END
$$;
