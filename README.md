# pg_partition


Here two steps to create partition using these functions.

1. Create function `sub_partition.sql`
2. Execute function `create_partition_tables.sql` with values.

```
      p_parent_table text:='public.actvty_details'; -- parent table
      p_part_col text:='actvty_dt' ; --partition COLUMN
      p_type text:='range';  -- partition type range,list, hash
      p_interval text:='monthly' ; --time/date:  daily, monthly,yearly , id : 10,1000 any range, list = 'west,north,south,east', maduler = 5,10,20 etc
      p_fk_cols text:=null; -- constraint COLUMN, null or 'id'
      p_premake int:=4;-- no of partition tables to be created
/****************************************/ 
/******* sub-partition ******************/   
/****************************************/   
      p_sub_partition BOOLEAN:= true;
      p_sub_part_col text:='actvty_rgn'; --partition COLUMN
      p_sub_part_type text:='list';  -- partition type range,list, hash
      p_sub_part_interval text:='E,NE,N,NW,W,SW,S,SE';
```
