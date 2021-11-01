DO $BODY$
DECLARE
	  num_s bigint;
      num_e bigint;
	  r1 text;
	  r2 text;
	  chk_cond text;
	  c_table TEXT;
	  c_table1 text;
	  m_table1 text;
      end_date date;
      v_k int;
      v_start_time date;
      v_parent_schema                 text;
      v_parent_tablename              text; 
      v_parent_tablespace             text;
      v_unlogged             text;
      v_control_type                  text;
      v_control_exact_type            text;
      v_intervel int;
      v_agg text[];
      v_list text;
      v_sql_default text;
      v_sql_default_pk text;
---
--
--
      p_parent_table text:='naga.test6';
      p_part_col text:='region' ;--partition COLUMN
      p_type text:='list';  -- partition type range,list, hash
      p_interval text:='west,north,south,east' ; --time:  daily, monthly,yearly , id : 10,1000 any range, list = 'west,north,south,east', maduler = 5,10,20 etc
      p_fk_cols text:='id'; -- constraint COLUMN
      p_uk_cols text:='id';
     -- p_constraint_type text[] DEFAULT NULL  -- constraint type PK,UK
      p_premake int:=20 ;-- no of partition tables to be created
      p_sub_partition BOOLEAN:= true;
      p_sub_part_col text:='id';
      p_sub_part_type text:='range';
      p_sub_part_interval text:='10000';
      
      --p_inherit_fk boolean DEFAULT true
      --p_epoch text DEFAULT 'none'
     -- p_upsert text DEFAULT ''
      --p_publications text[] DEFAULT NULL
      --start_date TIMESTAMP := '2021-10-27 00:00:00' ;
BEGIN


v_start_time := current_date;

-- check tables existence

SELECT n.nspname, c.relname, t.spcname, c.relpersistence
INTO v_parent_schema, v_parent_tablename, v_parent_tablespace, v_unlogged
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
LEFT OUTER JOIN pg_catalog.pg_tablespace t ON c.reltablespace = t.oid
WHERE n.nspname = split_part(p_parent_table, '.', 1)::name
AND c.relname = split_part(p_parent_table, '.', 2)::name;

set search_path = 'naga';
SELECT CASE
            WHEN typname IN ('timestamptz', 'timestamp', 'date') THEN
                'time'
            WHEN typname IN ('int2', 'int4', 'int8') THEN
                'id'
       END as _case
, typname::text INTO v_control_type, v_control_exact_type
FROM pg_catalog.pg_type t
JOIN pg_catalog.pg_attribute a ON t.oid = a.atttypid
JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
WHERE n.nspname = v_parent_schema::name
AND c.relname = v_parent_tablename::name
AND a.attname = p_part_col::name;

IF v_parent_tablename IS NULL THEN
            RAISE EXCEPTION '42P01 : Unable to find given parent table in system catalogs. Please create parent table first, Ex: CREATE TABLE % () PARTITION BY % (%);', p_parent_table,p_type,p_part_col;
END IF;

v_sql_default := FORMAT( 'CREATE TABLE  IF NOT EXISTS %s_default PARTITION OF %s default'
                ,v_parent_tablename, p_parent_table);  --  create default partition table
v_sql_default_pk := FORMAT( 'CREATE TABLE  IF NOT EXISTS %s_default PARTITION OF %s (CONSTRAINT %s_pkey PRIMARY KEY (%s))default'
                ,v_parent_tablename, p_parent_table,v_parent_tablename,p_fk_cols);  --  create default partition table

IF p_type = 'range' THEN
        IF v_control_type = 'time' then
           IF  p_interval = 'daily' THEN

               
                -- for backlog date
               SELECT current_date - interval '2 day' into v_start_time;

               for v_k in 1..p_premake loop
                     end_date= v_start_time+1;
                         r1 := v_parent_tablename||'_p'||to_char(v_start_time,'MMDD')::text;
                         IF p_fk_cols is null then
                            EXECUTE  v_sql_default;
                            EXECUTE FORMAT( 'CREATE TABLE  IF NOT EXISTS %s PARTITION OF %s FOR VALUES FROM (''%s'') TO (''%s'')',r1, p_parent_table,v_start_time,end_date);
                         ELSE  
                            EXECUTE  v_sql_default_pk;
                            EXECUTE FORMAT( 'CREATE TABLE  IF NOT EXISTS %s PARTITION OF %s ( CONSTRAINT %s_pkey PRIMARY KEY (%s) )
                                              FOR VALUES FROM (''%s'') TO (''%s'')',r1, p_parent_table,r1,p_fk_cols,v_start_time,end_date);
                         END IF;
                     v_start_time=end_date;
               end loop;
            ELSIF p_interval = 'monthly' THEN

               
                -- for backlog date
               v_start_time=to_char(v_start_time,'YYYY-MM-01');
               for v_k in 1..p_premake loop
                     end_date= v_start_time + interval '1 month';
                         r1 := v_parent_tablename||'_p'||to_char(v_start_time,'MM_DD')::text;
                         IF p_fk_cols is null then
                            EXECUTE  v_sql_default;
                            EXECUTE FORMAT( 'CREATE TABLE  IF NOT EXISTS %s PARTITION OF %s FOR VALUES FROM (''%s'') TO (''%s'')',r1, p_parent_table,v_start_time,end_date);
                         ELSE
                            EXECUTE  v_sql_default_pk;
                            EXECUTE FORMAT( 'CREATE TABLE  IF NOT EXISTS %s PARTITION OF %s ( CONSTRAINT %s_pkey PRIMARY KEY (%s) )
                                              FOR VALUES FROM (''%s'') TO (''%s'')',r1, p_parent_table,r1,p_fk_cols,v_start_time,end_date);
                         END IF;
                     v_start_time=end_date;
               end loop;
            ELSIF p_interval = 'yearly' THEN
            raise info 'Not compatable for now';
            END IF;  -- monthly
        ELSE     -- id
            v_intervel := p_interval::int;
            num_s = v_intervel;
            
            for v_k in 1..p_premake loop
                num_e=num_s+v_intervel;
                r2 = v_parent_tablename||'_p'||num_s;

                IF p_fk_cols is null then
                EXECUTE  v_sql_default;
                EXECUTE FORMAT( 'CREATE TABLE  IF NOT EXISTS %s.%s PARTITION OF %s FOR VALUES FROM (%s) TO (%s)'
                ,v_parent_schema,r2, p_parent_table,num_s,num_e);
                ELSE
                EXECUTE  v_sql_default_pk;
                EXECUTE FORMAT( 'CREATE TABLE  IF NOT EXISTS %s.%s PARTITION OF %s 
                ( CONSTRAINT %s_pkey PRIMARY KEY (%s) ) FOR VALUES FROM (%s) TO (%s)'
                ,v_parent_schema,r2, p_parent_table,r2,p_fk_cols,num_s,num_e);
                END IF;

                num_s=num_e;
         end loop;

        END IF; -- range  type end

ELSIF  p_type = 'list' THEN
  
    v_agg = string_to_array(p_interval,',');
    IF p_sub_partition is false THEN
        IF p_fk_cols is null THEN
            EXECUTE  v_sql_default;
            foreach v_list in array v_agg loop
                v_parent_tablename := p_parent_table||'_p_'||v_list;
                EXECUTE FORMAT('CREATE TABLE IF NOT exists %s PARTITION OF %s FOR VALUES IN (''%s'')',v_parent_tablename,p_parent_table,v_list);
            END LOOP;
        ELSE  
            EXECUTE  v_sql_default_pk;
        foreach v_list in array v_agg loop
                v_parent_tablename := p_parent_table||'_p_'||v_list;
                EXECUTE FORMAT('CREATE TABLE IF NOT exists %s PARTITION OF %s (CONSTRAINT %s_pkey PRIMARY KEY (%s)) FOR VALUES IN (''%s'')',v_parent_tablename,p_parent_table,v_list,p_fk_cols,v_list);

            END LOOP;
        END IF;
    ELSE
         foreach v_list in array v_agg loop
         v_parent_tablename := p_parent_table||'_p_'||v_list;
         EXECUTE FORMAT('CREATE TABLE IF NOT EXISTS %s PARTITION OF %s  FOR VALUES IN (''%s'') PARTITION BY RANGE (%s)'
         ,v_parent_tablename,p_parent_table,v_list,p_fk_cols,v_list,p_sub_part_col);
         execute FORMAT('select create_sub_partiton(''%s'',''%s'',''%s'',''%s'',''%s'',%s)'
         , v_parent_tablename ,p_sub_part_col,p_sub_part_type ,p_sub_part_interval ,p_fk_cols,p_premake);
                
                end loop;
    END IF;

ELSIF  p_type = 'hash' THEN
        v_k := p_interval::int;
        IF p_sub_partition is false THEN
            IF p_fk_cols is not null THEN
                for num_s in 0..v_k-1 loop
                execute FORMAT('CREATE TABLE IF NOT EXISTS %s_%s PARTITION OF %s( CONSTRAINT %s_%s_pkey PRIMARY KEY (%s)) 
                FOR VALUES WITH (modulus %s, remainder %s)',v_parent_tablename,num_s,p_parent_table,v_parent_tablename,num_s,p_fk_cols,v_k,num_s);

                end loop;
            ELSE
                for num_s in 0..v_k-1 loop
                execute FORMAT('CREATE TABLE IF NOT EXISTS %s_%s PARTITION OF %s 
                FOR VALUES WITH (modulus %s, remainder %s)',v_parent_tablename,num_s,p_parent_table,v_k,num_s);
                end loop;
            END IF;
        ELSE
                for num_s in 0..v_k-1 loop
                c_table :=v_parent_schema||'.'||v_parent_tablename||'_'||num_s;
                EXECUTE FORMAT('CREATE TABLE IF NOT EXISTS %s PARTITION OF %s FOR VALUES WITH (modulus %s, remainder %s) PARTITION BY RANGE (%s)'
                                                   ,c_table,p_parent_table,v_k,num_s,p_sub_part_col);
                execute FORMAT('select create_sub_partiton(''%s'',''%s'',''%s'',''%s'',''%s'',%s)'
                , c_table ,p_sub_part_col,p_sub_part_type ,p_sub_part_interval ,p_fk_cols,p_premake);
                
                end loop;

        END IF;
END IF;



END;
$BODY$;
