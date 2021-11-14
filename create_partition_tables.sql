-- FUNCTION: public.create_partiton(text, text, text, text, integer, text, boolean, text, text, text)

-- DROP FUNCTION IF EXISTS public.create_partiton(text, text, text, text, integer, text, boolean, text, text, text);

CREATE OR REPLACE FUNCTION naga.create_partiton(
	p_parent_table text,
	p_part_col text,
	p_type text,
	p_interval text,
	p_premake integer,
	p_fk_cols text DEFAULT NULL::text,
	p_sub_partition boolean DEFAULT false,
	p_sub_part_col text DEFAULT NULL::text,
	p_sub_part_type text DEFAULT NULL::text,
	p_sub_part_interval text DEFAULT NULL::text)
    RETURNS text
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE

      num_s bigint;
      num_e bigint;
      chk_cond text;
	  chk_boolean boolean;
	  v_partition_col text;
      end_date date;
      v_int int;
      v_start_time date;
      v_parent_schema   text;
      v_parent_tablename  text; 
      v_parent_tablespace   text;
      v_unlogged             text;
      v_control_type         text;
      v_control_exact_type    text;
      v_intervel int;
      v_agg text[];
      v_list text;
      v_sql_default text;
      v_sql_default_pk text;
      v_new_tablename text;
      v_fun_schema text;
      v_sub_func_name text;
	  v_sub_func_args text;
      v_stack text; 
      v_fcesig text;
      v_function_oid oid;
      
BEGIN

v_start_time := current_date;

GET DIAGNOSTICS v_stack = PG_CONTEXT;
        v_fcesig := substring(v_stack from 'function (.*?) line');
        v_function_oid := v_fcesig::regprocedure::oid;
 SELECT routine_schema INTO v_fun_schema FROM information_schema.routines WHERE regexp_replace(specific_name, '^.+?([^_]+)$', '\1')::int = v_function_oid;
 

v_sql_default := FORMAT( 'CREATE TABLE  IF NOT EXISTS %s_default PARTITION OF %s default'
                ,v_parent_tablename, p_parent_table);  --  create default partition table
v_sql_default_pk := FORMAT( 'CREATE TABLE  IF NOT EXISTS %s_default PARTITION OF %s (CONSTRAINT %s_pkey PRIMARY KEY (%s))default'
                ,v_parent_tablename, p_parent_table,v_parent_tablename,p_fk_cols);  --  create default partition table

-- check tables existence

SELECT n.nspname, c.relname, t.spcname, c.relpersistence
INTO v_parent_schema, v_parent_tablename, v_parent_tablespace, v_unlogged
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
LEFT OUTER JOIN pg_catalog.pg_tablespace t ON c.reltablespace = t.oid
WHERE n.nspname = split_part(p_parent_table, '.', 1)::name
AND c.relname = split_part(p_parent_table, '.', 2)::name;

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
            RAISE EXCEPTION 'Unable to find given parent table in system catalogs. Please create parent table first, Ex: CREATE TABLE % () PARTITION BY % (%);', p_parent_table,p_type,p_part_col;
END IF;

select partition_type,partition_key into chk_cond,v_partition_col from (
select c.relnamespace::regnamespace::text as schema,
       c.relname as table_name, 
	   TRIM(split_part(pg_get_partkeydef(c.oid), '(', 1)) as partition_type,
	   substring(pg_get_partkeydef(c.oid), '\((.+)\)') partition_key
from   pg_class c
where  c.relkind = 'p') as dt
where schema = v_parent_schema::name
and table_name = v_parent_tablename::name ;

chk_cond:=lower(chk_cond);
p_type:=lower(p_type);

IF p_type not like chk_cond  then

RAISE EXCEPTION 'Parent table is  partitioned with %. p_type values must be %.!!',chk_cond,chk_cond;
end if;


IF v_control_type = 'date' AND p_interval not in ('daily', 'monthly','yearly') then
RAISE EXCEPTION 'This is date range partition. Accepatable interval values for p_interval : daily, monthly,yearly';
end if;


IF v_control_type = 'id' and  p_interval ~ '^[0-9]+$' != true  then
RAISE EXCEPTION 'This is ID range partition. Accepatable interval values for p_interval values should be numbers. Ex: 1000,2000,3000';
end if;

IF p_type = 'hash' and  p_interval ~ '^[0-9]+$' != true  then
RAISE EXCEPTION 'This is HASH partition. Accepatable value''s for p_interval is numeric number. Ex: 5 or 10 or 20 or 30, based how many hash paritioned tables required.';
end if;


SELECT n.nspname||'.'||p.proname as "Name",
  pg_catalog.pg_get_function_arguments(p.oid) as "Argument_data_types" INTO v_sub_func_name,v_sub_func_args
FROM pg_catalog.pg_proc p
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
where p.proname = 'create_sub_partiton' and n.nspname = v_fun_schema::name;


IF p_sub_partition is TRUE and v_sub_func_name is null THEN
RAISE EXCEPTION 'Sub-partition function does not exist or created in diffenet schema. create_partiton() and create_sub_partiton() should be exists and in same schema 
HINT: Parent function is in  ''%'' schema',v_fun_schema;
END IF;


IF p_type = 'range' THEN
        IF v_control_type = 'time' then
           IF  p_interval = 'daily' THEN
                -- for backlog date
               SELECT current_date - interval '1 day' into v_start_time;
                SELECT current_date into v_start_time;
                
               for v_int in 1..p_premake loop
                     end_date= v_start_time+1;
                         v_new_tablename := v_parent_tablename||'_p'||to_char(v_start_time,'MM_DD')::text;
                         IF p_fk_cols is null then
                            EXECUTE  v_sql_default;
                            EXECUTE FORMAT( 'CREATE TABLE  IF NOT EXISTS %s PARTITION OF %s FOR VALUES FROM (%L) TO (%L)',v_new_tablename, p_parent_table,v_start_time,end_date);
                         ELSE  
                            EXECUTE  v_sql_default_pk;
                            EXECUTE FORMAT( 'CREATE TABLE  IF NOT EXISTS %s PARTITION OF %s ( CONSTRAINT %s_pkey PRIMARY KEY (%s) )
                                              FOR VALUES FROM (%L) TO (%L)',v_new_tablename, p_parent_table,v_new_tablename,p_fk_cols,v_start_time,end_date);
                         END IF;
                     v_start_time=end_date;
               end loop;
            
            ELSIF p_interval = 'monthly' THEN

                -- for backlog date
                SELECT current_date - interval '2 month' into v_start_time;
                
               v_start_time=to_char(v_start_time,'YYYY-MM-01');

               IF p_sub_partition IS FALSE THEN
               
                   for v_int in 1..p_premake loop
                         end_date= v_start_time + interval '1 month';
                             v_new_tablename := p_parent_table||'_p'||to_char(v_start_time,'YYYY_MM')::text;
                             IF p_fk_cols is null then
                                EXECUTE  v_sql_default;
							
                                EXECUTE FORMAT( 'CREATE TABLE  IF NOT EXISTS %s PARTITION OF %s FOR VALUES FROM (%L) TO (%L)',v_new_tablename, p_parent_table,v_start_time,end_date);
                             ELSE
                                EXECUTE  v_sql_default_pk;
                                EXECUTE FORMAT( 'CREATE TABLE  IF NOT EXISTS %s PARTITION OF %s ( CONSTRAINT %s_pkey PRIMARY KEY (%s) )
                                                  FOR VALUES FROM (%L) TO (%L)',v_new_tablename, p_parent_table,v_new_tablename,p_fk_cols,v_start_time,end_date);
                             END IF;
                         v_start_time=end_date;
                   end loop;

                ELSE

                      for v_int in 1..p_premake loop
                         end_date= v_start_time + interval '1 month';
                             v_new_tablename  := p_parent_table||'_p'||to_char(v_start_time,'YYYY_MM')::text;
                             
                                EXECUTE  v_sql_default_pk;
                                raise info '%',v_fun_schema;
                                EXECUTE FORMAT( 'CREATE TABLE  IF NOT EXISTS %s PARTITION OF %s 
                                                  FOR VALUES FROM (%L) TO (%L) PARTITION BY %s (%s)',v_new_tablename, p_parent_table,v_start_time,end_date,p_sub_part_type,p_sub_part_col);
                                 execute FORMAT('select %s(%L,%L,%L,%L,%L,%s)'
                                                                                    ,v_sub_func_name
                                                                                    , v_new_tablename 
                                                                                    ,p_sub_part_col
                                                                                    ,p_sub_part_type 
                                                                                    ,p_sub_part_interval 
                                                                                    ,p_fk_cols
                                                                                    ,p_premake);
                                                                                        
                         v_start_time=end_date;
                   end loop;

                END IF;
                   
               
            ELSIF p_interval = 'yearly' THEN
            raise info 'Not compatable for now';
            END IF;  -- monthly
        ELSE     -- id
            v_intervel := p_interval::int;
            num_s = v_intervel;
            
            IF p_sub_partition IS FALSE THEN
                    for v_int in 1..p_premake loop
                        num_e=num_s+v_intervel;
                        v_new_tablename = p_parent_table||'_p'||num_s;

                        IF p_fk_cols is null then
                        EXECUTE  v_sql_default;
                        EXECUTE FORMAT( 'CREATE TABLE  IF NOT EXISTS %s PARTITION OF %s FOR VALUES FROM (%s) TO (%s)'
                        ,v_new_tablename, p_parent_table,num_s,num_e);
                        ELSE
                        EXECUTE  v_sql_default_pk;
                        EXECUTE FORMAT( 'CREATE TABLE  IF NOT EXISTS %s PARTITION OF %s 
                        ( CONSTRAINT %s_pkey PRIMARY KEY (%s) ) FOR VALUES FROM (%s) TO (%s)'
                        ,v_new_tablename, p_parent_table,v_new_tablename,p_fk_cols,num_s,num_e);
                        END IF;

                        num_s=num_e;
                 end loop;
            ELSE

                        for v_int in 1..p_premake loop
                        num_e=num_s+v_intervel;
                        v_new_tablename = p_parent_table ||'_p'||num_s;

                        -- EXECUTE  v_sql_default;
                        EXECUTE FORMAT( 'CREATE TABLE  IF NOT EXISTS %s PARTITION OF %s FOR VALUES FROM (%s) TO (%s) PARTITION BY %s (%s)'
                        ,v_new_tablename, p_parent_table,num_s,num_e,p_sub_part_type,p_sub_part_col);

                        execute FORMAT('select %s(%L,%L,%L,%L,%L,%s)'
                        , v_sub_func_name
                        , v_new_tablename 
                        ,p_sub_part_col
                        ,p_sub_part_type 
                        ,p_sub_part_interval 
                        ,p_fk_cols
                        ,p_premake);
        

                        num_s=num_e;
                 end loop;

            END IF;

        END IF; -- range  type end

ELSIF  p_type = 'list' THEN
  
    v_agg = string_to_array(p_interval,',');
    IF p_sub_partition is false THEN
        IF p_fk_cols is null THEN
            EXECUTE  v_sql_default;
            foreach v_list in array v_agg loop
                v_parent_tablename := p_parent_table||'_p_'||lower(v_list);
                EXECUTE FORMAT('CREATE TABLE IF NOT exists %s PARTITION OF %s FOR VALUES IN (%L)',v_parent_tablename,p_parent_table,v_list);
            END LOOP;
        ELSE  
            EXECUTE  v_sql_default_pk;
        foreach v_list in array v_agg loop
                v_parent_tablename := p_parent_table||'_p_'||lower(v_list);
                EXECUTE FORMAT('CREATE TABLE IF NOT exists %s PARTITION OF %s (CONSTRAINT %s_pkey PRIMARY KEY (%s)) FOR VALUES IN (%L)',v_parent_tablename,p_parent_table,v_list,p_fk_cols,v_list);

            END LOOP;
        END IF;
    ELSE
         foreach v_list in array v_agg loop
         v_parent_tablename := p_parent_table||'_p_'||lower(v_list);
         EXECUTE FORMAT('CREATE TABLE IF NOT EXISTS %s PARTITION OF %s  FOR VALUES IN (%L) PARTITION BY %s (%s)'
         ,v_parent_tablename,p_parent_table,v_list,p_fk_cols,v_list,p_sub_part_type,p_sub_part_col);
         
         execute FORMAT('select %s(%L,%L,%L,%L,%L,%s)'
                        ,v_sub_func_name
                        ,v_parent_tablename 
                        ,p_sub_part_col
                        ,p_sub_part_type 
                        ,p_sub_part_interval 
                        ,p_fk_cols
                        ,p_premake);
        
                end loop;
                
    END IF;

ELSIF  p_type = 'hash' THEN
        v_int := p_interval::int;
        IF p_sub_partition is false THEN
            IF p_fk_cols is not null THEN
                for num_s in 0..v_int-1 loop
                execute FORMAT('CREATE TABLE IF NOT EXISTS %s_%s PARTITION OF %s( CONSTRAINT %s_%s_pkey PRIMARY KEY (%s)) 
                FOR VALUES WITH (modulus %s, remainder %s)',v_parent_tablename,num_s,p_parent_table,v_parent_tablename,num_s,p_fk_cols,v_int,num_s);

                end loop;
            ELSE
                for num_s in 0..v_int-1 loop
                execute FORMAT('CREATE TABLE IF NOT EXISTS %s_%s PARTITION OF %s 
                FOR VALUES WITH (modulus %s, remainder %s)',v_parent_tablename,num_s,p_parent_table,v_int,num_s);
                end loop;
            END IF;
        ELSE
                for num_s in 0..v_int-1 loop
                v_new_tablename :=v_parent_schema||'.'||v_parent_tablename||'_'||num_s;
                EXECUTE FORMAT('CREATE TABLE IF NOT EXISTS %s PARTITION OF %s FOR VALUES WITH (modulus %s, remainder %s) PARTITION BY %s (%s)'
                                                   ,v_new_tablename,p_parent_table,v_int,num_s,p_sub_part_type,p_sub_part_col);
                execute FORMAT('select %s(%L,%L,%L,%L,%L,%s)'
                ,v_sub_func_name, v_new_tablename ,p_sub_part_col,p_sub_part_type ,p_sub_part_interval ,p_fk_cols,p_premake);
                
                end loop;

        END IF;
END IF;

RETURN 'Partition creation done';
END;
$BODY$;
