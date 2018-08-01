CREATE OR REPLACE FUNCTION public.fnc_central_vacuum()
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO public
AS $function$

DECLARE
 h record;
 r record;
 t int := EXTRACT(EPOCH FROM clock_timestamp());
 l_now timestamp := now() at time zone 'UTC';
 l_sql1 text;
 l_sql2 text;
begin
  for h in select d.*, a.usern, a.pass 
           from db_cfg d 
           left join acc_info a
           on a.env_type = d.type_env 
           where d.maintain1 = true
  	loop
  			l_sql2 := 'host='||h.dbhost||' port=5432 dbname='||h.db||' user='||h.usern||' password='||h.pass;
        perform public.dblink_connect(l_sql2);
 			raise notice 'vacuumming:% %', h.dbhost, h.db;
       		l_sql1 := 'SELECT pt.schemaname || ''.''|| pt.relname AS TABLE
                    FROM pg_class pc JOIN pg_stat_all_tables pt ON pc.relname = pt.relname
             					CROSS JOIN pg_settings pgs_threshold
             					CROSS JOIN pg_settings pgs_scale
                    WHERE pt.schemaname in (select distinct schemaname from pg_stat_all_tables where schemaname not in (''public'',''pg_catalog'',''information_schema'', ''pg_toast'',''pglogical'',''utils''))
                      AND pgs_threshold.name = ''autovacuum_vacuum_threshold''
                      AND pgs_scale.name = ''autovacuum_vacuum_scale_factor''
                      AND ((pt.n_tup_del + pt.n_tup_upd) > pgs_threshold.setting::int + (pgs_scale.setting::float * pc.reltuples)) = ''t''';  
  		for r in select * from public.dblink(l_sql1) as r(tbl varchar)
    			loop  
    				 t := EXTRACT(EPOCH FROM clock_timestamp());
    				perform public.dblink('vacuum '|| r.tbl ||'');
             		insert into maintain1 ( host, db, tblvacu, execued, duration)
            		select h.dbhost, h.db, r.tbl, l_now,  EXTRACT(EPOCH FROM clock_timestamp()) - t;
      
	end loop;
perform public.dblink_disconnect();
      
end loop;
end;

$function$;
