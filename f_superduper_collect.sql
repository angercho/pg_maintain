CREATE OR REPLACE FUNCTION public.f_superduper_collect(text, text)
 RETURNS void
 LANGUAGE plpgsql
 STRICT
 SET search_path TO public
AS $function$
declare
schh     ALIAS FOR $1;
tbll	 ALIAS FOR $2;
schtbl text;
tblname text;
tblmap text;
seqname text;
 host_var text;
 db_var  text ;
 usern_var text;
 pass_var  text;
 viewn_var text;
 sql_str text;
 frotbl text;
start_time  TIMESTAMP WITHOUT TIME ZONE := clock_timestamp();

begin
	schtbl:= (schh||'.'||tbll);
	tblname:= ('coll_'||tbll);
	tblmap:= ('coll_map_'||tbll);
	seqname:= (tblmap||'_sno_seq');
	raise notice'Query start execution time: %',(clock_timestamp() - start_time);

for db_var in select db from db_cfg where mon_1 = true

	loop
			select dbhost into host_var from db_cfg where db = ''||db_var||'' and mon_1 = true;
			 	select a.usern into usern_var from acc_info as a right join db_cfg as d on a.env_type=d.type_env where d.dbhost=''||host_var||'' and d.db=''||db_var||'';
				select a.pass into pass_var from acc_info as a right join db_cfg as d on a.env_type=d.type_env where d.dbhost=''||host_var||'' and d.db=''||db_var||'';
                frotbl:= (host_var||'.'||db_var);
				raise notice 'Value db_var: %', db_var;
				raise notice 'Value host_var: %', host_var;

perform f_dblink_cr_view('host='||host_var||' dbname='||db_var||' user='||usern_var||' password='||pass_var||'', ''||schtbl||'', 'view_temp_1');

sql_str := 'create table if not exists '||tblname||' as select * from view_temp_1';
execute sql_str;
sql_str := 'CREATE SEQUENCE if not exists '||seqname;
execute sql_str;
sql_str := 'create table if not exists '||tblmap||'  (sno integer PRIMARY KEY DEFAULT nextval('''||seqname||'''), host varchar(30) NULL, exe_time timestamp NULL)';
execute sql_str;
sql_str := 'ALTER TABLE '||tblname||' ADD COLUMN IF NOT EXISTS sno int REFERENCES '||tblmap||' (sno)';
execute sql_str;

sql_str := 'WITH i AS ( insert into '||tblname||' SELECT *,(select nextval('''||seqname||'''))+1 from view_temp_1 returning *)
INSERT into '||tblmap||' SELECT nextval('''||seqname||'''), '''||frotbl||''' , now() from i';
execute sql_str;
				raise notice'Query stop execution time: %',(clock_timestamp() - start_time);

drop view view_temp_1;
end loop;
end;

$function$;
