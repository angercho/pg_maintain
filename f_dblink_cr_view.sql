CREATE OR REPLACE FUNCTION public.f_dblink_cr_view(text, text, text)
 RETURNS void
 LANGUAGE plpgsql
 STRICT
 SET search_path TO public
AS $function$
DECLARE
  conne     ALIAS FOR $1;
  remote_name ALIAS FOR $2;
  local_name  ALIAS FOR $3;
  schema_name text;
  table_name  text;
  rec         RECORD;
  col_names   text := '';
  col_defs    text := '';
  sql_str     text;
BEGIN

  schema_name := split_part(remote_name, '.', 1);
  table_name := split_part(remote_name, '.', 2);

  FOR rec IN
    SELECT * FROM dblink(conne,
      'SELECT
          a.attname,
          format_type(a.atttypid, a.atttypmod)
        FROM
          pg_catalog.pg_class c INNER JOIN
          pg_catalog.pg_namespace n ON (c.relnamespace = n.oid) INNER JOIN
          pg_catalog.pg_attribute a ON (a.attrelid = c.oid)
        WHERE
          (n.nspname = ' || quote_literal(schema_name) ||' or n.nspname = ''public'') AND
          c.relname = ' || quote_literal(table_name) || ' AND
          a.attisdropped = false AND
          a.attnum > 0')
      AS rel (n name, t text)
  LOOP
    col_names := col_names || quote_ident(rec.n) || ',';
    col_defs  := col_defs  || quote_ident(rec.n) || ' ' || rec.t || ',';
  END LOOP;

  sql_str := 'CREATE VIEW ' || local_name ||
    ' AS SELECT * FROM dblink(' || quote_literal(conne) || ',' ||
    quote_literal('SELECT ' || trim(trailing ',' from col_names) ||
      ' FROM ' || quote_ident(schema_name) || '.' || 
quote_ident(table_name)) ||
    ') AS rel (' || trim(trailing ',' from col_defs) || ')';

  EXECUTE sql_str;
  RETURN;
END
$function$;
