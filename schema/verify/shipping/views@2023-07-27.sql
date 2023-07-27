-- Verify seattleflu/schema:shipping/views on pg

begin;

select 1/(count(*) = 1)::int
  from information_schema.views
 where array[table_schema, table_name]::text[]
     = pg_catalog.parse_ident('shipping.presence_absence_result_v1');

select 1/(count(*) = 1)::int
  from information_schema.views
 where array[table_schema, table_name]::text[]
     = pg_catalog.parse_ident('shipping.presence_absence_result_v2');

rollback;
