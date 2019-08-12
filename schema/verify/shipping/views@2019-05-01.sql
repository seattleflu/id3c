-- Verify seattleflu/schema:shipping/views on pg

begin;

set local role id3c;

select 1/(count(*) = 1)::int
  from information_schema.views
 where array[table_schema, table_name]::text[]
     = pg_catalog.parse_ident('shipping.incidence_model_observation_v1');

rollback;
