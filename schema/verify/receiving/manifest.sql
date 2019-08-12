-- Verify seattleflu/schema:receiving/manifest on pg

begin;

set local role id3c;

select pg_catalog.has_table_privilege('receiving.manifest', 'select');

rollback;
