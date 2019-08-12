-- Verify seattleflu/schema:receiving/clinical on pg

begin;

set local role id3c;

select pg_catalog.has_table_privilege('receiving.clinical', 'select');

rollback;
