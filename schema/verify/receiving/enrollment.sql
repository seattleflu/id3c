-- Verify seattleflu/schema:receiving/enrollment on pg

begin;

set local role id3c;

select pg_catalog.has_table_privilege('receiving.enrollment', 'select');

rollback;
