-- Verify seattleflu/schema:receiving/kit-result on pg

begin;

select pg_catalog.has_table_privilege('receiving.kit_result', 'select');

rollback;
