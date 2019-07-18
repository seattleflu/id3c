-- Verify seattleflu/schema:receiving/redcap-det on pg

begin;

select pg_catalog.has_table_privilege('receiving.redcap_det', 'select');

rollback;
