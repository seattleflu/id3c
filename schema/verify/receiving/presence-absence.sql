-- Verify seattleflu/schema:receiving/presence-absence on pg

begin;

select pg_catalog.has_table_privilege('receiving.presence_absence', 'select');

rollback;
