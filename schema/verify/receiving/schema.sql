-- Verify seattleflu/schema:receiving/schema on pg

begin;

set local role id3c;

select 1/pg_catalog.has_schema_privilege('receiving', 'usage')::int;

rollback;
