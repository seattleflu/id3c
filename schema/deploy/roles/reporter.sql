-- Deploy seattleflu/schema:roles/reporter to pg
-- requires: receiving/schema

begin;

set local role id3c;

create role reporter;

grant connect on database :"DBNAME" to reporter;

-- Existing receiving schema and tables
grant usage on schema receiving to reporter;
grant select on all tables in schema receiving to reporter;

-- Future schema and tables
alter default privileges grant usage on schemas to reporter;
alter default privileges grant select on tables to reporter;

commit;
