-- Deploy seattleflu/schema:roles/reporter to pg
-- requires: staging/schema

begin;

create role reporter;

grant connect on database :"DBNAME" to reporter;

-- Existing staging schema and tables
grant usage on schema staging to reporter;
grant select on all tables in schema staging to reporter;

-- Future schema and tables
alter default privileges grant usage on schemas to reporter;
alter default privileges grant select on tables to reporter;

commit;
