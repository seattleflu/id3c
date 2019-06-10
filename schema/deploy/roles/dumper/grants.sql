-- Deploy seattleflu/schema:roles/dumper/grants to pg
-- requires: roles/dumper/create

begin;

-- This change is designed to be sqitch rework-able to make it easier to update
-- the grants for this role.

grant connect on database :"DBNAME" to dumper;

-- Existing schema, tables, and sequences
grant usage  on                  schema sqitch, receiving, warehouse, shipping to dumper;
grant select on all tables    in schema sqitch, receiving, warehouse, shipping to dumper;
grant select on all sequences in schema sqitch, receiving, warehouse, shipping to dumper;

-- Future schema, tables, and sequences
alter default privileges grant usage on schemas to dumper;
alter default privileges grant select on tables to dumper;
alter default privileges grant select on sequences to dumper;

commit;
