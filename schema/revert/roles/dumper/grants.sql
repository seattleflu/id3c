-- Revert seattleflu/schema:roles/dumper/revokes from pg

begin;

set local role id3c;

revoke connect on database :"DBNAME" from dumper;

-- Existing schema, tables, and sequences
revoke usage  on                  schema sqitch, receiving, warehouse, shipping from dumper;
revoke select on all tables    in schema sqitch, receiving, warehouse, shipping from dumper;
revoke select on all sequences in schema sqitch, receiving, warehouse, shipping from dumper;

-- Future schema, tables, and sequences
alter default privileges revoke usage on schemas from dumper;
alter default privileges revoke select on tables from dumper;
alter default privileges revoke select on sequences from dumper;

commit;
