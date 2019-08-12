-- Revert seattleflu/schema:roles/reporter/grant-select-on-sequences from pg

begin;

set local role id3c;

revoke select on all sequences in schema receiving, warehouse, shipping from reporter;
alter default privileges revoke select on sequences from reporter;

commit;
