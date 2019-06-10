-- Revert seattleflu/schema:roles/dumper/create from pg

begin;

drop role dumper;

commit;
