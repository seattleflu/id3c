-- Revert seattleflu/schema:roles/dumper/create from pg

begin;

set local role id3c;

drop role dumper;

commit;
