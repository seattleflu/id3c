-- Revert seattleflu/schema:warehouse/assigned-sex from pg

begin;

set local role id3c;

drop domain warehouse.assigned_sex;

commit;
