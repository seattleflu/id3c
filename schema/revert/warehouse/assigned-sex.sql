-- Revert seattleflu/schema:warehouse/assigned-sex from pg

begin;

drop domain warehouse.assigned_sex;

commit;
