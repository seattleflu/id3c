-- Revert seattleflu/schema:warehouse/identifier/slices from pg

begin;

alter table warehouse.identifier
drop column if exists slices;

commit;
