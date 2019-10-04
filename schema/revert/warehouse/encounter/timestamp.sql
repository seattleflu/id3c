-- Revert seattleflu/schema:warehouse/encounter/timestamp from pg

begin;

alter table warehouse.encounter
    drop column created,
    drop column modified;

commit;
