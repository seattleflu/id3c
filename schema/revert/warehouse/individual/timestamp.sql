-- Revert seattleflu/schema:warehouse/individual/timestamp from pg

begin;

alter table warehouse.individual
    drop column created,
    drop column modified;

commit;
