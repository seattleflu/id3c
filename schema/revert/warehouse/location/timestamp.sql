-- Revert seattleflu/schema:warehouse/location/timestamp from pg

begin;

alter table warehouse.location
    drop column created,
    drop column modified;

commit;
