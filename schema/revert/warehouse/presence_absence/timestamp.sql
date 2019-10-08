-- Revert seattleflu/schema:warehouse/presence_absence/timestamp from pg

begin;

alter table warehouse.presence_absence
    drop column created,
    drop column modified;

commit;
