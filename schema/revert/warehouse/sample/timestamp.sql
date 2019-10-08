-- Revert seattleflu/schema:warehouse/sample/timestamp from pg

begin;

alter table warehouse.sample
    drop column created,
    drop column modified;

commit;
