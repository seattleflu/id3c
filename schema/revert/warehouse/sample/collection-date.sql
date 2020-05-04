-- Revert seattleflu/schema:warehouse/sample/collection-date from pg

begin;

alter table warehouse.sample
    drop column collected;

commit;
