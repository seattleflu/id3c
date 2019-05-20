-- Revert seattleflu/schema:warehouse/sample/collection-identifier from pg

begin;

alter table warehouse.sample
    drop column collection_identifier;

commit;
