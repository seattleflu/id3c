-- Revert seattleflu/schema:warehouse/sample/collection-identifier from pg

begin;

set local role id3c;

alter table warehouse.sample
    drop column collection_identifier;

commit;
