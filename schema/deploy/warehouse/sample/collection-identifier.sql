-- Deploy seattleflu/schema:warehouse/sample/collection-identifier to pg
-- requires: warehouse/sample

begin;

alter table warehouse.sample
    add column collection_identifier text unique;

comment on column warehouse.sample.collection_identifier is
    'A unique external identifier assigned to the collection of this sample';

commit;
