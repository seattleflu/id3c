-- Deploy seattleflu/schema:warehouse/sample/collection-date to pg
-- requires: warehouse/sample

begin;

alter table warehouse.sample
    add column collected date;

comment on column warehouse.sample.collected is
    'Date when the sample was collected';

commit;
