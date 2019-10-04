-- Deploy seattleflu/schema:warehouse/sample/timestamp to pg
-- requires: warehouse/sample

begin;

alter table warehouse.sample
    add column created timestamp with time zone,
    add column modified timestamp with time zone;

alter table warehouse.sample
    alter column created set default now(),
    alter column modified set default now();

comment on column warehouse.sample.created is
    'When the record was first processed by ID3C';

comment on column warehouse.sample.modified is
    'When the record was last updated';

commit;
