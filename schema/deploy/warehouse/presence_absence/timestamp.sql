-- Deploy seattleflu/schema:warehouse/presence_absence/timestamp to pg
-- requires: warehouse/presence_absence

begin;

alter table warehouse.presence_absence
    add column created timestamp with time zone,
    add column modified timestamp with time zone;

alter table warehouse.presence_absence
    alter column created set default now(),
    alter column modified set default now();

comment on column warehouse.presence_absence.created is
    'When the record was first processed by ID3C';

comment on column warehouse.presence_absence.modified is
    'When the record was last updated';

commit;
