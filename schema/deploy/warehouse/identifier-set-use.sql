-- Deploy seattleflu/schema:warehouse/identifier-set-use to pg
-- requires: warehouse/schema
-- requires: citext

begin;

create table warehouse.identifier_set_use (
    use citext primary key,
    description text
);

comment on table warehouse.identifier_set_use is
    'Controlled vocabulary for warehouse.identifier_set.use';

comment on column warehouse.identifier_set_use.use is
    'A standard identifier use type, e.g. sample, collection, clia';

comment on column warehouse.encounter_location_relation.priority is
    'A plain text description of this identifier use type';

commit;
