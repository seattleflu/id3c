-- Deploy seattleflu/schema:warehouse/encounter-location/relation-fk to pg
-- requires: warehouse/encounter-location-relation

begin;

insert into warehouse.encounter_location_relation (relation)
    select distinct relation from warehouse.encounter_location
    on conflict (relation) do nothing
;

alter table warehouse.encounter_location
    add constraint encounter_location_relation_fkey
        foreign key (relation)
        references warehouse.encounter_location_relation (relation);

commit;
