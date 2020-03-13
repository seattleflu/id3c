-- Deploy seattleflu/schema:warehouse/primary-encounter-location to pg
-- requires: warehouse/encounter-location/relation-fk

begin;

create or replace view warehouse.primary_encounter_location as
    select distinct on (encounter_id)
        encounter_location.*
    from
        warehouse.encounter_location
        join warehouse.encounter_location_relation using (relation)
    order by
        encounter_id,
        priority nulls last
;

comment on view warehouse.primary_encounter_location is
    'The, broadly speaking, "most important" location related to an encounter when there is more than one.  Uses priorities from warehouse.encounter_location_relation.';


commit;
