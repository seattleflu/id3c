-- Deploy seattleflu/schema:warehouse/sample/encounter-fk to pg
-- requires: warehouse/sample

begin;

set local role id3c;

alter table warehouse.sample
    add constraint sample_encounter_id_fkey
        foreign key (encounter_id) references warehouse.encounter (encounter_id);

commit;
