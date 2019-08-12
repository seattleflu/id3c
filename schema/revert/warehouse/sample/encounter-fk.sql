-- Revert seattleflu/schema:warehouse/sample/encounter-fk from pg

begin;

set local role id3c;

alter table warehouse.sample
    drop constraint sample_encounter_id_fkey;

commit;
