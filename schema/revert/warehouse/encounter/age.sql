-- Revert seattleflu/schema:warehouse/encounter/age from pg

begin;

set local role id3c;

alter table warehouse.encounter
    drop column age;

commit;
