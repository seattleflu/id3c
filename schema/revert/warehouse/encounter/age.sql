-- Revert seattleflu/schema:warehouse/encounter/age from pg

begin;

alter table warehouse.encounter
    drop column age;

commit;
