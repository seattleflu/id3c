-- Revert seattleflu/schema:warehouse/target/organism from pg

begin;

alter table warehouse.target
    drop column organism_id;

commit;
