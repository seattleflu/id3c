-- Revert seattleflu/schema:warehouse/target/organism from pg

begin;

set local role id3c;

alter table warehouse.target
    drop column organism_id;

commit;
