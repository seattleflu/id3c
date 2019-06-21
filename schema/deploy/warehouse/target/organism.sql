-- Deploy seattleflu/schema:warehouse/target/organism to pg
-- requires: warehouse/target
-- requires: warehouse/organism

begin;

alter table warehouse.target
    add column organism_id integer references warehouse.organism (organism_id);

comment on column warehouse.target.organism_id is
    'Organism detected by this target; most-specific available';

create index target_organism_id_idx on warehouse.target (organism_id);

commit;
