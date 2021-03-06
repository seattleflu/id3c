-- Deploy seattleflu/schema:warehouse/sample to pg
-- requires: warehouse/encounter

begin;

set local search_path to warehouse;

create table sample (
    sample_id integer primary key generated by default as identity,
    identifier text unique not null,
    encounter_id integer, -- references encounter (encounter_id)
    details jsonb
);

-- We removed the encounter_id reference to the encounter table because this relationship
--   does not exist yet in the database. We will add the relationship back in once
--   Thomas has finished with the barcode ingestion part of the pipeline.
-- Once we reference encounter, upon upserting rows without an encounter, we will have to
--   explicitly specify a NULL value for encounter.
-- We are tracking some part of the JSON document in a column for additional details. 
--   We may or may not find this column useful down the road. 

comment on table sample is 'A sample collected from an individual during a specific encounter';
comment on column sample.sample_id is 'Internal id of this sample';
comment on column sample.identifier is 'A unique external identifier assigned to this sample';
comment on column sample.encounter_id is 'The encounter where the sample was collected';
comment on column sample.details is 'Additional information about this sample which does not have a place in the relational schema';

create index sample_encounter_id_idx on sample (encounter_id);
create index sample_details_idx on sample using gin (details jsonb_path_ops);

commit;
