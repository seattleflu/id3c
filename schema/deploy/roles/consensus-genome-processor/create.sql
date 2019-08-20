-- Deploy seattleflu/schema:roles/consensus-genome-processor/create to pg

begin;

create role "consensus-genome-processor";

comment on role "consensus-genome-processor" is 'For consensus genome ETL routines';

commit;
