-- Deploy seattleflu/schema:roles/clinical-processor/create to pg

begin;

create role "clinical-processor";

comment on role "clinical-processor" is 'For clinical ETL routines';

commit;
