-- Deploy seattleflu/schema:roles/kit-processor/create to pg

begin;

create role "kit-processor";

comment on role "kit-processor" is 'For kit ETL routines';

commit;
