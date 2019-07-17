-- Deploy seattleflu/schema:roles/longitudinal-processor/create to pg

begin;

create role "longitudinal-processor";

comment on role "longitudinal-processor" is 'For longitudinal ETL routines';

commit;
