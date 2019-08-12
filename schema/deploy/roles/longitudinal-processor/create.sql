-- Deploy seattleflu/schema:roles/longitudinal-processor/create to pg

begin;

set local role id3c;

create role "longitudinal-processor";

comment on role "longitudinal-processor" is 'For longitudinal ETL routines';

commit;
