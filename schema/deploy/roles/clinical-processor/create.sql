-- Deploy seattleflu/schema:roles/clinical-processor/create to pg

begin;

set local role id3c;

create role "clinical-processor";

comment on role "clinical-processor" is 'For clinical ETL routines';

commit;
