-- Deploy seattleflu/schema:roles/manifest-processor/create to pg

begin;

set local role id3c;

create role "manifest-processor";

comment on role "manifest-processor" is 'For manifest ETL routines';

commit;
