-- Deploy seattleflu/schema:roles/clinical-uploader/create to pg

begin;

set local role id3c;

create role "clinical-uploader";

comment on role "clinical-uploader" is 'For adding new clinical records';

commit;
