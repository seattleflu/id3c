-- Deploy seattleflu/schema:roles/clinical-uploader/create to pg

begin;

create role "clinical-uploader";

comment on role "clinical-uploader" is 'For adding new clinical records';

commit;
