-- Deploy seattleflu/schema:roles/manifest-uploader/create to pg

begin;

create role "manifest-uploader";

comment on role "manifest-uploader" is 'For adding new manifest records';

commit;
