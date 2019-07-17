-- Deploy seattleflu/schema:roles/longitudinal-uploader/create to pg

begin;

create role "longitudinal-uploader";

comment on role "longitudinal-uploader" is 'For adding new longitudinal records';

commit;
