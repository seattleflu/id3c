-- Deploy seattleflu/schema:roles/redcap-det-uploader/create to pg

begin;

create role "redcap-det-uploader";

comment on role "redcap-det-uploader" is 'For adding new REDCap data entry trigger records';

commit;
