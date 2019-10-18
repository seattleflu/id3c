-- Deploy seattleflu/schema:roles/fhir-uploader/create to pg

begin;

create role "fhir-uploader";

comment on role "fhir-uploader" is 'For uploading FHIR documents';

commit;
