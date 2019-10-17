-- Revert seattleflu/schema:roles/fhir-uploader/create from pg

begin;

drop role "fhir-uploader";

commit;
