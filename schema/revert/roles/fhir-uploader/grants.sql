-- Revert seattleflu/schema:roles/fhir-uploader/grants from pg

begin;

revoke insert (document)
    on receiving.fhir
  from "fhir-uploader";

revoke select (fhir_id)
    on receiving.fhir
  from "fhir-uploader";

revoke usage
    on schema receiving
  from "fhir-uploader";

revoke connect on database :"DBNAME" from "fhir-uploader";

commit;
