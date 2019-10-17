-- Deploy seattleflu/schema:roles/fhir-uploader/grants to pg
-- requires: roles/fhir-uploader/create
-- requires: receiving/fhir

begin;

-- This change is designed to be sqitch rework-able to make it easier to update
-- the grants for this role.

grant connect on database :"DBNAME" to "fhir-uploader";

grant usage
   on schema receiving
   to "fhir-uploader";

grant select (fhir_id)
   on receiving.fhir
   to "fhir-uploader";

grant insert (document)
   on receiving.fhir
   to "fhir-uploader";

commit;
