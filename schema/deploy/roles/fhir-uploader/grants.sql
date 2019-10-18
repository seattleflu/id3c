-- Deploy seattleflu/schema:roles/fhir-uploader/grants to pg
-- requires: roles/fhir-uploader/create
-- requires: receiving/fhir
-- requires: warehouse/schema
-- requires: shipping/schema

begin;

-- This change is designed to be sqitch rework-able to make it easier to update
-- the grants for this role.

-- Revoke everything…
revoke all on database :"DBNAME" from "fhir-uploader";
revoke all on schema receiving, warehouse, shipping from "fhir-uploader";
revoke all on all tables in schema receiving, warehouse, shipping from "fhir-uploader";

-- …then re-grant
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
