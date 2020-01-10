-- Revert seattleflu/schema:roles/fhir-processor/grants from pg

begin;

revoke all on database :"DBNAME" from "fhir-processor";
revoke all on schema receiving, warehouse, shipping from "fhir-processor";
revoke all on all tables in schema receiving, warehouse, shipping from "fhir-processor";

-- Add additional revokes here as necessary.

commit;
