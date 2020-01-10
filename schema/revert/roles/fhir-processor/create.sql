-- Revert seattleflu/schema:roles/fhir-processor/create from pg

begin;

drop role "fhir-processor";

commit;
