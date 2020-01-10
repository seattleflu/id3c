-- Deploy seattleflu/schema:roles/fhir-processor/grants to pg

begin;

-- This change is designed to be sqitch rework-able to make it easier to update
-- the grants for this role.

-- First, revoke everything…
revoke all on database :"DBNAME" from "fhir-processor";
revoke all on schema receiving, warehouse, shipping from "fhir-processor";
revoke all on all tables in schema receiving, warehouse, shipping from "fhir-processor";

-- Add additional revokes here if you add grants to other schemas or different
-- kinds of database objects below.


-- …then re-grant from scratch.
grant connect on database :"DBNAME" to "fhir-processor";

grant usage
   on schema receiving, warehouse
   to "fhir-processor";

grant select
   on receiving.fhir
   to "fhir-processor";

grant update (processing_log)
   on receiving.fhir
   to "fhir-processor";

grant select
   on warehouse.identifier,
      warehouse.identifier_set
   to "fhir-processor";

grant select, insert
   on warehouse.site,
      warehouse.target
   to "fhir-processor";

grant select, insert, update
   on warehouse.individual,
      warehouse.encounter,
      warehouse.encounter_location,
      warehouse.location,
      warehouse.sample,
      warehouse.presence_absence
   to "fhir-processor";

commit;
