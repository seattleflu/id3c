-- Deploy seattleflu/schema:roles/redcap-det-processor/grants to pg

begin;

-- This change is designed to be sqitch rework-able to make it easier to update
-- the grants for this role.

-- First, revoke everything…
revoke all on database :"DBNAME" from "redcap-det-processor";
revoke all on schema receiving, warehouse, shipping from "redcap-det-processor";
revoke all on all tables in schema receiving, warehouse, shipping from "redcap-det-processor";

-- Add additional revokes here if you add grants to other schemas or different
-- kinds of database objects below.


-- …then re-grant from scratch.
grant connect on database :"DBNAME" to "redcap-det-processor";

grant usage
   on schema receiving, warehouse
   to "redcap-det-processor";

grant select
   on receiving.redcap_det
   to "redcap-det-processor";

grant update (processing_log)
   on receiving.redcap_det
   to "redcap-det-processor";

grant insert (document)
   on receiving.fhir
   to "redcap-det-processor";

grant select (fhir_id)
   on receiving.fhir
   to "redcap-det-processor";

grant select
   on warehouse.location
   to "redcap-det-processor";

commit;
