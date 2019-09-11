-- Deploy seattleflu/schema:roles/clinical-processor/grants to pg
-- requires: receiving/clinical
-- requires: roles/clinical-processor/create


begin;

-- This change is designed to be sqitch rework-able to make it easier to update
-- the grants for this role.

-- Revoke everything…
revoke all on database :"DBNAME" from "clinical-processor";
revoke all on schema receiving, warehouse from "clinical-processor";
revoke all on all tables in schema receiving, warehouse from "clinical-processor";

-- …then re-grant
grant connect on database :"DBNAME" to "clinical-processor";

grant usage
   on schema receiving, warehouse
   to "clinical-processor";

grant select
   on receiving.clinical
   to "clinical-processor";

grant update (processing_log)
   on receiving.clinical
   to "clinical-processor";

grant select
   on warehouse.identifier,
      warehouse.identifier_set,
      warehouse.sample
   to "clinical-processor";

grant update (encounter_id) 
    on warehouse.sample
    to "clinical-processor";

grant select, insert, update
    on warehouse.encounter,
       warehouse.individual
    to "clinical-processor";

grant select, insert 
    on warehouse.site 
    to "clinical-processor";

commit;
