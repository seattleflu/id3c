-- Deploy seattleflu/schema:roles/longitudinal-processor/grants to pg
-- requires: receiving/longitudinal
-- requires: roles/longitudinal-processor/create


begin;

-- This change is designed to be sqitch rework-able to make it easier to update
-- the grants for this role.

-- Revoke everything…
revoke all on database :"DBNAME" from "longitudinal-processor";
revoke all on schema receiving, warehouse from "longitudinal-processor";
revoke all on all tables in schema receiving, warehouse from "longitudinal-processor";

-- …then re-grant
grant connect on database :"DBNAME" to "longitudinal-processor";

grant usage
   on schema receiving, warehouse
   to "longitudinal-processor";

grant select
   on receiving.longitudinal
   to "longitudinal-processor";

grant update (processing_log)
   on receiving.longitudinal
   to "longitudinal-processor";

grant select
   on warehouse.identifier,
      warehouse.identifier_set,
      warehouse.sample
   to "longitudinal-processor";

grant update (encounter_id)
    on warehouse.sample
    to "longitudinal-processor";

grant select, insert, update
    on warehouse.encounter,
       warehouse.individual
    to "longitudinal-processor";

grant select, insert
    on warehouse.site
    to "longitudinal-processor";

commit;
