-- Revert seattleflu/schema:roles/longitudinal-processor/grants from pg


begin;

revoke select, insert
    on warehouse.site
  from "longitudinal-processor";

revoke select, insert, update
    on warehouse.encounter,
       warehouse.individual
  from "longitudinal-processor";

revoke update (encounter_id)
    on warehouse.sample
  from "longitudinal-processor";

revoke select
    on warehouse.identifier,
       warehouse.identifier_set,
       warehouse.sample
  from "longitudinal-processor";

revoke update (processing_log)
    on receiving.longitudinal
  from "longitudinal-processor";

revoke select
    on receiving.longitudinal
  from "longitudinal-processor";

revoke usage
    on schema receiving, warehouse
  from "longitudinal-processor";

revoke connect on database :"DBNAME" from "longitudinal-processor";

commit;
