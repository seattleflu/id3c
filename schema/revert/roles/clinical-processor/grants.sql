-- Revert seattleflu/schema:roles/clinical-processor/grants from pg


begin;

revoke select, insert
    on warehouse.site
  from "clinical-processor";

revoke select, insert, update
    on warehouse.encounter,
       warehouse.individual
  from "clinical-processor";

revoke update (encounter_id)
    on warehouse.sample
  from "clinical-processor";

revoke select
    on warehouse.identifier,
       warehouse.identifier_set,
       warehouse.sample
  from "clinical-processor";

revoke update (processing_log)
    on receiving.clinical
  from "clinical-processor";

revoke select
    on receiving.clinical
  from "clinical-processor";

revoke usage
    on schema receiving, warehouse
  from "clinical-processor";

revoke connect on database :"DBNAME" from "clinical-processor";

commit;
