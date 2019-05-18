-- Revert seattleflu/schema:roles/enrollment-processor/grants from pg

begin;

revoke select, insert, update
    on warehouse.site,
       warehouse.individual,
       warehouse.encounter
  from "enrollment-processor";

revoke update (processing_log)
    on receiving.enrollment
  from "enrollment-processor";

revoke select
    on receiving.enrollment
  from "enrollment-processor";

revoke usage
    on schema receiving, warehouse
  from "enrollment-processor";

revoke connect on database :"DBNAME" from "enrollment-processor";

commit;
