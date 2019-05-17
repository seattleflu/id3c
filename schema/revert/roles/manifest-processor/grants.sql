-- Revert seattleflu/schema:roles/manifest-processor/grants from pg

begin;

revoke select, insert, update
    on warehouse.sample
  from "manifest-processor";

revoke select
    on warehouse.identifier,
       warehouse.identifier_set
  from "manifest-processor";

revoke update (processing_log)
    on receiving.manifest
  from "manifest-processor";

revoke select
    on receiving.manifest
  from "manifest-processor";

revoke usage
    on schema receiving, warehouse
  from "manifest-processor";

revoke connect on database :"DBNAME" from "manifest-processor";

commit;
