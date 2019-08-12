-- Revert seattleflu/schema:roles/presence-absence-processor from pg

begin;

set local role id3c;

revoke select
    on warehouse.identifier,
       warehouse.identifier_set
  from "presence-absence-processor";

revoke select, insert, update
    on warehouse.sample,
       warehouse.target,
       warehouse.presence_absence
  from "presence-absence-processor";

revoke update (processing_log)
    on receiving.presence_absence
  from "presence-absence-processor";

revoke select
    on receiving.presence_absence
  from "presence-absence-processor";

revoke usage
    on schema receiving, warehouse
  from "presence-absence-processor";

revoke connect on database :"DBNAME" from "presence-absence-processor";

drop role "presence-absence-processor";

commit;
