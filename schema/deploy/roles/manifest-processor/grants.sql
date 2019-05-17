-- Deploy seattleflu/schema:roles/manifest-processor/grants to pg
-- requires: roles/manifest-processor/create
-- requires: receiving/manifest
-- requires: warehouse/sample
-- requires: warehouse/identifier

begin;

-- This change is designed to be sqitch rework-able to make it easier to update
-- the grants for this role.

grant connect on database :"DBNAME" to "manifest-processor";

grant usage
   on schema receiving, warehouse
   to "manifest-processor";

grant select
   on receiving.manifest
   to "manifest-processor";

grant update (processing_log)
   on receiving.manifest
   to "manifest-processor";

grant select
   on warehouse.identifier,
      warehouse.identifier_set
   to "manifest-processor";

grant select, insert, update
   on warehouse.sample
   to "manifest-processor";

commit;
