-- Deploy seattleflu/schema:roles/enrollment-processor/grants to pg
-- requires: roles/enrollment-processor
-- requires: roles/enrollment-processor/rename
-- requires: warehouse/identifier
-- requires: warehouse/sample
-- requires: warehouse/sample/collection-identifier
-- requires: warehouse/sample/encounter-fk

begin;

-- This change is designed to be sqitch rework-able to make it easier to update
-- the grants for this role.

-- Revoke everything…
revoke all on database :"DBNAME" from "enrollment-processor";
revoke all on schema receiving, warehouse from "enrollment-processor";
revoke all on all tables in schema receiving, warehouse from "enrollment-processor";

-- …then re-grant
grant connect on database :"DBNAME" to "enrollment-processor";

grant usage
   on schema receiving, warehouse
   to "enrollment-processor";

grant select
   on receiving.enrollment
   to "enrollment-processor";

grant update (processing_log)
   on receiving.enrollment
   to "enrollment-processor";

grant select
   on warehouse.identifier,
      warehouse.identifier_set
   to "enrollment-processor";

grant select, insert, update
   on warehouse.site,
      warehouse.individual,
      warehouse.encounter,
      warehouse.sample
   to "enrollment-processor";

commit;
