-- Deploy seattleflu/schema:roles/kit-processor/grants to pg
-- requires: roles/kit-processor/create
-- requires: warehouse/kit
-- requires: receiving/manifest
-- requires: receiving/enrollment/processing-log
-- requires: warehouse/identifier

begin;

-- This change is designed to be sqitch rework-able to make it easier to update
-- the grants for this role.

grant connect on database :"DBNAME" to "kit-processor";

grant usage
    on schema receiving, warehouse
    to "kit-processor";

grant select
    on receiving.enrollment,
       receiving.manifest
    to "kit-processor";

grant update (processing_log)
    on receiving.enrollment,
       receiving.manifest
    to "kit-processor";

grant select
    on warehouse.identifier,
       warehouse.identifier_set,
       warehouse.encounter,
       warehouse.site,
       warehouse.sample
    to "kit-processor";

grant update (encounter_id)
    on warehouse.sample
    to "kit-processor";

grant select, insert, update
    on warehouse.kit
    to "kit-processor";

commit;
