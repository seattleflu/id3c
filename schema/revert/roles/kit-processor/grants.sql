-- Revert seattleflu/schema:roles/kit-processor/grants from pg

begin;

revoke select, insert, update
    on warehouse.kit
    from "kit-processor";

revoke update (encounter_id)
    on warehouse.sample
    from "kit-processor";

revoke select
    on warehouse.identifier,
       warehouse.identifier_set,
       warehouse.encounter,
       warehouse.site,
       warehouse.sample
    from "kit-processor";

revoke update (processing_log)
    on receiving.enrollment,
       receiving.manifest
    from "kit-processor";

revoke select
    on receiving.enrollment,
       receiving.manifest
    from "kit-processor";

revoke usage
    on schema receiving, warehouse
    from "kit-processor";

revoke connect on database :"DBNAME" from "kit-processor";

commit;
