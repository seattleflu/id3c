-- Revert seattleflu/schema:roles/enrollment-processor from pg

begin;

set local role id3c;

revoke select, insert, update
    on warehouse.site,
       warehouse.individual,
       warehouse.encounter
  from enrollment_processor;

revoke update (processing_log)
    on receiving.enrollment
  from enrollment_processor;

revoke select
    on receiving.enrollment
  from enrollment_processor;

revoke usage
    on schema receiving, warehouse
  from enrollment_processor;

revoke connect on database :"DBNAME" from enrollment_processor;

drop role enrollment_processor;

commit;
