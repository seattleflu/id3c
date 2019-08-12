-- Deploy seattleflu/schema:roles/enrollment-processor to pg
-- requires: receiving/enrollment/processing-log
-- requires: warehouse/schema

begin;

set local role id3c;

create role enrollment_processor;

grant connect on database :"DBNAME" to enrollment_processor;

grant usage
   on schema receiving, warehouse
   to enrollment_processor;

grant select
   on receiving.enrollment
   to enrollment_processor;

grant update (processing_log)
   on receiving.enrollment
   to enrollment_processor;

grant select, insert, update
   on warehouse.site,
      warehouse.individual,
      warehouse.encounter
   to enrollment_processor;

commit;
