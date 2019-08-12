-- Deploy seattleflu/schema:roles/presence-absence-processor to pg

begin;

set local role id3c;

create role "presence-absence-processor";

grant connect on database :"DBNAME" to "presence-absence-processor";

grant usage
   on schema receiving, warehouse
   to "presence-absence-processor";

grant select
   on receiving.presence_absence
   to "presence-absence-processor";

grant update (processing_log)
   on receiving.presence_absence
   to "presence-absence-processor";

grant select, insert, update
   on warehouse.sample,
      warehouse.target,
      warehouse.presence_absence
   to "presence-absence-processor";

grant select
   on warehouse.identifier,
      warehouse.identifier_set
   to "presence-absence-processor";

comment on role "presence-absence-processor" is 'For presence-absence ETL routines';

commit;
