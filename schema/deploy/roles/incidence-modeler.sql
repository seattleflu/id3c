-- Deploy seattleflu/schema:roles/incidence-modeler to pg
-- requires: shipping/views

begin;

create role "incidence-modeler";

comment on role "incidence-modeler" is
    'For access to incidence model observation data';

grant connect on database :"DBNAME" to "incidence-modeler";

grant usage
   on schema shipping
   to "incidence-modeler";

commit;
