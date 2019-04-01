-- Revert seattleflu/schema:roles/incidence-modeler from pg

begin;

revoke usage
    on schema shipping
  from "incidence-modeler";

revoke connect
    on database :"DBNAME"
  from "incidence-modeler";

drop role "incidence-modeler";

commit;
