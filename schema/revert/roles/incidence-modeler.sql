-- Revert seattleflu/schema:roles/incidence-modeler from pg

begin;

set local role id3c;

revoke usage
    on schema shipping
  from "incidence-modeler";

revoke connect
    on database :"DBNAME"
  from "incidence-modeler";

drop role "incidence-modeler";

commit;
