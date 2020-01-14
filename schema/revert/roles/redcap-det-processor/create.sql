-- Revert seattleflu/schema:roles/redcap-det-processor/create from pg

begin;

drop role "redcap-det-processor";

commit;
