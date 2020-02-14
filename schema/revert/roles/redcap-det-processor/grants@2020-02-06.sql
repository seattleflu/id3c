-- Revert seattleflu/schema:roles/redcap-det-processor/grants from pg

begin;

revoke all on database :"DBNAME" from "redcap-det-processor";
revoke all on schema receiving, warehouse, shipping from "redcap-det-processor";
revoke all on all tables in schema receiving, warehouse, shipping from "redcap-det-processor";

-- Add additional revokes here as necessary.

commit;
