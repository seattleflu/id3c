-- Revert seattleflu/schema:roles/sample-editor/grants from pg

begin;

revoke all on database :"DBNAME" from "sample-editor";
revoke all on schema receiving, warehouse, shipping from "sample-editor";
revoke all on all tables in schema receiving, warehouse, shipping from "sample-editor";

commit;
