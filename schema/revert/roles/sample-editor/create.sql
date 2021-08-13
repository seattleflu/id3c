-- Revert seattleflu/schema:roles/sample-editor/create from pg

begin;

drop role "sample-editor";

commit;
