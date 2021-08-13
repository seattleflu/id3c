-- Deploy seattleflu/schema:roles/sample-editor/grants to pg
-- requires: roles/sample-editor/create
-- requires: warehouse/sample

begin;

grant connect on database :"DBNAME" to "sample-editor";

grant usage
   on schema warehouse
   to "sample-editor";

grant select
   on warehouse.identifier
   to "sample-editor";

grant select
   on warehouse.identifier_set
   to "sample-editor";

grant select, insert, update
   on warehouse.sample
   to "sample-editor";

commit;
