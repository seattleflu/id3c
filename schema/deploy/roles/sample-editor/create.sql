-- Deploy seattleflu/schema:roles/sample-editor/create to pg

begin;

create role "sample-editor";

comment on role "sample-editor" is 'For adding and updating sample records';


commit;
