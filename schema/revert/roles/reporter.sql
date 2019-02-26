-- Revert seattleflu/schema:roles/reporter from pg

begin;

alter default privileges revoke select on tables from reporter;
alter default privileges revoke usage on schemas from reporter;

revoke select on all tables in schema staging from reporter;
revoke usage on schema staging from reporter;

revoke connect on database :"DBNAME" from reporter;

drop role reporter;

commit;
