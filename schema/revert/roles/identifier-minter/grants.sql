-- Revert seattleflu/schema:roles/identifier-minter/grants from pg

begin;

set local role id3c;

revoke select, insert
  on warehouse.identifier
  from "identifier-minter";

revoke select
  on warehouse.identifier_set
  from "identifier-minter";

revoke usage
    on schema warehouse
  from "identifier-minter";

revoke connect on database :"DBNAME" from "identifier-minter";

commit;
