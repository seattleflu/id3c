-- Revert seattleflu/schema:roles/identifier-minter/create from pg

begin;

drop role "identifier-minter";

commit;

