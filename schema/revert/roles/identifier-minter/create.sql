-- Revert seattleflu/schema:roles/identifier-minter/create from pg

begin;

set local role id3c;

drop role "identifier-minter";

commit;

