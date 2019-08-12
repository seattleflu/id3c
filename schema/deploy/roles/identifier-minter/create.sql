-- Deploy seattleflu/schema:roles/identifier-minter/create to pg

begin;

set local role id3c;

create role "identifier-minter";

comment on role "identifier-minter" is 'For minting identifiers and making barcode labels';

commit;
