-- Deploy seattleflu/schema:roles/identifier-minter/create to pg

begin;

create role "identifier-minter";

comment on role "identifier-minter" is 'For minting identifiers and making barcode labels';

commit;
