-- Deploy seattleflu/schema:warehouse/schema to pg

begin;

set local role id3c;

create schema warehouse;

comment on schema warehouse is 'Standardized relational data ready for analysis';

commit;
