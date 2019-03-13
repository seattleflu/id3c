-- Deploy seattleflu/schema:warehouse/schema to pg

begin;

create schema warehouse;

comment on schema warehouse is 'Standardized relational data ready for analysis';

commit;
