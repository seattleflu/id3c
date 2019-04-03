-- Deploy seattleflu/schema:receiving/schema to pg

begin;

create schema receiving;

comment on schema receiving is
    'Non-relational (or minimally-relational) data before it is transformed and loaded into the warehouse';

commit;
