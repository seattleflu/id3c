-- Deploy seattleflu/schema:staging/schema to pg

begin;

create schema staging;

comment on schema staging is
    'Staging area for non-relational data before it is transformed and loaded into the research schema';

commit;
