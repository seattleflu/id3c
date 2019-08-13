-- Deploy seattleflu/schema:hstore to pg

begin;

create extension hstore with schema public;

commit;
