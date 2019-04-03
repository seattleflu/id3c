-- Deploy seattleflu/schema:citext to pg

begin;

create extension citext with schema public;

commit;
