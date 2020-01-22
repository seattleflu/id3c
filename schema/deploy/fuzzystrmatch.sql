-- Deploy seattleflu/schema:fuzzystrmatch to pg

begin;

create extension fuzzystrmatch with schema public;

commit;
