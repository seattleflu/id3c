-- Deploy seattleflu/schema:hstore to pg

begin;

set local role id3c;

create extension hstore with schema public;

commit;
