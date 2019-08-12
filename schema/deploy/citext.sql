-- Deploy seattleflu/schema:citext to pg

begin;

set local role id3c;

create extension citext with schema public;

commit;
