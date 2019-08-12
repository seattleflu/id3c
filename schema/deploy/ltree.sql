-- Deploy seattleflu/schema:ltree to pg

begin;

set local role id3c;

create extension ltree with schema public;

commit;
