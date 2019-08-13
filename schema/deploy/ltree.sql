-- Deploy seattleflu/schema:ltree to pg

begin;

create extension ltree with schema public;

commit;
