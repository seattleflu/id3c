-- Deploy seattleflu/schema:postgis to pg

begin;

create extension postgis with schema public;

commit;
