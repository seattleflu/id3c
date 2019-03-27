-- Deploy seattleflu/schema:uuid-ossp to pg

begin;

create extension "uuid-ossp" with schema public;

commit;
