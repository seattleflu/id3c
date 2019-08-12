-- Deploy seattleflu/schema:uuid-ossp to pg

begin;

set local role id3c;

create extension "uuid-ossp" with schema public;

commit;
