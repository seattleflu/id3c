-- Revert seattleflu/schema:hstore from pg

begin;

drop extension hstore;

commit;
