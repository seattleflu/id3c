-- Revert seattleflu/schema:hstore from pg

begin;

set local role id3c;

drop extension hstore;

commit;
