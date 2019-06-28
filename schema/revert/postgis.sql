-- Revert seattleflu/schema:postgis from pg

begin;

drop extension postgis;

commit;
