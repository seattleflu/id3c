-- Revert seattleflu/schema:fuzzystrmatch from pg

begin;

drop extension fuzzystrmatch;

commit;
