-- Revert seattleflu/schema:staging/scan from pg

begin;

set search_path to staging;

drop table aliquot;
drop table sample;
drop table collection;
drop table scan_set;

commit;
