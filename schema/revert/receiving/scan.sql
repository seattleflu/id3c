-- Revert seattleflu/schema:receiving/scan from pg

begin;

set search_path to receiving;

drop table aliquot;
drop table sample;
drop table collection;
drop table scan_set;

commit;
