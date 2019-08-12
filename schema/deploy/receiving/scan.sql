-- Revert seattleflu/schema:receiving/scan from pg

begin;

set local role id3c;

set local search_path to receiving;

drop table aliquot;
drop table sample;
drop table collection;
drop table scan_set;

commit;
