-- Revert seattleflu/schema:receiving/redcap-det from pg

begin;

drop table receiving.redcap_det;

commit;
