-- Revert seattleflu/schema:receiving/kit-result from pg

begin;

drop table receiving.kit_result;

commit;
