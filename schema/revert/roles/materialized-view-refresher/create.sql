-- Revert seattleflu/schema:roles/materialized-view-refresher/create from pg

begin;

drop role "materialized-view-refresher";

commit;
