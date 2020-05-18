-- Revert seattleflu/schema:roles/materialized-view-refresher/grants from pg

begin;

revoke execute
    on function public.refresh_materialized_view
from "materialized-view-refresher";

drop function public.refresh_materialized_view;

revoke all on database :"DBNAME" from "materialized-view-refresher";
revoke all on all tables in schema public from "materialized-view-refresher";
revoke all on schema public from "materialized-view-refresher";

commit;
