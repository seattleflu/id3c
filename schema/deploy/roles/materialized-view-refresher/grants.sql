-- Deploy seattleflu/schema:roles/materialized-view-refresher/grants to pg
-- requires: roles/materialized-view-refresher/create

begin;

revoke all on database :"DBNAME" from "materialized-view-refresher";
revoke all on schema receiving, warehouse, shipping from "materialized-view-refresher";
revoke all on all tables in schema receiving, warehouse, shipping from "materialized-view-refresher";

grant connect on database :"DBNAME" to "materialized-view-refresher";

-- Regular users cannot refresh materialized views because only the view
-- owner has permission to refresh them.
-- Create a function that runs in the security context of its owner so that
-- other users can refresh the view.
create or replace function public.refresh_materialized_view(view_schema text, view_name text)
    returns void
    security definer
    language plpgsql as $$
        declare
            message text;
        begin
            execute format('refresh materialized view concurrently %I.%I', view_schema, view_name);
            raise info 'Refreshed materialized view %', view_name;
            return;
        exception when others then
            get stacked diagnostics message = message_text;
            raise exception 'ERROR: %', message;
        end;
    $$
    volatile
    parallel unsafe;

revoke execute
    on function public.refresh_materialized_view
from public;

grant execute
    on function public.refresh_materialized_view
    to "materialized-view-refresher";


commit;
