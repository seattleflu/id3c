-- Deploy seattleflu/schema:roles/materialized-view-refresher/create to pg

begin;

create role "materialized-view-refresher";

comment on role "materialized-view-refresher" is
    'For refreshing materialized views';

commit;
