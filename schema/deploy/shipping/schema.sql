-- Deploy seattleflu/schema:shipping/schema to pg

begin;

create schema shipping;

comment on schema shipping is 'Outgoing warehouse data prepared for external consumers';

commit;
