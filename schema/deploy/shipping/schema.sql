-- Deploy seattleflu/schema:shipping/schema to pg

begin;

set local role id3c;

create schema shipping;

comment on schema shipping is 'Outgoing warehouse data prepared for external consumers';

commit;
