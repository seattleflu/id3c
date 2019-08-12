-- Deploy seattleflu/schema:roles/dumper/create to pg
-- requires: receiving/schema
-- requires: warehouse/schema
-- requires: shipping/schema

begin;

set local role id3c;

create role dumper;

comment on role dumper is 'For making copies with pg_dump';

commit;
