-- Deploy seattleflu/schema:warehouse/identifier-set/use to pg
-- requires: warehouse/identifier
-- requires: warehouse/identifier-set-use

begin;

alter table warehouse.identifier_set
    add column use citext;

alter table warehouse.identifier_set
    add constraint identifier_set_use_fkey
        foreign key (use)
        references warehouse.identifier_set_use (use);

commit;
