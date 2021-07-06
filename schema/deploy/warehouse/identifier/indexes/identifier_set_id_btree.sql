-- Deploy seattleflu/schema:warehouse/identifier/indexes/identifier_set_id_btree to pg
-- requires: warehouse/identifier

begin;

create index identifier_set_id_idx on warehouse.identifier using btree (identifier_set_id);

commit;
