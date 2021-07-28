-- Revert seattleflu/schema:warehouse/identifier/indexes/identifier_set_id_btree from pg

begin;

drop index warehouse.identifier_set_id_idx;

commit;
