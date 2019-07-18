-- Revert seattleflu/schema:warehouse/sequence-read-set/triggers/urls-unique-to-one-set from pg

begin;

drop trigger sequence_read_set_urls_unique_to_one_set_check_before_update on warehouse.sequence_read_set;
drop trigger sequence_read_set_urls_unique_to_one_set_check_before_insert on warehouse.sequence_read_set;
drop function warehouse.sequence_read_set_urls_unique_to_one_set_check();

commit;
