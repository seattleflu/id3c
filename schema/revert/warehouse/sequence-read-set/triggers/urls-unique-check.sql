-- Revert seattleflu/schema:warehouse/sequence-read-set/triggers/urls_unique_check from pg

begin;

drop trigger sequence_read_set_urls_unique_check_before_update on warehouse.sequence_read_set;
drop trigger sequence_read_set_urls_unique_check_before_insert on warehouse.sequence_read_set;
drop function warehouse.sequence_read_set_urls_unique_check();

commit;
