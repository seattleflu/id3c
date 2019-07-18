-- Verify seattleflu/schema:warehouse/sequence-read-set/triggers/urls_unique_check on pg

begin;

do $$
declare
    test_sample integer;
begin
    insert into warehouse.sample (identifier) values ('__SAMPLE__')
        returning sample_id into strict test_sample;

    insert into warehouse.sequence_read_set (sample_id, urls)
        values (test_sample, '{"__URL_1__"}');

    -- Test inserting with duplicate urls
    begin
        insert into warehouse.sequence_read_set (sample_id, urls)
            values (test_sample, '{"__URL_2__", "__URL_2__"}');
        assert false, 'duplicated urls inserted';
    exception
        when unique_violation then
            null; --expected
    end;

    -- Test updating existing set with duplicated urls
    begin
        update warehouse.sequence_read_set
           set urls = array_cat(urls, '{"__URL_1__"}')
         where urls = array['__URL_1__'];
        assert false, 'updated with duplicated urls';
    exception
        when unique_violation then
            null; --expected
    end;
end $$;

rollback;
