-- Verify seattleflu/schema:warehouse/sequence-read-set/triggers/urls-not-null-check on pg

begin;

do $$
declare
    test_sample integer;
begin
    insert into warehouse.sample (identifier) values ('__SAMPLE__')
        returning sample_id into strict test_sample;

    insert into warehouse.sequence_read_set (sample_id, urls)
        values (test_sample, '{__URL_1__}');

    -- Test inserting null urls
    begin
        insert into warehouse.sequence_read_set (sample_id , urls)
            values (test_sample, '{"__URL_2__", null}');
        assert false, 'null urls inserted';
    exception
        when not_null_violation then
            null; --expected
    end;

    -- Test updating existing set with null urls
    begin
        update warehouse.sequence_read_set
           set urls = array_cat(urls, '{"__URL_2__", null}')
         where urls = array['__URL_1__'];
        assert false, 'updated with null urls';
    exception
        when not_null_violation then
            null; --expected
    end;
end $$;

rollback;
