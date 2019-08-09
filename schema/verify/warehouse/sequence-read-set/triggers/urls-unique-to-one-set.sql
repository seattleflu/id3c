-- Verify seattleflu/schema:warehouse/sequence-read-set/triggers/urls-unique-to-one-set on pg

begin;

do $$
declare
    test_sample integer;
begin
    insert into warehouse.sample (identifier) values ('__SAMPLE__')
        returning sample_id into strict test_sample;

    insert into warehouse.sequence_read_set (sample_id, urls)
        values (test_sample, '{"__URL_1__", "__URL_2__"}'),
               (test_sample, '{"__URL_3__", "__URL_4__"}'),
               (test_sample, '{"__URL_5__", "__URL_6__"}');

    -- Test inserting with urls already part of another set
    begin
        insert into warehouse.sequence_read_set (sample_id, urls)
            values (test_sample, '{"__URL_1__", "__URL_5__"}');
        assert false, 'urls not unique to set inserted';
    exception
        when unique_violation then
            null; --expected
    end;

    -- Test updating existing set with urls part of another set
    begin
        update warehouse.sequence_read_set
           set urls = array_cat(urls, '{"__URL_3__", "__URL_5__"}')
         where urls = array['__URL_1__', '__URL_2__'];
        assert false, 'updated urls not unique to set';
    exception
        when unique_violation then
            null; --expected
    end;

    -- Test updating existing set with new urls, expected to be allowed
    update warehouse.sequence_read_set
       set urls = array_cat(urls, '{"__URL_7__"}')
     where urls = array['__URL_5__', '__URL_6__'];
end $$;



rollback;
