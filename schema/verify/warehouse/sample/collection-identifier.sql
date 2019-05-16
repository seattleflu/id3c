-- Verify seattleflu/schema:warehouse/sample/collection-identifier on pg

begin;

insert into warehouse.sample (identifier, collection_identifier) values
    ('__SAMPLE_1__', '__COLLECTION__'),
    ('__SAMPLE_2__', null);

do $$
    begin
        insert into warehouse.sample (identifier, collection_identifier) values
            ('__SAMPLE_3__', '__COLLECTION__');
    exception
        when unique_violation then
            null; -- expected!
    end
$$;

rollback;
