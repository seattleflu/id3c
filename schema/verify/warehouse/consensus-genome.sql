-- Verify seattleflu/schema:warehouse/consensus-genome on pg

begin;

select pg_catalog.has_table_privilege('warehouse.consensus_genome', 'select');

do $$ begin
    declare
        _constraint text;
        sample_ids int[];
        organism_ids int[];
        sequence_read_set_ids int[];
    begin
        -- Create two samples
        with samples as (
            insert into warehouse.sample (identifier) values ('__SAMPLE__'), ('__SAMPLE_2')
            returning sample_id as id
        )
        select array_agg(id) into sample_ids from samples;

        -- Creat two organisms
        with organisms as (
            insert into warehouse.organism (lineage) values ('__ORGANISM_1__'), ('__ORGANISM_2__')
            returning organism_id as id
        )
        select array_agg(id) into organism_ids from organisms;

        -- Create two sequence read sets
        with sequence_read_sets as (
            insert into warehouse.sequence_read_set (sample_id, urls)
                values (sample_ids[1], '{"__URL_1__"}'), (sample_ids[2], '{"__URL_2__"}')
            returning sequence_read_set_id as id
        )
        select array_agg(id) into sequence_read_set_ids from sequence_read_sets;

        -- Create consensus genomes with different combinations of sample/organism/sequence_read_set
        insert into warehouse.consensus_genome (sample_id, organism_id, sequence_read_set_id)
            values (sample_ids[1], organism_ids[1], null),
                   (sample_ids[1], organism_ids[2], null),
                   (sample_ids[2], organism_ids[1], null),
                   (sample_ids[2], organism_ids[2], null),
                   (sample_ids[1], organism_ids[1], sequence_read_set_ids[1]),
                   (sample_ids[1], organism_ids[2], sequence_read_set_ids[1]),
                   (sample_ids[2], organism_ids[1], sequence_read_set_ids[1]),
                   (sample_ids[2], organism_ids[2], sequence_read_set_ids[1]),
                   (sample_ids[1], organism_ids[1], sequence_read_set_ids[2]),
                   (sample_ids[1], organism_ids[2], sequence_read_set_ids[2]),
                   (sample_ids[2], organism_ids[1], sequence_read_set_ids[2]),
                   (sample_ids[2], organism_ids[2], sequence_read_set_ids[2]);
        assert true, 'insert failed';

        -- Try to create consensus genome with repeated combination of sample/organism/sequence_read_set
        -- This is expected to fail
        insert into warehouse.consensus_genome (sample_id, organism_id, sequence_read_set_id)
            values (sample_ids[1], organism_ids[1], sequence_read_set_ids[1]);
        assert false, 'insert succeeded';
    exception
        when unique_violation then
            get stacked diagnostics _constraint = CONSTRAINT_NAME;
            assert _constraint = 'genome_has_unique_combination_of_sample_organism_sequence_reads', 'wrong constraint';
    end;

    -- Try inserting the same sample_id and organism_id with sequence_read_set_id as null
    -- This is expected to fail
    declare
        _constraint text;
    begin
        with
        sample as (
            insert into warehouse.sample (identifier) values ('__SAMPLE_3__')
            returning sample_id as id
        ),
        organism as (
            insert into warehouse.organism (lineage) values ('__ORGANISM_3__')
            returning organism_id as id
        )
        insert into warehouse.consensus_genome (sample_id, organism_id)
            values ((select id from sample), (select id from organism)),
                   ((select id from sample), (select id from organism));

        assert false, 'insert succeeded';
    exception
        when unique_violation then
            get stacked diagnostics _constraint = CONSTRAINT_NAME;
            assert _constraint = 'genome_has_unique_sample_organism_when_sequence_reads_null';
    end;
end $$;

rollback;
