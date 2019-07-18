-- Verify seattleflu/schema:warehouse/genomic_sequence on pg

begin;

select pg_catalog.has_table_privilege('warehouse.genomic_sequence', 'select');

do $$ begin
    declare
        _constraint text;
    begin
        with
        sample as (
            insert into warehouse.sample (identifier) values ('__SAMPLE__')
            returning sample_id as id
        ),
        organism as (
            insert into warehouse.organism (lineage) values ('__ORGANISM__')
            returning organism_id as id
        ),
        genome as (
            insert into warehouse.genome (sample_id, organism_id)
                values ((select id from sample), (select id from organism))
            returning genome_id as id
        )
        insert into warehouse.genomic_sequence (identifier, segment, seq, genome_id)
            values ('__GENOMIC_SEQUENCE_1__', '__SEGMENT_1__', '__SEQUENCE_1__', (select id from genome)),
                   ('__GENOMIC_SEQUENCE_2__', '__SEGMENT_1__', '__SEQUENCE_2__', (select id from genome));
        assert false, 'insert succeeded';
    exception
        when unique_violation then
            get stacked diagnostics _constraint = CONSTRAINT_NAME;
            assert _constraint = 'one_genomic_sequence_per_segment_per_genome', 'wrong constraint';
    end;
end $$;

rollback;
