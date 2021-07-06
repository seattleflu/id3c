-- Deploy seattleflu/schema:warehouse/identifier-set-use/data to pg
-- requires: warehouse/identifier-set-use

begin;

insert into warehouse.identifier_set_use (use, description)
    values
        ('sample', 'Sample ID'),
        ('collection', 'Collection ID'),
        ('clia', 'CLIA ID'),
        ('kit','Test kit ID'),
        ('test-strip','Test strip ID')
    on conflict (use) do update set
        description = excluded.description
;

commit;
