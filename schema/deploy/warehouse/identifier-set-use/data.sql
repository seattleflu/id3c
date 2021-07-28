-- Deploy seattleflu/schema:warehouse/identifier-set-use/data to pg
-- requires: warehouse/identifier-set-use

begin;

insert into warehouse.identifier_set_use (use, description)
    values
        ('sample', 'Identifiers for samples received and processed by the lab'),
        ('collection', 'Identifiers for collection tubes'),
        ('clia', 'Secondary identifiers for CLIA compliance'),
        ('kit','Identifiers for test kits'),
        ('test-strip','Identifiers for test strips')
    on conflict (use) do update set
        description = excluded.description
;

commit;
