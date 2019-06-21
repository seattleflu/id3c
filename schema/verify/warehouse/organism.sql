-- Verify seattleflu/schema:warehouse/organism on pg

begin;

copy warehouse.organism (lineage, identifiers) from stdin;
Influenza	
Influenza.A	NCBITaxon=>11320
Influenza.A.H1N1	NCBITaxon=>114727
Influenza.A.H3N2	NCBITaxon=>119210
Influenza.B	NCBITaxon=>11520
Influenza.B.Vic	
Influenza.B.Yam	
Influenza.C	NCBITaxon=>11552
RSV	NCBITaxon=>11250
RSV.A	NCBITaxon=>208893
RSV.B	NCBITaxon=>208895
\.

do $$
    declare
        count int;
    begin
        select count(*) into count
        from warehouse.organism
        where 'Influenza.A' @> lineage;

        assert count = 3;

        select count(*) into count
        from warehouse.organism
        where 'Influenza.D' @> lineage;

        assert count = 0;
end $$;

rollback;
