-- Revert seattleflu/schema:functions/mint_identifiers from pg

begin;

drop function if exists public.mint_identifiers(integer, integer);

commit;
