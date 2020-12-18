-- Verify seattleflu/schema:functions/barcode_slices on pg

begin;

create temporary table tests (
    input citext,
    expected citext[]
);

insert into tests values ('abcdefg1','{1__ab,2__bc,3__cd,4__de,5__ef,6__fg,7__g1}'); -- even number of chars
insert into tests values ('abcdefg1234','{1__ab,2__bc,3__cd,4__de,5__ef,6__fg,7__g1,8__12,9__23,10__34}'); -- odd number of chars
insert into tests values ('abc','{1__ab,2__bc}'); -- number of chars == slice_width
insert into tests values ('a', null); -- number of chars < slice_width
insert into tests values ('', null); -- 0 chars
insert into tests values (null, null); -- null

do $$
  declare
  test tests;
  begin
    for test in table tests loop
      if test.expected is distinct from public.barcode_slices(test.input) then
        raise exception 'text_slices(%) returned %, expected %',
          test.input, public.barcode_slices(test.input), test.expected;
      end if;
    end loop;
  end
  $$;

rollback;
