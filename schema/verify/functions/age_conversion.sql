-- Verify seattleflu/schema:functions/age_conversion on pg

begin;

set local role id3c;

do $$ 
	declare
    	age record;
	begin
		create temporary table ages (age_as_interval, age_in_months, age_in_years) as values
			('0 mon'::interval, 0::int, 0::numeric),
			('1 mon', 1, 0.08),
			('2 mons', 2, 0.17),
			('3 mons', 3, 0.25),
			('4 mons', 4, 0.33),
			('5 mons', 5, 0.42),
			('6 mons', 6, 0.5),
			('7 mons', 7, 0.58),
			('8 mons', 8, 0.67),
			('9 mons', 9, 0.75),
			('10 mons', 10, 0.83),
			('11 mons', 11, 0.92),
			('1 year', 12, 1),
			('2 years', 24, 2),
			('3 years', 36, 3), 
			('4 years', 48, 4), 
			('5 years', 60, 5),
			('6 years', 72, 6);

		for age in select ages.age_in_months, ages.age_in_years, age_in_months(age_as_interval) as months, age_in_years(age_as_interval) as years
		  from ages loop
  
  			assert age.age_in_months = age.months;
			assert age.age_in_years = age.years;
		end loop;

end $$;

rollback;
