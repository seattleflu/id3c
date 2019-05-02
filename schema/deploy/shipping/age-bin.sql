-- Deploy seattleflu/schema:shipping/age-bin to pg

begin;

set search_path to shipping;

create table age_bin_fine (
    range int4range primary key,

    constraint age_only_in_one_fine_range
        exclude using gist (range with &&)
);

create index age_bin_fine_range_idx on age_bin_fine using gist(range);

comment on table age_bin_fine is 'A set of fine-grained age bins';
comment on column age_bin_fine.range is 'The range of each age bin';

insert into age_bin_fine values 
     ('[0,1]'::int4range)
    ,('[2,4]'::int4range)
    ,('[5,9]'::int4range)
    ,('[10,14]'::int4range)
    ,('[15,19]'::int4range)
    ,('[20,24]'::int4range)
    ,('[25,29]'::int4range)
    ,('[30,34]'::int4range)
    ,('[35,39]'::int4range)
    ,('[40,44]'::int4range)
    ,('[45,49]'::int4range)
    ,('[50,54]'::int4range)
    ,('[55,59]'::int4range)
    ,('[60,64]'::int4range)
    ,('[65,69]'::int4range)
    ,('[70,74]'::int4range)
    ,('[75,79]'::int4range)
    ,('[80,84]'::int4range)
    ,('[85,89]'::int4range)
    ,('[90,]'::int4range);

create table age_bin_coarse (
    range int4range primary key,

    constraint age_only_in_one_coarse_range
        exclude using gist (range with &&)
);

create index age_bin_coarse_range_idx on age_bin_coarse using gist(range);

comment on table age_bin_coarse is 'A set of coarse-grained age bins';
comment on column age_bin_coarse.range is 'The range of each age bin';

insert into age_bin_coarse values
     ('[0,1]'::int4range)
    ,('[2,4]'::int4range)
    ,('[5,17]'::int4range)
    ,('[18,64]'::int4range)
    ,('[65,]'::int4range);

commit;
