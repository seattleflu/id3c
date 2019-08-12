-- Deploy seattleflu/schema:shipping/age-bin-v2 to pg
-- requires: types/intervalrange

begin;

set local role id3c;

create table shipping.age_bin_fine_v2 (
    range intervalrange primary key,

    constraint age_only_in_one_fine_range_v2
        exclude using gist (range with &&)
);

create index age_bin_fine_range_idx_v2 on shipping.age_bin_fine_v2 using gist(range);

comment on table shipping.age_bin_fine_v2 is 
    'Version 2 of fine-grained age bins that uses intervalrange type';
comment on column shipping.age_bin_fine_v2.range is
    'The range of each age bin defined as intervalrange';

insert into shipping.age_bin_fine_v2 values
    ('[0 mon,1 mon)'::intervalrange),
    ('[1 mon,6 mons)'::intervalrange),
    ('[6 mons,1 year)'::intervalrange),
    ('[1 years,5 years)'::intervalrange),
    ('[5 years,10 years)'::intervalrange),
    ('[10 years,15 years)'::intervalrange),
    ('[15 years,20 years)'::intervalrange),
    ('[20 years,25 years)'::intervalrange),
    ('[25 years,30 years)'::intervalrange),
    ('[30 years,35 years)'::intervalrange),
    ('[35 years,40 years)'::intervalrange),
    ('[40 years,45 years)'::intervalrange),
    ('[45 years,50 years)'::intervalrange),
    ('[50 years,55 years)'::intervalrange),
    ('[55 years,60 years)'::intervalrange),
    ('[60 years,65 years)'::intervalrange),
    ('[65 years,70 years)'::intervalrange),
    ('[70 years,75 years)'::intervalrange),
    ('[75 years,80 years)'::intervalrange),
    ('[80 years,85 years)'::intervalrange),
    ('[85 years,90 years)'::intervalrange),
    ('[90 years,)'::intervalrange);

create table shipping.age_bin_coarse_v2 (
    range intervalrange primary key,

    constraint age_only_in_one_coarse_range_v2
        exclude using gist (range with &&)
);

create index age_bin_coarse_range_idx_v2 on shipping.age_bin_coarse_v2 using gist(range);

comment on table shipping.age_bin_coarse_v2 is
    'Version 2 of coarse-grained age bins that uses intervalrange type';
comment on column shipping.age_bin_coarse_v2.range is
    'The range of each age bin defined as intervalrange';

insert into shipping.age_bin_coarse_v2 values
    ('[0 mon,6 mons)'::intervalrange),
    ('[6 mons, 5 years)'::intervalrange),
    ('[5 years,18 years)'::intervalrange),
    ('[18 years,65 years)'::intervalrange),
    ('[65 years,)'::intervalrange);

commit;
