-- Deploy seattleflu/schema:shipping/age-bin-decade to pg
-- requires: shipping/schema
-- requires: types/intervalrange

begin;

create table shipping.age_bin_decade_v1 (
    range intervalrange primary key,

    constraint age_only_in_one_decade_range_v1
        exclude using gist (range with &&)
);

create index age_bin_decade_range_idx_v1 on shipping.age_bin_decade_v1 using gist(range);

comment on table shipping.age_bin_decade_v1 is
    'Version 1 of decade age bins that uses intervalrange type';
comment on column shipping.age_bin_decade_v1.range is
    'The range of each age bin defined as intervalrange';

insert into shipping.age_bin_decade_v1 values
    ('[0 years, 10 years)'::intervalrange),
    ('[10 years, 20 years)'::intervalrange),
    ('[20 years, 30 years)'::intervalrange),
    ('[30 years, 40 years)'::intervalrange),
    ('[40 years, 50 years)'::intervalrange),
    ('[50 years, 60 years)'::intervalrange),
    ('[60 years, 70 years)'::intervalrange),
    ('[70 years, 80 years)'::intervalrange),
    ('[80 years, 90 years)'::intervalrange),
    ('[90 years,)'::intervalrange);


commit;
