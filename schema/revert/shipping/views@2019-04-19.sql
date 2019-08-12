-- Revert seattleflu/schema:shipping/views from pg

begin;

set local role id3c;

drop view shipping.incidence_model_observation_v1;

commit;
