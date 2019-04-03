-- Revert seattleflu/schema:shipping/views from pg

begin;

drop view shipping.incidence_model_observation_v1;

commit;
