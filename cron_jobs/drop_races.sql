
ALTER TABLE public.races
DROP CONSTRAINT races_fk_course;

-- Modify foreign key constraints for runners
ALTER TABLE public.runners 
DROP CONSTRAINT IF EXISTS fk_races_runners;

ALTER TABLE public.runners 
ADD CONSTRAINT fk_racedata_runners 
FOREIGN KEY (course_cd, race_date, race_number) 
REFERENCES public.racedata(course_cd, race_date, race_number) 
ON DELETE RESTRICT 
ON UPDATE RESTRICT;

-- Modify foreign key constraints for results_entries
ALTER TABLE public.results_entries
DROP CONSTRAINT IF EXISTS fk_races_results_entries;

ALTER TABLE public.results_entries
ADD CONSTRAINT fk_race_results_results_entries 
FOREIGN KEY (course_cd, race_date, race_number) 
REFERENCES public.race_results(course_cd, race_date, race_number) 
ON DELETE RESTRICT 
ON UPDATE RESTRICT;

-- Drop the view if it exists
DROP VIEW IF EXISTS public.v_races CASCADE;

DROP VIEW IF EXISTS public.v_sectionals_agg CASCADE;

DROP VIEW IF EXISTS public.v_gpspoint CASCADE;

drop table races cascade;
