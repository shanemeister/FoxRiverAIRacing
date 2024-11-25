
-- Create or replace the view
CREATE OR REPLACE VIEW public.v_races AS
select distinct 
    COALESCE(rr.course_cd, rd.course_cd, rl.course_cd) AS course_cd,
    COALESCE(rr.race_date, rd.race_date, rl.race_date) AS race_date,
    COALESCE(rr.race_number, rd.race_number, rl.race_number) AS race_number,
    rr.course_name AS rr_course_name,  -- Columns from race_results (alias rr)
    rr.type as race_type,
    rr.purse AS purse,
    rd.purse AS rd_purse,
    rr.race_text AS race_text,
    rr.age_restr_cd as age_restriction,
    rr.surface AS surface,
    rd.surface AS rd_surface,
    rr.trk_cond AS trk_cond,
    rr.class_rating as class_rating,
    rd.todays_cls AS todays_cls,
    rr.weather as weather,
    rd.post_time AS post_time,     -- Columns from racedata (alias rd)
    rr.post_time AS rr_post_time,
	rr.win_time as win_time,
	rr.par_time as par_time,
	rr.pace_call1 as pace_call1,
	rr.pace_call2 as pace_call2,
    rr.fraction_1 as fraction1,
	rr.fraction_2 as fraction2,
	rr.fraction_3 as fraction3,
	rr.fraction_4 as fraction4,
	rr.fraction_5 as fraction5,
	rr.pace_final as pace_final,
	rr.wps_pool as wps_pool,
	rr.distance as rr_distance,
	rr.dist_unit as dist_unit,
	rd.distance AS distance,
    rd.stkorclm as stkorclm,
    rd.stk_clm_md as stk_clm_md,
    rd.breed_cd as breed_cd,
    rd.race_text AS rd_race_text,
    rl.race_course AS rl_race_course,     -- Columns from race_list (alias rl)
    rl.race_type AS rl_race_type,
    rl.race_length AS rl_race_length
FROM
    race_results rr
FULL OUTER JOIN
    racedata rd ON rr.course_cd = rd.course_cd
                AND rr.race_date = rd.race_date
                AND rr.race_number = rd.race_number
FULL OUTER JOIN
    race_list rl ON COALESCE(rr.course_cd, rd.course_cd) = rl.course_cd
                 AND COALESCE(rr.race_date, rd.race_date) = rl.race_date
                 AND COALESCE(rr.race_number, rd.race_number) = rl.race_number;

-- Create the races table without copying data
CREATE TABLE IF NOT EXISTS public.races AS
SELECT * FROM public.v_races
WITH NO DATA;

alter table races 
add constraint races_pk primary key (course_cd, race_date, race_number) ;

-- Add foreign keys Course

ALTER TABLE public.races
ADD CONSTRAINT races_fk_course
FOREIGN KEY (course_cd)
REFERENCES public.course(course_cd)
ON UPDATE CASCADE
ON DELETE RESTRICT;

-- Insert data into races table
INSERT INTO public.races
SELECT * FROM public.v_races;

-- Modify foreign key constraints for gpspoint
alter table public.gpspoint 
drop constraint gpspoint_race_list_fkey;

ALTER TABLE public.gpspoint 
ADD CONSTRAINT gpspoint_races_fkey 
FOREIGN KEY (course_cd, race_date, race_number) 
REFERENCES public.races(course_cd, race_date, race_number) 
ON DELETE RESTRICT 
ON UPDATE RESTRICT;

-- Modify foreign key constraints for sectionals
ALTER TABLE public.sectionals 
drop CONSTRAINT sectionals_race_list_fkey;

ALTER TABLE public.sectionals 
ADD CONSTRAINT sectionals_races_fkey 
FOREIGN KEY (course_cd, race_date, race_number) 
REFERENCES public.races(course_cd, race_date, race_number) 
ON DELETE RESTRICT 
ON UPDATE RESTRICT;

-- Modify foreign key constraints for runners
ALTER TABLE public.runners 
drop CONSTRAINT fk_racedata_runners;

ALTER TABLE public.runners 
ADD CONSTRAINT fk_races_runners 
FOREIGN KEY (course_cd, race_date, race_number) 
REFERENCES public.races(course_cd, race_date, race_number) 
ON DELETE RESTRICT 
ON UPDATE RESTRICT;

-- Modify foreign key constraints for results_entries

ALTER TABLE public.results_entries
drop CONSTRAINT fk_race_results_results_entries;

ALTER TABLE public.results_entries
ADD CONSTRAINT fk_races_results_entries 
FOREIGN KEY (course_cd, race_date, race_number) 
REFERENCES public.races(course_cd, race_date, race_number) 
ON DELETE RESTRICT 
ON UPDATE RESTRICT;
