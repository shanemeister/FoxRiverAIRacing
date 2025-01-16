
-- Create or replace the view
CREATE OR REPLACE VIEW public.v_races AS
select distinct 
    COALESCE(rr.course_cd, rd.course_cd, rl.course_cd) AS course_cd,
    COALESCE(rr.race_date, rd.race_date, rl.race_date) AS race_date,
    COALESCE(rr.race_number, rd.race_number, rl.race_number) AS race_number,
    c.track_name AS c_track_name, 
    rd.stk_clm_md as race_type,
    rd.purse AS purse,
    rd.race_text AS race_text,
    rd.age_restr as age_restriction,
    rd.surface AS surface,
    rr.trk_cond AS trk_cond,
    rd.todays_cls as class_rating,
    rr.weather as rr_weather,
    rd.post_time AS post_time,     -- Columns from racedata (alias rd)
    rr.post_time AS rr_post_time,
	rr.win_time as rr_win_time,
	rr.par_time as rr_par_time,
	rr.pace_call1 as rr_pace_call1,
	rr.pace_call2 as rr_pace_call2,
    rr.fraction_1 as rr_fraction1,
	rr.fraction_2 as rr_fraction2,
	rr.fraction_3 as rr_fraction3,
	rr.fraction_4 as rr_fraction4,
	rr.fraction_5 as rr_fraction5,
	rr.pace_final as rr_pace_final,
	rr.wps_pool as wps_pool,
	rd.distance as distance,
	rd.dist_unit as dist_unit,
    rd.distance_meters as distance_meters,
    rd.stkorclm as stkorclm,
    rd.stk_clm_md as stk_clm_md,
    rd.breed_cd as breed_cd,
    rd.race_text AS rd_race_text,
    rl.race_course AS rl_race_course,     -- Columns from race_list (alias rl)
    rl.race_type AS rl_race_type,
    rl.race_length AS rl_race_length
FROM
    racedata rd
FULL OUTER JOIN
     race_results rr ON rd.course_cd = rr.course_cd
                AND rd.race_date = rr.race_date
                AND rd.race_number = rr.race_number
FULL OUTER JOIN
    race_list rl ON COALESCE(rr.course_cd, rd.course_cd) = rl.course_cd
                 AND COALESCE(rr.race_date, rd.race_date) = rl.race_date
                 AND COALESCE(rr.race_number, rd.race_number) = rl.race_number
LEFT JOIN 
    course c ON rd.course_cd = c.course_cd;

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

CREATE or REPLACE VIEW v_races AS
SELECT * FROM races
WHERE course_cd IN ('CNL', 'SAR', 'PIM', 'TSA', 'BEL', 'MVR', 'TWO', 'CLS', 'KEE', 'TAM', 'TTP', 'TKD', 
                    'ELP', 'PEN', 'HOU', 'DMR', 'TLS', 'AQU', 'MTH', 'TGP', 'TGG', 'CBY', 'LRL', 
                    'TED', 'IND', 'CTD', 'ASD', 'TCD', 'LAD', 'MED', 'TOP')
and race_date > '2021-12-31';

-- Create a view with data from shared tracks only
CREATE or REPLACE VIEW v_results_entries AS
SELECT * FROM results_entries 
WHERE course_cd IN ('CNL', 'SAR', 'PIM', 'TSA', 'BEL', 'MVR', 'TWO', 'CLS', 'KEE', 'TAM', 'TTP', 'TKD', 
                    'ELP', 'PEN', 'HOU', 'DMR', 'TLS', 'AQU', 'MTH', 'TGP', 'TGG', 'CBY', 'LRL', 
                    'TED', 'IND', 'CTD', 'ASD', 'TCD', 'LAD', 'MED', 'TOP');

-- Create a view with data from shared tracks only
CREATE or REPLACE VIEW v_exotic_wagers AS
SELECT * FROM exotic_wagers ew 
WHERE course_cd IN ('CNL', 'SAR', 'PIM', 'TSA', 'BEL', 'MVR', 'TWO', 'CLS', 'KEE', 'TAM', 'TTP', 'TKD', 
                    'ELP', 'PEN', 'HOU', 'DMR', 'TLS', 'AQU', 'MTH', 'TGP', 'TGG', 'CBY', 'LRL', 
                    'TED', 'IND', 'CTD', 'ASD', 'TCD', 'LAD', 'MED', 'TOP')
and race_date > '2021-12-31';

-- Create a view with data from shared tracks only
CREATE or REPLACE VIEW v_runners AS
SELECT * FROM runners 
WHERE course_cd IN ('CNL', 'SAR', 'PIM', 'TSA', 'BEL', 'MVR', 'TWO', 'CLS', 'KEE', 'TAM', 'TTP', 'TKD', 
                    'ELP', 'PEN', 'HOU', 'DMR', 'TLS', 'AQU', 'MTH', 'TGP', 'TGG', 'CBY', 'LRL', 
                    'TED', 'IND', 'CTD', 'ASD', 'TCD', 'LAD', 'MED', 'TOP')
and race_date > '2021-12-31';

-- Create a view with data from shared tracks only
CREATE or REPLACE VIEW v_results_earnings AS
SELECT * FROM results_earnings 
WHERE course_cd IN ('CNL', 'SAR', 'PIM', 'TSA', 'BEL', 'MVR', 'TWO', 'CLS', 'KEE', 'TAM', 'TTP', 'TKD', 
                    'ELP', 'PEN', 'HOU', 'DMR', 'TLS', 'AQU', 'MTH', 'TGP', 'TGG', 'CBY', 'LRL', 
                    'TED', 'IND', 'CTD', 'ASD', 'TCD', 'LAD', 'MED', 'TOP')
and race_date > '2021-12-31';
