CREATE OR REPLACE FUNCTION fill_previous_race_columns()
RETURNS void AS
$$
DECLARE
    rec              RECORD;  
    last_axciskey    TEXT := NULL;
    last_class       numeric := NULL;
    last_distance    numeric := NULL;  -- from the races table
    last_surface     TEXT := NULL;
    last_speed_rating numeric := NULL; 
    last_off_finish  numeric := NULL;
BEGIN

    /* 
       We'll iterate over each (runners + results_entries + races) row,
       sorted by (horse, date, race_number) so that we can keep track of
       the "previous race" data in local variables, then update the current row.
    */
    FOR rec IN
        SELECT 
            run.axciskey,
            run.course_cd,
            run.race_date,
            run.race_number,
            run.saddle_cloth_number,
            run.todays_cls,           -- If "class" is stored in runners as "todays_cls"
            ra.distance_meters,       -- from the races table
            ra.surface,              -- or if surface is in runners (verify your schema)
            re.speed_rating,          -- from results_entries
            re.official_fin
        FROM runners run
        JOIN results_entries re
          ON run.course_cd          = re.course_cd
         AND run.race_date          = re.race_date
         AND run.race_number        = re.race_number
         AND run.saddle_cloth_number = re.program_num
        JOIN races ra 
          ON run.course_cd   = ra.course_cd
         AND run.race_date   = ra.race_date
         AND run.race_number = ra.race_number
        ORDER BY 
            run.axciskey,
            run.race_date,
            run.race_number,
            run.course_cd   -- optional if needed
    LOOP

        -- If we moved to a new horse, reset the "previous race" data
        IF rec.axciskey IS DISTINCT FROM last_axciskey THEN
            last_class        := NULL;
            last_distance     := NULL;
            last_surface      := NULL;
            last_speed_rating := NULL;
            last_off_finish   := NULL;

            last_axciskey := rec.axciskey; 
        END IF;

        /* 
          Update the current row in `runners`, using our local "last_*" variables
          for the "previous_*" columns. We match on
          (course_cd, race_date, race_number, saddle_cloth_number),
          your existing composite PK in `runners`.
        */
        UPDATE runners
        SET 
            previous_class       = COALESCE(last_class, -1),
            previous_distance    = COALESCE(last_distance, -1),
            previous_surface     = COALESCE(last_surface, 'NONE'),
            prev_speed_rating    = COALESCE(last_speed_rating, -1),
            off_finish_last_race = COALESCE(last_off_finish, -1),
            race_count = (
                SELECT COUNT(*) 
                FROM runners r2
                WHERE r2.axciskey = rec.axciskey
            )
        WHERE 
            runners.course_cd           = rec.course_cd
            AND runners.race_date       = rec.race_date
            AND runners.race_number     = rec.race_number
            AND runners.saddle_cloth_number = rec.saddle_cloth_number;

        /* 
          Now set the local "last_*" variables from the CURRENT row's data,
          so the next iteration can use them as "previous" data.
        */
        last_class         := rec.todays_cls;
        last_distance      := rec.distance_meters;  -- from races
        last_surface       := rec.surface;
        last_speed_rating  := rec.speed_rating;
        last_off_finish    := rec.official_fin;

        last_axciskey := rec.axciskey;
    END LOOP;

    RETURN;
END;
$$ LANGUAGE plpgsql;