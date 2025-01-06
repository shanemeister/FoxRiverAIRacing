#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
accumulate_stats.py

Demonstration script that unifies the approach for:
- Horse-level stats (stat_type = 'ALL_RACES')
- Jockey stats (stat_type = 'ALL_RACES_J')
- Trainer stats (stat_type = 'ALL_RACES_T')
- Jockey–Trainer combo stats (stat_type = 'ALL_RACES_JT')

Each 'stat_type' has its own logic for how we accumulate prior results.

We track which race_dates are processed for each stat_type separately 
by using ingestion_files.file_name = str(race_date), message = <stat_type>.
Hence each stat_type can be advanced independently.

Author: You
Date: 2025-01-05
"""

import os
import sys
import logging
import traceback
import psycopg2
import configparser
import decimal

###############################################################################
# Utility / Setup
###############################################################################

def setup_logging(log_dir="/home/exx/myCode/horse-racing/FoxRiverAIRacing/logs"):
    """
    Simple logging init, overwriting or appending as you prefer.
    Writes logs to 'accumulate_stats.log' in the specified logs directory.
    """
    import os
    import logging

    # Ensure the directory exists
    os.makedirs(log_dir, exist_ok=True)

    # Prepare the full path for the log file
    log_file_path = os.path.join(log_dir, "stat_type_update_jy.log")

    logger = logging.getLogger()
    logger.handlers.clear()
    logger.setLevel(logging.INFO)

    fh = logging.FileHandler(log_file_path, mode="w")
    fmt = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    fh.setFormatter(fmt)
    fh.setLevel(logging.INFO)
    logger.addHandler(fh)

    logging.info(f"Logging initialized in {log_file_path}")
    
def read_config(script_dir):
    """
    Reads config.ini from (script_dir)/../../config.ini, or adapt as needed.
    """
    config = configparser.ConfigParser()
    config_fp = os.path.join(os.path.abspath(os.path.join(script_dir, '../../')), 'config.ini')
    if not os.path.exists(config_fp):
        raise FileNotFoundError(f"config.ini not found at '{config_fp}'")
    config.read(config_fp)
    if 'database' not in config:
        raise KeyError("Missing 'database' section in config.ini")
    return config

def get_connection(config):
    dbsec = config['database']
    conn = psycopg2.connect(
        dbname=dbsec['dbname'],
        user=dbsec.get('user'),
        password=dbsec.get('password'),
        host=dbsec.get('host', 'localhost'),
        port=dbsec.get('port', '5432')
    )
    conn.autocommit = False
    return conn


###############################################################################
# ingestion_files helpers
###############################################################################

def mark_race_date_processed(conn, race_date_str, stat_type):
    """
    Insert or update ingestion_files so race_date is marked 'processed' for given stat_type.
    """
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO ingestion_files (file_name, message, status)
            VALUES (%s, %s, 'processed')
            ON CONFLICT (file_name, message)
            DO UPDATE SET status='processed';
        """, (race_date_str, stat_type))

def ensure_stat_type_exists(conn, stat_type):
    """
    Insert a record into stat_type_code if not existing.
    Or do nothing if it does exist.
    """
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO stat_type_code (stat_type)
            VALUES (%s)
            ON CONFLICT (stat_type)
            DO NOTHING;
        """, (stat_type,))

def get_last_processed_date(conn, stat_type):
    """
    Return last_processed from stat_type_code for the given stat_type, or None if none.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT last_processed
            FROM stat_type_code
            WHERE stat_type = %s
        """, (stat_type,))
        row = cur.fetchone()
        if not row:
            return None
        return row[0]  # date or None

def update_stat_type_last_processed(conn, stat_type, race_date):
    """
    Update stat_type_code.last_processed with race_date if race_date is greater 
    than existing or if existing is NULL.
    """
    with conn.cursor() as cur:
        # Just set it to the new date. Or do logic if you only want to store the max.
        cur.execute("""
            UPDATE stat_type_code
            SET last_processed = %s
            WHERE stat_type = %s
        """, (race_date, stat_type))

###############################################################################
# Jockey-level logic (ALL_RACES_J)
###############################################################################

def accumulate_jockey_stats_for_date(cur, race_date):
    """
    Similar to horse-level logic, but for jock_accum_stats table.
    """
    stat_type = 'ALL_RACES_J'
    # find jockeys
    cur.execute("""
        SELECT DISTINCT re.jock_key
        FROM results_entries re
        JOIN race_results rr
          ON re.course_cd=rr.course_cd
         AND re.race_date=rr.race_date
         AND re.race_number=rr.race_number
        WHERE rr.race_date=%s
          AND re.jock_key IS NOT NULL
    """, (race_date,))
    jockeys = [r[0] for r in cur.fetchall()]

    for jock_key in jockeys:
        # gather prior
        cur.execute("""
            SELECT rr.race_date, re.official_fin, COALESCE(re2.earnings,0.0)
            FROM results_entries re
            JOIN race_results rr 
              ON re.course_cd=rr.course_cd
             AND re.race_date=rr.race_date
             AND re.race_number=rr.race_number
            LEFT JOIN results_earnings re2
              ON re.course_cd=re2.course_cd
             AND re.race_date=re2.race_date
             AND re.race_number=re2.race_number
             AND re.official_fin=re2.split_num
            WHERE re.jock_key=%s
              AND rr.race_date < %s
        """, (jock_key, race_date))

        rows = cur.fetchall()
        stats = {
            'starts': 0,
            'win': 0,
            'place': 0,
            'show': 0,
            'fourth': 0,
            'win_earnings': decimal.Decimal('0.00'),
            'place_earnings': decimal.Decimal('0.00'),
            'show_earnings': decimal.Decimal('0.00')
        }
        for rd, off_fin, part_earn in rows:
            stats['starts'] += 1
            if off_fin == 1:
                stats['win'] += 1
                stats['win_earnings'] += decimal.Decimal(part_earn)
            elif off_fin == 2:
                stats['place'] += 1
                stats['place_earnings'] += decimal.Decimal(part_earn)
            elif off_fin == 3:
                stats['show'] += 1
                stats['show_earnings'] += decimal.Decimal(part_earn)
            elif off_fin == 4:
                stats['fourth'] += 1

        # upsert
        cur.execute("""
            INSERT INTO jock_accum_stats(
                jock_key, stat_type, as_of_date,
                win, place, "show", starts, fourth,
                win_earnings, place_earnings, show_earnings
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (jock_key, stat_type, as_of_date)
            DO UPDATE SET
                win=EXCLUDED.win,
                place=EXCLUDED.place,
                "show"=EXCLUDED."show",
                starts=EXCLUDED.starts,
                fourth=EXCLUDED.fourth,
                win_earnings=EXCLUDED.win_earnings,
                place_earnings=EXCLUDED.place_earnings,
                show_earnings=EXCLUDED.show_earnings
        """, (
            jock_key, stat_type, race_date,
            stats['win'], stats['place'], stats['show'], stats['starts'], stats['fourth'],
            float(stats['win_earnings']), float(stats['place_earnings']), float(stats['show_earnings'])
        ))

###############################################################################
# Trainer-level logic (ALL_RACES_T)
###############################################################################

def accumulate_trainer_stats_for_date(cur, race_date):
    """
    Similar approach for trainer_accum_stats.
    """
    stat_type = 'ALL_RACES_T'
    cur.execute("""
        SELECT DISTINCT re.train_key
        FROM results_entries re
        JOIN race_results rr
          ON re.course_cd=rr.course_cd
         AND re.race_date=rr.race_date
         AND re.race_number=rr.race_number
        WHERE rr.race_date=%s
          AND re.train_key IS NOT NULL
    """, (race_date,))
    trainers = [r[0] for r in cur.fetchall()]

    for train_key in trainers:
        cur.execute("""
            SELECT rr.race_date, re.official_fin, COALESCE(re2.earnings,0.0)
            FROM results_entries re
            JOIN race_results rr 
              ON re.course_cd=rr.course_cd
             AND re.race_date=rr.race_date
             AND re.race_number=rr.race_number
            LEFT JOIN results_earnings re2
              ON re.course_cd=re2.course_cd
             AND re.race_date=re2.race_date
             AND re.race_number=re2.race_number
             AND re.official_fin=re2.split_num
            WHERE re.train_key=%s
              AND rr.race_date<%s
        """, (train_key, race_date))

        rows = cur.fetchall()
        stats = {
            'starts': 0,
            'win': 0,
            'place': 0,
            'show': 0,
            'fourth': 0,
            'win_earnings': decimal.Decimal('0.00'),
            'place_earnings': decimal.Decimal('0.00'),
            'show_earnings': decimal.Decimal('0.00')
        }
        for rd, off_fin, part_earn in rows:
            stats['starts'] += 1
            if off_fin == 1:
                stats['win'] += 1
                stats['win_earnings'] += decimal.Decimal(part_earn)
            elif off_fin == 2:
                stats['place'] += 1
                stats['place_earnings'] += decimal.Decimal(part_earn)
            elif off_fin == 3:
                stats['show'] += 1
                stats['show_earnings'] += decimal.Decimal(part_earn)
            elif off_fin == 4:
                stats['fourth'] += 1

        # upsert
        cur.execute("""
            INSERT INTO trainer_accum_stats(
                train_key, stat_type, as_of_date,
                win, place, "show", starts, fourth,
                win_earnings, place_earnings, show_earnings
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (train_key, stat_type, as_of_date)
            DO UPDATE SET
                win=EXCLUDED.win,
                place=EXCLUDED.place,
                "show"=EXCLUDED."show",
                starts=EXCLUDED.starts,
                fourth=EXCLUDED.fourth,
                win_earnings=EXCLUDED.win_earnings,
                place_earnings=EXCLUDED.place_earnings,
                show_earnings=EXCLUDED.show_earnings
        """, (
            train_key, stat_type, race_date,
            stats['win'], stats['place'], stats['show'], stats['starts'], stats['fourth'],
            float(stats['win_earnings']), float(stats['place_earnings']), float(stats['show_earnings'])
        ))

###############################################################################
# Jockey–Trainer combo (ALL_RACES_JT)
###############################################################################

def accumulate_jock_trainer_for_date(cur, race_date):
    """
    For all jock_key–train_key combos that ran on race_date, gather prior combos.
    """
    stat_type = 'ALL_RACES_JT'
    cur.execute("""
        SELECT DISTINCT re.jock_key, re.train_key
        FROM results_entries re
        JOIN race_results rr
         ON re.course_cd=rr.course_cd
        AND re.race_date=rr.race_date
        AND re.race_number=rr.race_number
        WHERE rr.race_date=%s
          AND re.jock_key IS NOT NULL
          AND re.train_key IS NOT NULL
    """, (race_date,))
    combos = cur.fetchall()

    for jock_key, train_key in combos:
        cur.execute("""
            SELECT rr.race_date, re.official_fin
            FROM results_entries re
            JOIN race_results rr
              ON re.course_cd=rr.course_cd
             AND re.race_date=rr.race_date
             AND re.race_number=rr.race_number
            WHERE re.jock_key=%s
              AND re.train_key=%s
              AND rr.race_date < %s
        """, (jock_key, train_key, race_date))

        rows = cur.fetchall()
        stats = {
            'starts': 0, 'win': 0, 'place': 0, 'show': 0, 'fourth': 0
        }
        for rd, off_fin in rows:
            stats['starts'] += 1
            if off_fin == 1:
                stats['win'] += 1
            elif off_fin == 2:
                stats['place'] += 1
            elif off_fin == 3:
                stats['show'] += 1
            elif off_fin == 4:
                stats['fourth'] += 1

        cur.execute("""
            INSERT INTO trainer_jockey_stats(
                train_key, jock_key, stat_type, as_of_date,
                starts, win, place, "show", fourth
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT(train_key, jock_key, stat_type, as_of_date)
            DO UPDATE SET
                starts=EXCLUDED.starts,
                win=EXCLUDED.win,
                place=EXCLUDED.place,
                "show"=EXCLUDED."show",
                fourth=EXCLUDED.fourth
        """, (
            train_key, jock_key, stat_type, race_date,
            stats['starts'], stats['win'], stats['place'], stats['show'], stats['fourth']
        ))

###############################################################################
# Jockey By Track/Course Code (ALL_RACES_J_TRACK)
###############################################################################
def accumulate_jockey_for_date_by_track(cur, race_date):
    """
    For all jockeys who ran on race_date, grouped by course_cd (track),
    gather prior starts *at the same track* with rr.race_date < race_date,
    then insert/upsert into jock_accum_stats_by_track.
    """
    stat_type = 'ALL_RACES_J_TRACK'

    # 1) Which jockeys, which track on this date?
    cur.execute("""
        SELECT DISTINCT re.jock_key, re.course_cd
        FROM results_entries re
        JOIN race_results rr
          ON re.course_cd=rr.course_cd
         AND re.race_date=rr.race_date
         AND re.race_number=rr.race_number
        WHERE rr.race_date = %s
          AND re.jock_key IS NOT NULL
    """, (race_date,))
    rows = cur.fetchall()  # list of (jock_key, course_cd)

    for (jock_key, course_cd) in rows:
        # 2) Fetch all prior starts for that jock at the same track
        cur.execute("""
            SELECT rr.race_date, re.official_fin
            FROM results_entries re
            JOIN race_results rr
              ON re.course_cd = rr.course_cd
             AND re.race_date = rr.race_date
             AND re.race_number = rr.race_number
            WHERE re.jock_key = %s
              AND re.course_cd = %s
              AND rr.race_date < %s
        """, (jock_key, course_cd, race_date))

        prior_rows = cur.fetchall()
        stats = {'starts': 0, 'win': 0, 'place': 0, 'show': 0, 'fourth': 0}

        for (_, off_fin) in prior_rows:
            stats['starts'] += 1
            if off_fin == 1:
                stats['win'] += 1
            elif off_fin == 2:
                stats['place'] += 1
            elif off_fin == 3:
                stats['show'] += 1
            elif off_fin == 4:
                stats['fourth'] += 1

        # 3) Upsert into jock_accum_stats_by_track
        cur.execute("""
            INSERT INTO jock_accum_stats_by_track (
                jock_key, course_cd, stat_type, as_of_date,
                starts, win, place, "show", fourth
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (jock_key, course_cd, stat_type, as_of_date)
            DO UPDATE SET
                starts = EXCLUDED.starts,
                win = EXCLUDED.win,
                place = EXCLUDED.place,
                "show" = EXCLUDED."show",
                fourth = EXCLUDED.fourth
        """, (
            jock_key, course_cd, stat_type, race_date,
            stats['starts'], stats['win'], stats['place'], 
            stats['show'], stats['fourth']
        ))

###############################################################################
# Trainer By Track/Course Code (ALL_RACES_J_TRACK)
###############################################################################
def accumulate_trainer_for_date_by_track(cur, race_date):
    """
    For all trainers who ran on race_date, grouped by course_cd (track),
    gather prior starts *at the same track*, then insert/upsert into
    trainer_accum_stats_by_track.
    """
    stat_type = 'ALL_RACES_T_TRACK'

    # 1) Which trainer, which track on this date?
    cur.execute("""
        SELECT DISTINCT re.train_key, re.course_cd
        FROM results_entries re
        JOIN race_results rr
          ON re.course_cd=rr.course_cd
         AND re.race_date=rr.race_date
         AND re.race_number=rr.race_number
        WHERE rr.race_date = %s
          AND re.train_key IS NOT NULL
    """, (race_date,))
    rows = cur.fetchall()  # list of (train_key, course_cd)

    for (train_key, course_cd) in rows:
        # 2) Fetch all prior starts for that trainer at the same track
        cur.execute("""
            SELECT rr.race_date, re.official_fin
            FROM results_entries re
            JOIN race_results rr
              ON re.course_cd = rr.course_cd
             AND re.race_date = rr.race_date
             AND re.race_number = rr.race_number
            WHERE re.train_key = %s
              AND re.course_cd = %s
              AND rr.race_date < %s
        """, (train_key, course_cd, race_date))

        prior_rows = cur.fetchall()
        stats = {'starts': 0, 'win': 0, 'place': 0, 'show': 0, 'fourth': 0}

        for (_, off_fin) in prior_rows:
            stats['starts'] += 1
            if off_fin == 1:
                stats['win'] += 1
            elif off_fin == 2:
                stats['place'] += 1
            elif off_fin == 3:
                stats['show'] += 1
            elif off_fin == 4:
                stats['fourth'] += 1

        # 3) Upsert into trainer_accum_stats_by_track
        cur.execute("""
            INSERT INTO trainer_accum_stats_by_track (
                train_key, course_cd, stat_type, as_of_date,
                starts, win, place, "show", fourth
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (train_key, course_cd, stat_type, as_of_date)
            DO UPDATE SET
                starts = EXCLUDED.starts,
                win = EXCLUDED.win,
                place = EXCLUDED.place,
                "show" = EXCLUDED."show",
                fourth = EXCLUDED.fourth
        """, (
            train_key, course_cd, stat_type, race_date,
            stats['starts'], stats['win'], stats['place'], 
            stats['show'], stats['fourth']
        ))

###############################################################################
# Jock/Trainer By Track/Course Code (ALL_RACES_J_TRACK)
###############################################################################
def accumulate_jock_trainer_for_date_by_track(cur, race_date):
    """
    For all jock_key–trainer_key combos that ran on race_date,
    grouped by course_cd (track). Then gather prior combos at the same track
    with rr.race_date < race_date. Insert/upsert into jt_accum_stats_by_track.
    """
    stat_type = 'ALL_RACES_JT_TRACK'

    # 1) Which jock/trainer combos, which track on this date?
    cur.execute("""
        SELECT DISTINCT re.jock_key, re.train_key, re.course_cd
        FROM results_entries re
        JOIN race_results rr
          ON re.course_cd=rr.course_cd
         AND re.race_date=rr.race_date
         AND re.race_number=rr.race_number
        WHERE rr.race_date = %s
          AND re.jock_key IS NOT NULL
          AND re.train_key IS NOT NULL
    """, (race_date,))
    combos = cur.fetchall()  # list of (jock_key, train_key, course_cd)

    for (jock_key, train_key, course_cd) in combos:
        # 2) Fetch all prior starts for that (jock, trainer) at same track
        cur.execute("""
            SELECT rr.race_date, re.official_fin
            FROM results_entries re
            JOIN race_results rr
              ON re.course_cd = rr.course_cd
             AND re.race_date = rr.race_date
             AND re.race_number = rr.race_number
            WHERE re.jock_key = %s
              AND re.train_key = %s
              AND re.course_cd = %s
              AND rr.race_date < %s
        """, (jock_key, train_key, course_cd, race_date))

        rows = cur.fetchall()
        stats = {'starts': 0, 'win': 0, 'place': 0, 'show': 0, 'fourth': 0}
        for (_, off_fin) in rows:
            stats['starts'] += 1
            if off_fin == 1:
                stats['win'] += 1
            elif off_fin == 2:
                stats['place'] += 1
            elif off_fin == 3:
                stats['show'] += 1
            elif off_fin == 4:
                stats['fourth'] += 1

        # 3) Upsert into jt_accum_stats_by_track
        cur.execute("""
            INSERT INTO jt_accum_stats_by_track (
                jock_key, train_key, course_cd, stat_type, as_of_date,
                starts, win, place, "show", fourth
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (jock_key, train_key, course_cd, stat_type, as_of_date)
            DO UPDATE SET
                starts = EXCLUDED.starts,
                win = EXCLUDED.win,
                place = EXCLUDED.place,
                "show" = EXCLUDED."show",
                fourth = EXCLUDED.fourth
        """, (
            jock_key, train_key, course_cd, stat_type, race_date,
            stats['starts'], stats['win'], stats['place'],
            stats['show'], stats['fourth']
        ))

###############################################################################
# process_stat_type - unified approach
###############################################################################

def process_stat_type(conn, cursor, stat_type, surface=None, max_distance=None, min_distance=None, filter_condition=None):
    """
    Main orchestrator that:
      1) ensures stat_type in stat_type_code
      2) fetches last_processed_date
      3) finds new race_dates to process
      4) for each date, calls the appropriate accumulation logic
      5) marks them processed in ingestion_files, updates last_processed in stat_type_code

    For 'ALL_RACES' => horse
    'ALL_RACES_J'  => jockey
    'ALL_RACES_T'  => trainer
    'ALL_RACES_JT' => jock–trainer
    (if you want more e.g. "DIRT_SPRNT" etc., you can adapt the logic below.)
    """
    last_race_date_processed = None

    try:
        # 1) Ensure stat_type is in stat_type_code
        ensure_stat_type_exists(conn, stat_type)

        # 2) last_processed from stat_type_code
        last_processed_date = get_last_processed_date(conn, stat_type)
        logging.info(f"Last processed date for {stat_type} is {last_processed_date}")

        # 3) Build a query to find distinct rr.race_date that is > last_processed_date (if set),
        #    also not in ingestion_files with status='processed' for this stat_type.
        #    You also have surface, filter_condition, etc. if needed.
        conds = []
        params = []
        if last_processed_date:
            conds.append("rr.race_date > %s")
            params.append(last_processed_date)
        # e.g. if surface is not None => conds.append("rr.surface = %s"); params.append(surface)
        # if filter_condition => conds.append(filter_condition) # or apply differently

        where_clause = ""
        if conds:
            where_clause = "WHERE " + " AND ".join(conds)

        # We'll left join ingestion_files, ignoring date if it’s processed for this stat_type
        query = f"""
            SELECT DISTINCT rr.race_date
            FROM race_results rr
            LEFT JOIN ingestion_files if2
                   ON rr.race_date::text = if2.file_name
                  AND if2.message = %s
                  AND if2.status='processed'
            {where_clause}
            AND if2.file_name IS NULL
            ORDER BY rr.race_date ASC
        """
        # param: message = stat_type
        query_params = [stat_type] + params

        cursor.execute(query, query_params)
        new_dates = [row[0] for row in cursor.fetchall()]

        if not new_dates:
            logging.info(f"No new race_dates to process for {stat_type}.")
            return None

        logging.info(f"{len(new_dates)} new race_dates to process for {stat_type}: {new_dates[:10]} ...")

        # 4) For each race_date, we do:
        for rd in new_dates:
            rd_str = str(rd)
            logging.info(f"Processing stat_type={stat_type}, race_date={rd_str}")
            # We'll do a savepoint to rollback just in case
            cursor.execute("SAVEPOINT sp_stat;")
            try:
                # route to the correct function
                if stat_type == "ALL_RACES_J":
                    accumulate_jockey_stats_for_date(cursor, rd)
                elif stat_type == "ALL_RACES_T":
                    accumulate_trainer_stats_for_date(cursor, rd)
                elif stat_type == "ALL_RACES_JT":
                    accumulate_jock_trainer_for_date(cursor, rd)
                elif stat_type == "ALL_RACES_J_TRACK":
                    accumulate_jockey_for_date_by_track(cursor, rd)
                elif stat_type == "ALL_RACES_T_TRACK":
                     accumulate_trainer_for_date_by_track(cursor, rd)
                elif stat_type == "ALL_RACES_JT_TRACK":
                    accumulate_jock_trainer_for_date_by_track(cursor, rd)
                else:
                    # If you have other categories or raise an error
                    logging.warning(f"Unknown stat_type: {stat_type}, skipping.")
                    continue

                # Mark ingestion_files
                mark_race_date_processed(conn, rd_str, stat_type)
                # We'll keep track of the last_race_date
                last_race_date_processed = rd
                # end
                cursor.execute("RELEASE SAVEPOINT sp_stat;")
                conn.commit()
                logging.info(f"Committed {stat_type} stats for {rd_str}.")
            except Exception as e:
                logging.error(f"Error in race_date {rd_str} for {stat_type}: {e}")
                traceback.print_exc()
                conn.rollback()
                logging.info("Rolled back to sp_stat")

        # 5) If all done, update stat_type_code.last_processed
        if last_race_date_processed:
            update_stat_type_last_processed(conn, stat_type, last_race_date_processed)
            conn.commit()

    except Exception as e:
        logging.error(f"Error in process_stat_type({stat_type}): {e}")
        traceback.print_exc()
        conn.rollback()

    return last_race_date_processed


###############################################################################
# Main driver
###############################################################################

def main():
    setup_logging()
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config = read_config(script_dir)
    
    try:
        conn = get_connection(config)
        cur = conn.cursor()

        # Example usage: Suppose you want to handle these 6 stat_types:
        all_stat_types = [
            ("ALL_RACES_J",        "All races stats for jockey"),
            ("ALL_RACES_JT",       "All races stats for jockey-trainer"),
            ("ALL_RACES_T",        "All races stats for trainer"),

            ("ALL_RACES_J_TRACK",  "All races stats for jockey by track"),
            ("ALL_RACES_T_TRACK",  "All races stats for trainer by track"),
            ("ALL_RACES_JT_TRACK", "All races stats for jockey-trainer by track"),
        ]

        # For each (code, description), call process_stat_type
        for (stat_code, stat_desc) in all_stat_types:
            logging.info(f"=== Begin processing for {stat_code} ({stat_desc}) ===")
            
            # If your process_stat_type signature expects only the stat_type code,
            # pass stat_code here. If you also want a description, you can adapt it
            # or store the description in your database if needed.
            last_date = process_stat_type(conn, cur, stat_code)

            logging.info(f"=== Done processing {stat_code}, last_date={last_date} ===")

        # If all goes well, commit changes
        conn.commit()
        logging.info("All stat_types processed successfully.")

    except Exception as e:
        logging.error(f"Error in main: {e}", exc_info=True)
        if 'conn' in locals():
            conn.rollback()
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()
        logging.info("Database connection closed.")

if __name__ == "__main__":
    main()