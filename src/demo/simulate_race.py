import os
import logging
import sys
import configparser
import psycopg2
from psycopg2 import pool, DatabaseError
import matplotlib
matplotlib.use("Agg")

import matplotlib.pyplot as plt
import matplotlib.animation as animation
from datetime import datetime
from collections import defaultdict

def setup_logging(script_dir, log_file):
    """Sets up logging to a file."""
    try:
        with open(log_file, 'w'):
            pass
        logger = logging.getLogger()
        if logger.hasHandlers():
            logger.handlers.clear()
        logger.setLevel(logging.INFO)

        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)

        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.info("Logging has been set up successfully.")
    except Exception as e:
        print(f"Failed to set up logging: {e}", file=sys.stderr)
        sys.exit(1)

def read_config(script_dir, config_relative_path='../../config.ini'):
    """Reads the config file."""
    try:
        config = configparser.ConfigParser()
        config_file_path = os.path.abspath(os.path.join(script_dir, config_relative_path))
        if not os.path.exists(config_file_path):
            raise FileNotFoundError(f"Config file '{config_file_path}' does not exist.")
        config.read(config_file_path)
        if 'database' not in config:
            raise KeyError("No 'database' section in config.")
        return config
    except Exception as e:
        logging.error(f"Error reading config file: {e}")
        sys.exit(1)

def get_db_pool(config):
    """Creates a connection pool."""
    try:
        db_pool_args = {
            'user': config['database']['user'],
            'host': config['database']['host'],
            'port': config['database']['port'],
            'database': config['database']['dbname']
        }
        password = config['database'].get('password')
        if password:
            db_pool_args['password'] = password
            logging.info("Using password from config.")
        db_pool = pool.SimpleConnectionPool(1, 20, **db_pool_args)
        if db_pool:
            logging.info("Connection pool created successfully.")
        return db_pool
    except DatabaseError as e:
        logging.error(f"Database error: {e}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        sys.exit(1)

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    log_file = os.path.join(script_dir, "demo_race.log")
    setup_logging(script_dir, log_file)

    config = read_config(script_dir)
    db_pool = get_db_pool(config)

    # 1) Query: includes lon/lat with ST_X, ST_Y
    query = """
    SELECT
        r2.distance_meters,
        c.tpd_track_cd 
           || TO_CHAR(r.race_date, 'YYYYMMDD')
           || TO_CHAR(r.post_time, 'HH24MI') AS sharecode,
        r.saddle_cloth_number,
        g.time_stamp,
        g.speed,
        g.progress,
        g.stride_frequency,
        s.gate_numeric,
        s.gate_name,
        s.length_to_finish,
        s.sectional_time,
        s.running_time,
        s.distance_back,
        s.distance_ran,
        s.number_of_strides,
        ST_X(g.location) AS lon,
        ST_Y(g.location) AS lat
    FROM races r2 
    JOIN runners r 
      ON r2.course_cd = r.course_cd
     AND r2.race_date = r.race_date
     AND r2.race_number = r.race_number
    JOIN horse h ON r.axciskey = h.axciskey
    JOIN course c ON r.course_cd = c.course_cd
    JOIN gpspoint g 
      ON r.course_cd = g.course_cd
     AND r.race_date = g.race_date
     AND r.race_number = g.race_number
     AND r.saddle_cloth_number = g.saddle_cloth_number
    JOIN sectionals s
      ON g.course_cd = s.course_cd
     AND g.race_date = s.race_date
     AND g.race_number = s.race_number
     AND g.saddle_cloth_number = s.saddle_cloth_number
    WHERE r.race_date = '20240504'
      AND c.course_cd = 'TCD'
      AND r.race_number = 12
    ORDER BY g.time_stamp, g.saddle_cloth_number;
    """

    logging.info("Acquiring DB connection...")
    conn = db_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
            logging.info(f"Fetched {len(rows)} rows.")
    finally:
        db_pool.putconn(conn)

    if not rows:
        logging.warning("No data returned; exiting.")
        return

    # 2) Build data list
    from datetime import datetime
    race_data = []
    for row in rows:
        (distance_meters, sharecode, scn,
         time_stamp, speed, progress_m, stride_freq,
         gate_num, gate_name, length_to_finish,
         sectional_time, running_time, distance_back,
         distance_ran, number_of_strides, lon, lat) = row

        if isinstance(time_stamp, str):
            dt = datetime.strptime(time_stamp, "%Y-%m-%d %H:%M:%S.%f")
        else:
            dt = time_stamp

        race_data.append({
            "saddle_cloth_number": scn,
            "time_stamp": dt,
            "lon": lon,
            "lat": lat
            # plus other fields if needed
        })

    race_data.sort(key=lambda r: r["time_stamp"])

    # If we want to animate by *unique* timestamps:
    from collections import defaultdict
    time_dict = defaultdict(list)
    for r in race_data:
        time_dict[r["time_stamp"]].append(r)
    unique_times = sorted(time_dict.keys())
    num_frames = len(unique_times)

    # 3) SHIFT & SCALE lat/lon => (x,y)
    #    a) find min_lon, min_lat
    #    b) find range, pick scale factor
    all_lons = [r["lon"] for r in race_data]
    all_lats = [r["lat"] for r in race_data]

    min_lon = min(all_lons)
    max_lon = max(all_lons)
    min_lat = min(all_lats)
    max_lat = max(all_lats)

    lon_range = max_lon - min_lon
    lat_range = max_lat - min_lat

    # Example scale factor so largest dimension becomes ~1000 units wide
    # If your track is bigger or smaller, adjust as needed
    # We'll pick the bigger dimension to map to ~1000
    bigger_range = max(lon_range, lat_range)
    if bigger_range > 0:
        scale_factor = 1000.0 / bigger_range
    else:
        scale_factor = 1.0  # avoid division by zero

    # Shift + scale: define a helper function
    def transform(lon, lat):
        x = (lon - min_lon) * scale_factor
        y = (lat - min_lat) * scale_factor
        return x, y

    # 4) We'll store x,y in time_dict now
    for ts in unique_times:
        for row in time_dict[ts]:
            (row["x"], row["y"]) = transform(row["lon"], row["lat"])

    # 5) Setup the figure
    fig, ax = plt.subplots()
    ax.set_title("Kentucky Derby - SHIFT & SCALE GPS")
    ax.set_xlabel("X (scaled coords)")
    ax.set_ylabel("Y (scaled coords)")

    # bounding box in x,y
    # after transform, min x=0, min y=0
    # max x = scale_factor*(max_lon-min_lon), etc.
    max_x = (max_lon - min_lon) * scale_factor
    max_y = (max_lat - min_lat) * scale_factor
    margin = 50  # extra space around edges
    ax.set_xlim(-margin, max_x + margin)
    ax.set_ylim(-margin, max_y + margin)

    # We won't do aspect="equal" unless you want to keep shape strictly
    # ax.set_aspect("equal", "box")

    # Identify all horses
    horses = sorted(set(r["saddle_cloth_number"] for r in race_data))

    # We'll place a scatter & text label for each horse
    scatters = {}
    labels = {}
    colors = plt.cm.tab20
    for i, scn in enumerate(horses):
        c = colors(i % 20)
        sc = ax.scatter([], [], color=c, s=40)
        txt = ax.text(0, 0, str(scn), color=c, fontsize=8, visible=False)
        scatters[scn] = sc
        labels[scn] = txt

    # Optional: If you want a legend outside
    for scn in horses:
        scatters[scn].set_label(f"#{scn}")
    ax.legend(loc="upper left", bbox_to_anchor=(1.05, 1))

    # 6) Animation frames => each unique timestamp
    #    We'll also do a time stretch so if real race was 10s, we can multiply
    #    If real race is from first to last = ~some seconds,
    #    we can do: time_stretch = 4 => 4x slower
    if num_frames > 1:
        real_start = unique_times[0]
        real_end = unique_times[-1]
        real_duration_s = (real_end - real_start).total_seconds()
        # If the race was 10s in real life & we do time_stretch=4 => 40s
        time_stretch = 10.0
        desired_s = real_duration_s * time_stretch
        interval_ms = (desired_s / num_frames)*1000
    else:
        interval_ms = 1000.0

    def init_anim():
        # Hide all at start
        for scn in scatters:
            scatters[scn].set_offsets([[9999, 9999]])
            labels[scn].set_position((9999, 9999))
            labels[scn].set_visible(False)
        return list(scatters.values()) + list(labels.values())

    def update_anim(frame_idx):
        if frame_idx >= num_frames:
            return []

        current_ts = unique_times[frame_idx]
        # For each row at this timestamp, move the horse
        for row in time_dict[current_ts]:
            scn = row["saddle_cloth_number"]
            x = row["x"]
            y = row["y"]
            scatters[scn].set_offsets([[x, y]])
            labels[scn].set_position((x, y))
            labels[scn].set_visible(True)

        return list(scatters.values()) + list(labels.values())

    ani = animation.FuncAnimation(
        fig,
        update_anim,
        init_func=init_anim,
        frames=num_frames,
        interval=interval_ms,
        blit=False
    )

    mp4_filename = os.path.join(script_dir, "kentucky_derby_demo.mp4")
    WriterClass = animation.writers["ffmpeg"]
    writer = WriterClass(fps=15, metadata={"artist": "FoxRiverAI"}, bitrate=1800)
    ani.save(mp4_filename, writer=writer)
    logging.info(f"Animation saved to {mp4_filename}")

    if db_pool:
        db_pool.closeall()
        logging.info("Closed all DB connections.")

if __name__ == "__main__":
    main()