def ingest_age_restriction(conn):
    """Ingest age restriction data."""
    # Example data to ingest
    age_restriction_data = [
        ('02', '2 yo'),
        ('03', '3 yo'),
        ('04', '4 yo'),
        ('05', '5 yo'),
        ('06', '6 yo'),
        ('07', '7 yo'),
        ('08', '8 yo'),
        ('09', '9 yo'),
        ('23', '2 & 3 yos'),
        ('24', '2, 3 & 4 - foreign only'),
        ('2U', '2 yos & up'),
        ('34', '3 & 4 yos'),
        ('35', '3, 4, & 5 yos'),
        ('36', '3, 4, 5 & 6 yos'),
        ('3U', '3 yos & up'),
        ('45', '4 & 5 yos'),
        ('46', '4, 5 & 6 yos'),
        ('47', '4, 5, 6 & 7 yos'),
        ('4U', '4 yos & up'),
        ('56', '5 & 6 yos'),
        ('57', '5, 6 & 7 yos'),
        ('58', '5, 6, 7 & 8 yos'),
        ('59', '5, 6, 7, 8 & 9 yos'),
        ('5U', '5 yos & up'),
        ('67', '6 & 7 yos'),
        ('68', '6, 7 & 8 yos'),
        ('69', '6, 7, 8 & 9 yos'),
        ('6U', '6 yos & up'),
        ('78', '7 & 8 yos'),
        ('79', '7, 8 & 9 yos'),
        ('7U', '7 yos & up'),
        ('8U', '8 yos & up'),
        ('9U', '9 yos & up')
        ]

    try:
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO age_restriction (age_restr_cd, description) VALUES (%s, %s) ON CONFLICT DO NOTHING;",
                age_restriction_data
            )
        conn.commit()
    except Exception as e:
        raise RuntimeError(f"Failed to ingest age_restriction: {e}")