CREATE MATERIALIZED VIEW `weather-forecast-accuracy.raw_data.wide_kma_ultrashort_fc` AS
SELECT
    baseTimestamp,fcstTimestamp, nx, ny,
    MAX(CASE WHEN category = 'T1H' THEN fcstValue ELSE NULL END) AS temp_c,
    MAX(CASE WHEN category = 'RN1' THEN fcstValue ELSE NULL END) AS precipitation_mm,
    MAX(CASE WHEN category = 'SKY' THEN fcstValue ELSE NULL END) AS sky_code,
    MAX(CASE WHEN category = 'UUU' THEN fcstValue ELSE NULL END) AS ew_wind_ms,
    MAX(CASE WHEN category = 'VVV' THEN fcstValue ELSE NULL END) AS ns_wind_ms,
    MAX(CASE WHEN category = 'REH' THEN fcstValue ELSE NULL END) AS humidity_pct,
    MAX(CASE WHEN category = 'PTY' THEN fcstValue ELSE NULL END) AS precipitation_code,
    MAX(CASE WHEN category = 'LGT' THEN fcstValue ELSE NULL END) AS lightning_code,
    MAX(CASE WHEN category = 'VEC' THEN fcstValue ELSE NULL END) AS wind_dir_deg,
    MAX(CASE WHEN category = 'WSD' THEN fcstValue ELSE NULL END) AS wind_speed_ms
FROM `weather-forecast-accuracy.raw_data.ultrashort_fc_kma`
GROUP BY baseTimestamp,fcstTimestamp, nx, ny