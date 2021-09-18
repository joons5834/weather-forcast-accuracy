CREATE MATERIALIZED VIEW `weather-forecast-accuracy.raw_data.wide_kma_ultrashort_fc` 
PARTITION BY TIMESTAMP_TRUNC(fcstTimestamp, YEAR) AS
SELECT
    baseTimestamp,fcstTimestamp, nx, ny,
    MAX(CASE WHEN category = 'T1H' THEN CAST(fcstValue AS INT64) ELSE NULL END) AS temp_c,
    MAX(CASE WHEN category = 'RN1' THEN fcstValue ELSE NULL END) AS precipitation_mm,
    MAX(CASE WHEN category = 'SKY' THEN CAST(fcstValue AS INT64) ELSE NULL END) AS sky_code,
    MAX(CASE WHEN category = 'UUU' THEN CAST(fcstValue AS FLOAT64) ELSE NULL END) AS ew_wind_ms,
    MAX(CASE WHEN category = 'VVV' THEN CAST(fcstValue AS FLOAT64) ELSE NULL END) AS ns_wind_ms,
    MAX(CASE WHEN category = 'REH' THEN CAST(fcstValue AS INT64) ELSE NULL END) AS humidity_pct,
    MAX(CASE WHEN category = 'PTY' THEN CAST(fcstValue AS INT64) ELSE NULL END) AS precipitation_code,
    MAX(CASE WHEN category = 'LGT' THEN CAST(fcstValue AS INT64) ELSE NULL END) AS lightning_code,
    MAX(CASE WHEN category = 'VEC' THEN CAST(fcstValue AS INT64) ELSE NULL END) AS wind_dir_deg,
    MAX(CASE WHEN category = 'WSD' THEN CAST(fcstValue AS INT64) ELSE NULL END) AS wind_speed_ms
FROM `weather-forecast-accuracy.raw_data.ultrashort_fc_kma`
GROUP BY baseTimestamp,fcstTimestamp, nx, ny