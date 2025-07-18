CREATE TABLE IF NOT EXISTS weather (
    date DATE PRIMARY KEY,
    avg_temperature_2m_24h FLOAT,
    avg_relative_humidity_2m_24h FLOAT,
    avg_dew_point_2m_24h FLOAT,
    avg_apparent_temperature_24h FLOAT,
    avg_temperature_80m_24h FLOAT,
    avg_temperature_120m_24h FLOAT,
    avg_wind_speed_10m_24h FLOAT,
    avg_wind_speed_80m_24h FLOAT,
    avg_visibility_24h FLOAT,
    total_rain_24h FLOAT,
    total_showers_24h FLOAT,
    total_snowfall_24h FLOAT,
    avg_temperature_2m_daylight FLOAT,
    avg_relative_humidity_2m_daylight FLOAT,
    avg_dew_point_2m_daylight FLOAT,
    avg_apparent_temperature_daylight FLOAT,
    avg_temperature_80m_daylight FLOAT,
    avg_temperature_120m_daylight FLOAT,
    avg_wind_speed_10m_daylight FLOAT,
    avg_wind_speed_80m_daylight FLOAT,
    avg_visibility_daylight FLOAT,
    total_rain_daylight FLOAT,
    total_showers_daylight FLOAT,
    total_snowfall_daylight FLOAT,
    temperature_2m_celsius FLOAT,
    apparent_temperature_celsius FLOAT,
    temperature_80m_celsius FLOAT,
    temperature_120m_celsius FLOAT,
    soil_temperature_0cm_celsius FLOAT,
    soil_temperature_6cm_celsius FLOAT,
    rain_mm FLOAT,
    showers_mm FLOAT,
    snowfall_mm FLOAT,
    wind_speed_10m_m_per_s FLOAT,
    wind_speed_80m_m_per_s FLOAT,
    daylight_hours FLOAT,
    sunrise_iso TIMESTAMPTZ,
    sunset_iso TIMESTAMPTZ
);