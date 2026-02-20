"""OpenMeteo tap class."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING

from singer_sdk import Tap
from singer_sdk import typing as th

from tap_openmeteo import streams

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    pass


class TapOpenMeteo(Tap):
    """Singer tap for OpenMeteo weather data.

    This tap extracts weather forecast and historical data from the Open-Meteo API.
    It supports multiple locations, various weather variables, and incremental updates.
    """

    name = "tap-openmeteo"

    # Comprehensive configuration schema exposing Open-Meteo options
    config_jsonschema = th.PropertiesList(
        # API Configuration
        th.Property(
            "api_key",
            th.StringType,
            required=False,
            secret=True,
            title="API Key",
            description=(
                "Optional API key for commercial use. Required for accessing "
                "reserved API resources. See https://open-meteo.com/en/pricing"
            ),
        ),
        th.Property(
            "api_url",
            th.StringType,
            default="https://api.open-meteo.com",
            title="API URL",
            description=(
                "Base URL for the Open-Meteo API. Use 'https://customer-api.open-meteo.com' "
                "for commercial API access with an API key."
            ),
        ),
        # Location Configuration
        th.Property(
            "locations",
            th.ArrayType(
                th.ObjectType(
                    th.Property(
                        "name",
                        th.StringType,
                        required=True,
                        description="Human-readable name for this location",
                    ),
                    th.Property(
                        "latitude",
                        th.NumberType,
                        required=True,
                        description="Latitude in WGS84 coordinates (-90 to 90)",
                    ),
                    th.Property(
                        "longitude",
                        th.NumberType,
                        required=True,
                        description="Longitude in WGS84 coordinates (-180 to 180)",
                    ),
                    th.Property(
                        "elevation",
                        th.NumberType,
                        required=False,
                        description=(
                            "Elevation in meters for statistical downscaling. "
                            "If not set, a 90m digital elevation model is used."
                        ),
                    ),
                    th.Property(
                        "timezone",
                        th.StringType,
                        required=False,
                        description=(
                            "Timezone for this location (e.g., 'Europe/Berlin'). "
                            "Use 'auto' to automatically resolve from coordinates."
                        ),
                    ),
                )
            ),
            required=True,
            title="Locations",
            description="List of locations to fetch weather data for",
        ),
        # Time Configuration
        th.Property(
            "timezone",
            th.StringType,
            default="auto",
            title="Default Timezone",
            description=(
                "Default timezone for all locations. Any timezone from the "
                "IANA time zone database is supported. Use 'auto' to resolve "
                "from coordinates or 'UTC' for GMT+0."
            ),
        ),
        th.Property(
            "forecast_days",
            th.IntegerType,
            default=7,
            title="Forecast Days",
            description="Number of forecast days (0-16). Default is 7 days. Ignored if forecast_hours is set.",
        ),
        th.Property(
            "forecast_hours",
            th.IntegerType,
            required=False,
            title="Forecast Hours",
            description=(
                "Number of forecast hours. When set, takes priority over "
                "forecast_days for hourly/minutely streams."
            ),
        ),
        th.Property(
            "past_days",
            th.IntegerType,
            default=0,
            title="Past Days",
            description="Include past days in forecast (0-92). Default is 0. Ignored if past_hours is set.",
        ),
        th.Property(
            "past_hours",
            th.IntegerType,
            required=False,
            title="Past Hours",
            description=(
                "Number of past hours to include. When set, takes priority "
                "over past_days for hourly/minutely streams."
            ),
        ),
        th.Property(
            "start_date",
            th.DateType,
            required=False,
            title="Start Date",
            description=(
                "Start date for historical data (YYYY-MM-DD format). "
                "Used for historical weather stream."
            ),
        ),
        th.Property(
            "end_date",
            th.DateType,
            required=False,
            title="End Date",
            description=(
                "End date for historical data (YYYY-MM-DD format). "
                "If not set, defaults to yesterday."
            ),
        ),
        # Hourly Variables Configuration
        th.Property(
            "hourly_variables",
            th.ArrayType(th.StringType),
            default=[
                "temperature_2m",
                "relative_humidity_2m",
                "precipitation",
                "weather_code",
                "wind_speed_10m",
                "wind_direction_10m",
            ],
            title="Hourly Weather Variables",
            description=(
                "List of hourly weather variables to fetch. Available: "
                "temperature_2m, relative_humidity_2m, dew_point_2m, apparent_temperature, "
                "precipitation_probability, precipitation, rain, showers, snowfall, "
                "snow_depth, weather_code, pressure_msl, surface_pressure, "
                "cloud_cover, cloud_cover_low, cloud_cover_mid, cloud_cover_high, "
                "visibility, evapotranspiration, et0_fao_evapotranspiration, "
                "vapour_pressure_deficit, wind_speed_10m, wind_speed_80m, "
                "wind_speed_120m, wind_speed_180m, wind_direction_10m, wind_direction_80m, "
                "wind_direction_120m, wind_direction_180m, wind_gusts_10m, "
                "temperature_80m, temperature_120m, temperature_180m, "
                "soil_temperature_0cm, soil_temperature_6cm, soil_temperature_18cm, "
                "soil_temperature_54cm, soil_moisture_0_to_1cm, soil_moisture_1_to_3cm, "
                "soil_moisture_3_to_9cm, soil_moisture_9_to_27cm, soil_moisture_27_to_81cm, "
                "uv_index, uv_index_clear_sky, is_day, sunshine_duration, "
                "shortwave_radiation, direct_radiation, diffuse_radiation, "
                "direct_normal_irradiance, global_tilted_irradiance, "
                "terrestrial_radiation, cape, freezing_level_height"
            ),
        ),
        # Daily Variables Configuration
        th.Property(
            "daily_variables",
            th.ArrayType(th.StringType),
            default=[
                "weather_code",
                "temperature_2m_max",
                "temperature_2m_min",
                "precipitation_sum",
                "sunrise",
                "sunset",
            ],
            title="Daily Weather Variables",
            description=(
                "List of daily weather variables to fetch. Available: "
                "weather_code, temperature_2m_max, temperature_2m_min, "
                "apparent_temperature_max, apparent_temperature_min, "
                "sunrise, sunset, daylight_duration, sunshine_duration, "
                "uv_index_max, uv_index_clear_sky_max, precipitation_sum, "
                "rain_sum, showers_sum, snowfall_sum, precipitation_hours, "
                "precipitation_probability_max, wind_speed_10m_max, "
                "wind_gusts_10m_max, wind_direction_10m_dominant, "
                "shortwave_radiation_sum, et0_fao_evapotranspiration"
            ),
        ),
        # Current Weather Configuration
        th.Property(
            "current_variables",
            th.ArrayType(th.StringType),
            default=[
                "temperature_2m",
                "relative_humidity_2m",
                "apparent_temperature",
                "is_day",
                "precipitation",
                "weather_code",
                "cloud_cover",
                "wind_speed_10m",
                "wind_direction_10m",
            ],
            title="Current Weather Variables",
            description="List of current weather variables to include in responses.",
        ),
        # 15-Minutely Variables Configuration
        th.Property(
            "minutely_15_variables",
            th.ArrayType(th.StringType),
            default=[],
            title="15-Minutely Weather Variables",
            description=(
                "List of 15-minutely weather variables. Only available in Central Europe "
                "and North America; other regions use interpolated hourly data. "
                "Available: temperature_2m, relative_humidity_2m, dew_point_2m, "
                "precipitation, rain, snowfall, weather_code, wind_speed_10m, "
                "wind_direction_10m, wind_gusts_10m, shortwave_radiation, "
                "direct_radiation, diffuse_radiation, sunshine_duration, "
                "visibility, cape, lightning_potential, is_day"
            ),
        ),
        # Unit Configuration
        th.Property(
            "temperature_unit",
            th.StringType,
            default="celsius",
            title="Temperature Unit",
            description="Temperature unit: 'celsius' or 'fahrenheit'",
            allowed_values=["celsius", "fahrenheit"],
        ),
        th.Property(
            "wind_speed_unit",
            th.StringType,
            default="kmh",
            title="Wind Speed Unit",
            description="Wind speed unit: 'kmh', 'ms', 'mph', or 'kn' (knots)",
            allowed_values=["kmh", "ms", "mph", "kn"],
        ),
        th.Property(
            "precipitation_unit",
            th.StringType,
            default="mm",
            title="Precipitation Unit",
            description="Precipitation unit: 'mm' or 'inch'",
            allowed_values=["mm", "inch"],
        ),
        th.Property(
            "timeformat",
            th.StringType,
            default="iso8601",
            title="Time Format",
            description=(
                "Time format: 'iso8601' (e.g., 2024-01-01T00:00) or "
                "'unixtime' (UNIX epoch seconds)"
            ),
            allowed_values=["iso8601", "unixtime"],
        ),
        # Model Selection
        th.Property(
            "models",
            th.ArrayType(th.StringType),
            default=[],
            title="Weather Models",
            description=(
                "Specific weather models to use. Empty list uses 'best_match' "
                "which combines the best models for each location. "
                "Options include: ecmwf_ifs025, gfs_seamless, icon_seamless, "
                "meteofrance_seamless, jma_seamless, gem_seamless, etc."
            ),
        ),
        # Grid Cell Selection
        th.Property(
            "cell_selection",
            th.StringType,
            default="land",
            title="Grid Cell Selection",
            description=(
                "How to select grid cells: 'land' (finds suitable cell on land), "
                "'sea' (prefers cells on sea), 'nearest' (selects nearest cell)"
            ),
            allowed_values=["land", "sea", "nearest"],
        ),
        # Solar Radiation Configuration
        th.Property(
            "tilt",
            th.NumberType,
            required=False,
            title="Panel Tilt",
            description=(
                "Panel tilt angle for global tilted irradiance (0-90 degrees). "
                "0° is horizontal, typically around 45°."
            ),
        ),
        th.Property(
            "azimuth",
            th.NumberType,
            required=False,
            title="Panel Azimuth",
            description=(
                "Panel azimuth for global tilted irradiance. "
                "0° = South, -90° = East, 90° = West, ±180° = North"
            ),
        ),
        # Stream Selection
        th.Property(
            "streams_to_sync",
            th.ArrayType(th.StringType),
            default=["weather_forecast", "weather_hourly", "weather_daily"],
            title="Streams to Sync",
            description=(
                "List of streams to sync. Available: "
                "weather_forecast, weather_hourly, weather_daily, "
                "weather_current, weather_minutely_15, weather_historical"
            ),
        ),
        # Request Configuration
        th.Property(
            "user_agent",
            th.StringType,
            required=False,
            title="User Agent",
            description=(
                "Custom User-Agent header for API requests. "
                "Helps Open-Meteo track usage from your application."
            ),
        ),
        th.Property(
            "request_timeout",
            th.IntegerType,
            default=30,
            title="Request Timeout",
            description="HTTP request timeout in seconds",
        ),
    ).to_dict()

    @override
    def discover_streams(self) -> list[streams.OpenMeteoStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams based on configuration.
        """
        available_streams = {
            "weather_forecast": streams.WeatherForecastStream,
            "weather_hourly": streams.WeatherHourlyStream,
            "weather_daily": streams.WeatherDailyStream,
            "weather_current": streams.WeatherCurrentStream,
            "weather_minutely_15": streams.WeatherMinutely15Stream,
            "weather_historical": streams.WeatherHistoricalStream,
        }

        streams_to_sync = self.config.get(
            "streams_to_sync",
            ["weather_forecast", "weather_hourly", "weather_daily"],
        )

        discovered = []
        for stream_name in streams_to_sync:
            if stream_name in available_streams:
                discovered.append(available_streams[stream_name](self))
            else:
                self.logger.warning("Unknown stream requested: %s", stream_name)

        return discovered


if __name__ == "__main__":
    TapOpenMeteo.cli()
