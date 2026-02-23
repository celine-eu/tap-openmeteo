"""Stream type classes for tap-openmeteo.

This module defines all available streams for extracting data from Open-Meteo API:
- WeatherForecastStream: Combined forecast data with metadata
- WeatherHourlyStream: Hourly weather variables (incremental)
- WeatherDailyStream: Daily weather aggregations (incremental)
- WeatherCurrentStream: Current weather conditions
- WeatherMinutely15Stream: 15-minute resolution data
- WeatherHistoricalStream: Historical weather data (incremental)
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, ClassVar

from singer_sdk import typing as th

from tap_openmeteo.client import OpenMeteoStream

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from collections.abc import Iterable

    import requests
    from singer_sdk.helpers.types import Context


# Re-export OpenMeteoStream for backwards compatibility
__all__ = [
    "OpenMeteoStream",
    "WeatherForecastStream",
    "WeatherHourlyStream",
    "WeatherDailyStream",
    "WeatherCurrentStream",
    "WeatherMinutely15Stream",
    "WeatherHistoricalStream",
]


class WeatherForecastStream(OpenMeteoStream):
    """Weather Forecast stream - returns forecast metadata and summary.

    This stream provides the raw API response including location metadata,
    timezone information, and generation time.
    """

    name = "weather_forecast"
    path = "/v1/forecast"
    primary_keys: ClassVar[list[str]] = ["location_name", "generated_at"]
    replication_key = "generated_at"

    schema = th.PropertiesList(
        # Location identifiers
        th.Property("location_name", th.StringType, description="Human-readable location name"),
        th.Property("latitude", th.NumberType, description="Latitude returned by API"),
        th.Property("longitude", th.NumberType, description="Longitude returned by API"),
        th.Property("elevation", th.NumberType, description="Elevation in meters"),
        # Metadata
        th.Property("timezone", th.StringType, description="Timezone identifier"),
        th.Property("timezone_abbreviation", th.StringType, description="Timezone abbreviation"),
        th.Property("utc_offset_seconds", th.IntegerType, description="UTC offset in seconds"),
        th.Property(
            "generationtime_ms",
            th.NumberType,
            description="API generation time in milliseconds",
        ),
        th.Property("generated_at", th.DateTimeType, description="Timestamp when data was generated"),
        # Configuration used
        th.Property("forecast_days", th.IntegerType, description="Number of forecast days requested"),
        th.Property("past_days", th.IntegerType, description="Number of past days included"),
        th.Property(
            "hourly_variables",
            th.ArrayType(th.StringType),
            description="Hourly variables requested",
        ),
        th.Property(
            "daily_variables",
            th.ArrayType(th.StringType),
            description="Daily variables requested",
        ),
    ).to_dict()

    @override
    def get_url_params(
        self,
        context: Context | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        """Return URL parameters for the forecast endpoint.

        Args:
            context: Stream context with location info.
            next_page_token: Not used.

        Returns:
            Dictionary of URL parameters.
        """
        params = super().get_url_params(context, next_page_token)

        # Time range — prefer hours over days when configured
        forecast_hours = self.config.get("forecast_hours")
        if forecast_hours is not None:
            params["forecast_hours"] = forecast_hours
        else:
            params["forecast_days"] = self.config.get("forecast_days", 7)

        past_hours = self.config.get("past_hours")
        if past_hours is not None:
            params["past_hours"] = past_hours
        else:
            params["past_days"] = self.config.get("past_days", 0)

        # Request at least one variable to get metadata
        hourly_vars = self.config.get("hourly_variables", ["temperature_2m"])
        if hourly_vars:
            params["hourly"] = ",".join(hourly_vars[:1])  # Just one for metadata

        return params

    @override
    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the forecast response.

        Args:
            response: HTTP response from Open-Meteo.

        Yields:
            Forecast metadata records.
        """
        data = response.json()

        # Get context from the request URL for location name
        location_name = "unknown"
        if hasattr(self, "_current_context") and self._current_context:
            location_name = self._current_context.get("location_name", "unknown")

        record = {
            "location_name": location_name,
            "latitude": float(data["latitude"]) if data.get("latitude") is not None else None,
            "longitude": float(data["longitude"]) if data.get("longitude") is not None else None,
            "elevation": float(data["elevation"]) if data.get("elevation") is not None else None,
            "timezone": data.get("timezone"),
            "timezone_abbreviation": data.get("timezone_abbreviation"),
            "utc_offset_seconds": data.get("utc_offset_seconds"),
            "generationtime_ms": data.get("generationtime_ms"),
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "forecast_days": self.config.get("forecast_days", 7),
            "past_days": self.config.get("past_days", 0),
            "hourly_variables": self.config.get("hourly_variables", []),
            "daily_variables": self.config.get("daily_variables", []),
        }

        yield record

    @override
    def get_records(self, context: Context | None) -> Iterable[dict]:
        """Get records with context tracking.

        Args:
            context: Stream context.

        Yields:
            Weather forecast records.
        """
        self._current_context = context
        yield from super().get_records(context)


class WeatherHourlyStream(OpenMeteoStream):
    """Hourly weather data stream with incremental support.

    This stream extracts hourly weather variables and supports incremental
    updates based on the timestamp.
    """

    name = "weather_hourly"
    path = "/v1/forecast"
    primary_keys: ClassVar[list[str]] = ["location_name", "time"]
    replication_key = "time"
    is_sorted = True

    # Dynamic schema built from configuration
    @property
    def schema(self) -> dict:
        """Build schema dynamically based on configured variables.

        Returns:
            JSON Schema dictionary.
        """
        properties = th.PropertiesList(
            # Keys
            th.Property("location_name", th.StringType, description="Location identifier"),
            th.Property("latitude", th.NumberType, description="Latitude"),
            th.Property("longitude", th.NumberType, description="Longitude"),
            th.Property("time", th.DateTimeType, description="Timestamp (ISO8601)"),
            th.Property("time_unix", th.IntegerType, description="Unix timestamp"),
        )

        # Add properties for each configured hourly variable
        hourly_vars = self.config.get(
            "hourly_variables",
            ["temperature_2m", "relative_humidity_2m", "precipitation", "weather_code"],
        )

        for var in hourly_vars:
            # Determine type based on variable name
            if var in ("weather_code", "is_day"):
                properties.append(th.Property(var, th.IntegerType))
            elif var in ("time", "sunrise", "sunset"):
                properties.append(th.Property(var, th.StringType))
            else:
                properties.append(th.Property(var, th.NumberType))

        return properties.to_dict()

    @override
    def get_url_params(
        self,
        context: Context | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        """Return URL parameters for hourly data.

        Args:
            context: Stream context.
            next_page_token: Not used.

        Returns:
            Dictionary of URL parameters.
        """
        params = super().get_url_params(context, next_page_token)

        # Time range — prefer hours over days when configured
        forecast_hours = self.config.get("forecast_hours")
        if forecast_hours is not None:
            params["forecast_hours"] = forecast_hours
        else:
            params["forecast_days"] = self.config.get("forecast_days", 7)

        past_hours = self.config.get("past_hours")
        if past_hours is not None:
            params["past_hours"] = past_hours
        else:
            params["past_days"] = self.config.get("past_days", 0)

        # Hourly variables
        hourly_vars = self.config.get(
            "hourly_variables",
            ["temperature_2m", "relative_humidity_2m", "precipitation", "weather_code"],
        )
        params["hourly"] = ",".join(hourly_vars)

        # Check for incremental state
        state = self.get_context_state(context)
        if state and state.get("replication_key_value"):
            last_sync = state["replication_key_value"]
            if isinstance(last_sync, str):
                try:
                    dt = datetime.fromisoformat(last_sync.replace("Z", "+00:00"))
                    params["start_hour"] = dt.strftime("%Y-%m-%dT%H:%M")
                    # end_hour is required when start_hour is set
                    forecast_hours = self.config.get("forecast_hours", 48)
                    end_dt = datetime.now(timezone.utc) + timedelta(
                        hours=int(forecast_hours),
                    )
                    params["end_hour"] = end_dt.strftime("%Y-%m-%dT%H:%M")
                    # Remove mutually exclusive params
                    params.pop("forecast_hours", None)
                    params.pop("past_hours", None)
                    params.pop("forecast_days", None)
                    params.pop("past_days", None)
                except ValueError:
                    pass

        return params

    @override
    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse hourly data from response.

        Args:
            response: HTTP response from Open-Meteo.

        Yields:
            Hourly weather records.
        """
        data = response.json()

        # Get location context
        location_name = "unknown"
        if hasattr(self, "_current_context") and self._current_context:
            location_name = self._current_context.get("location_name", "unknown")

        latitude = float(data["latitude"]) if data.get("latitude") is not None else None
        longitude = float(data["longitude"]) if data.get("longitude") is not None else None

        # Get hourly data
        hourly = data.get("hourly", {})
        times = hourly.get("time", [])

        if not times:
            return

        # Get all variable values
        hourly_vars = self.config.get(
            "hourly_variables",
            ["temperature_2m", "relative_humidity_2m", "precipitation", "weather_code"],
        )

        for i, time_val in enumerate(times):
            record = {
                "location_name": location_name,
                "latitude": latitude,
                "longitude": longitude,
                "time": time_val,
            }

            # Convert time to unix if we have it in ISO format
            if isinstance(time_val, str):
                try:
                    dt = datetime.fromisoformat(time_val)
                    record["time_unix"] = int(dt.timestamp())
                except ValueError:
                    record["time_unix"] = None
            else:
                record["time_unix"] = time_val
                # Convert unix to ISO
                record["time"] = datetime.fromtimestamp(
                    time_val, tz=timezone.utc
                ).isoformat()

            # Add each variable value
            for var in hourly_vars:
                values = hourly.get(var, [])
                if i < len(values):
                    record[var] = values[i]
                else:
                    record[var] = None

            yield record

    @override
    def get_records(self, context: Context | None) -> Iterable[dict]:
        """Get records with context tracking.

        Args:
            context: Stream context.

        Yields:
            Hourly weather records.
        """
        self._current_context = context
        yield from super().get_records(context)


class WeatherDailyStream(OpenMeteoStream):
    """Daily weather aggregations stream with incremental support.

    This stream extracts daily weather aggregations and supports
    incremental updates based on the date.
    """

    name = "weather_daily"
    path = "/v1/forecast"
    primary_keys: ClassVar[list[str]] = ["location_name", "date"]
    replication_key = "date"
    is_sorted = True

    @property
    def schema(self) -> dict:
        """Build schema dynamically based on configured variables.

        Returns:
            JSON Schema dictionary.
        """
        properties = th.PropertiesList(
            # Keys
            th.Property("location_name", th.StringType, description="Location identifier"),
            th.Property("latitude", th.NumberType, description="Latitude"),
            th.Property("longitude", th.NumberType, description="Longitude"),
            th.Property("date", th.DateType, description="Date"),
        )

        # Add properties for each configured daily variable
        daily_vars = self.config.get(
            "daily_variables",
            ["weather_code", "temperature_2m_max", "temperature_2m_min", "precipitation_sum"],
        )

        for var in daily_vars:
            if var == "weather_code":
                properties.append(th.Property(var, th.IntegerType))
            elif var in ("sunrise", "sunset"):
                properties.append(th.Property(var, th.DateTimeType))
            else:
                properties.append(th.Property(var, th.NumberType))

        return properties.to_dict()

    @override
    def get_url_params(
        self,
        context: Context | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        """Return URL parameters for daily data.

        Args:
            context: Stream context.
            next_page_token: Not used.

        Returns:
            Dictionary of URL parameters.
        """
        params = super().get_url_params(context, next_page_token)

        # Time range — prefer hours over days when configured
        forecast_hours = self.config.get("forecast_hours")
        if forecast_hours is not None:
            params["forecast_hours"] = forecast_hours
        else:
            params["forecast_days"] = self.config.get("forecast_days", 7)

        past_hours = self.config.get("past_hours")
        if past_hours is not None:
            params["past_hours"] = past_hours
        else:
            params["past_days"] = self.config.get("past_days", 0)

        # Daily variables
        daily_vars = self.config.get(
            "daily_variables",
            ["weather_code", "temperature_2m_max", "temperature_2m_min", "precipitation_sum"],
        )
        params["daily"] = ",".join(daily_vars)

        # Check for incremental state
        state = self.get_context_state(context)
        if state and state.get("replication_key_value"):
            last_sync = state["replication_key_value"]
            if isinstance(last_sync, str):
                try:
                    dt = datetime.fromisoformat(last_sync.replace("Z", "+00:00"))
                    params["start_date"] = dt.strftime("%Y-%m-%d")
                    # end_date is required when start_date is set
                    forecast_days = self.config.get("forecast_days", 7)
                    end_dt = datetime.now(timezone.utc) + timedelta(
                        days=int(forecast_days),
                    )
                    params["end_date"] = end_dt.strftime("%Y-%m-%d")
                    # Remove mutually exclusive params
                    params.pop("forecast_hours", None)
                    params.pop("past_hours", None)
                    params.pop("forecast_days", None)
                    params.pop("past_days", None)
                except ValueError:
                    pass

        return params

    @override
    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse daily data from response.

        Args:
            response: HTTP response from Open-Meteo.

        Yields:
            Daily weather records.
        """
        data = response.json()

        location_name = "unknown"
        if hasattr(self, "_current_context") and self._current_context:
            location_name = self._current_context.get("location_name", "unknown")

        latitude = float(data["latitude"]) if data.get("latitude") is not None else None
        longitude = float(data["longitude"]) if data.get("longitude") is not None else None

        # Get daily data
        daily = data.get("daily", {})
        times = daily.get("time", [])

        if not times:
            return

        daily_vars = self.config.get(
            "daily_variables",
            ["weather_code", "temperature_2m_max", "temperature_2m_min", "precipitation_sum"],
        )

        for i, time_val in enumerate(times):
            record = {
                "location_name": location_name,
                "latitude": latitude,
                "longitude": longitude,
                "date": time_val,
            }

            # Add each variable value
            for var in daily_vars:
                values = daily.get(var, [])
                if i < len(values):
                    record[var] = values[i]
                else:
                    record[var] = None

            yield record

    @override
    def get_records(self, context: Context | None) -> Iterable[dict]:
        """Get records with context tracking.

        Args:
            context: Stream context.

        Yields:
            Daily weather records.
        """
        self._current_context = context
        yield from super().get_records(context)


class WeatherCurrentStream(OpenMeteoStream):
    """Current weather conditions stream.

    This stream provides the current weather conditions for each location.
    """

    name = "weather_current"
    path = "/v1/forecast"
    primary_keys: ClassVar[list[str]] = ["location_name", "time"]
    replication_key = "time"

    @property
    def schema(self) -> dict:
        """Build schema based on configured current variables.

        Returns:
            JSON Schema dictionary.
        """
        properties = th.PropertiesList(
            th.Property("location_name", th.StringType),
            th.Property("latitude", th.NumberType),
            th.Property("longitude", th.NumberType),
            th.Property("time", th.DateTimeType),
            th.Property("interval", th.IntegerType, description="Data interval in seconds"),
        )

        current_vars = self.config.get(
            "current_variables",
            [
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
        )

        for var in current_vars:
            if var in ("weather_code", "is_day"):
                properties.append(th.Property(var, th.IntegerType))
            else:
                properties.append(th.Property(var, th.NumberType))

        return properties.to_dict()

    @override
    def get_url_params(
        self,
        context: Context | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        """Return URL parameters for current weather.

        Args:
            context: Stream context.
            next_page_token: Not used.

        Returns:
            Dictionary of URL parameters.
        """
        params = super().get_url_params(context, next_page_token)

        # Current variables
        current_vars = self.config.get(
            "current_variables",
            [
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
        )
        params["current"] = ",".join(current_vars)

        return params

    @override
    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse current weather from response.

        Args:
            response: HTTP response from Open-Meteo.

        Yields:
            Current weather record.
        """
        data = response.json()

        location_name = "unknown"
        if hasattr(self, "_current_context") and self._current_context:
            location_name = self._current_context.get("location_name", "unknown")

        current = data.get("current", {})
        if not current:
            return

        record = {
            "location_name": location_name,
            "latitude": float(data["latitude"]) if data.get("latitude") is not None else None,
            "longitude": float(data["longitude"]) if data.get("longitude") is not None else None,
            "time": current.get("time"),
            "interval": current.get("interval"),
        }

        # Add all current variables
        current_vars = self.config.get(
            "current_variables",
            [
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
        )

        for var in current_vars:
            record[var] = current.get(var)

        yield record

    @override
    def get_records(self, context: Context | None) -> Iterable[dict]:
        """Get records with context tracking.

        Args:
            context: Stream context.

        Yields:
            Current weather records.
        """
        self._current_context = context
        yield from super().get_records(context)


class WeatherMinutely15Stream(OpenMeteoStream):
    """15-minutely weather data stream.

    Available in Central Europe and North America with native 15-minute resolution.
    Other regions use interpolated hourly data.
    """

    name = "weather_minutely_15"
    path = "/v1/forecast"
    primary_keys: ClassVar[list[str]] = ["location_name", "time"]
    replication_key = "time"
    is_sorted = True

    @property
    def schema(self) -> dict:
        """Build schema based on configured 15-minutely variables.

        Returns:
            JSON Schema dictionary.
        """
        properties = th.PropertiesList(
            th.Property("location_name", th.StringType),
            th.Property("latitude", th.NumberType),
            th.Property("longitude", th.NumberType),
            th.Property("time", th.DateTimeType),
        )

        minutely_vars = self.config.get(
            "minutely_15_variables",
            ["temperature_2m", "precipitation", "weather_code"],
        )

        for var in minutely_vars:
            if var in ("weather_code", "is_day"):
                properties.append(th.Property(var, th.IntegerType))
            else:
                properties.append(th.Property(var, th.NumberType))

        return properties.to_dict()

    @override
    def get_url_params(
        self,
        context: Context | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        """Return URL parameters for 15-minutely data.

        Args:
            context: Stream context.
            next_page_token: Not used.

        Returns:
            Dictionary of URL parameters.
        """
        params = super().get_url_params(context, next_page_token)

        # Time range — prefer hours over days when configured
        forecast_hours = self.config.get("forecast_hours")
        if forecast_hours is not None:
            params["forecast_hours"] = forecast_hours
        else:
            params["forecast_days"] = self.config.get("forecast_days", 7)

        # 15-minutely variables
        minutely_vars = self.config.get(
            "minutely_15_variables",
            ["temperature_2m", "precipitation", "weather_code"],
        )

        if minutely_vars:
            params["minutely_15"] = ",".join(minutely_vars)

        return params

    @override
    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse 15-minutely data from response.

        Args:
            response: HTTP response from Open-Meteo.

        Yields:
            15-minutely weather records.
        """
        data = response.json()

        location_name = "unknown"
        if hasattr(self, "_current_context") and self._current_context:
            location_name = self._current_context.get("location_name", "unknown")

        latitude = float(data["latitude"]) if data.get("latitude") is not None else None
        longitude = float(data["longitude"]) if data.get("longitude") is not None else None

        # Get minutely_15 data
        minutely = data.get("minutely_15", {})
        times = minutely.get("time", [])

        if not times:
            return

        minutely_vars = self.config.get(
            "minutely_15_variables",
            ["temperature_2m", "precipitation", "weather_code"],
        )

        for i, time_val in enumerate(times):
            record = {
                "location_name": location_name,
                "latitude": latitude,
                "longitude": longitude,
                "time": time_val,
            }

            for var in minutely_vars:
                values = minutely.get(var, [])
                if i < len(values):
                    record[var] = values[i]
                else:
                    record[var] = None

            yield record

    @override
    def get_records(self, context: Context | None) -> Iterable[dict]:
        """Get records with context tracking.

        Args:
            context: Stream context.

        Yields:
            15-minutely weather records.
        """
        self._current_context = context
        yield from super().get_records(context)


class WeatherHistoricalStream(OpenMeteoStream):
    """Historical weather data stream with incremental support.

    Uses the /v1/archive endpoint for historical reanalysis data
    going back to 1940.
    """

    name = "weather_historical"
    path = "/v1/archive"
    primary_keys: ClassVar[list[str]] = ["location_name", "time"]
    replication_key = "time"
    is_sorted = True

    @property
    def schema(self) -> dict:
        """Build schema based on configured hourly variables.

        Returns:
            JSON Schema dictionary.
        """
        properties = th.PropertiesList(
            th.Property("location_name", th.StringType),
            th.Property("latitude", th.NumberType),
            th.Property("longitude", th.NumberType),
            th.Property("time", th.DateTimeType),
            th.Property("date", th.DateType),
        )

        hourly_vars = self.config.get(
            "hourly_variables",
            ["temperature_2m", "relative_humidity_2m", "precipitation", "weather_code"],
        )

        for var in hourly_vars:
            if var in ("weather_code", "is_day"):
                properties.append(th.Property(var, th.IntegerType))
            else:
                properties.append(th.Property(var, th.NumberType))

        return properties.to_dict()

    @override
    def get_url_params(
        self,
        context: Context | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        """Return URL parameters for historical data.

        Args:
            context: Stream context.
            next_page_token: Not used.

        Returns:
            Dictionary of URL parameters.
        """
        params = super().get_url_params(context, next_page_token)

        # Determine date range
        start_date = self.config.get("start_date")
        end_date = self.config.get("end_date")

        # Check for incremental state
        state = self.get_context_state(context)
        if state and state.get("replication_key_value"):
            last_sync = state["replication_key_value"]
            if isinstance(last_sync, str):
                try:
                    dt = datetime.fromisoformat(last_sync.replace("Z", "+00:00"))
                    start_date = dt.strftime("%Y-%m-%d")
                except ValueError:
                    pass

        # Default to last 30 days if no start date
        if not start_date:
            start_date = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%d")

        # Default end date to yesterday
        if not end_date:
            end_date = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")

        params["start_date"] = start_date
        params["end_date"] = end_date

        # Hourly variables
        hourly_vars = self.config.get(
            "hourly_variables",
            ["temperature_2m", "relative_humidity_2m", "precipitation", "weather_code"],
        )
        params["hourly"] = ",".join(hourly_vars)

        return params

    @override
    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse historical data from response.

        Args:
            response: HTTP response from Open-Meteo.

        Yields:
            Historical weather records.
        """
        data = response.json()

        location_name = "unknown"
        if hasattr(self, "_current_context") and self._current_context:
            location_name = self._current_context.get("location_name", "unknown")

        latitude = float(data["latitude"]) if data.get("latitude") is not None else None
        longitude = float(data["longitude"]) if data.get("longitude") is not None else None

        hourly = data.get("hourly", {})
        times = hourly.get("time", [])

        if not times:
            return

        hourly_vars = self.config.get(
            "hourly_variables",
            ["temperature_2m", "relative_humidity_2m", "precipitation", "weather_code"],
        )

        for i, time_val in enumerate(times):
            record = {
                "location_name": location_name,
                "latitude": latitude,
                "longitude": longitude,
                "time": time_val,
            }

            # Extract date from time
            if isinstance(time_val, str) and "T" in time_val:
                record["date"] = time_val.split("T")[0]
            else:
                record["date"] = None

            for var in hourly_vars:
                values = hourly.get(var, [])
                if i < len(values):
                    record[var] = values[i]
                else:
                    record[var] = None

            yield record

    @override
    def get_records(self, context: Context | None) -> Iterable[dict]:
        """Get records with context tracking.

        Args:
            context: Stream context.

        Yields:
            Historical weather records.
        """
        self._current_context = context
        yield from super().get_records(context)
