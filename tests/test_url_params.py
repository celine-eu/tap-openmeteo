"""Tests for URL parameter construction in streams."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from tap_openmeteo.streams import WeatherDailyStream, WeatherHourlyStream
from tap_openmeteo.tap import TapOpenMeteo


MINIMAL_CONFIG = {
    "api_url": "https://api.open-meteo.com",
    "locations": [
        {
            "name": "TestCity",
            "latitude": 45.0,
            "longitude": 11.0,
            "timezone": "Europe/Rome",
        }
    ],
    "timezone": "Europe/Rome",
    "forecast_hours": 48,
    "past_hours": 120,
    "models": ["icon_d2"],
    "hourly_variables": ["temperature_2m"],
    "streams_to_sync": ["weather_hourly"],
}


@pytest.fixture()
def tap() -> TapOpenMeteo:
    """Create a TapOpenMeteo instance with minimal config."""
    return TapOpenMeteo(config=MINIMAL_CONFIG)


@pytest.fixture()
def hourly_stream(tap: TapOpenMeteo) -> WeatherHourlyStream:
    """Create a WeatherHourlyStream instance."""
    return WeatherHourlyStream(tap)


class TestWeatherHourlyParams:
    """Tests for WeatherHourlyStream.get_url_params."""

    def test_no_state_no_start_hour(self, hourly_stream: WeatherHourlyStream) -> None:
        """Without incremental state, start_hour should not be set."""
        context = {
            "location_name": "TestCity",
            "latitude": "45.0",
            "longitude": "11.0",
            "timezone": "Europe/Rome",
        }
        params = hourly_stream.get_url_params(context, None)

        assert "start_hour" not in params
        assert "end_hour" not in params
        assert params["forecast_hours"] == 48
        assert params["past_hours"] == 120

    def test_with_state_sets_both_start_and_end_hour(
        self, hourly_stream: WeatherHourlyStream
    ) -> None:
        """With incremental state, both start_hour and end_hour must be set."""
        context = {
            "location_name": "TestCity",
            "latitude": "45.0",
            "longitude": "11.0",
            "timezone": "Europe/Rome",
        }
        with patch.object(
            hourly_stream,
            "get_context_state",
            return_value={"replication_key_value": "2026-02-20T10:00"},
        ):
            params = hourly_stream.get_url_params(context, None)

        assert "start_hour" in params
        assert "end_hour" in params
        assert params["start_hour"] == "2026-02-20T10:00"
        # end_hour should be roughly now + forecast_hours (48h)
        end_dt = datetime.strptime(params["end_hour"], "%Y-%m-%dT%H:%M")
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        assert abs((end_dt - now).total_seconds() - 48 * 3600) < 300

    def test_with_state_removes_mutually_exclusive_params(
        self, hourly_stream: WeatherHourlyStream
    ) -> None:
        """forecast_hours/past_hours must be removed when start_hour/end_hour are set."""
        context = {
            "location_name": "TestCity",
            "latitude": "45.0",
            "longitude": "11.0",
            "timezone": "Europe/Rome",
        }
        with patch.object(
            hourly_stream,
            "get_context_state",
            return_value={"replication_key_value": "2026-02-23T00:00"},
        ):
            params = hourly_stream.get_url_params(context, None)

        assert "start_hour" in params
        assert "end_hour" in params
        assert "forecast_hours" not in params
        assert "past_hours" not in params
        assert "forecast_days" not in params
        assert "past_days" not in params


class TestWeatherDailyParams:
    """Tests for WeatherDailyStream.get_url_params."""

    def test_with_state_sets_both_start_and_end_date(self, tap: TapOpenMeteo) -> None:
        """With incremental state, both start_date and end_date must be set."""
        config = {**MINIMAL_CONFIG, "streams_to_sync": ["weather_daily"]}
        daily_tap = TapOpenMeteo(config=config)
        daily_stream = WeatherDailyStream(daily_tap)

        context = {
            "location_name": "TestCity",
            "latitude": "45.0",
            "longitude": "11.0",
            "timezone": "Europe/Rome",
        }
        with patch.object(
            daily_stream,
            "get_context_state",
            return_value={"replication_key_value": "2026-02-15T00:00"},
        ):
            params = daily_stream.get_url_params(context, None)

        assert "start_date" in params
        assert "end_date" in params
        assert params["start_date"] == "2026-02-15"
        assert "forecast_hours" not in params
        assert "past_hours" not in params
        assert "forecast_days" not in params
        assert "past_days" not in params

    def test_no_state_no_start_date(self, tap: TapOpenMeteo) -> None:
        """Without state, start_date should not be set."""
        config = {**MINIMAL_CONFIG, "streams_to_sync": ["weather_daily"]}
        daily_tap = TapOpenMeteo(config=config)
        daily_stream = WeatherDailyStream(daily_tap)

        context = {
            "location_name": "TestCity",
            "latitude": "45.0",
            "longitude": "11.0",
            "timezone": "Europe/Rome",
        }
        params = daily_stream.get_url_params(context, None)

        assert "start_date" not in params
        assert "end_date" not in params
