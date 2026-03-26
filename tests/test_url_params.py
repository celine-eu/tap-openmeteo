"""Tests for URL parameter construction and rolling-window re-extraction."""

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

    def test_no_state_uses_rolling_window(self, hourly_stream: WeatherHourlyStream) -> None:
        """Without incremental state, rolling window params are used."""
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

    def test_with_state_still_uses_rolling_window(
        self, hourly_stream: WeatherHourlyStream
    ) -> None:
        """With incremental state, rolling window is still used (not start_hour/end_hour).

        This ensures forecast NULLs at the edge are overwritten with actuals
        on subsequent runs.
        """
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

        assert "start_hour" not in params
        assert "end_hour" not in params
        assert params["forecast_hours"] == 48
        assert params["past_hours"] == 120


class TestWeatherDailyParams:
    """Tests for WeatherDailyStream.get_url_params."""

    def test_with_state_still_uses_rolling_window(self, tap: TapOpenMeteo) -> None:
        """With incremental state, rolling window is still used (not start_date/end_date).

        This ensures forecast NULLs at the edge are overwritten with actuals
        on subsequent runs.
        """
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

        assert "start_date" not in params
        assert "end_date" not in params
        assert "forecast_hours" in params or "forecast_days" in params
        assert "past_hours" in params or "past_days" in params

    def test_no_state_uses_rolling_window(self, tap: TapOpenMeteo) -> None:
        """Without state, rolling window params are used."""
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


class TestRollingWindowReExtraction:
    """Verify that the tap re-emits the full rolling window on every run.

    This is the core fix: previously, state caused the tap to only fetch
    hours *after* the bookmark, so forecast-edge NULLs were never replaced
    with actuals.  Now every run fetches past_hours + forecast_hours,
    and the target upserts to replace stale values.
    """

    def test_hourly_emits_records_before_bookmark(self) -> None:
        """Records with time < bookmark must be emitted (they carry actuals)."""
        config = {
            **MINIMAL_CONFIG,
            "past_hours": 24,
            "forecast_hours": 6,
        }
        tap = TapOpenMeteo(config=config)
        stream = WeatherHourlyStream(tap)

        context = {
            "location_name": "TestCity",
            "latitude": "45.0",
            "longitude": "11.0",
            "timezone": "Europe/Rome",
        }

        # Simulate bookmark set to 2 hours in the future (yesterday's forecast edge)
        bookmark = (datetime.now(timezone.utc) + timedelta(hours=2)).isoformat()

        with patch.object(
            stream,
            "get_context_state",
            return_value={"replication_key_value": bookmark},
        ):
            records = list(stream.get_records(context))

        # We requested 24h past + 6h forecast = ~30 hours of data
        assert len(records) >= 25, (
            f"Expected >= 25 records (24h past + some forecast), got {len(records)}. "
            "The tap may still be narrowing the window based on state."
        )

        # Records before the bookmark must exist (these are the ones that
        # would replace NULLs in the target)
        bookmark_dt = datetime.fromisoformat(bookmark.replace("Z", "+00:00"))
        # API returns naive datetimes in the configured timezone; make
        # bookmark naive too for comparison.
        bookmark_naive = bookmark_dt.replace(tzinfo=None)
        times_before_bookmark = [
            r["time"]
            for r in records
            if datetime.fromisoformat(r["time"]).replace(tzinfo=None) < bookmark_naive
        ]
        assert len(times_before_bookmark) >= 20, (
            f"Expected >= 20 records before bookmark, got {len(times_before_bookmark)}. "
            "Past hours are not being re-fetched."
        )

    def test_hourly_records_have_non_null_values(self) -> None:
        """Past-window records from the API should have actual values, not NULLs."""
        config = {
            **MINIMAL_CONFIG,
            "past_hours": 24,
            "forecast_hours": 0,
            "hourly_variables": ["temperature_2m"],
        }
        tap = TapOpenMeteo(config=config)
        stream = WeatherHourlyStream(tap)

        context = {
            "location_name": "TestCity",
            "latitude": "45.0",
            "longitude": "11.0",
            "timezone": "Europe/Rome",
        }

        records = list(stream.get_records(context))
        assert len(records) > 0

        non_null_temps = [r for r in records if r.get("temperature_2m") is not None]
        null_ratio = 1 - len(non_null_temps) / len(records)
        assert null_ratio < 0.1, (
            f"{null_ratio:.0%} of past-24h records have NULL temperature_2m. "
            "The API should return actuals for past hours."
        )
