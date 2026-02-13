"""Tests standard tap features using the built-in SDK tests library."""

from __future__ import annotations

from typing import Any

import pytest
from singer_sdk.testing import SuiteConfig, get_tap_test_class

from tap_openmeteo.tap import TapOpenMeteo

# Sample configuration for testing
SAMPLE_CONFIG: dict[str, Any] = {
    "locations": [
        {
            "name": "Berlin",
            "latitude": 52.52,
            "longitude": 13.41,
        },
    ],
    "forecast_days": 3,
    "hourly_variables": [
        "temperature_2m",
        "relative_humidity_2m",
        "precipitation",
        "weather_code",
    ],
    "daily_variables": [
        "weather_code",
        "temperature_2m_max",
        "temperature_2m_min",
        "precipitation_sum",
    ],
    "streams_to_sync": [
        "weather_forecast",
        "weather_hourly",
        "weather_daily",
    ],
}

# Configuration for SDK standard tests
SUITE_CONFIG = SuiteConfig(
    ignore_no_records_for_streams=[
        "weather_minutely_15",  # May not have data in all regions
    ],
)


# Run standard built-in tap tests from the SDK
TestTapOpenMeteo = get_tap_test_class(
    tap_class=TapOpenMeteo,
    config=SAMPLE_CONFIG,
    suite_config=SUITE_CONFIG,
)


class TestTapOpenMeteoCustom:
    """Custom tests for tap-openmeteo."""

    def test_config_validation(self) -> None:
        """Test that configuration is validated properly."""
        # Missing required locations should raise error
        with pytest.raises(Exception):
            TapOpenMeteo(config={})

    def test_multiple_locations(self) -> None:
        """Test tap with multiple locations configured."""
        config = {
            "locations": [
                {"name": "Berlin", "latitude": 52.52, "longitude": 13.41},
                {"name": "Paris", "latitude": 48.85, "longitude": 2.35},
                {"name": "London", "latitude": 51.51, "longitude": -0.13},
            ],
            "forecast_days": 2,
            "hourly_variables": ["temperature_2m"],
            "daily_variables": ["temperature_2m_max"],
            "streams_to_sync": ["weather_forecast"],
        }
        tap = TapOpenMeteo(config=config)
        streams = tap.discover_streams()
        assert len(streams) > 0

    def test_api_key_configuration(self) -> None:
        """Test that API key is properly configured when provided."""
        config = {
            "locations": [{"name": "Berlin", "latitude": 52.52, "longitude": 13.41}],
            "api_key": "test-api-key",
            "api_url": "https://customer-api.open-meteo.com",
            "streams_to_sync": ["weather_forecast"],
        }
        tap = TapOpenMeteo(config=config)
        assert tap.config.get("api_key") == "test-api-key"

    def test_stream_selection(self) -> None:
        """Test that only selected streams are discovered."""
        config = {
            "locations": [{"name": "Berlin", "latitude": 52.52, "longitude": 13.41}],
            "streams_to_sync": ["weather_current"],
        }
        tap = TapOpenMeteo(config=config)
        streams = tap.discover_streams()
        assert len(streams) == 1
        assert streams[0].name == "weather_current"

    def test_all_streams_available(self) -> None:
        """Test that all streams can be selected."""
        config = {
            "locations": [{"name": "Berlin", "latitude": 52.52, "longitude": 13.41}],
            "streams_to_sync": [
                "weather_forecast",
                "weather_hourly",
                "weather_daily",
                "weather_current",
                "weather_minutely_15",
                "weather_historical",
            ],
            "minutely_15_variables": ["temperature_2m"],
        }
        tap = TapOpenMeteo(config=config)
        streams = tap.discover_streams()
        assert len(streams) == 6

    def test_unit_configuration(self) -> None:
        """Test that unit configuration is applied."""
        config = {
            "locations": [{"name": "Berlin", "latitude": 52.52, "longitude": 13.41}],
            "temperature_unit": "fahrenheit",
            "wind_speed_unit": "mph",
            "precipitation_unit": "inch",
            "streams_to_sync": ["weather_forecast"],
        }
        tap = TapOpenMeteo(config=config)
        assert tap.config.get("temperature_unit") == "fahrenheit"
        assert tap.config.get("wind_speed_unit") == "mph"
        assert tap.config.get("precipitation_unit") == "inch"

    def test_timezone_configuration(self) -> None:
        """Test timezone configuration options."""
        config = {
            "locations": [
                {
                    "name": "Berlin",
                    "latitude": 52.52,
                    "longitude": 13.41,
                    "timezone": "Europe/Berlin",
                }
            ],
            "timezone": "UTC",
            "streams_to_sync": ["weather_forecast"],
        }
        tap = TapOpenMeteo(config=config)
        # Default timezone should be overridden by location-specific timezone
        assert tap.config.get("timezone") == "UTC"
        assert tap.config["locations"][0].get("timezone") == "Europe/Berlin"
