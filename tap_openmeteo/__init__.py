"""Tap for OpenMeteo weather data.

This Singer tap extracts weather forecast, historical, and current conditions
data from the Open-Meteo API (https://open-meteo.com).

Features:
- Multiple location support
- Forecast data (hourly, daily, 15-minutely)
- Historical weather data
- Current conditions
- Incremental syncs
- Configurable weather variables
- Unit customization
- API key support for commercial use
"""

from tap_openmeteo.tap import TapOpenMeteo

__all__ = ["TapOpenMeteo"]
