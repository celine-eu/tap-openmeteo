"""OpenMeteo entry point."""

from __future__ import annotations

from tap_openmeteo.tap import TapOpenMeteo

TapOpenMeteo.cli()
