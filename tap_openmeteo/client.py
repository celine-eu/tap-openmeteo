"""REST client handling, including OpenMeteoStream base class."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any, ClassVar
from urllib.parse import urlencode

from singer_sdk.streams import RESTStream

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from collections.abc import Iterable

    import requests
    from singer_sdk.helpers.types import Context


class OpenMeteoStream(RESTStream):
    """OpenMeteo stream base class.

    This class provides common functionality for all Open-Meteo API streams,
    including authentication, URL construction, and response parsing.
    """

    # Base configuration
    rest_method = "GET"
    records_jsonpath = "$"  # Open-Meteo returns data at root level

    # Disable pagination - Open-Meteo doesn't use pagination
    next_page_token_jsonpath: ClassVar[str | None] = None

    # Replication configuration - can be overridden in subclasses
    replication_key: str | None = None
    is_sorted = True

    @property
    @override
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings.

        For commercial API access with an API key, use:
        https://customer-api.open-meteo.com

        Returns:
            The base URL for API requests.
        """
        base_url = self.config.get("api_url", "https://api.open-meteo.com")
        # Ensure no trailing slash
        return base_url.rstrip("/")

    @property
    @override
    def http_headers(self) -> dict[str, str]:
        """Return the http headers needed.

        Returns:
            A dictionary of HTTP headers.
        """
        headers = {
            "Accept": "application/json",
        }

        # Add custom user agent if configured
        user_agent = self.config.get("user_agent")
        if user_agent:
            headers["User-Agent"] = user_agent
        else:
            headers["User-Agent"] = f"{self.tap_name}/{self._tap.plugin_version}"

        return headers

    @property
    def request_timeout(self) -> int:
        """Return the request timeout in seconds.

        Returns:
            Timeout value in seconds.
        """
        return self.config.get("request_timeout", 30)

    def get_url_params(
        self,
        context: Context | None,
        next_page_token: Any | None,  # noqa: ARG002
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context containing location info.
            next_page_token: Not used - Open-Meteo doesn't paginate.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict[str, Any] = {}

        # Location parameters from context
        if context:
            params["latitude"] = context.get("latitude")
            params["longitude"] = context.get("longitude")

            # Optional elevation override
            if context.get("elevation") is not None:
                params["elevation"] = context["elevation"]

            # Per-location timezone override
            if context.get("timezone"):
                params["timezone"] = context["timezone"]
            else:
                params["timezone"] = self.config.get("timezone", "auto")
        else:
            # Fallback to first location if no context
            locations = self.config.get("locations", [])
            if locations:
                params["latitude"] = locations[0]["latitude"]
                params["longitude"] = locations[0]["longitude"]
                if locations[0].get("elevation") is not None:
                    params["elevation"] = locations[0]["elevation"]
                params["timezone"] = locations[0].get(
                    "timezone", self.config.get("timezone", "auto")
                )

        # Unit configuration
        temp_unit = self.config.get("temperature_unit", "celsius")
        if temp_unit == "fahrenheit":
            params["temperature_unit"] = "fahrenheit"

        wind_unit = self.config.get("wind_speed_unit", "kmh")
        if wind_unit != "kmh":
            params["wind_speed_unit"] = wind_unit

        precip_unit = self.config.get("precipitation_unit", "mm")
        if precip_unit != "mm":
            params["precipitation_unit"] = precip_unit

        timeformat = self.config.get("timeformat", "iso8601")
        if timeformat != "iso8601":
            params["timeformat"] = timeformat

        # Model selection
        models = self.config.get("models", [])
        if models:
            params["models"] = ",".join(models)

        # Grid cell selection
        cell_selection = self.config.get("cell_selection", "land")
        if cell_selection != "land":
            params["cell_selection"] = cell_selection

        # API key for commercial use
        api_key = self.config.get("api_key")
        if api_key:
            params["apikey"] = api_key

        # Solar radiation panel configuration
        if self.config.get("tilt") is not None:
            params["tilt"] = self.config["tilt"]
        if self.config.get("azimuth") is not None:
            params["azimuth"] = self.config["azimuth"]

        return params

    def build_prepared_request(self, *args: Any, **kwargs: Any) -> requests.PreparedRequest:
        """Build the prepared request with timeout handling.

        Args:
            *args: Positional arguments to pass through.
            **kwargs: Keyword arguments to pass through.

        Returns:
            A prepared request object.
        """
        return super().build_prepared_request(*args, **kwargs)

    @override
    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records.

        This method should be overridden in subclasses to handle
        the specific response format for each stream type.

        Args:
            response: The HTTP response object.

        Yields:
            Each record from the source.
        """
        yield from []

    def get_location_contexts(self) -> list[dict]:
        """Generate contexts for each configured location.

        Returns:
            A list of context dictionaries, one per location.
        """
        locations = self.config.get("locations", [])
        contexts = []

        for location in locations:
            ctx = {
                "location_name": location["name"],
                "latitude": location["latitude"],
                "longitude": location["longitude"],
            }
            if location.get("elevation") is not None:
                ctx["elevation"] = location["elevation"]
            if location.get("timezone"):
                ctx["timezone"] = location["timezone"]

            contexts.append(ctx)

        return contexts

    @property
    def partitions(self) -> list[dict] | None:
        """Return a list of partition contexts for parallel processing.

        Each location becomes a separate partition.

        Returns:
            A list of context dictionaries, one per location.
        """
        return self.get_location_contexts() or None

    def build_request_url(self, params: dict[str, Any]) -> str:
        """Build the full request URL with parameters.

        Args:
            params: URL parameters to include.

        Returns:
            The full request URL.
        """
        # Filter out None values and convert lists to comma-separated strings
        clean_params = {}
        for key, value in params.items():
            if value is None:
                continue
            if isinstance(value, list):
                clean_params[key] = ",".join(str(v) for v in value)
            elif isinstance(value, bool):
                clean_params[key] = str(value).lower()
            else:
                clean_params[key] = value

        base = f"{self.url_base}{self.path}"
        if clean_params:
            return f"{base}?{urlencode(clean_params)}"
        return base
