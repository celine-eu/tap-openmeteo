# tap-openmeteo

`tap-openmeteo` is a Singer tap for the [Open-Meteo](https://open-meteo.com) weather API.

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

## Features

- **Multiple Locations**: Fetch weather data for any number of locations worldwide
- **Comprehensive Weather Data**: Hourly, daily, 15-minutely, current conditions, and historical data
- **Incremental Syncs**: Efficient syncing with support for incremental updates
- **Flexible Configuration**: Extensive configuration options for weather variables, units, and models
- **API Key Support**: Optional API key for commercial usage
- **Multiple Weather Models**: Select specific weather models or use best-match automatic selection

## Available Streams

| Stream | Description | Incremental |
|--------|-------------|-------------|
| `weather_forecast` | Forecast metadata and configuration | ✓ |
| `weather_hourly` | Hourly weather variables (up to 16 days forecast) | ✓ |
| `weather_daily` | Daily weather aggregations | ✓ |
| `weather_current` | Current weather conditions | ✓ |
| `weather_minutely_15` | 15-minute resolution data (Central Europe & North America) | ✓ |
| `weather_historical` | Historical weather data (back to 1940) | ✓ |

## Installation

Install from source:

```bash
pip install git+https://github.com/YOUR_ORG/tap-openmeteo.git
```

Or with [uv](https://docs.astral.sh/uv/):

```bash
uv tool install git+https://github.com/YOUR_ORG/tap-openmeteo.git
```

## Configuration

### Required Settings

| Setting | Type | Description |
|---------|------|-------------|
| `locations` | array | List of locations with `name`, `latitude`, `longitude` |

### Optional Settings

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `api_key` | string | - | API key for commercial use |
| `api_url` | string | `https://api.open-meteo.com` | API base URL |
| `forecast_days` | integer | 7 | Number of forecast days (0-16) |
| `past_days` | integer | 0 | Include past days in forecast (0-92) |
| `start_date` | date | - | Start date for historical data |
| `end_date` | date | - | End date for historical data |
| `hourly_variables` | array | See below | Hourly weather variables |
| `daily_variables` | array | See below | Daily weather variables |
| `current_variables` | array | See below | Current weather variables |
| `minutely_15_variables` | array | [] | 15-minutely variables |
| `temperature_unit` | string | `celsius` | `celsius` or `fahrenheit` |
| `wind_speed_unit` | string | `kmh` | `kmh`, `ms`, `mph`, or `kn` |
| `precipitation_unit` | string | `mm` | `mm` or `inch` |
| `timezone` | string | `auto` | Default timezone for all locations |
| `models` | array | [] | Specific weather models to use |
| `cell_selection` | string | `land` | Grid cell selection: `land`, `sea`, `nearest` |
| `streams_to_sync` | array | See below | Which streams to sync |

### Default Hourly Variables

```json
[
  "temperature_2m",
  "relative_humidity_2m",
  "precipitation",
  "weather_code",
  "wind_speed_10m",
  "wind_direction_10m"
]
```

### Default Daily Variables

```json
[
  "weather_code",
  "temperature_2m_max",
  "temperature_2m_min",
  "precipitation_sum",
  "sunrise",
  "sunset"
]
```

### Default Streams

```json
[
  "weather_forecast",
  "weather_hourly",
  "weather_daily"
]
```

### Available Hourly Variables

- `temperature_2m`, `temperature_80m`, `temperature_120m`, `temperature_180m`
- `relative_humidity_2m`, `dew_point_2m`, `apparent_temperature`
- `precipitation`, `precipitation_probability`, `rain`, `showers`, `snowfall`, `snow_depth`
- `weather_code`, `cloud_cover`, `cloud_cover_low`, `cloud_cover_mid`, `cloud_cover_high`
- `pressure_msl`, `surface_pressure`, `visibility`
- `wind_speed_10m`, `wind_speed_80m`, `wind_speed_120m`, `wind_speed_180m`
- `wind_direction_10m`, `wind_direction_80m`, `wind_direction_120m`, `wind_direction_180m`
- `wind_gusts_10m`
- `soil_temperature_0cm`, `soil_temperature_6cm`, `soil_temperature_18cm`, `soil_temperature_54cm`
- `soil_moisture_0_to_1cm`, `soil_moisture_1_to_3cm`, `soil_moisture_3_to_9cm`, `soil_moisture_9_to_27cm`, `soil_moisture_27_to_81cm`
- `uv_index`, `uv_index_clear_sky`, `is_day`, `sunshine_duration`
- `shortwave_radiation`, `direct_radiation`, `diffuse_radiation`, `direct_normal_irradiance`
- `global_tilted_irradiance`, `terrestrial_radiation`
- `evapotranspiration`, `et0_fao_evapotranspiration`, `vapour_pressure_deficit`
- `cape`, `freezing_level_height`

### Available Daily Variables

- `weather_code`
- `temperature_2m_max`, `temperature_2m_min`, `temperature_2m_mean`
- `apparent_temperature_max`, `apparent_temperature_min`, `apparent_temperature_mean`
- `sunrise`, `sunset`, `daylight_duration`, `sunshine_duration`
- `uv_index_max`, `uv_index_clear_sky_max`
- `precipitation_sum`, `rain_sum`, `showers_sum`, `snowfall_sum`
- `precipitation_hours`, `precipitation_probability_max`
- `wind_speed_10m_max`, `wind_gusts_10m_max`, `wind_direction_10m_dominant`
- `shortwave_radiation_sum`, `et0_fao_evapotranspiration`

## Example Configuration

### Basic Configuration

```json
{
  "locations": [
    {
      "name": "Berlin",
      "latitude": 52.52,
      "longitude": 13.41
    }
  ]
}
```

### Full Configuration

```json
{
  "api_key": "your-commercial-api-key",
  "api_url": "https://customer-api.open-meteo.com",
  "locations": [
    {
      "name": "Berlin",
      "latitude": 52.52,
      "longitude": 13.41,
      "elevation": 34,
      "timezone": "Europe/Berlin"
    },
    {
      "name": "New York",
      "latitude": 40.71,
      "longitude": -74.01,
      "timezone": "America/New_York"
    }
  ],
  "forecast_days": 14,
  "past_days": 2,
  "hourly_variables": [
    "temperature_2m",
    "relative_humidity_2m",
    "apparent_temperature",
    "precipitation",
    "rain",
    "snowfall",
    "weather_code",
    "cloud_cover",
    "wind_speed_10m",
    "wind_direction_10m",
    "wind_gusts_10m",
    "uv_index"
  ],
  "daily_variables": [
    "weather_code",
    "temperature_2m_max",
    "temperature_2m_min",
    "apparent_temperature_max",
    "apparent_temperature_min",
    "sunrise",
    "sunset",
    "precipitation_sum",
    "rain_sum",
    "snowfall_sum",
    "wind_speed_10m_max",
    "wind_gusts_10m_max"
  ],
  "current_variables": [
    "temperature_2m",
    "relative_humidity_2m",
    "apparent_temperature",
    "is_day",
    "precipitation",
    "weather_code",
    "cloud_cover",
    "wind_speed_10m",
    "wind_direction_10m"
  ],
  "temperature_unit": "celsius",
  "wind_speed_unit": "kmh",
  "precipitation_unit": "mm",
  "timezone": "auto",
  "streams_to_sync": [
    "weather_forecast",
    "weather_hourly",
    "weather_daily",
    "weather_current"
  ]
}
```

### Historical Data Configuration

```json
{
  "locations": [
    {
      "name": "Berlin",
      "latitude": 52.52,
      "longitude": 13.41
    }
  ],
  "start_date": "2024-01-01",
  "end_date": "2024-01-31",
  "hourly_variables": [
    "temperature_2m",
    "precipitation",
    "weather_code"
  ],
  "streams_to_sync": ["weather_historical"]
}
```

## Usage

### Direct Execution

```bash
# Show version
tap-openmeteo --version

# Show help
tap-openmeteo --help

# Discover streams
tap-openmeteo --config config.json --discover > catalog.json

# Run sync
tap-openmeteo --config config.json --catalog catalog.json
```

### With Meltano

Add to your `meltano.yml`:

```yaml
plugins:
  extractors:
    - name: tap-openmeteo
      namespace: tap_openmeteo
      pip_url: git+https://github.com/YOUR_ORG/tap-openmeteo.git
      config:
        locations:
          - name: Berlin
            latitude: 52.52
            longitude: 13.41
        forecast_days: 7
        hourly_variables:
          - temperature_2m
          - precipitation
          - weather_code
```

Run the pipeline:

```bash
meltano run tap-openmeteo target-jsonl
```

## Incremental Sync

The tap supports incremental syncing for efficient data updates:

- **Hourly/Daily/Historical streams**: Use timestamp-based replication keys
- **State tracking**: Maintains sync state between runs
- **Configurable lookback**: Use `past_days` to include recent historical data

## Weather Models

Open-Meteo combines data from multiple national weather services. You can either:

1. **Use automatic selection** (default): Best models are selected for each location
2. **Specify models**: Set `models` config to use specific weather models

Available models include:
- `ecmwf_ifs025` - ECMWF IFS (global)
- `gfs_seamless` - NOAA GFS (global)
- `icon_seamless` - DWD ICON (Europe)
- `meteofrance_seamless` - Météo-France
- `jma_seamless` - JMA (Japan)
- `gem_seamless` - GEM (Canada)
- And many more regional models

## API Key (Commercial Use)

For commercial use:

1. Get an API key from [Open-Meteo pricing](https://open-meteo.com/en/pricing)
2. Set `api_key` in your configuration
3. Use `api_url: https://customer-api.open-meteo.com`

Free tier is available for non-commercial use without an API key.

## Development

### Setup

```bash
# Clone the repository
git clone https://github.com/YOUR_ORG/tap-openmeteo.git
cd tap-openmeteo

# Install with dev dependencies
pip install -e ".[dev]"
```

### Running Tests

```bash
pytest
```

### Code Quality

```bash
# Linting
ruff check .

# Type checking
mypy tap_openmeteo
```

## License

Apache 2.0

## Contributing

Contributions are welcome! Please open an issue or submit a pull request.
