import marimo

__generated_with = "0.16.1"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    from pathlib import Path
    import polars as pl
    import polars.selectors as cs
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    from datetime import date
    import base64
    return Path, base64, mo, pl


@app.cell
def _(Path, pl):
    # Get Göteborgs Stad air quality data
    notebook_dir = Path(__file__).parent
    aq_path = notebook_dir.parent / "data" / "parquet_files" / "aq_data_2025.parquet"
    df_aq = pl.read_parquet(aq_path)
    df_aq = df_aq.sort(['date', 'time'])
    return df_aq, notebook_dir


@app.cell
def _(df_aq, pl):
    # Add datetime column to df 
    df_air_quality = df_aq.with_columns(
        pl.when(pl.col("time").str.starts_with("24:"))
        .then(
            (pl.col("date").cast(pl.Date) + pl.duration(days=1)).cast(pl.String) + " 00:" +
            pl.col("time").str.slice(3)
        )
        .otherwise(
            pl.col("date").cast(pl.String) + " " + pl.col("time")
        )
        .str.replace(r"\+\d{2}:\d{2}$", "")
        .str.strptime(pl.Datetime, "%Y-%m-%d %H:%M")
        .alias("datetime")
    )
    return (df_air_quality,)


@app.cell
def _(pl):

    def get_sensor_columns(pollutant: str, stations: list[str] = None, df: pl.DataFrame = None):
        station_map = {
            "femman": f"femman_{pollutant}",
            "haga": f"hagasodra_{pollutant}" if "pm" in pollutant else f"haganorra_{pollutant}",
            "mobil2": f"mobil2_{pollutant}",
            "mobil3": f"mobil3_{pollutant}"
        }

        if stations is None:
            stations = list(station_map.keys())

        selected_columns = [station_map[station] for station in stations if station in station_map]

        if df is not None:
            existing_columns = set(df.columns)
            selected_columns = [col for col in selected_columns if col in existing_columns]

        return selected_columns


    # WHO-limits: Recommended short-term (24-hour) AQG level and interim targets
    WHO_LIMITS = {
        "pm10": 45,
        "pm25": 15,
        "no2": 25
        # nox: no limit value
    }

    def clean_column(col_name):
        return (
            pl.col(col_name)
            .str.strip_chars()
            .replace("", None)
            .cast(pl.Float64)
        )

    def compute_daily_averages(df: pl.DataFrame, pollutant: str, stations: list[str] = None):
        columns = get_sensor_columns(pollutant, stations, df)

        if not columns:
            raise ValueError("Inga giltiga kolumner hittades för angivna stationer och förorening.")

        df_ext = df.with_columns([
            pl.col("datetime").dt.truncate("1d").alias("day"),
            *[clean_column(col) for col in columns]
        ])

        daily_avgs = {
            col: df_ext.group_by("day")
                .agg(pl.col(col).mean().alias(f"avg_{col}"))
                .sort("day")
            for col in columns
        }

        timestamps = daily_avgs[columns[0]]["day"].cast(pl.Datetime).to_list()
        daily_values = {
            col: daily_avgs[col][f"avg_{col}"].to_list()
            for col in columns
        }

        return timestamps, daily_values, columns


    def plot_pollutant(timestamps, daily_values, columns, pollutant: str, temperature_data=None):
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates

        color_map = ["red", "blue", "yellow", "green"]
        label_map = {
            "femman": "Femman",
            "hagasodra": "Haga Södra",
            "haganorra": "Haga Norra",
            "mobil2": "Mobil 2",
            "mobil3": "Mobil 3"
        }

        fig, ax1 = plt.subplots(figsize=(12, 6))

        for i, col in enumerate(columns):
            label_key = next((k for k in label_map if k in col), col)
            ax1.plot(timestamps, daily_values[col], label=label_map[label_key], color=color_map[i], linewidth=1, alpha=0.8)

        ax1.set_xlabel('Date', fontsize=12, fontweight='bold')
        ax1.set_ylabel(f"{pollutant.upper()} (µg/m³)", fontsize=12, fontweight='bold')
        ax1.set_title(f'Daily Average {pollutant.upper()} Levels (Göteborg)', fontsize=14, fontweight='bold', pad=20)

        ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        ax1.xaxis.set_major_locator(mdates.AutoDateLocator())
        ax1.tick_params(axis='x', rotation=45)

        ax1.grid(True, alpha=0.3, linestyle='--', linewidth=0.5)
        ax1.set_axisbelow(True)

        # WHO guideline line
        who_limit = WHO_LIMITS.get(pollutant.lower())
        if who_limit is not None:
            ax1.axhline(
                y=who_limit,
                color='#A23B72',
                linestyle='--',
                linewidth=2,
                label=f'WHO 24h guideline ({who_limit} µg/m³)',
                alpha=0.7
            )

        # Temperatur – sekundär y-axel
        if temperature_data:
            ax2 = ax1.twinx()
            ax2.plot(timestamps, temperature_data, color='black', linestyle='dotted', linewidth=1.5, alpha=0.6, label='Temp (°C)')
            ax2.set_ylabel('Temperature (°C)', fontsize=12, fontweight='bold', color='black')
            ax2.tick_params(axis='y', labelcolor='black')

            # Kombinera legender
            lines1, labels1 = ax1.get_legend_handles_labels()
            lines2, labels2 = ax2.get_legend_handles_labels()
            ax1.legend(lines1 + lines2, labels1 + labels2, loc='best', framealpha=0.9)
        else:
            ax1.legend(loc='best', framealpha=0.9)

        plt.tight_layout()
        return fig
    return compute_daily_averages, plot_pollutant


@app.cell
def _(mo):
    mo.md(r"""# Air Quality Data (Göteborgs Stad) & Traffic Flow Data (Trafikverket)""")
    return


@app.cell
def _(mo):
    mo.md(r"""#### This plot visualizes daily average levels of air pollutants (PM2.5, PM10, NO₂, and NOₓ) across different monitoring stations in Gothenburg. The data represents daily averages calculated from hourly measurements. The dotted black line shows temperature data from the "Lejonet" station, while the dashed purple line indicates WHO 24-hour air quality guidelines for each pollutant.""")
    return


@app.cell
def _(mo):
    pollutant = mo.ui.dropdown(
        options=["pm10", "pm25", "no2", "nox"],
        value="pm25",
        label="Select pollutant",
        full_width=True
    )

    stations = mo.ui.dropdown(
        options={
            "All stations": None,
            "Femman": "femman",
            "Haga": "haga",
            "Mobil2": "mobil2",
            "Mobil3": "mobil3"
        },
        value=None,
        label="Select station",
        full_width=True
    )

    mo.vstack([
        mo.md("#### Select pollutant and station/stations"),
        pollutant,
        stations,
    ])
    return pollutant, stations


@app.cell
def _(compute_daily_averages, df_air_quality, pollutant, stations):
    #pollutant = "pm25"
    #stations = ["femman"]

    timestamps, values, sensor_cols = compute_daily_averages(df_air_quality, pollutant.value, stations.value)
    return sensor_cols, timestamps, values


@app.cell
def _(df_air_quality, pl):
    # Temperature data
    df_temp = df_air_quality.with_columns([
        pl.col("datetime").dt.truncate("1d").alias("day"),
        pl.col("lejonet_temp").str.strip_chars().replace("", None).cast(pl.Float64)
    ])

    temp_daily = df_temp.group_by("day").agg(
        pl.col("lejonet_temp").mean().alias("avg_temp")
    ).sort("day")

    temperature_data = temp_daily["avg_temp"].to_list()
    return (temperature_data,)


@app.cell
def _(
    plot_pollutant,
    pollutant,
    sensor_cols,
    temperature_data,
    timestamps,
    values,
):
    plot_pollutant(timestamps, values, sensor_cols, pollutant.value, temperature_data=temperature_data)
    return


@app.cell
def _(mo):
    mo.md(r"""## Maps for air quality monitoring stations, traffic flow detectors and the mapping between the two""")
    return


@app.cell
def _(mo, notebook_dir):
    map_monitoring_stations = notebook_dir.parent / "data" / "maps" / "monitoring_stations.html"
    map_detectors = notebook_dir.parent / "data" / "maps" / "detectors.html"
    map_merged = notebook_dir.parent / "data" / "maps" / "merged_map.html"

    map_selector = mo.ui.dropdown(
        options={
            "Monitoring Stations": str(map_monitoring_stations),
            "Detectors": str(map_detectors),
            "Merged": str(map_merged)
        },
        value="Monitoring Stations",  # Set default to Monitoring Stations
        full_width=True
    )
    mo.vstack([
        mo.md("#### Select map view"),
        map_selector,
    ])
    return (map_selector,)


@app.cell
def _(base64, map_selector, mo):
    with open(map_selector.value, 'r', encoding='utf-8') as f:
        map_html = f.read()

    encoded = base64.b64encode(map_html.encode()).decode()

    mo.Html(f'<iframe src="data:text/html;base64,{encoded}" width="100%" height="600px" style="border:none;"></iframe>')
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
