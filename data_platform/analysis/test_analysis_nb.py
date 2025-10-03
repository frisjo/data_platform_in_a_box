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
    return Path, cs, pl, plt


@app.cell
def _(Path, pl):
    # Paths to parquet and json files
    notebook_dir = Path(__file__).parent
    aq_path = notebook_dir.parent / "data" / "parquet_files" / "aq_data_sep25.parquet"
    tf_path = notebook_dir.parent / "data" / "parquet_files" / "tf_data_sep25.parquet"
    mapping_path = notebook_dir.parent / "data" / "station_detector_matches.json"

    # Create dfs
    df_aq = pl.read_parquet(aq_path)
    df_aq = df_aq.sort('time')
    df_tf = pl.read_parquet(tf_path)
    mapping = pl.read_json(mapping_path)

    # Add column to mapping df with station names in aq data
    station_names = ['femman', 'haganorra', 'hagasodra', 'mobil2', 'mobil3']
    mapping = mapping.with_columns(pl.Series('aq_data_station_names', station_names))

    # Filter tf data on mapped detectors
    selected_site_ids = mapping['closest_detector_id'].to_list()
    df_tf_filtered = df_tf.filter(pl.col('site_id').is_in(selected_site_ids))
    return df_aq, df_tf_filtered, mapping, selected_site_ids


@app.cell
def _(cs, df_aq, mapping):
    # Separate dfs per station
    aq_dfs = []
    for station in mapping['aq_data_station_names']:
        cols = df_aq.select(cs.starts_with(station), 'date', 'time')
        aq_dfs.append(cols)
    return (aq_dfs,)


@app.cell
def _(df_tf_filtered, pl, selected_site_ids):
    # Separate dfs for selected siteids
    tv_dfs = []
    for siteid in selected_site_ids:
        tv_dfs.append(df_tf_filtered.filter(pl.col("site_id") == siteid))
    return (tv_dfs,)


@app.cell
def _(aq_dfs, cs, mapping, pl, plt, tv_dfs):
    timestamps_hourly_all = []
    vfr_hourly_avg_all = []

    for tv_df, aq_df, name in zip(tv_dfs, aq_dfs, mapping['aq_data_station_names']):

        # Airquality data - get as datetime series
        timestamps_aq = (
            aq_df.with_columns(
                # Replace " 24:" with next day " 00:"
                pl.when(pl.col("time").str.starts_with("24:"))
                .then(
                    (pl.col("date").cast(pl.Date) + pl.duration(days=1)).cast(pl.String) + " 00:" +
                    pl.col("time").str.slice(3)  # Keep the rest (e.g., "00+01:00")
                )
                .otherwise(
                    pl.col("date").cast(pl.String) + " " + pl.col("time")
                )
                .str.replace(r"\+\d{2}:\d{2}$", "")  # Remove timezone
                .str.strptime(pl.Datetime, "%Y-%m-%d %H:%M")
                .alias("datetime")
            )
            .select("datetime")
            .to_series()
            .to_list()
        )

        if name == "haganorra" or name == "hagasodra":
            aq_df = aq_dfs[1].join(aq_dfs[2], on=["date", "time"], how="inner")  # or "outer", "left", "right"
        
        pm_10 = aq_df.select(cs.ends_with("pm10")).to_series().str.strip_chars().cast(pl.Float64).to_list()
        pm_10_subset = pm_10[7:15]  
        timestamps_aq_subset = timestamps_aq[7:15]  
    
    
        # Raw trafficflow data
        timestamps_raw = tv_df["measurement_time_local"].to_list()
        vfr_raw = tv_df["vehicle_flow_rate"].to_list()

        # Hourly average trafficflow data 
        tv_df = tv_df.with_columns(pl.col("measurement_time_local").dt.truncate("1h").alias("hour"))
        hourly_avg = tv_df.group_by("hour").agg(
            pl.col("vehicle_flow_rate").mean().alias("avg_vehicle_flow_rate")
        ).sort("hour")
        timestamps_hourly = hourly_avg["hour"].cast(pl.Datetime).to_list()
        vfr_hourly_avg = hourly_avg["avg_vehicle_flow_rate"].to_list()

        # To lists
        timestamps_hourly_all.append(timestamps_hourly)
        vfr_hourly_avg_all.append(vfr_hourly_avg)

        # Create figure with two subplots side by side
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
    
        # Plot vehicle flow rate
        ax1.plot(timestamps_hourly, vfr_hourly_avg, label='Hourly Avg Vehicle Flow', color='red', linewidth=2)
        ax1.set_xlabel("Time")
        ax1.set_ylabel("Vehicle Flow Rate")
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        ax1.tick_params(axis='x', rotation=45)
    
        # Plot PM10
        ax2.plot(timestamps_aq, pm_10, label='PM10 Levels', color='blue', linewidth=2)
        ax2.set_xlabel("Time")
        ax2.set_ylabel("PM10 (µg/m³)")
        ax2.legend()
        ax2.grid(True, alpha=0.3)
        ax2.tick_params(axis='x', rotation=45)
    
        plt.tight_layout()
        plt.show()
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
