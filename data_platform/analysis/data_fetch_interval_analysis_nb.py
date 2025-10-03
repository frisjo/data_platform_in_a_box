import marimo

__generated_with = "0.16.1"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    from pathlib import Path
    import matplotlib.pyplot as plt
    import numpy as np
    return Path, mo, pl, plt


@app.cell
def _(Path, pl):
    # Get traffic flow data from parquet file
    notebook_dir = Path(__file__).parent
    parquet_path = notebook_dir.parent / "data" / "parquet_files" / "test_table_data_fetching_analysis.parquet"
    df = pl.read_parquet(parquet_path)
    return (df,)


@app.cell
def _(pl, plt):

    def get_selected_detector_df(df, detector_id):
        return df.filter(pl.col("site_id") == detector_id), detector_id

    def downsampling(df_selected, values_per_hour):

        df_with_hour = df_selected.with_columns(
            pl.col("measurement_time_local").dt.truncate("1h").alias("hour")
        )
        points_per_hour = df_with_hour.group_by("hour").len().select("len").mean().item()
        interval = max(1, int(points_per_hour / values_per_hour))

        df_downsampled = df_selected.gather_every(interval)

        hourly_avg = df_downsampled.group_by("hour").agg(
            pl.col("vehicle_flow_rate").mean().alias("avg_vehicle_flow_rate_3rd")
        ).sort("hour")

        timestamps_hourly_downsampled = hourly_avg["hour"].to_list()
        vfr_hourly_avg_downsampled = hourly_avg["avg_vehicle_flow_rate_3rd"].to_list()

        return timestamps_hourly_downsampled, vfr_hourly_avg_downsampled, values_per_hour

    def plot_raw_data_with_averages(df, siteid, values_per_hour):

        df_selected, selected_site_id = get_selected_detector_df(df, siteid)

        # Raw data
        timestamps_raw = df_selected["measurement_time_local"].to_list()
        vfr_raw = df_selected["vehicle_flow_rate"].to_list()

        # Hourly average (from all data)
        df_selected = df_selected.with_columns(pl.col("measurement_time_local").dt.truncate("1h").alias("hour"))
        hourly_avg = df_selected.group_by("hour").agg(
            pl.col("vehicle_flow_rate").mean().alias("avg_vehicle_flow_rate")
        ).sort("hour")
        timestamps_hourly = hourly_avg["hour"].to_list()
        vfr_hourly_avg = hourly_avg["avg_vehicle_flow_rate"].to_list()

        # Downsampled data
        timestamps_hourly_downsampled, vfr_hourly_avg_downsampled, values_per_hour = downsampling(df_selected, values_per_hour)

        plt.figure(figsize=(12, 6))
        plt.plot(timestamps_raw, vfr_raw, label='Raw Vehicle Flow Rate', color='blue', alpha=0.4)
        plt.plot(timestamps_hourly, vfr_hourly_avg, label='Hourly Avg (All Rows)', color='red', linewidth=2)
        plt.plot(timestamps_hourly_downsampled, vfr_hourly_avg_downsampled, label=f'Hourly Avg using {values_per_hour} values per hour', color='green', linewidth=2)

        plt.title(f"Vehicle Flow Rate for site_id = {selected_site_id} (Raw, Hourly, Downsampled)")
        plt.xlabel("Time")
        plt.ylabel("Vehicle Flow Rate")
        plt.xticks(rotation=45)
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        plt.show()
    return (plot_raw_data_with_averages,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""Plot traffic flow""")
    return


@app.cell
def _(df, plot_raw_data_with_averages):
    plot_raw_data_with_averages(df, 2205, 5)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
