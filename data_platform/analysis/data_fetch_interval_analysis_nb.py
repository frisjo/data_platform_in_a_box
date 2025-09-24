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
    return Path, np, pl, plt


@app.cell
def _(Path, pl):
    # Get traffic flow data from parquet file
    notebook_dir = Path(__file__).parent
    parquet_path = notebook_dir.parent / "data" / "parquet_files" / "test_table_data_fetching_analysis.parquet"

    df = pl.read_parquet(parquet_path)
    return (df,)


@app.cell
def _(df, pl):
    from collections import Counter

    # Get row counts per site_id
    row_counts_per_site = df.group_by('site_id').agg(pl.len().alias('row_count'))['row_count'].to_list()

    # Number of site ids per row count
    row_count_distribution = Counter(row_counts_per_site)

    print("Row count distribution:")
    print(dict(row_count_distribution))
    return


@app.cell
def _(df, pl):
    # Get site_id and their row counts
    site_row_counts = df.group_by("site_id").agg(pl.len().alias("row_count"))

    # Filter to only those site_ids with exactly 30 rows
    site_ids_with_30_rows = site_row_counts.filter(pl.col("row_count") == 30)["site_id"]

    # Now filter the main df using those site_ids
    df_with_30_rows = df.filter(pl.col("site_id").is_in(site_ids_with_30_rows))
    df_with_30_rows
    return


@app.cell
def _(df, pl, plt):
    # --- Filter and prepare original data ---
    df_1604 = df.filter(pl.col("site_id") == 2136)

    # --- Raw data ---
    timestamps_raw = df_1604["measurement_time_local"].to_list()
    vfr_raw = df_1604["vehicle_flow_rate"].to_list()

    # --- Hourly average (from all data) ---
    df_1604 = df_1604.with_columns(
        pl.col("measurement_time_local").dt.truncate("1h").alias("hour")
    )

    hourly_avg = df_1604.group_by("hour").agg(
        pl.col("vehicle_flow_rate").mean().alias("avg_vehicle_flow_rate")
    ).sort("hour")

    timestamps_hourly = hourly_avg["hour"].to_list()
    vfr_hourly_avg = hourly_avg["avg_vehicle_flow_rate"].to_list()

    # --- Hourly average (every second row) ---
    df_1604_every_second = df_1604.gather_every(2)

    hourly_avg_2nd = df_1604_every_second.group_by("hour").agg(
        pl.col("vehicle_flow_rate").mean().alias("avg_vehicle_flow_rate_2nd")
    ).sort("hour")


    timestamps_hourly_2nd = hourly_avg_2nd["hour"].to_list()
    vfr_hourly_avg_2nd = hourly_avg_2nd["avg_vehicle_flow_rate_2nd"].to_list()

    # --- Hourly average (every third row) ---
    df_1604_every_third = df_1604.gather_every(3)

    hourly_avg_3rd = df_1604_every_third.group_by("hour").agg(
        pl.col("vehicle_flow_rate").mean().alias("avg_vehicle_flow_rate_3rd")
    ).sort("hour")

    timestamps_hourly_3rd = hourly_avg_3rd["hour"].to_list()
    vfr_hourly_avg_3rd = hourly_avg_3rd["avg_vehicle_flow_rate_3rd"].to_list()

    # print(hourly_avg, print(len(timestamps_hourly)))
    # print(hourly_avg_2nd, print(len(timestamps_hourly_2nd)))
    # print(hourly_avg_3rd, print(len(timestamps_hourly_3rd)))

    # --- Plot all three lines ---
    plt.figure(figsize=(12, 6))

    plt.plot(timestamps_raw, vfr_raw, label='Raw Vehicle Flow Rate', color='blue', alpha=0.4)
    plt.plot(timestamps_hourly, vfr_hourly_avg, label='Hourly Avg (All Rows)', color='red', linewidth=2)
    plt.plot(timestamps_hourly_2nd, vfr_hourly_avg_2nd, label='Hourly Avg (Every 2nd Row)', color='green', linewidth=2)

    # Labels and formatting
    plt.title("Vehicle Flow Rate for site_id = 1604 (Raw, Hourly, Downsampled)")
    plt.xlabel("Time")
    plt.ylabel("Vehicle Flow Rate")
    plt.xticks(rotation=45)
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()
    return df_1604, df_1604_every_second, df_1604_every_third


@app.cell
def _(df_1604_every_second):
    df_1604_every_second
    return


@app.cell
def _(df_1604_every_third):
    df_1604_every_third
    return


@app.cell
def _(df_1604, np):
    # Assuming df_1604 has 'vehicle_flow_rate' and 'measurement_time_local' columns
    vfr = df_1604.sort("measurement_time_local")['vehicle_flow_rate'].to_numpy()

    def autocorr(x, lag):
        return np.corrcoef(x[:-lag], x[lag:])[0, 1]

    lags = [1, 5, 10, 15, 30, 60]  # lags in minutes
    autocorrelations = {lag: autocorr(vfr, lag) for lag in lags}

    print("Autocorrelations at various lags (in minutes):")
    print(autocorrelations)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
