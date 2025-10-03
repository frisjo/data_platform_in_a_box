import dagster as dg

# Select asset for jobs
GBGS_raw_data = dg.AssetSelection.assets("GBGS_raw_data")
TV_raw_data = dg.AssetSelection.assets("TV_raw_data")

# ADD RETRY POLICY LATER!

# Job for running GBGS_raw_data (fetching and loading air quality data from Göteborgs Stad into duckdb)
GBGS_update_job = dg.define_asset_job(
    name="GBGS_update_job",
    selection=GBGS_raw_data
)

# Job for running TV_raw_data (fetching and loading traffic flow data from Trafikverket into duckdb)
TV_update_job = dg.define_asset_job(
    name="TV_update_job",
    selection=TV_raw_data
)