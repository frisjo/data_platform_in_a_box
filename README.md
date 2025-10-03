# data_platform_in_a_box
To run project:
* Install docker
* In data_platform run:
- activate virtual environment
- docker build -t data-platform
- docker run --name dagster-app -v $(pwd)/data:/opt/dagster/app/data  -p 3000:3000 data-platform
* Now the Dagster project can be viewed at http://localhost:3000/

View visualizations, in analysis run:
- marimo run air_quality_analysis_nb.py
    - To view visualisations of Göteborgs stad air quality data for 2025
    