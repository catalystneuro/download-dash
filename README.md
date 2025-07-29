To run the dashboard

1. Put daily_ip_dandiset_stats.parquet in data/
2. run pip install -r requirements.txt
3. Run `python app.py`

To update the data:
1. Put new database.parquet and analytics.duckdb in /data
2. Run `python analytics.duckdb`
