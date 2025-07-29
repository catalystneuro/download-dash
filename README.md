To run the dashboard

1. Put `daily_ip_dandiset_stats.parquet` in data/ (found [here](https://drive.google.com/drive/u/2/folders/1jptzbO2BvnbizKuPjEiQe_e_6vAp6Rt5))
2. run pip install -r requirements.txt
3. Run `python app.py`

To update the data:
1. Put new database.parquet and analytics.duckdb in /data (found [here](https://drive.google.com/drive/u/2/folders/1jptzbO2BvnbizKuPjEiQe_e_6vAp6Rt5))
2. Run `python analytics.duckdb`
