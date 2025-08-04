#!/usr/bin/env python3
"""
Calculate the percentage of DANDI assets that are in zarr format.
"""

import duckdb
import sys
from pathlib import Path

def calculate_zarr_percentage():
    """Calculate and display the percentage of zarr assets."""
    
    # Check if the database exists
    db_path = "data/analytics.duckdb"
    if not Path(db_path).exists():
        print(f"Error: Database file {db_path} not found.")
        print("Make sure to run 'python duckdb_analytics.py' first to build the database.")
        return
    
    # Connect to the database
    conn = duckdb.connect(db_path)
    
    try:
        # Get total count of assets
        total_result = conn.execute("SELECT COUNT(*) FROM assets").fetchone()
        total_assets = total_result[0] if total_result else 0
        
        if total_assets == 0:
            print("No assets found in the database.")
            return
        
        # Get count of zarr assets
        zarr_result = conn.execute("SELECT COUNT(*) FROM assets WHERE asset_type = 'zarr'").fetchone()
        zarr_assets = zarr_result[0] if zarr_result else 0
        
        # Get count of blob assets
        blob_result = conn.execute("SELECT COUNT(*) FROM assets WHERE asset_type = 'blob'").fetchone()
        blob_assets = blob_result[0] if blob_result else 0
        
        # Calculate percentage
        zarr_percentage = (zarr_assets / total_assets) * 100 if total_assets > 0 else 0
        blob_percentage = (blob_assets / total_assets) * 100 if total_assets > 0 else 0
        
        # Display results
        print("DANDI Asset Type Analysis")
        print("=" * 40)
        print(f"Total assets: {total_assets:,}")
        print(f"Zarr assets: {zarr_assets:,}")
        print(f"Blob assets: {blob_assets:,}")
        print()
        print(f"Percentage of assets that are zarr: {zarr_percentage:.2f}%")
        print(f"Percentage of assets that are blob: {blob_percentage:.2f}%")
        
        # Check for any other asset types
        other_result = conn.execute("""
            SELECT asset_type, COUNT(*) as count 
            FROM assets 
            WHERE asset_type NOT IN ('zarr', 'blob')
            GROUP BY asset_type
        """).fetchall()
        
        if other_result:
            print("\nOther asset types found:")
            for asset_type, count in other_result:
                percentage = (count / total_assets) * 100
                print(f"  {asset_type}: {count:,} ({percentage:.2f}%)")
        
    except Exception as e:
        print(f"Error querying the database: {e}")
        print("Make sure the database has been properly initialized with asset data.")
    
    finally:
        conn.close()

if __name__ == "__main__":
    calculate_zarr_percentage()
