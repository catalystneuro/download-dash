"""
DuckDB-based analytics using direct parquet querying.
No need to ingest the large parquet file - query it directly!
"""

import duckdb
import yaml
import pandas as pd
import pathlib
from pathlib import Path
from typing import Optional, Dict, List
import logging
from datetime import datetime
from dandi.dandiapi import DandiAPIClient
from tqdm import tqdm

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DuckDBAnalytics:
    """DuckDB-based analytics that queries parquet files directly."""
    
    def __init__(self, 
                 db_path: str = "analytics.duckdb",
                 parquet_path: str = "database.parquet"):
        """Initialize DuckDB connection and setup."""
        self.db_path = db_path
        self.parquet_path = parquet_path
        self.conn = duckdb.connect(db_path)
        self._setup_schema()
    
    def _setup_schema(self):
        """Create the database schema for metadata only."""
        logger.info("Setting up DuckDB schema...")
        
        # Create dandisets table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS dandisets (
                identifier VARCHAR PRIMARY KEY,
                name VARCHAR,
                description TEXT,
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            )
        """)
        
        # Create assets table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS assets (
                blob_id VARCHAR PRIMARY KEY,
                asset_path VARCHAR,
                asset_size UBIGINT,
                asset_type VARCHAR,
                created_at TIMESTAMP
            )
        """)
        
        # Create blob mapping table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS blob_mapping (
                blob_index INTEGER PRIMARY KEY,
                blob_id VARCHAR
            )
        """)
        
        # Create regions table (normalized - one row per unique region)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS regions (
                region_code VARCHAR PRIMARY KEY,
                country VARCHAR,
                region VARCHAR,
                provider VARCHAR,
                latitude DOUBLE,
                longitude DOUBLE
            )
        """)
        
        # Create IP to region mapping table (many IPs can map to same region)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS ip_regions (
                indexed_ip UBIGINT PRIMARY KEY,
                region_code VARCHAR NOT NULL,
                FOREIGN KEY (region_code) REFERENCES regions(region_code)
            )
        """)
        
        # Create dandiset versions tracking table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS dandiset_versions (
                dandiset_id VARCHAR,
                version_id VARCHAR,
                processed_at TIMESTAMP,
                PRIMARY KEY (dandiset_id, version_id),
                FOREIGN KEY (dandiset_id) REFERENCES dandisets(identifier)
            )
        """)
        
        # Create asset-dandiset mappings table (one-to-one since blob_id is unique)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS asset_dandiset_mappings (
                blob_id VARCHAR PRIMARY KEY,
                dandiset_id VARCHAR,
                version_id VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (blob_id) REFERENCES assets(blob_id),
                FOREIGN KEY (dandiset_id, version_id) REFERENCES dandiset_versions(dandiset_id, version_id)
            )
        """)
        
        logger.info("Schema setup complete")
    
    def load_blob_mapping(self, yaml_path: str = "blob_index_to_id.yaml"):
        """Load blob index to ID mapping from YAML file."""
        logger.info(f"Loading blob mapping from {yaml_path}")
        
        with open(yaml_path, 'r') as f:
            blob_mapping = yaml.safe_load(f)
        
        # Convert to DataFrame and insert
        blob_data = [{"blob_index": int(k), "blob_id": v} for k, v in blob_mapping.items()]
        blob_df = pd.DataFrame(blob_data)
        
        # Clear existing data and insert new
        self.conn.execute("DELETE FROM blob_mapping")
        self.conn.register('blob_df_temp', blob_df)
        self.conn.execute("""
            INSERT INTO blob_mapping SELECT * FROM blob_df_temp
        """)
        self.conn.execute("DROP VIEW blob_df_temp")
        
        logger.info(f"Loaded {len(blob_data):,} blob mappings")
    
    def load_ip_region_mapping(self, yaml_path: str = "index_to_region.yaml", 
                              coordinates_path: str = "region_codes_to_coordinates.yaml"):
        """Load IP index to region mapping from YAML file and update with coordinates."""
        logger.info(f"Loading IP region mapping from {yaml_path}")
        
        with open(yaml_path, 'r') as f:
            ip_region_mapping = yaml.safe_load(f)
        
        logger.info(f"Processing {len(ip_region_mapping):,} IP region mappings")
        
        regions_added = 0
        mappings_added = 0
        
        for indexed_ip_str, region_code in ip_region_mapping.items():
            try:
                indexed_ip = int(indexed_ip_str)
                
                # Parse region code to extract components
                country = None
                region = None
                provider = None
                
                if '/' in region_code:
                    # Format: "Country/Region" or "Provider/Region"
                    parts = region_code.split('/', 1)
                    first_part = parts[0].strip()
                    second_part = parts[1].strip()
                    
                    # Determine if first part is a cloud provider or country
                    if first_part in ['AWS', 'GCP', 'Azure']:
                        provider = first_part
                        region = second_part
                    else:
                        country = first_part
                        region = second_part
                else:
                    # Single value (e.g., "GitHub", "VPN", "unknown")
                    provider = region_code.strip()
                
                # Insert or get region
                try:
                    # First try to get existing region
                    result = self.conn.execute("""
                        SELECT region_code FROM regions 
                        WHERE region_code = ?
                    """, (region_code,)).fetchone()
                    
                    if not result:
                        # Insert new region
                        self.conn.execute("""
                            INSERT INTO regions (region_code, country, region, provider)
                            VALUES (?, ?, ?, ?)
                        """, (region_code, country, region, provider))
                        regions_added += 1
                    
                    # Map IP to region
                    self.conn.execute("""
                        INSERT OR REPLACE INTO ip_regions (indexed_ip, region_code)
                        VALUES (?, ?)
                    """, (indexed_ip, region_code))
                    mappings_added += 1
                
                except Exception as e:
                    logger.warning(f"Error processing region mapping {indexed_ip}: {region_code} - {e}")
                    continue
                    
            except ValueError:
                logger.warning(f"Invalid IP index: {indexed_ip_str}")
                continue
            except Exception as e:
                logger.warning(f"Error processing mapping {indexed_ip_str}: {region_code} - {e}")
                continue
        
        logger.info(f"IP region mapping completed:")
        logger.info(f"  Regions processed: {regions_added:,}")
        logger.info(f"  IP mappings added: {mappings_added:,}")
        
        # Now load and apply coordinates
        logger.info(f"Loading coordinates from {coordinates_path}")
        try:
            with open(coordinates_path, 'r') as f:
                coordinates_data = yaml.safe_load(f)
            
            coordinates_updated = 0
            
            for region_code, coords in coordinates_data.items():
                try:
                    # Handle None coordinates for special regions like GitHub, VPN, unknown
                    if coords.get('latitude') is None or coords.get('longitude') is None:
                        continue
                    
                    latitude = float(coords['latitude'])
                    longitude = float(coords['longitude'])
                    
                    # Update region with coordinates
                    result = self.conn.execute("""
                        UPDATE regions 
                        SET latitude = ?, longitude = ?
                        WHERE region_code = ?
                    """, (latitude, longitude, region_code))
                    
                    if self.conn.rowcount > 0:
                        coordinates_updated += 1
                
                except (ValueError, TypeError, KeyError) as e:
                    logger.warning(f"Error processing coordinates for {region_code}: {e}")
                    continue
                except Exception as e:
                    logger.warning(f"Unexpected error processing coordinates for {region_code}: {e}")
                    continue
            
            logger.info(f"Coordinates update completed:")
            logger.info(f"  Regions updated with coordinates: {coordinates_updated:,}")
            
        except FileNotFoundError:
            logger.warning(f"Coordinates file not found: {coordinates_path}")
        except Exception as e:
            logger.warning(f"Error loading coordinates file: {e}")
    
    def build_asset_mappings(self, incremental: bool = True, clear_existing: bool = False):
        """Build asset mappings from DANDI API with incremental updates."""
        logger.info("Starting to build asset mappings from DANDI API...")
        
        if clear_existing:
            logger.info("Clearing all existing data (fresh start mode)")
            # Clear all tables in the correct order to respect foreign key constraints
            self.conn.execute("DELETE FROM asset_dandiset_mappings")
            self.conn.execute("DELETE FROM dandiset_versions")
            self.conn.execute("DELETE FROM assets")
            self.conn.execute("DELETE FROM dandisets")
            logger.info("All existing data cleared")
        
        if incremental:
            logger.info("Using incremental mode - will skip already processed dandisets")
        else:
            logger.info("Using full rebuild mode - will process all dandisets")
        
        # Initialize API client
        client = DandiAPIClient()
        
        # Get all dandisets
        logger.info("Fetching all dandisets...")
        dandisets = list(client.get_dandisets())
        logger.info(f"Found {len(dandisets)} dandisets to process")
        
        total_skipped = 0
        total_processed = 0
        
        # Process each dandiset
        for dandiset in tqdm(dandisets, desc="Processing dandisets", position=0):
            dandiset_id = dandiset.identifier
            
            # Skip problematic dandisets
            if dandiset_id in ["000571", "000773"]:
                logger.info(f"Skipping dandiset {dandiset_id} (too many assets)")
                continue
            
            try:
                # Get all versions for this dandiset
                versions = list(dandiset.get_versions())
                
                for version in tqdm(versions, desc=f"Versions for {dandiset_id}", 
                                  position=1, leave=False):
                    version_id = version.identifier
                    
                    # Skip if already processed and not updated (in incremental mode)
                    if incremental:
                        # Check if version was already processed
                        result = self.conn.execute("""
                            SELECT processed_at FROM dandiset_versions 
                            WHERE dandiset_id = ? AND version_id = ? AND processed_at IS NOT NULL
                        """, (dandiset_id, version_id)).fetchone()
                        
                        if result:
                            processed_at = result[0]
                            
                            # Get version's modified date to check if it's been updated
                            version_modified = getattr(version, 'modified', None)
                            if version_modified and processed_at:
                                # Convert timestamps for comparison if needed
                                if isinstance(version_modified, str):
                                    version_modified = datetime.fromisoformat(version_modified.replace('Z', '+00:00'))
                                if isinstance(processed_at, str):
                                    processed_at = datetime.fromisoformat(processed_at)
                                
                                # Skip if version hasn't been modified since we processed it
                                if version_modified <= processed_at:
                                    total_skipped += 1
                                    continue
                                else:
                                    logger.info(f"Dandiset {dandiset_id}/{version_id} updated since last processed - reprocessing")
                    
                    total_processed += 1
                    
                    try:
                        # Get full dandiset object for this version
                        versioned_dandiset = client.get_dandiset(
                            dandiset_id=dandiset_id, 
                            version_id=version_id
                        )
                        
                        # Get dandiset metadata
                        title = getattr(versioned_dandiset, 'name', None)
                        
                        # Insert dandiset metadata
                        self.conn.execute("""
                            INSERT OR REPLACE INTO dandisets 
                            (identifier, name, created_at, updated_at)
                            VALUES (?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                        """, (dandiset_id, title))
                        
                        # Insert version record BEFORE processing assets (to satisfy foreign key constraint)
                        self.conn.execute("""
                            INSERT OR REPLACE INTO dandiset_versions 
                            (dandiset_id, version_id, processed_at)
                            VALUES (?, ?, NULL)
                        """, (dandiset_id, version_id))
                        
                        # Process all assets in this version
                        assets = list(versioned_dandiset.get_assets())
                        
                        for asset in tqdm(assets, desc=f"Assets in {dandiset_id}/{version_id}", 
                                        position=2, leave=False):
                            try:
                                # Determine blob ID and asset type
                                asset_path = asset.path
                                asset_size = getattr(asset, 'size', None)
                                is_zarr = ".zarr" in pathlib.Path(asset_path).suffixes
                                
                                if is_zarr:
                                    # For zarr assets, try to get zarr attribute, fallback to identifier
                                    blob_id = asset.zarr
                                    asset_type = "zarr"
                                else:
                                    # For blob assets, get blob attribute - let it error if missing
                                    blob_id = asset.blob
                                    asset_type = "blob"
                                
                                # Insert asset
                                self.conn.execute("""
                                    INSERT OR REPLACE INTO assets 
                                    (blob_id, asset_path, asset_size, asset_type, created_at)
                                    VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                                """, (blob_id, asset_path, asset_size, asset_type))
                                
                                # Insert mapping
                                self.conn.execute("""
                                    INSERT OR REPLACE INTO asset_dandiset_mappings 
                                    (blob_id, dandiset_id, version_id)
                                    VALUES (?, ?, ?)
                                """, (blob_id, dandiset_id, version_id))
                                
                            except Exception as e:
                                logger.warning(f"Error processing asset {asset.path} in {dandiset_id}/{version_id}: {e}")
                                continue
                        
                        # Insert version record with processed timestamp (more efficient than separate UPDATE)
                        self.conn.execute("""
                            INSERT OR REPLACE INTO dandiset_versions 
                            (dandiset_id, version_id, processed_at)
                            VALUES (?, ?, CURRENT_TIMESTAMP)
                        """, (dandiset_id, version_id))
                        
                    except Exception as e:
                        logger.warning(f"Error processing version {version_id} of dandiset {dandiset_id}: {e}")
                        continue
                
            except Exception as e:
                logger.warning(f"Error processing dandiset {dandiset_id}: {e}")
                continue
            
            # Log progress periodically
            if int(dandiset_id) % 10 == 0:
                stats = self.get_asset_stats()
                logger.info(f"Progress: {stats['total_assets']:,} assets, "
                           f"{stats['total_dandisets']:,} dandisets, "
                           f"Processed: {total_processed:,}, Skipped: {total_skipped:,}")
        
        # Final statistics
        logger.info("Asset mapping build completed!")
        stats = self.get_asset_stats()
        logger.info("Final Statistics:")
        logger.info(f"  Total assets: {stats['total_assets']:,}")
        logger.info(f"  Total dandisets: {stats['total_dandisets']:,}")
        logger.info(f"  Versions processed this run: {total_processed:,}")
        logger.info(f"  Versions skipped (already done): {total_skipped:,}")
        
        if total_skipped + total_processed > 0:
            efficiency = (total_skipped / (total_skipped + total_processed)) * 100
            logger.info(f"  Efficiency: {efficiency:.1f}% versions skipped")
    
    def get_asset_stats(self) -> Dict:
        """Get asset database statistics."""
        stats = {}
        
        # Count assets
        result = self.conn.execute("SELECT COUNT(*) FROM assets").fetchone()
        stats['total_assets'] = result[0] if result else 0
        
        # Count dandisets
        result = self.conn.execute("SELECT COUNT(*) FROM dandisets").fetchone()
        stats['total_dandisets'] = result[0] if result else 0
        
        # Count versions
        result = self.conn.execute("SELECT COUNT(*) FROM dandiset_versions WHERE processed_at IS NOT NULL").fetchone()
        stats['processed_versions'] = result[0] if result else 0
        
        # Count mappings
        result = self.conn.execute("SELECT COUNT(*) FROM asset_dandiset_mappings").fetchone()
        stats['total_mappings'] = result[0] if result else 0
        
        return stats
    
    def ingest_asset_data(self, assets_data: List[Dict] = None, incremental: bool = True):
        """
        Ingest asset data into the database.
        
        Args:
            assets_data: Optional pre-built asset data. If None, will fetch from DANDI API.
            incremental: If True, skip already processed dandisets.
        """
        if assets_data is not None:
            # Legacy mode: insert pre-built data
            logger.info(f"Ingesting {len(assets_data):,} pre-built assets")
            
            assets_df = pd.DataFrame(assets_data)
            self.conn.register('assets_df_temp', assets_df)
            
            self.conn.execute("""
                INSERT OR REPLACE INTO assets 
                SELECT * FROM assets_df_temp
            """)
            self.conn.execute("DROP VIEW assets_df_temp")
            
            logger.info("Asset data ingested successfully")
        else:
            # New mode: build from DANDI API
            self.build_asset_mappings(incremental=incremental)
    
    def ingest_dandiset_data(self, dandisets_data: List[Dict]):
        """Ingest dandiset metadata."""
        logger.info(f"Ingesting {len(dandisets_data):,} dandisets")
        
        dandisets_df = pd.DataFrame(dandisets_data)
        self.conn.register('dandisets_df_temp', dandisets_df)
        
        self.conn.execute("""
            INSERT OR REPLACE INTO dandisets 
            SELECT * FROM dandisets_df_temp
        """)
        self.conn.execute("DROP VIEW dandisets_df_temp")
        
        logger.info("Dandiset data ingested successfully")
    
    def create_analytics_views(self):
        """Create optimized views that query parquet directly."""
        logger.info("Creating analytics views...")
        
        # Base downloads view that queries parquet directly
        self.conn.execute(f"""
            CREATE OR REPLACE VIEW downloads_base AS
            SELECT 
                bm.blob_id,
                p.day,
                p.time,
                p.bytes_sent,
                p.indexed_ip,
                -- Convert day (YYMMDD) to proper date format (YYYY-MM-DD)
                CAST(
                    '20' || 
                    SUBSTR(LPAD(CAST(p.day AS VARCHAR), 6, '0'), 1, 2) || '-' ||
                    SUBSTR(LPAD(CAST(p.day AS VARCHAR), 6, '0'), 3, 2) || '-' ||
                    SUBSTR(LPAD(CAST(p.day AS VARCHAR), 6, '0'), 5, 2)
                    AS DATE
                ) as download_date,
                -- Convert time (HHMMSS) to proper time format (HH:MM:SS)
                CAST(
                    SUBSTR(LPAD(CAST(p.time AS VARCHAR), 6, '0'), 1, 2) || ':' ||
                    SUBSTR(LPAD(CAST(p.time AS VARCHAR), 6, '0'), 3, 2) || ':' ||
                    SUBSTR(LPAD(CAST(p.time AS VARCHAR), 6, '0'), 5, 2)
                    AS TIME
                ) as download_time
            FROM read_parquet('{self.parquet_path}') p
            JOIN blob_mapping bm ON p.blob_index = bm.blob_index
        """)
        
        # Enriched downloads view with asset and dandiset information
        self.conn.execute("""
            CREATE OR REPLACE VIEW downloads_enriched AS
            SELECT 
                d.*,
                adm.dandiset_id,
                adm.version_id,
                a.asset_path,
                a.asset_size,
                ds.name as dandiset_name,
                r.region_code,
                r.country,
                r.region,
                r.provider,
                r.latitude,
                r.longitude
            FROM downloads_base d
            JOIN assets a ON d.blob_id = a.blob_id
            LEFT JOIN asset_dandiset_mappings adm ON a.blob_id = adm.blob_id
            LEFT JOIN dandisets ds ON adm.dandiset_id = ds.identifier
            LEFT JOIN ip_regions ir ON d.indexed_ip = ir.indexed_ip
            LEFT JOIN regions r ON ir.region_code = r.region_code
        """)
    
    def add_region(self, region_code: str, country: str = None, region: str = None,
                   provider: str = None, latitude: float = None, longitude: float = None):
        """Add a region."""
        # First try to get existing region
        result = self.conn.execute("""
            SELECT region_code FROM regions 
            WHERE region_code = ?
        """, (region_code,)).fetchone()
        
        if result:
            # Update coordinates if provided
            if latitude is not None and longitude is not None:
                self.conn.execute("""
                    UPDATE regions 
                    SET latitude = ?, longitude = ?
                    WHERE region_code = ?
                """, (latitude, longitude, region_code))
        else:
            # Insert new region
            self.conn.execute("""
                INSERT INTO regions (region_code, country, region, provider, latitude, longitude)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (region_code, country, region, provider, latitude, longitude))
    
    def map_ip_to_region(self, indexed_ip: int, region_code: str):
        """Map an IP to a region."""
        # Map IP to region
        self.conn.execute("""
            INSERT OR REPLACE INTO ip_regions (indexed_ip, region_code)
            VALUES (?, ?)
        """, (indexed_ip, region_code))
    
    def analyze_asset_dandiset_relationships(self) -> Dict:
        """Analyze if assets belong to multiple dandisets and return statistics."""
        logger.info("Analyzing asset-dandiset relationships...")
        
        # Check for assets mapped to multiple dandisets
        multi_dandiset_assets = self.conn.execute("""
            SELECT 
                blob_id,
                COUNT(DISTINCT dandiset_id) as dandiset_count,
                STRING_AGG(DISTINCT dandiset_id, ', ') as dandiset_ids
            FROM asset_dandiset_mappings
            GROUP BY blob_id
            HAVING COUNT(DISTINCT dandiset_id) > 1
            ORDER BY dandiset_count DESC
        """).fetchdf()
        
        # Get overall statistics
        stats = self.conn.execute("""
            SELECT 
                COUNT(*) as total_assets,
                COUNT(DISTINCT blob_id) as unique_assets,
                (SELECT COUNT(DISTINCT dandiset_id) FROM asset_dandiset_mappings) as unique_dandisets,
                MAX(dandiset_count) as max_dandisets_per_asset
            FROM (
                SELECT 
                    blob_id,
                    COUNT(DISTINCT dandiset_id) as dandiset_count
                FROM asset_dandiset_mappings
                GROUP BY blob_id
            )
        """).fetchone()
        
        total_mappings = stats[0]
        unique_assets = stats[1]
        unique_dandisets = stats[2]
        max_dandisets_per_asset = stats[3]
        
        assets_with_multiple_dandisets = len(multi_dandiset_assets)
        
        logger.info(f"Asset-dandiset relationship analysis:")
        logger.info(f"  Total asset-dandiset mappings: {total_mappings:,}")
        logger.info(f"  Unique assets: {unique_assets:,}")
        logger.info(f"  Unique dandisets: {unique_dandisets:,}")
        logger.info(f"  Assets belonging to multiple dandisets: {assets_with_multiple_dandisets:,}")
        logger.info(f"  Maximum dandisets per asset: {max_dandisets_per_asset}")
        
        if assets_with_multiple_dandisets > 0:
            logger.warning(f"Found {assets_with_multiple_dandisets} assets belonging to multiple dandisets!")
            logger.info("Sample multi-dandiset assets:")
            print(multi_dandiset_assets.head(10).to_string(index=False))
        else:
            logger.info("✓ All assets belong to exactly one dandiset")
        
        return {
            "total_mappings": total_mappings,
            "unique_assets": unique_assets,
            "unique_dandisets": unique_dandisets,
            "assets_with_multiple_dandisets": assets_with_multiple_dandisets,
            "max_dandisets_per_asset": max_dandisets_per_asset,
            "multi_dandiset_assets": multi_dandiset_assets,
            "has_multiple_mappings": assets_with_multiple_dandisets > 0
        }
    
    def create_daily_ip_dandiset_view(self):
        """Create daily IP-dandiset aggregation view with region information."""
        logger.info("Creating daily IP-dandiset aggregation view...")
        
        # First check if we need to handle multiple dandiset mappings
        relationship_stats = self.analyze_asset_dandiset_relationships()
        
        if relationship_stats["has_multiple_mappings"]:
            logger.info("Creating view with multiple dandiset handling (selecting first dandiset alphabetically)...")
            
            # Create a helper view that selects one dandiset per asset
            self.conn.execute("""
                CREATE OR REPLACE VIEW asset_single_dandiset AS
                SELECT DISTINCT ON (blob_id)
                    blob_id,
                    dandiset_id
                FROM asset_dandiset_mappings
                ORDER BY blob_id, dandiset_id  -- Choose first dandiset alphabetically
            """)
            
            # Create downloads view using single dandiset mapping
            self.conn.execute("""
                CREATE OR REPLACE VIEW downloads_single_dandiset AS
                SELECT 
                    d.*,
                    asd.dandiset_id,
                    a.asset_path,
                    a.asset_size,
                    ds.name as dandiset_name,
                    r.region_code,
                    r.country,
                    r.region,
                    r.provider,
                    r.latitude,
                    r.longitude
                FROM downloads_base d
                JOIN assets a ON d.blob_id = a.blob_id
                JOIN asset_single_dandiset asd ON a.blob_id = asd.blob_id
                LEFT JOIN dandisets ds ON asd.dandiset_id = ds.identifier
                LEFT JOIN ip_regions ir ON d.indexed_ip = ir.indexed_ip
                LEFT JOIN regions r ON ir.region_code = r.region_code
            """)
            
            source_view = "downloads_single_dandiset"
        else:
            logger.info("Using existing downloads_enriched view (no multiple dandiset mappings found)...")
            source_view = "downloads_enriched"
        
        # Create the main aggregation view
        self.conn.execute(f"""
            CREATE OR REPLACE VIEW daily_ip_dandiset_stats AS
            SELECT 
                indexed_ip,
                dandiset_id,
                dandiset_name,
                download_date,
                SUM(bytes_sent) as total_bytes_downloaded,
                COUNT(*) as total_downloads,
                COUNT(DISTINCT blob_id) as unique_assets_downloaded,
                -- Region information (same for all records with same IP)
                region_code,
                country,
                region,
                provider,
                latitude,
                longitude
            FROM {source_view}
            WHERE dandiset_id IS NOT NULL  -- Exclude downloads without dandiset mapping
            GROUP BY indexed_ip, dandiset_id, dandiset_name, download_date, 
                     region_code, country, region, provider, latitude, longitude
            ORDER BY download_date DESC, total_bytes_downloaded DESC
        """)
        
        logger.info("Daily IP-dandiset aggregation view created successfully")
        
    
    def export_daily_ip_dandiset_stats(self, output_path: str = "daily_ip_dandiset_stats.parquet"):
        """Export daily IP-dandiset aggregation to parquet file efficiently."""
        logger.info(f"Exporting daily IP-dandiset stats to {output_path}...")
        
        # Use DuckDB's native COPY TO for efficient parquet export
        self.conn.execute(f"""
            COPY (SELECT * FROM daily_ip_dandiset_stats) 
            TO '{output_path}' (FORMAT PARQUET)
        """)
    
    def get_daily_ip_dandiset_sample(self, limit: int = 10) -> pd.DataFrame:
        """Get a sample of daily IP-dandiset aggregated data."""
        logger.info(f"Getting sample of {limit} daily IP-dandiset records...")
        
        # First ensure the view exists
        try:
            result = self.conn.execute(f"""
                SELECT * FROM daily_ip_dandiset_stats 
                LIMIT {limit}
            """).fetchdf()
            return result
        except Exception as e:
            logger.warning(f"daily_ip_dandiset_stats view not found, creating it first...")
            self.create_daily_ip_dandiset_view()
            result = self.conn.execute(f"""
                SELECT * FROM daily_ip_dandiset_stats 
                LIMIT {limit}
            """).fetchdf()
            return result
    
    def get_database_stats(self) -> Dict:
        """Get comprehensive database statistics."""
        logger.info("Getting database statistics...")
        
        stats = {}
        
        # Get parquet file stats
        try:
            result = self.conn.execute(f"""
                SELECT COUNT(*) as total_download_records
                FROM read_parquet('{self.parquet_path}')
            """).fetchone()
            stats['total_download_records'] = result[0] if result else 0
        except Exception as e:
            logger.warning(f"Could not get parquet stats: {e}")
            stats['total_download_records'] = 0
        
        # Get blob mapping stats
        result = self.conn.execute("SELECT COUNT(*) FROM blob_mapping").fetchone()
        stats['blob_mappings'] = result[0] if result else 0
        
        # Get region stats
        result = self.conn.execute("SELECT COUNT(*) FROM regions").fetchone()
        stats['total_regions'] = result[0] if result else 0
        
        # Get IP-region mapping stats
        result = self.conn.execute("SELECT COUNT(*) FROM ip_regions").fetchone()
        stats['ip_region_mappings'] = result[0] if result else 0
        
        # Get asset stats
        result = self.conn.execute("SELECT COUNT(*) FROM assets").fetchone()
        stats['total_assets'] = result[0] if result else 0
        
        # Get dandiset stats
        result = self.conn.execute("SELECT COUNT(*) FROM dandisets").fetchone()
        stats['total_dandisets'] = result[0] if result else 0
        
        # Get asset-dandiset mapping stats
        result = self.conn.execute("SELECT COUNT(*) FROM asset_dandiset_mappings").fetchone()
        stats['asset_dandiset_mappings'] = result[0] if result else 0
        
        return stats
    
    def close(self):
        """Close the database connection."""
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()
            logger.info("Database connection closed")


def main():
    """Full demo with all metadata loaded."""
    print("🦆 DuckDB Analytics with Direct Parquet Querying")
    print("=" * 50)
    
    db = DuckDBAnalytics(db_path="data/analytics.duckdb", parquet_path="data/database.parquet")
    # db.load_blob_mapping("maps/blob_index_to_id.yaml")
    # db.load_ip_region_mapping("maps/index_to_region.yaml", "maps/region_codes_to_coordinates.yaml")
    db.build_asset_mappings()
    db.analyze_asset_dandiset_relationships()
    db.create_analytics_views()
    db.create_daily_ip_dandiset_view()
    db.export_daily_ip_dandiset_stats("data/daily_ip_dandiset_stats.parquet")
    
    return db


if __name__ == "__main__":
    db = main()
