from flask import Flask, jsonify, request, render_template
from flask_cors import CORS
import pandas as pd
from datetime import datetime
import requests

app = Flask(__name__)
CORS(app)

# Path to the parquet file
PARQUET_PATH = 'data/daily_ip_dandiset_downloads.parquet'

# Global variable to cache the dataframe
_df_cache = None
_df_cache_timestamp = None

def load_data():
    """Load and cache the parquet data"""
    global _df_cache, _df_cache_timestamp
    
    # Cache for 5 minutes to avoid reloading on every request
    cache_duration = 300
    current_time = datetime.now().timestamp()
    
    if _df_cache is None or (_df_cache_timestamp is None) or (current_time - _df_cache_timestamp) > cache_duration:
        try:
            _df_cache = pd.read_parquet(PARQUET_PATH)
            _df_cache_timestamp = current_time
            print(f"Loaded parquet data with {len(_df_cache)} rows")
        except Exception as e:
            print(f"Error loading parquet file: {e}")
            # Return empty dataframe if file can't be loaded
            _df_cache = pd.DataFrame()
    
    return _df_cache

@app.route('/')
def index():
    """Serve the main dashboard page"""
    return render_template('index.html')

def format_bytes(bytes_value):
    """Format bytes into human readable format"""
    if bytes_value is None or pd.isna(bytes_value):
        return "0 B"
    
    bytes_value = float(bytes_value)
    for unit in ['B', 'KB', 'MB', 'GB', 'TB', 'PB']:
        if bytes_value < 1024.0:
            return f"{bytes_value:.1f} {unit}"
        bytes_value /= 1024.0
    return f"{bytes_value:.1f} EB"

@app.route('/api/regions')
def get_regions():
    """Get all regions with their download statistics"""
    dataset_filter = request.args.get('dataset_id')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    
    df = load_data()
    if df.empty:
        return jsonify([])
    
    # Apply filters
    filtered_df = df.copy()
    
    if dataset_filter and dataset_filter != 'ALL':
        filtered_df = filtered_df[filtered_df['dandiset_id'] == int(dataset_filter)]
    
    if start_date:
        start_date_obj = pd.to_datetime(start_date).date()
        filtered_df = filtered_df[filtered_df['download_date'] >= start_date_obj]
    
    if end_date:
        end_date_obj = pd.to_datetime(end_date).date()
        filtered_df = filtered_df[filtered_df['download_date'] <= end_date_obj]
    
    # Aggregate by region
    region_stats = filtered_df.groupby(['region', 'latitude', 'longitude']).agg({
        'total_bytes_sent': 'sum',
        'dandiset_id': 'nunique'
    }).reset_index()
    
    region_stats.columns = ['region', 'latitude', 'longitude', 'total_bytes', 'dataset_count']
    
    # Remove regions with no downloads
    region_stats = region_stats[region_stats['total_bytes'] > 0]
    
    # Format output
    regions = []
    for _, row in region_stats.iterrows():
        # Parse region code to get name and country
        region_code = row['region']
        if '/' in region_code:
            country, name = region_code.split('/', 1)
        else:
            country = region_code
            name = region_code
        
        regions.append({
            'code': region_code,
            'name': name,
            'country': country,
            'latitude': float(row['latitude']),
            'longitude': float(row['longitude']),
            'total_bytes': int(row['total_bytes']),
            'total_bytes_formatted': format_bytes(row['total_bytes']),
            'dataset_count': int(row['dataset_count'])
        })
    
    return jsonify(regions)

@app.route('/api/downloads/region/<path:region_code>')
def get_region_downloads(region_code):
    """Get time series downloads for a specific region"""
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    dataset_filter = request.args.get('dataset_id')
    
    df = load_data()
    if df.empty:
        return jsonify({
            'region_code': region_code,
            'time_series': [],
            'top_datasets': [],
            'dataset_totals': {}
        })
    
    # Filter by region
    region_df = df[df['region'] == region_code].copy()
    
    if region_df.empty:
        return jsonify({
            'region_code': region_code,
            'time_series': [],
            'top_datasets': [],
            'dataset_totals': {}
        })
    
    # Apply dataset filter
    if dataset_filter and dataset_filter != 'ALL':
        region_df = region_df[region_df['dandiset_id'] == int(dataset_filter)]
    
    # Apply date filters
    if start_date:
        start_date_obj = pd.to_datetime(start_date).date()
        region_df = region_df[region_df['download_date'] >= start_date_obj]
    
    if end_date:
        end_date_obj = pd.to_datetime(end_date).date()
        region_df = region_df[region_df['download_date'] <= end_date_obj]
    
    # Get dataset totals for this region
    dataset_totals = region_df.groupby('dandiset_id')['total_bytes_sent'].sum().sort_values(ascending=False)
    
    # Get top 7 datasets
    top_datasets = [str(dataset_id) for dataset_id in dataset_totals.head(7).index]
    dataset_totals_dict = {str(k): int(v) for k, v in dataset_totals.head(7).items()}
    
    # Create time series data
    daily_data = region_df.groupby(['download_date', 'dandiset_id'])['total_bytes_sent'].sum().reset_index()
    
    # Pivot to get datasets as columns
    time_series_pivot = daily_data.pivot(index='download_date', columns='dandiset_id', values='total_bytes_sent').fillna(0)
    
    # Prepare time series output
    time_series = []
    for date, row in time_series_pivot.iterrows():
        day_data = {'date': str(date)}
        other_bytes = 0
        
        for dataset_id, bytes_sent in row.items():
            dataset_str = str(int(dataset_id))
            if dataset_str in top_datasets:
                day_data[dataset_str] = int(float(bytes_sent))
            else:
                other_bytes += int(float(bytes_sent))
        
        if other_bytes > 0:
            day_data['OTHER'] = other_bytes
        
        time_series.append(day_data)
    
    return jsonify({
        'region_code': region_code,
        'time_series': time_series,
        'top_datasets': top_datasets,
        'dataset_totals': dataset_totals_dict
    })

@app.route('/api/downloads/global')
def get_global_downloads():
    """Get global time series downloads across all regions"""
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    dataset_filter = request.args.get('dataset_id')
    
    df = load_data()
    if df.empty:
        return jsonify({
            'time_series': [],
            'top_datasets': [],
            'dataset_totals': {}
        })
    
    # Apply filters
    filtered_df = df.copy()
    
    if dataset_filter and dataset_filter != 'ALL':
        filtered_df = filtered_df[filtered_df['dandiset_id'] == int(dataset_filter)]
    
    if start_date:
        start_date_obj = pd.to_datetime(start_date).date()
        filtered_df = filtered_df[filtered_df['download_date'] >= start_date_obj]
    
    if end_date:
        end_date_obj = pd.to_datetime(end_date).date()
        filtered_df = filtered_df[filtered_df['download_date'] <= end_date_obj]
    
    # When a specific dataset is selected, show regions instead of datasets
    if dataset_filter and dataset_filter != 'ALL':
        # Aggregate by date and region for the selected dataset
        daily_data = filtered_df.groupby(['download_date', 'region'])['total_bytes_sent'].sum().reset_index()
        
        # Get region totals for this dataset
        region_totals = filtered_df.groupby('region')['total_bytes_sent'].sum().sort_values(ascending=False)
        
        # Get top 7 regions
        top_regions = [str(region) for region in region_totals.head(7).index]
        region_totals_dict = {str(k): int(v) for k, v in region_totals.head(7).items()}
        
        # Pivot to get regions as columns
        time_series_pivot = daily_data.pivot(index='download_date', columns='region', values='total_bytes_sent').fillna(0)
        
        # Prepare time series output
        time_series = []
        for date, row in time_series_pivot.iterrows():
            day_data = {'date': str(date)}
            other_bytes = 0
            
            for region, bytes_sent in row.items():
                region_str = str(region)
                if region_str in top_regions:
                    day_data[region_str] = int(float(bytes_sent))
                else:
                    other_bytes += int(float(bytes_sent))
            
            if other_bytes > 0:
                day_data['OTHER'] = other_bytes
            
            time_series.append(day_data)
        
        return jsonify({
            'time_series': time_series,
            'top_datasets': top_regions,  # Using 'top_datasets' for consistency, but contains regions
            'dataset_totals': region_totals_dict,  # Using 'dataset_totals' for consistency, but contains region totals
            'view_type': 'regions'  # Add indicator for frontend
        })
    
    else:
        # Default behavior: show datasets across all regions
        # Aggregate by date and dataset
        daily_data = filtered_df.groupby(['download_date', 'dandiset_id'])['total_bytes_sent'].sum().reset_index()
        
        # Get dataset totals
        dataset_totals = filtered_df.groupby('dandiset_id')['total_bytes_sent'].sum().sort_values(ascending=False)
        
        # Get top 7 datasets
        top_datasets = [str(dataset_id) for dataset_id in dataset_totals.head(7).index]
        dataset_totals_dict = {str(k): int(v) for k, v in dataset_totals.head(7).items()}
        
        # Pivot to get datasets as columns
        time_series_pivot = daily_data.pivot(index='download_date', columns='dandiset_id', values='total_bytes_sent').fillna(0)
        
        # Prepare time series output
        time_series = []
        for date, row in time_series_pivot.iterrows():
            day_data = {'date': str(date)}
            other_bytes = 0
            
            for dataset_id, bytes_sent in row.items():
                dataset_str = str(int(dataset_id))
                if dataset_str in top_datasets:
                    day_data[dataset_str] = int(float(bytes_sent))
                else:
                    other_bytes += int(float(bytes_sent))
            
            if other_bytes > 0:
                day_data['OTHER'] = other_bytes
            
            time_series.append(day_data)
        
        return jsonify({
            'time_series': time_series,
            'top_datasets': top_datasets,
            'dataset_totals': dataset_totals_dict,
            'view_type': 'datasets'  # Add indicator for frontend
        })

@app.route('/api/datasets')
def get_datasets():
    """Get list of all datasets"""
    df = load_data()
    if df.empty:
        return jsonify([])
    
    # Aggregate by dataset
    dataset_stats = df.groupby('dandiset_id').agg({
        'total_bytes_sent': 'sum',
        'region': 'nunique',
        'latitude': 'count'  # Use this as a proxy for records count
    }).reset_index()
    
    # Count unique countries by parsing region codes
    country_counts = []
    for dataset_id in dataset_stats['dandiset_id']:
        dataset_regions = df[df['dandiset_id'] == dataset_id]['region'].unique()
        countries = set()
        for region in dataset_regions:
            if '/' in region:
                country = region.split('/')[0]
                countries.add(country)
            else:
                countries.add(region)
        country_counts.append(len(countries))
    
    dataset_stats['unique_countries'] = country_counts
    dataset_stats.columns = ['id', 'total_bytes', 'unique_regions', 'record_count', 'unique_countries']
    
    # Sort by total bytes descending
    dataset_stats = dataset_stats.sort_values('total_bytes', ascending=False)
    
    datasets = []
    for _, row in dataset_stats.iterrows():
        datasets.append({
            'id': str(int(row['id'])).zfill(6),  # Zero-pad to 6 digits
            'total_bytes': int(row['total_bytes']),
            'total_bytes_formatted': format_bytes(row['total_bytes']),
            'unique_regions': int(row['unique_regions']),
            'unique_countries': int(row['unique_countries'])
        })
    
    return jsonify(datasets)

@app.route('/api/stats')
def get_stats():
    """Get overall statistics"""
    df = load_data()
    if df.empty:
        return jsonify({
            'total_bytes': 0,
            'total_bytes_formatted': '0 B',
            'total_datasets': 0,
            'unique_regions': 0,
            'unique_countries': 0,
            'active_regions': 0
        })
    
    # Calculate overall statistics
    total_bytes = int(df['total_bytes_sent'].sum())
    total_datasets = int(df['dandiset_id'].nunique())
    unique_regions = int(df['region'].nunique())
    
    # Count unique countries
    countries = set()
    for region in df['region'].unique():
        if '/' in region:
            country = region.split('/')[0]
            countries.add(country)
        else:
            countries.add(region)
    unique_countries = len(countries)
    
    # Active regions are all regions with downloads
    active_regions = unique_regions
    
    return jsonify({
        'total_bytes': total_bytes,
        'total_bytes_formatted': format_bytes(total_bytes),
        'total_datasets': total_datasets,
        'unique_regions': unique_regions,
        'unique_countries': unique_countries,
        'active_regions': active_regions
    })

# Global cache for DANDI API data to avoid repeated calls
_dandi_cache = None
_cache_timestamp = None

def get_dandi_metadata():
    """Get DANDI metadata with caching"""
    global _dandi_cache, _cache_timestamp
    
    # Cache for 1 hour
    cache_duration = 3600
    current_time = datetime.now().timestamp()
    
    if _dandi_cache is None or (_cache_timestamp is None) or (current_time - _cache_timestamp) > cache_duration:
        try:
            api_url = "https://api.dandiarchive.org/api/dandisets/"
            params = {
                'page_size': 1000,
                'ordering': '-created'
            }
            
            response = requests.get(api_url, params=params, timeout=10)
            response.raise_for_status()
            
            api_data = response.json()
            
            # Create a mapping of dandiset IDs from the API
            api_dandisets = {}
            for dandiset in api_data.get('results', []):
                dandiset_id = dandiset.get('identifier', '')
                if dandiset_id:
                    # Get the most recent version
                    most_recent_version = dandiset.get('most_recent_published_version', {})
                    if not most_recent_version:
                        most_recent_version = dandiset.get('draft_version', {})
                    
                    version = most_recent_version.get('version', 'draft')
                    
                    api_dandisets[dandiset_id] = {
                        'name': most_recent_version.get('name', f'Dataset {dandiset_id}'),
                        'version': version,
                        'landing_url': f"https://dandiarchive.org/dandiset/{dandiset_id}/{version}"
                    }
            
            _dandi_cache = api_dandisets
            _cache_timestamp = current_time
            
        except Exception as e:
            print(f"Failed to fetch DANDI metadata: {e}")
            if _dandi_cache is None:
                _dandi_cache = {}
    
    return _dandi_cache

@app.route('/api/featured-dandisets')
def get_featured_dandisets():
    """Get featured dandisets - the top datasets shown in the global bar plot"""
    try:
        df = load_data()
        if df.empty:
            return jsonify({
                'featured_dandisets': [],
                'count': 0
            })
        
        # Get the top datasets by download volume
        dataset_totals = df.groupby('dandiset_id')['total_bytes_sent'].sum().sort_values(ascending=False).head(7)
        
        # Get DANDI metadata
        dandi_metadata = get_dandi_metadata()
        
        # Create featured dandisets list
        featured_dandisets = []
        for dataset_id, total_bytes in dataset_totals.items():
            dataset_id_str = str(dataset_id).zfill(6)  # Format as 6-digit string
            
            if dataset_id_str in dandi_metadata:
                metadata = dandi_metadata[dataset_id_str]
                featured_dandisets.append({
                    'id': dataset_id_str,
                    'name': metadata['name'],
                    'landing_url': metadata['landing_url'],
                    'version': metadata['version'],
                    'total_bytes': int(total_bytes),
                    'total_bytes_formatted': format_bytes(total_bytes)
                })
            else:
                featured_dandisets.append({
                    'id': dataset_id_str,
                    'name': f'Dataset {dataset_id_str}',
                    'landing_url': f'https://dandiarchive.org/dandiset/{dataset_id_str}/draft',
                    'version': 'draft',
                    'total_bytes': int(total_bytes),
                    'total_bytes_formatted': format_bytes(total_bytes)
                })
        
        return jsonify({
            'featured_dandisets': featured_dandisets,
            'count': len(featured_dandisets)
        })
        
    except Exception as e:
        return jsonify({
            'error': f'Failed to fetch featured dandisets: {str(e)}'
        }), 500

@app.route('/api/dandisets/metadata', methods=['POST'])
def get_dandisets_metadata():
    """Get metadata for specific dataset IDs"""
    try:
        data = request.get_json()
        dataset_ids = data.get('dataset_ids', [])
        dataset_totals = data.get('dataset_totals', {})
        
        if not dataset_ids:
            return jsonify({'error': 'No dataset IDs provided'}), 400
        
        # Get DANDI metadata
        dandi_metadata = get_dandi_metadata()
        
        # Create response with metadata for requested datasets
        dandisets = []
        for dataset_id in dataset_ids:
            # Format dataset ID as 6-digit string
            dataset_id_str = str(dataset_id).zfill(6)
            total_bytes = dataset_totals.get(dataset_id, 0)
            
            if dataset_id_str in dandi_metadata:
                metadata = dandi_metadata[dataset_id_str]
                dandisets.append({
                    'id': dataset_id_str,
                    'name': metadata['name'],
                    'landing_url': metadata['landing_url'],
                    'version': metadata['version'],
                    'total_bytes': total_bytes,
                    'total_bytes_formatted': format_bytes(total_bytes)
                })
            else:
                dandisets.append({
                    'id': dataset_id_str,
                    'name': f'Dataset {dataset_id_str}',
                    'landing_url': f'https://dandiarchive.org/dandiset/{dataset_id_str}/draft',
                    'version': 'draft',
                    'total_bytes': total_bytes,
                    'total_bytes_formatted': format_bytes(total_bytes)
                })
        
        return jsonify({
            'dandisets': dandisets,
            'count': len(dandisets)
        })
        
    except Exception as e:
        return jsonify({
            'error': f'Failed to fetch dandisets metadata: {str(e)}'
        }), 500

@app.route('/api/dataset/<dataset_id>/details')
def get_dataset_details(dataset_id):
    """Get detailed information for a specific dataset"""
    try:
        df = load_data()
        if df.empty:
            return jsonify({'error': 'No data available'}), 404
        
        # Convert dataset_id to integer for filtering
        try:
            dataset_id_int = int(dataset_id)
        except ValueError:
            return jsonify({'error': 'Invalid dataset ID'}), 400
        
        # Filter data for this dataset
        dataset_df = df[df['dandiset_id'] == dataset_id_int]
        
        if dataset_df.empty:
            return jsonify({'error': 'Dataset not found'}), 404
        
        # Calculate statistics
        total_bytes = int(dataset_df['total_bytes_sent'].sum())
        unique_regions = int(dataset_df['region'].nunique())
        
        # Count unique countries
        countries = set()
        for region in dataset_df['region'].unique():
            if '/' in region:
                country = region.split('/')[0]
                countries.add(country)
            else:
                countries.add(region)
        unique_countries = len(countries)
        
        # Get DANDI metadata
        dandi_metadata = get_dandi_metadata()
        
        # Format dataset ID as 6-digit string
        dataset_id_str = str(dataset_id).zfill(6)
        
        # Build response with all available information
        dataset_info = {
            'id': dataset_id_str,
            'total_bytes': total_bytes,
            'total_bytes_formatted': format_bytes(total_bytes),
            'unique_regions': unique_regions,
            'unique_countries': unique_countries
        }
        
        # Add DANDI metadata if available
        if dataset_id_str in dandi_metadata:
            metadata = dandi_metadata[dataset_id_str]
            dataset_info.update({
                'name': metadata['name'],
                'landing_url': metadata['landing_url'],
                'version': metadata['version']
            })
        else:
            dataset_info.update({
                'name': f'Dataset {dataset_id_str}',
                'landing_url': f'https://dandiarchive.org/dandiset/{dataset_id_str}/draft',
                'version': 'draft'
            })
        
        # Try to get additional metadata from DANDI API for this specific dataset
        try:
            api_url = f"https://api.dandiarchive.org/api/dandisets/{dataset_id_str}/"
            response = requests.get(api_url, timeout=10)
            
            if response.status_code == 200:
                api_data = response.json()
                
                # Get the most recent version for detailed info
                most_recent_version = api_data.get('most_recent_published_version', {})
                if not most_recent_version:
                    most_recent_version = api_data.get('draft_version', {})
                
                if most_recent_version:
                    # Add detailed metadata
                    dataset_info.update({
                        'description': most_recent_version.get('metadata', {}).get('description', 'No description available'),
                        'contributors': most_recent_version.get('metadata', {}).get('contributor', []),
                        'created': api_data.get('created', ''),
                        'modified': api_data.get('modified', ''),
                        'contact_person': most_recent_version.get('metadata', {}).get('contactPerson', [])
                    })
                    
                    # Format contributors for display
                    if dataset_info['contributors']:
                        formatted_contributors = []
                        for contributor in dataset_info['contributors'][:5]:  # Limit to first 5
                            name = contributor.get('name', 'Unknown')
                            if isinstance(name, dict):
                                # Handle structured name
                                given_name = name.get('givenName', '')
                                family_name = name.get('familyName', '')
                                name = f"{given_name} {family_name}".strip()
                            formatted_contributors.append(name)
                        
                        dataset_info['contributors_formatted'] = formatted_contributors
                        dataset_info['contributors_count'] = len(dataset_info['contributors'])
                    else:
                        dataset_info['contributors_formatted'] = []
                        dataset_info['contributors_count'] = 0
        
        except Exception as e:
            print(f"Failed to fetch detailed DANDI metadata for {dataset_id_str}: {e}")
            # Set defaults if API call fails
            dataset_info.update({
                'description': 'Description not available',
                'contributors_formatted': [],
                'contributors_count': 0
            })
        
        return jsonify(dataset_info)
        
    except Exception as e:
        return jsonify({
            'error': f'Failed to fetch dataset details: {str(e)}'
        }), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5001)
