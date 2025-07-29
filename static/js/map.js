// Map visualization module - v2.0
const MapVisualization = {
    map: null,
    markersLayer: null,
    selectedMarker: null,
    colorScheme: 'volume', // 'volume' or 'datasets'

    init() {
        this.initializeMap();
        this.setupMarkerLayer();
        this.loadRegions();
    },

    initializeMap() {
        // Initialize Leaflet map
        this.map = L.map('map', {
            center: [20, 0], // Center on world
            zoom: 2,
            minZoom: 1,
            maxZoom: 10,
            worldCopyJump: true,
            zoomControl: true
        });

        // Add tile layer
        L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
            attribution: '© OpenStreetMap contributors',
            maxZoom: 18,
            noWrap: false
        }).addTo(this.map);

        // Store reference in AppState
        AppState.map = this.map;

        // Add zoom event listener to resize bubbles dynamically
        this.map.on('zoomend', () => {
            this.resizeMarkersOnZoom();
        });
    },

    setupMarkerLayer() {
        // Create layer group for markers
        this.markersLayer = L.layerGroup().addTo(this.map);
    },

    async loadRegions() {
        try {
            const regions = AppState.regions;
            this.updateRegions(regions);
        } catch (error) {
            console.error('Error loading regions:', error);
        }
    },

    updateRegions(regions) {
        // Clear existing markers
        this.markersLayer.clearLayers();
        this.selectedMarker = null;

        if (!regions || regions.length === 0) {
            this.updateLegend([]);
            return;
        }

        // Filter regions with data and sort by volume (ascending order)
        // This ensures higher volume regions are added last and appear on top
        const sortedRegions = regions
            .filter(region => region.total_bytes > 0)
            .sort((a, b) => a.total_bytes - b.total_bytes);

        // Calculate size scaling for bubbles (always based on volume)
        const maxBytes = Math.max(...sortedRegions.map(r => r.total_bytes));
        const minBytes = Math.min(...sortedRegions.map(r => r.total_bytes));
        const logMax = Math.log(maxBytes);
        const logMin = Math.log(minBytes);

        // Calculate color scaling based on selected scheme
        let colorMax, colorMin, colorLogMax, colorLogMin;
        if (this.colorScheme === 'datasets') {
            const maxDatasets = Math.max(...sortedRegions.map(r => r.dataset_count));
            const minDatasets = Math.min(...sortedRegions.map(r => r.dataset_count));
            colorMax = maxDatasets;
            colorMin = minDatasets;
            colorLogMax = Math.log(Math.max(maxDatasets, 1));
            colorLogMin = Math.log(Math.max(minDatasets, 1));
        } else {
            colorMax = maxBytes;
            colorMin = minBytes;
            colorLogMax = logMax;
            colorLogMin = logMin;
        }

        // Create markers in ascending volume order (low to high)
        // Higher volume markers will be on top
        sortedRegions.forEach(region => {
            this.createRegionMarker(region, logMin, logMax, colorLogMin, colorLogMax);
        });

        // Update legend with actual data ranges
        this.updateLegend(sortedRegions);
    },

    createRegionMarker(region, logMin, logMax, colorLogMin, colorLogMax) {
        // Calculate bubble size based on download volume and zoom level
        const logBytes = Math.log(region.total_bytes);
        const normalizedSize = (logBytes - logMin) / (logMax - logMin);
        
        // Get current zoom level for responsive sizing
        const currentZoom = this.map.getZoom();
        const zoomFactor = Math.max(0.3, Math.min(1.5, currentZoom / 10)); // Scale factor based on zoom
        
        // Smaller base radius values and zoom-responsive scaling
        const minRadius = Math.max(2, 3 * zoomFactor);
        const maxRadius = Math.max(8, 15 * zoomFactor);
        const radius = minRadius + (normalizedSize * (maxRadius - minRadius));

        // Calculate color based on selected scheme
        let colorValue;
        if (this.colorScheme === 'datasets') {
            const logDatasets = Math.log(Math.max(region.dataset_count, 1));
            colorValue = (logDatasets - colorLogMin) / (colorLogMax - colorLogMin);
        } else {
            // For volume scheme, use actual bytes value
            colorValue = region.total_bytes;
        }

        // Create color scale
        const color = this.getColor(colorValue, this.colorScheme, region);
        
        // Determine opacity based on dataset diversity
        const opacity = Math.min(0.3 + (region.dataset_count / 20), 0.8);

        // Create circle marker
        const marker = L.circleMarker([region.latitude, region.longitude], {
            radius: radius,
            fillColor: color.fill,
            color: color.stroke,
            weight: 2,
            opacity: 0.8,
            fillOpacity: opacity
        });

        // Create popup content
        const popupContent = this.createPopupContent(region);
        marker.bindPopup(popupContent, {
            maxWidth: 300,
            className: 'custom-popup'
        });

        // Add click handler
        marker.on('click', (e) => {
            this.selectRegion(region, marker);
        });

        // Add hover effects
        marker.on('mouseover', (e) => {
            marker.setStyle({
                weight: 3,
                fillOpacity: Math.min(opacity + 0.2, 1.0)
            });
        });

        marker.on('mouseout', (e) => {
            if (marker !== this.selectedMarker) {
                marker.setStyle({
                    weight: 2,
                    fillOpacity: opacity
                });
            }
        });

        // Add to markers layer
        marker.addTo(this.markersLayer);

        // Store region data in marker
        marker.regionData = region;
    },

    createPopupContent(region) {
        const displayName = region.name !== region.code ? 
            `${region.name} (${region.code})` : region.code;
        
        // Try to get chart data total first, fallback to backend total
        let totalBytes = region.total_bytes;
        let totalBytesFormatted = region.total_bytes_formatted;
        
        const chartTotal = Utils.calculateChartTotal(region.code);
        if (chartTotal !== null) {
            totalBytes = chartTotal;
            totalBytesFormatted = Utils.formatBytes(chartTotal);
        }
        
        // Only show datasets count when "All Datasets" is selected
        const showDatasets = AppState.selectedDataset === 'ALL';
        const datasetsLine = showDatasets ? 
            `<div><strong>Datasets:</strong> ${Utils.formatNumber(region.dataset_count)}</div>` : '';
        
        return `
            <div class="popup-content">
                <div class="popup-title">${displayName}</div>
                <div class="popup-stats">
                    <div><strong>Country:</strong> ${region.country}</div>
                    <div><strong>Total Downloads:</strong> ${totalBytesFormatted}</div>
                    ${datasetsLine}
                </div>
            </div>
        `;
    },

    selectRegion(region, marker) {
        // Reset previous selection
        if (this.selectedMarker && this.selectedMarker !== marker) {
            const prevRegion = this.selectedMarker.regionData;
            const prevOpacity = Math.min(0.3 + (prevRegion.dataset_count / 20), 0.8);
            this.selectedMarker.setStyle({
                weight: 2,
                fillOpacity: prevOpacity,
                color: '#4a5fb8'
            });
        }

        // Highlight selected marker
        marker.setStyle({
            weight: 4,
            fillOpacity: 0.9,
            color: '#e74c3c'
        });

        this.selectedMarker = marker;

        // Trigger region selection in main app
        App.selectRegion(region.code, region.name);

        // Center map on selected region (with some offset for better view)
        this.map.setView([region.latitude, region.longitude], Math.max(this.map.getZoom(), 4));
    },

    resetSelection() {
        if (this.selectedMarker) {
            const region = this.selectedMarker.regionData;
            const opacity = Math.min(0.3 + (region.dataset_count / 20), 0.8);
            this.selectedMarker.setStyle({
                weight: 2,
                fillOpacity: opacity,
                color: '#4a5fb8'
            });
            this.selectedMarker = null;
        }
    },

    fitToRegions() {
        if (this.markersLayer.getLayers().length > 0) {
            const group = new L.featureGroup(this.markersLayer.getLayers());
            this.map.fitBounds(group.getBounds(), { padding: [20, 20] });
        }
    },

    // Utility methods for external calls
    zoomToRegion(regionCode) {
        const marker = this.findMarkerByRegionCode(regionCode);
        if (marker) {
            this.selectRegion(marker.regionData, marker);
        }
    },

    findMarkerByRegionCode(regionCode) {
        const markers = this.markersLayer.getLayers();
        return markers.find(marker => marker.regionData && marker.regionData.code === regionCode);
    },

    // Method to update map based on dataset filter
    filterByDataset(datasetId) {
        // This will be called when dataset filter changes
        // The actual filtering is handled by updating regions from the API
        AppState.selectedDataset = datasetId;
    },

    // Method to highlight regions based on search or other criteria
    highlightRegions(regionCodes) {
        const markers = this.markersLayer.getLayers();
        markers.forEach(marker => {
            if (marker.regionData) {
                const isHighlighted = regionCodes.includes(marker.regionData.code);
                marker.setStyle({
                    fillOpacity: isHighlighted ? 0.9 : 0.3,
                    weight: isHighlighted ? 3 : 2
                });
            }
        });
    },

    // Add legend update method
    updateLegend(regions) {
        const legendItems = document.querySelectorAll('.legend-item span');
        
        if (!regions || regions.length === 0) {
            // Reset legend when no data
            legendItems.forEach((item, index) => {
                const labels = this.colorScheme === 'datasets' ? 
                    ['Low Count', 'Medium Count', 'Medium-High Count', 'High Count'] :
                    ['Low Volume', 'Medium Volume', 'Medium-High Volume', 'High Volume'];
                if (item && labels[index]) {
                    item.textContent = labels[index];
                }
            });
            return;
        }

        if (this.colorScheme === 'datasets') {
            // Fixed legend categories for dataset count
            if (legendItems.length >= 4) {
                legendItems[0].textContent = '1';
                legendItems[1].textContent = '1-10';
                legendItems[2].textContent = '11-100';
                legendItems[3].textContent = '≥100';
            }
        } else {
            // Legend for data volume - use centralized category system for perfect consistency
            const categories = ['low', 'medium', 'high', 'very-high'];
            
            // Update legend text using the centralized category system
            legendItems.forEach((item, index) => {
                if (categories[index]) {
                    const categoryInfo = this.getCategoryInfo(categories[index]);
                    item.textContent = categoryInfo.range;
                }
            });
        }
    },

    // Method to resize markers based on zoom level
    resizeMarkersOnZoom() {
        const markers = this.markersLayer.getLayers();
        if (markers.length === 0) return;

        // Calculate new size scaling based on current zoom
        const currentZoom = this.map.getZoom();
        const zoomFactor = Math.max(0.3, Math.min(1.5, currentZoom / 10));
        
        // Get the overall size range for all markers
        const allRegions = markers.map(marker => marker.regionData);
        const maxBytes = Math.max(...allRegions.map(r => r.total_bytes));
        const minBytes = Math.min(...allRegions.filter(r => r.total_bytes > 0).map(r => r.total_bytes));
        const logMax = Math.log(maxBytes);
        const logMin = Math.log(minBytes);

        // Update each marker's radius
        markers.forEach(marker => {
            if (marker.regionData && marker.regionData.total_bytes > 0) {
                const region = marker.regionData;
                const logBytes = Math.log(region.total_bytes);
                const normalizedSize = (logBytes - logMin) / (logMax - logMin);
                
                // Calculate new radius with zoom factor
                const minRadius = Math.max(2, 3 * zoomFactor);
                const maxRadius = Math.max(8, 15 * zoomFactor);
                const newRadius = minRadius + (normalizedSize * (maxRadius - minRadius));
                
                // Update marker radius
                marker.setRadius(newRadius);
            }
        });
    },

    // Static thresholds for volume-based coloring (in bytes)
    getVolumeThresholds() {
        return {
            threshold1: 10485760,       // 10 MB
            threshold2: 10737418240,    // 10 GB
            threshold3: 10995116277760  // 10 TB
        };
    },

    // Centralized function to determine volume category
    getVolumeCategory(totalBytes) {
        const thresholds = this.getVolumeThresholds();
        
        if (totalBytes <= thresholds.threshold1) {
            return 'low';               // ≤ 10 MB
        } else if (totalBytes <= thresholds.threshold2) {
            return 'medium';            // 10 MB - 10 GB
        } else if (totalBytes <= thresholds.threshold3) {
            return 'high';              // 10 GB - 10 TB
        } else {
            return 'very-high';         // > 10 TB
        }
    },

    // Get color based on category
    getCategoryColor(category) {
        const colorMap = {
            'low': {
                fill: '#26c6da',     // Cyan
                stroke: '#0097a7'     // Dark cyan
            },
            'medium': {
                fill: '#66bb6a',     // Light green
                stroke: '#388e3c'     // Dark green
            },
            'high': {
                fill: '#ffca28',     // Yellow
                stroke: '#f57f17'     // Dark yellow
            },
            'very-high': {
                fill: '#ff7043',     // Orange-red
                stroke: '#d84315'     // Dark red
            }
        };
        
        return colorMap[category] || colorMap['medium']; // Default fallback
    },

    // Get category display name and range
    getCategoryInfo(category) {
        const thresholds = this.getVolumeThresholds();
        
        const categoryInfo = {
            'low': {
                name: 'Low Volume', 
                range: `≤ ${Utils.formatBytes(thresholds.threshold1)}`
            },
            'medium': {
                name: 'Medium Volume',
                range: `${Utils.formatBytes(thresholds.threshold1)} - ${Utils.formatBytes(thresholds.threshold2)}`
            },
            'high': {
                name: 'High Volume',
                range: `${Utils.formatBytes(thresholds.threshold2)} - ${Utils.formatBytes(thresholds.threshold3)}`
            },
            'very-high': {
                name: 'Very High Volume',
                range: `> ${Utils.formatBytes(thresholds.threshold3)}`
            }
        };
        
        return categoryInfo[category] || categoryInfo['medium']; // Default fallback
    },

    // Method to get color based on the selected scheme
    getColor(value, scheme, region = null) {
        if (scheme === 'datasets') {
            // For datasets, use actual dataset count to match fixed legend categories
            const datasetCount = region ? region.dataset_count : 1;
            
            // Map to fixed categories: 1, 1-10, 11-100, ≥100
            if (datasetCount === 1) {
                return {
                    fill: '#4caf50',     // Green for exactly 1 dataset
                    stroke: '#388e3c'     // Dark green
                };
            } else if (datasetCount >= 2 && datasetCount <= 10) {
                return {
                    fill: '#cddc39',     // Lime for 2-10 range (to complete 1-10 category)
                    stroke: '#9e9d24'     // Dark lime
                };
            } else if (datasetCount >= 11 && datasetCount <= 100) {
                return {
                    fill: '#e91e63',     // Pink for 11-100 range
                    stroke: '#ad1457'     // Dark pink
                };
            } else { // datasetCount >= 100
                return {
                    fill: '#9c27b0',     // Purple/Magenta for ≥100 range
                    stroke: '#7b1fa2'     // Dark purple
                };
            }
        } else {
            // Volume scheme: Use centralized category system to eliminate confusion
            const totalBytes = value; // value is now the actual bytes for volume scheme
            const category = this.getVolumeCategory(totalBytes);
            return this.getCategoryColor(category);
        }
    },

    // Method to change color scheme
    setColorScheme(scheme) {
        this.colorScheme = scheme;
        // Update legend circles CSS classes
        this.updateLegendCircles(scheme);
        // Refresh the visualization with current regions
        this.updateRegions(AppState.regions);
    },

    // Update legend circle CSS classes based on color scheme
    updateLegendCircles(scheme) {
        const legendCircles = document.querySelectorAll('.legend-circle');
        const suffixes = ['low', 'medium', 'medium-high', 'high'];
        const schemeSuffix = scheme === 'datasets' ? 'datasets' : 'volume';
        
        legendCircles.forEach((circle, index) => {
            // Remove all existing scheme classes
            circle.classList.remove('low-volume', 'medium-volume', 'medium-high-volume', 'high-volume');
            circle.classList.remove('low-datasets', 'medium-datasets', 'medium-high-datasets', 'high-datasets');
            
            // Add the appropriate class for the current scheme
            if (suffixes[index]) {
                circle.classList.add(`${suffixes[index]}-${schemeSuffix}`);
            }
        });
    },

    // Method to get color based on download volume (for backward compatibility)
    getVolumeColor(normalizedSize) {
        return this.getColor(normalizedSize, 'volume');
    }
};

// Export for global access
window.MapVisualization = MapVisualization;
