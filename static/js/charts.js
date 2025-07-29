// Charts visualization module using D3.js
const ChartsVisualization = {
    globalChart: null,
    regionChart: null,
    tooltip: null,

    init() {
        this.createTooltip();
        this.setupResizeHandler();
    },

    setupResizeHandler() {
        // Add debounced window resize listener
        let resizeTimeout;
        window.addEventListener('resize', () => {
            clearTimeout(resizeTimeout);
            resizeTimeout = setTimeout(() => {
                this.handleResize();
            }, 250); // Debounce resize events
        });
    },

    handleResize() {
        // Use the existing app refresh mechanism for reliable re-rendering
        if (window.App && typeof window.App.refreshCurrentChart === 'function') {
            window.App.refreshCurrentChart();
        }
    },

    createTooltip() {
        // Create shared tooltip element
        this.tooltip = d3.select('body')
            .append('div')
            .attr('class', 'tooltip')
            .style('opacity', 0);
    },

    renderGlobalChart(data) {
        // Backward compatibility - redirect to renderMainChart
        this.renderMainChart(data);
    },

    renderRegionChart(data) {
        // Backward compatibility - redirect to renderMainChart
        this.renderMainChart(data);
    },

    renderMainChart(data) {
        // Clear previous chart
        d3.select('#main-chart').selectAll('*').remove();

        if (!data.time_series || data.time_series.length === 0) {
            this.showEmptyChart('#main-chart', 'No download data available');
            return;
        }

        // Store chart data in AppState for access by other components
        AppState.chartData = data;

        const title = data.region_code ? `Downloads in ${data.region_code}` : 'Global Downloads';
        this.renderStackedBarChart('#main-chart', data, title);
    },

    clearRegionChart() {
        // Clear main chart and show default state
        d3.select('#main-chart').selectAll('*').remove();
        this.showEmptyChart('#main-chart', 'Loading download data...');
    },

    renderStackedBarChart(selector, data, title) {
        // Store current chart data and title for resize handling
        this.currentChartData = data;
        this.currentChartTitle = title;
        
        const container = d3.select(selector);
        const containerNode = container.node();
        const rect = containerNode.getBoundingClientRect();
        
        // Process data first to determine if legend is needed
        let timeSeries = [...data.time_series]; // Create a copy
        const datasets = data.top_datasets || [];
        
        // Add "OTHER" to datasets if it exists in the data
        const allDatasets = [...datasets];
        if (timeSeries.some(d => d.OTHER)) {
            allDatasets.push('OTHER');
        }
        
        // Set up dimensions - adjust right margin based on whether legend will be shown
        const showLegend = (AppState.selectedDataset === 'ALL' && allDatasets.length > 1) || 
                          (data.view_type === 'regions' && allDatasets.length > 1);
        const margin = { 
            top: 20, 
            right: showLegend ? 120 : 20, 
            bottom: 60, 
            left: 80 
        };
        const width = rect.width - margin.left - margin.right;
        const height = rect.height - margin.top - margin.bottom;

        // Create SVG
        const svg = container
            .append('svg')
            .attr('width', rect.width)
            .attr('height', rect.height);

        const chartArea = svg
            .append('g')
            .attr('transform', `translate(${margin.left},${margin.top})`);

        // Parse dates and prepare data
        const parseDate = d3.timeParse('%Y-%m-%d');
        timeSeries.forEach(d => {
            d.date = parseDate(d.date);
            d.total = allDatasets.reduce((sum, dataset) => sum + (d[dataset] || 0), 0);
        });

        // Filter out entries with invalid dates and sort by date
        timeSeries = timeSeries.filter(d => d.date !== null);
        timeSeries.sort((a, b) => a.date - b.date);

        // Filter datasets to only include those that have data in the time series
        // This ensures that when date filters are applied, only active datasets appear
        const activeDatasetsInTimeRange = new Set();
        timeSeries.forEach(d => {
            allDatasets.forEach(dataset => {
                if (d[dataset] && d[dataset] > 0) {
                    activeDatasetsInTimeRange.add(dataset);
                }
            });
        });

        // Update allDatasets to only include active ones
        const filteredDatasets = allDatasets.filter(dataset => activeDatasetsInTimeRange.has(dataset));
        
        // Recalculate totals with filtered datasets
        timeSeries.forEach(d => {
            d.total = filteredDatasets.reduce((sum, dataset) => sum + (d[dataset] || 0), 0);
        });

        // Apply cumulative transformation if enabled (use filtered datasets)
        if (AppState.isCumulative) {
            timeSeries = this.transformToCumulative(timeSeries, filteredDatasets);
        }

        // Set up scales - use band scale for better bar positioning
        const xScale = d3.scaleBand()
            .domain(timeSeries.map(d => d.date.getTime()))
            .range([0, width])
            .padding(0.1);

        const yScale = d3.scaleLinear()
            .domain([0, d3.max(timeSeries, d => d.total)])
            .range([height, 0]);

        // Color scale for datasets (use filtered datasets)
        const colorScale = d3.scaleOrdinal()
            .domain(filteredDatasets)
            .range(DATASET_COLORS);

        // Create stack generator (use filtered datasets)
        const stack = d3.stack()
            .keys(filteredDatasets)
            .value((d, key) => d[key] || 0);

        const stackedData = stack(timeSeries);

        chartArea.selectAll('.dataset-group')
            .data(stackedData)
            .enter()
            .append('g')
            .attr('class', 'dataset-group')
            .attr('fill', d => colorScale(d.key))
            .selectAll('rect')
            .data(d => d)
            .enter()
            .append('rect')
            .attr('x', d => xScale(d.data.date.getTime()))
            .attr('y', d => yScale(d[1]))
            .attr('height', d => yScale(d[0]) - yScale(d[1]))
            .attr('width', xScale.bandwidth())
            .style('opacity', 0.8)
            .on('mouseover', (event, d) => {
                const dataset = d3.select(event.target.parentNode).datum().key;
                const value = d[1] - d[0];
                
                this.tooltip.transition()
                    .duration(200)
                    .style('opacity', 0.9);
                
                this.tooltip.html(this.formatTooltip(d.data, dataset, value))
                    .style('left', (event.pageX + 10) + 'px')
                    .style('top', (event.pageY - 10) + 'px');
            })
            .on('mouseout', () => {
                this.tooltip.transition()
                    .duration(500)
                    .style('opacity', 0);
            });

        // Add axes - create time scale for axis display
        const xTimeScale = d3.scaleTime()
            .domain(d3.extent(timeSeries, d => d.date))
            .range([0, width]);

        const xAxis = d3.axisBottom(xTimeScale)
            .tickFormat(d3.timeFormat('%Y-%m'))
            .ticks(Math.min(6, timeSeries.length));

        const yAxis = d3.axisLeft(yScale)
            .tickFormat(d => Utils.formatBytes(d))
            .ticks(5);

        chartArea.append('g')
            .attr('class', 'axis')
            .attr('transform', `translate(0,${height})`)
            .call(xAxis)
            .selectAll('text')
            .style('text-anchor', 'end')
            .attr('dx', '-.8em')
            .attr('dy', '.15em')
            .attr('transform', 'rotate(-45)');

        chartArea.append('g')
            .attr('class', 'axis')
            .call(yAxis);

        // Add axis labels
        chartArea.append('text')
            .attr('class', 'axis-label')
            .attr('transform', 'rotate(-90)')
            .attr('y', 0 - margin.left)
            .attr('x', 0 - (height / 2))
            .attr('dy', '1em')
            .style('text-anchor', 'middle')
            .text('Download Volume');

        chartArea.append('text')
            .attr('class', 'axis-label')
            .attr('transform', `translate(${width / 2}, ${height + margin.bottom - 5})`)
            .style('text-anchor', 'middle')
            .text('Date');

        // Add legend when showing multiple datasets OR when showing regions for a specific dataset
        if (showLegend) {
            this.addLegend(svg, filteredDatasets, colorScale, rect.width - margin.right + 10, margin.top);
        }
        
        // Update featured dandisets with filtered data
        if (window.UI && typeof window.UI.updateFeaturedDandisetsFromChartData === 'function') {
            const regionName = data.region_code || data.region_name || AppState.selectedRegionName;
            // Create filtered data object with only active datasets
            const filteredData = {
                ...data,
                top_datasets: filteredDatasets.filter(d => d !== 'OTHER'), // Remove 'OTHER' from top datasets list
                dataset_totals: {}
            };
            
            // Calculate totals for filtered datasets from time series data
            filteredDatasets.forEach(dataset => {
                if (dataset !== 'OTHER') {
                    filteredData.dataset_totals[dataset] = 0;
                    timeSeries.forEach(d => {
                        filteredData.dataset_totals[dataset] += (d[dataset] || 0);
                    });
                }
            });
            
            window.UI.updateFeaturedDandisetsFromChartData(filteredData, regionName);
        }
    },

    addLegend(svg, datasets, colorScale, x, y) {
        const legend = svg.append('g')
            .attr('class', 'legend')
            .attr('transform', `translate(${x}, ${y})`);

        const legendItems = legend.selectAll('.legend-item')
            .data(datasets)
            .enter()
            .append('g')
            .attr('class', 'legend-item dataset-legend')
            .attr('transform', (d, i) => `translate(0, ${i * 20})`)
            .style('cursor', 'pointer');

        legendItems.append('rect')
            .attr('x', 0)
            .attr('y', 0)
            .attr('width', 12)
            .attr('height', 12)
            .attr('fill', d => colorScale(d))
            .style('opacity', 0.8);

        legendItems.append('text')
            .attr('x', 18)
            .attr('y', 9)
            .attr('dy', '0.35em')
            .style('font-size', '11px')
            .text(d => this.formatDatasetName(d));

        // Add legend interactivity
        legendItems.on('click', function(event, d) {
            const item = d3.select(this);
            const rect = item.select('rect');
            const text = item.select('text');
            
            const isActive = rect.style('opacity') == 0.8;
            
            if (isActive) {
                // Hide dataset
                rect.style('opacity', 0.2);
                text.style('opacity', 0.5);
                svg.selectAll('.dataset-group')
                    .filter(group => group.key === d)
                    .style('opacity', 0);
            } else {
                // Show dataset
                rect.style('opacity', 0.8);
                text.style('opacity', 1);
                svg.selectAll('.dataset-group')
                    .filter(group => group.key === d)
                    .style('opacity', 0.8);
            }
        });
    },

    formatTooltip(data, dataset, value) {
        const date = d3.timeFormat('%Y-%m-%d')(data.date);
        const formattedValue = Utils.formatBytes(value);
        const datasetName = this.formatDatasetName(dataset);
        
        const downloadLabel = AppState.isCumulative ? 'Cumulative Downloads:' : 'Downloads:';
        const totalLabel = AppState.isCumulative ? 'Total Cumulative:' : 'Total Day:';
        
        return `
            <div><strong>Date:</strong> ${date}</div>
            <div><strong>Dataset:</strong> ${datasetName}</div>
            <div><strong>${downloadLabel}</strong> ${formattedValue}</div>
            <div><strong>${totalLabel}</strong> ${Utils.formatBytes(data.total)}</div>
        `;
    },

    formatDatasetName(dataset) {
        if (dataset === 'OTHER') {
            // Check if we're in region view mode
            if (AppState.chartData && AppState.chartData.view_type === 'regions') {
                return 'Other Regions';
            } else {
                return 'Other Datasets';
            }
        }
        
        // Check if this is a region (contains '/')
        if (dataset.includes('/')) {
            // Format region name (e.g., "US/California" -> "California, US")
            const parts = dataset.split('/');
            if (parts.length === 2) {
                return `${parts[1]}, ${parts[0]}`;
            }
        }
        
        // Check if this is a numeric dandiset ID and zero-pad it
        if (/^\d+$/.test(dataset)) {
            return dataset.padStart(6, '0');
        }
        
        return dataset;
    },

    showEmptyChart(selector, message) {
        const container = d3.select(selector);
        const containerNode = container.node();
        const rect = containerNode.getBoundingClientRect();

        const emptyState = container
            .append('div')
            .attr('class', 'empty-state')
            .style('height', '100%')
            .style('display', 'flex')
            .style('align-items', 'center')
            .style('justify-content', 'center');

        emptyState.append('p')
            .style('color', '#6c757d')
            .style('font-style', 'italic')
            .text(message);
    },

    // Method to update chart based on time range (future enhancement)
    updateTimeRange(startDate, endDate) {
        // This could be implemented to filter charts by date range
        console.log('Time range update:', startDate, endDate);
    },

    // Method to highlight specific datasets
    highlightDatasets(datasetIds) {
        d3.selectAll('.dataset-group')
            .style('opacity', d => datasetIds.includes(d.key) ? 0.8 : 0.3);
    },

    // Method to reset all highlights
    resetHighlights() {
        d3.selectAll('.dataset-group')
            .style('opacity', 0.8);
    },

    // Transform data to cumulative values
    transformToCumulative(timeSeries, allDatasets) {
        const cumulativeData = [];
        const cumulativeTotals = {};
        
        // Initialize cumulative totals for each dataset
        allDatasets.forEach(dataset => {
            cumulativeTotals[dataset] = 0;
        });
        
        timeSeries.forEach(dataPoint => {
            const newDataPoint = { ...dataPoint };
            
            // Calculate cumulative sum for each dataset
            allDatasets.forEach(dataset => {
                cumulativeTotals[dataset] += (dataPoint[dataset] || 0);
                newDataPoint[dataset] = cumulativeTotals[dataset];
            });
            
            // Recalculate total as sum of cumulative values
            newDataPoint.total = allDatasets.reduce((sum, dataset) => sum + newDataPoint[dataset], 0);
            
            cumulativeData.push(newDataPoint);
        });
        
        return cumulativeData;
    },

    // Export chart as image (future enhancement)
    exportChart(selector, filename) {
        const svg = d3.select(selector).select('svg');
        const svgString = new XMLSerializer().serializeToString(svg.node());
        
        // This would require additional libraries for full implementation
        console.log('Export functionality would be implemented here');
    }
};

// Initialize charts when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
    ChartsVisualization.init();
});

// Export for global access
window.ChartsVisualization = ChartsVisualization;
