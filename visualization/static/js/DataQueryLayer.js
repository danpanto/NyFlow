import { BaseLayer } from "./BaseLayer.js";
import { DataQueryController } from "./DataQueryController.js";
import { queryService } from "./services/QueryService.js";
import { Gradient } from "./color.js";
import { GradientLegend } from "./components/GradientLegend.js";
import { RightDrawer } from "./components/RightDrawer.js";
import { filterService } from "./services/FilterService.js";
import { zoneData } from "./services/ZoneDataService.js";
import { ZoneInfo } from "./components/ZoneInfo.js";
import { VARIABLE_CONFIG } from "./queryVariables.js";

export class DataQueryLayer extends BaseLayer {
    constructor(mapManager, backend, variable) {
        super();
        this.variable = variable;
        this.mapManager = mapManager;
        
        this.unsubscribe = null;
        this.unsubscribeTime = null; 
        this.unsubscribeSelectZone = null;

        this.baseAppName = "NyFlow";
        this.data = null;
        this.timeSeriesData = null; 
        
        this.gradient = new Gradient('magma', 0);
        this.mapController = new DataQueryController(backend, this.gradient);
        mapManager.addLayer(this.variable, this.mapController);

        this.formatter = new Intl.NumberFormat('en-US', { 
            notation: "compact", 
            maximumFractionDigits: 1 
        });

        // Map legend
        this.legend = document.createElement("gradient-legend");
        this.legend.setAttribute("layer-id", this.variable);

        // Hover info for the map
        this.zoneInfoDiv = document.createElement("zone-info");
        this.controlPanel = document.createElement("control-panel");

        this.uiWrapper = document.createElement("div");
        this.uiWrapper.style.position = "absolute";
        this.uiWrapper.style.bottom = "20px";
        this.uiWrapper.style.right = "20px";
        this.uiWrapper.style.display = "flex";
        this.uiWrapper.style.gap = "20px"; 
        this.uiWrapper.style.alignItems = "flex-end";
        this.uiWrapper.style.zIndex = "1"; 

        this.zoneInfoDiv.style.position = "relative";
        this.zoneInfoDiv.style.inset = "auto";
        this.legend.style.position = "relative";
        this.legend.style.inset = "auto";

        this.uiWrapper.appendChild(this.zoneInfoDiv);
        this.uiWrapper.appendChild(this.legend);

        // Right Drawer for the Graph
        this.drawer = document.createElement("right-drawer");
        this.drawer.style.display = "none";
        
        this.drawerContent = document.createElement("div");
        this.drawerContent.style.display = "flex";
        this.drawerContent.style.flexDirection = "column";
        this.drawerContent.style.justifyContent = "center"; // Vertical centering
        this.drawerContent.style.alignItems = "center";     // Horizontal centering
        this.drawerContent.style.height = "100%";           // Necessary to center vertically
        this.drawerContent.style.width = "100%";            // Occupy full width of drawer
        this.drawerContent.style.gap = "20px";
        this.drawer.appendChild(this.drawerContent);

        this.chartInstance = null;
    }

    bind() {
        document.body.appendChild(this.drawer);
        document.body.appendChild(this.controlPanel);
        document.body.appendChild(this.uiWrapper);

        const config = VARIABLE_CONFIG[this.variable] || {};
        const layerName = config.longName || this.variable;
        document.title = `${this.baseAppName} • ${layerName}`;

        const layerTitleElement = document.querySelector('.layer-title-text');
        if (layerTitleElement) layerTitleElement.textContent = `• ${layerName}`; 

        this.mapManager.toggleLayer(this.variable, true);

        this.unsubscribeSelectZone = filterService.addListener("zones", () => { 
            this.onSelectedZone(filterService.lastZone, filterService.zones);
            this._updateDrawerSelections(); 
        });

        this.unsubscribe = queryService.addListener(this.variable, (data, loading) => {
            if (!loading && data) {
                this.mapController.update({ query: data });
                this.data = data;
                const { min, max, absoluteMax } = this.mapController.dataBounds;
                this.legend.update(this.gradient, min, max, absoluteMax);
                
                this.onSelectedZone(filterService.lastZone);
                this._updateDrawerSelections();
            }
        });

        this.unsubscribeTime = queryService.addListener(`agg-${this.variable}`, (data, loading) => {
            if (!loading && data) {
                this.timeSeriesData = data;
                this._updateDrawerSelections(); 
            }
        });

        this.onSelectedZone(filterService.lastZone);
        this._updateDrawerSelections();

        this.themeObserver = new MutationObserver(() => {
            if (this.chartInstance && this.timeSeriesData) {
                const selectedZones = Array.from(filterService.zones);
                this._renderLineChart(selectedZones, this.timeSeriesData);
            }
        });

        this.themeObserver.observe(document.documentElement, { attributes: true, attributeFilter: ['class'] });
    }

    unbind() {
        this.drawer.remove();
        this.controlPanel.remove();
        this.uiWrapper.remove();
        this.mapManager.toggleLayer(this.variable, false);

        const layerTitleElement = document.querySelector('.layer-title-text');
        if (layerTitleElement) layerTitleElement.textContent = ""; 

        if (this.unsubscribe) {
            this.unsubscribe();
            this.unsubscribe = null;
        }
        if (this.unsubscribeTime) {
            this.unsubscribeTime();
            this.unsubscribeTime = null;
        }
        if (this.unsubscribeSelectZone) {
            this.unsubscribeSelectZone();
            this.unsubscribeSelectZone = null;
        }

        if (this.chartInstance) {
            echarts.dispose(this.chartInstance);
            this.chartInstance = null;
        }

        document.title = this.baseAppName;
    }

    // Handles the small hover card on the main map
    onSelectedZone(zone) {
        if(!zone || !this.data) {
            this.zoneInfoDiv.visible = false;
            return;
        }
        const lastZoneData = this.data[zone]; 
        if(lastZoneData === undefined || lastZoneData === null) {
            this.zoneInfoDiv.visible = false;
            return;
        }
        this.zoneInfoDiv.visible = true;
        const name = zoneData.getName(zone);
        const config = VARIABLE_CONFIG[this.variable] || {};
        let formattedValue = config.formatter ? config.formatter(lastZoneData) : lastZoneData;
        if (config.units && config.units !== "USD") formattedValue = `${formattedValue} ${config.units}`;
        
        this.zoneInfoDiv.heading = name;
        this.zoneInfoDiv.data = { "Zone ID": zone, [config.shortName || "Value"]: formattedValue };
    }

    _updateDrawerSelections() {
        const selectedZones = Array.from(filterService.zones);

        if (!selectedZones || selectedZones.length === 0) {
            if (this.drawer.isOpen) {
                this.drawer.close();
            }
            this.drawer.style.display = "none";

            if (this.chartInstance) {
                this.chartInstance.setOption({ series: [] }, true);
            }
            return;
        }

        this.drawer.style.display = ""; 
        this.drawer.setAttribute('title', `${selectedZones.length} Selected`);

        if (this.drawer.isOpen) {
            this.drawer.open();
        }

        if (this.timeSeriesData) {
            this._renderLineChart(selectedZones, this.timeSeriesData);
        }
    }

    _renderLineChart(selectedZones, timeData) {
        let chartContainer = this.drawerContent.querySelector('.chart-container');

        if (!chartContainer) {
            this.drawerContent.innerHTML = ''; // Only clear once on first load
            chartContainer = document.createElement('div');
            chartContainer.className = 'chart-container';
            chartContainer.style.width = '100%'; 
            chartContainer.style.height = '600px'; 
            this.drawerContent.appendChild(chartContainer);
        }

        // 2. Initialize instance ONLY if it doesn't exist
        if (!this.chartInstance) {
            this.chartInstance = echarts.init(chartContainer);
        }

        const config = VARIABLE_CONFIG[this.variable] || {};

        const timestamps = Object.keys(timeData).sort();
        const totalMs = timestamps.length > 1 
            ? new Date(timestamps[timestamps.length-1]).getTime() - new Date(timestamps[0]).getTime() 
            : 0;
        const totalDays = totalMs / (1000 * 60 * 60 * 24);

        const xAxisData = timestamps.map(ts => {
            const d = new Date(ts);
            const options = { month: 'short', day: 'numeric' };
            if (totalDays > 365) {
                options.year = 'numeric'; 
            } else if (totalDays <= 7) {
                options.hour = 'numeric';
            }
            return d.toLocaleString(undefined, options);
        });

        const seriesData = selectedZones.map(zoneId => {
            const name = zoneData.getName(zoneId);
            const dataPoints = timestamps.map(ts => timeData[ts][zoneId] || 0);

            return {
                name: name,
                type: 'line',
                smooth: true,      
                showSymbol: false, 
                data: dataPoints,
                lineStyle: { width: 2.5 } 
            };
        });

        let groupLabel = "";
        if (timestamps.length > 1) {
            const diffMs = new Date(timestamps[1]).getTime() - new Date(timestamps[0]).getTime();
            const diffHours = diffMs / (1000 * 60 * 60);

            if (diffHours <= 1.5) {
                groupLabel = " (Grouped by Hour)";
            } else if (diffHours > 1.5 && diffHours <= 25) {
                groupLabel = " (Grouped by Day)";
            } else if (diffHours > 25) {
                groupLabel = " (Grouped by 7 Days)";
            }
        }

        const style = getComputedStyle(document.documentElement);
        const textColor = style.getPropertyValue('--app-text').trim() || '#333';
        const titleColor = style.getPropertyValue('--app-text-title').trim() || '#000';
        const gridColor = style.getPropertyValue('--app-bg-secondary').trim() || '#ccc';

        const option = {
            title: {
                text: `${config.longName || this.variable}${groupLabel}`,
                left: 'center',
                top: 0,
                textStyle: {
                    color: titleColor,
                    fontSize: 16,
                    fontWeight: '600'
                }
            },
            tooltip: {
                trigger: 'axis',
                backgroundColor: 'rgba(30, 30, 30, 0.95)', 
                borderColor: '#444',
                padding: 12,

                // Custom CSS Grid formatter for the Tooltip
                formatter: (params) => {
                    let html = `<div style="margin-bottom: 10px; font-weight: bold; font-size: 14px; color: #fff; border-bottom: 1px solid #555; padding-bottom: 4px;">${params[0].axisValue}</div>`;
                    html += `<div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: 6px 20px; align-items: center;">`;

                    params.forEach(p => {
                        let val = config.formatter ? config.formatter(p.value) : p.value;
                        let displayVal = config.units && config.units !== "USD" ? `${val} ${config.units}` : val;
                        html += `
                            <div style="display: flex; align-items: center; justify-content: space-between; font-size: 13px;">
                                <div style="display: flex; align-items: center; overflow: hidden; white-space: nowrap; text-overflow: ellipsis; max-width: 140px;">
                                    <span style="display:inline-block; margin-right:6px; border-radius:50%; width:8px; height:8px; background-color:${p.color}"></span>
                                    <span style="color: #ccc;">${p.seriesName}</span>
                                </div>
                                <strong style="color: #fff; margin-left: 12px;">${displayVal}</strong>
                            </div>`;
                    });
                    html += `</div>`;
                    return html;
                }

            }, 
            legend: {
                type: 'plain', // Removes scroll, enables wrapping
                bottom: 0, 
                width: '95%',  // Constrain width to force items into a grid-like wrap
                icon: 'roundRect', 
                itemGap: 15,
                textStyle: { color: textColor, fontSize: 12 }
            },
            grid: {
                left: '2%',
                right: '8%',
                bottom: '100px', // Extra bottom space for the multi-line grid legend
                top: '50px',    
                containLabel: true
            },
            xAxis: {
                type: 'category',
                boundaryGap: false,
                data: xAxisData,
                axisLabel: { color: textColor }, // DYNAMIC
                axisLine: { lineStyle: { color: gridColor } }, // DYNAMIC
                name: 'Time',
                nameLocation: 'middle',
                nameGap: 35, // Pushes the title down below the axis labels
                nameTextStyle: {
                    color: textColor,
                    fontSize: 13,
                    fontWeight: 'bold'
                },
            },
            yAxis: {
                type: 'value',
                position: 'right',
                name: config.units ? `Value (${config.units})` : 'Value',
                nameLocation: 'middle',
                nameGap: 50,
                nameTextStyle: {
                    color: textColor,
                    fontSize: 13,
                    fontWeight: 'bold'
                },
                axisLabel: {
                    color: textColor,
                    formatter: (val) => this.formatter.format(val) 
                },
                splitLine: { 
                    lineStyle: { color: gridColor }
                }
            },
            series: seriesData
        };

        this.chartInstance.setOption(option, true);

        window.addEventListener('resize', () => {
            if (this.chartInstance) this.chartInstance.resize();
        });
    }
}
