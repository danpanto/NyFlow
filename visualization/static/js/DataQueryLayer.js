import { BaseLayer } from "./BaseLayer.js";
import { DataQueryController } from "./DataQueryController.js";
import { queryService } from "./services/QueryService.js";
import { Gradient } from "./color.js";
import { GradientLegend } from "./components/GradientLegend.js";
import { filterService } from "./services/FilterService.js";
import { zoneData } from "./services/ZoneDataService.js";
import { ZoneInfo } from "./components/ZoneInfo.js";

export class DataQueryLayer extends BaseLayer {
    constructor(mapManager, backend, variable) {
        super();

        this.variable = variable;
        this.mapManager = mapManager;
        this.unsubscribe = null;
        this.unsubscribeSelectZone = null;

        this.baseAppName = "NyFlow";

        this.data = null;
        this.gradient = new Gradient('magma', 12);
        this.mapController = new DataQueryController(backend, this.gradient);

        mapManager.addLayer(this.variable, this.mapController);

        this.formatter = new Intl.NumberFormat('en-US', { 
            notation: "compact", 
            maximumFractionDigits: 1 
        });

        this.legend = document.createElement("gradient-legend");
        this.legend.setAttribute("title", this.variable);

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
    }

    bind() {
        document.body.appendChild(this.controlPanel);
        document.body.appendChild(this.uiWrapper);

        const layerTitleElement = document.querySelector('.layer-title-text');
        if (layerTitleElement) {
            layerTitleElement.textContent = `• ${this.variable}`; // Uses a bullet/dot
        }

        document.title = `${this.baseAppName} • ${this.variable}`;


        this.mapManager.toggleLayer(this.variable, true);

        this.unsubscribeSelectZone = filterService.addListener("zones", (_) => { this.onSelectedZone(filterService.lastZone); })
        this.unsubscribe = queryService.addListener(this.variable, (data, loading) => {
            if (!loading && data) {
                this.mapController.update({ query: data });
                this.data = data;
                const { min, max, absoluteMax } = this.mapController.dataBounds;
                this.legend.update(this.gradient, min, max, absoluteMax);
                this.onSelectedZone(filterService.lastZone);
            }
        });

        this.onSelectedZone(filterService.lastZone);
    }

    unbind() {
        this.controlPanel.remove();
        this.uiWrapper.remove();
        this.mapManager.toggleLayer(this.variable, false);

        const layerTitleElement = document.querySelector('.layer-title-text');
        if (layerTitleElement) {
            layerTitleElement.textContent = ""; 
        }

        if (this.unsubscribe) {
            this.unsubscribe();
            this.unsubscribe = null;
        }

        if (this.unsubscribeSelectZone) {
            this.unsubscribeSelectZone();
            this.unsubscribeSelectZone = null;
        }

        document.title = this.baseAppName;
    }

    onSelectedZone(zone) {
        const lastZoneData = this.data[zone];
        if(!zone || !lastZoneData) {
            this.zoneInfoDiv.visible = false;
            return;
        }
        
        this.zoneInfoDiv.visible = true;
        
        const name = zoneData.getName(zone);
        const value = this.formatter.format(lastZoneData);
        
        this.zoneInfoDiv.heading = name;
        this.zoneInfoDiv.data = {
            "Zone ID": zone,
            "Value": value,
        };
    }
}
