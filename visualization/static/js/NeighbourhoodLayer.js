import { BaseLayer } from "./BaseLayer.js";
import { NeighbourhoodZoneController } from "./NeighbourhoodZoneController.js";
import { filterService } from "./services/FilterService.js";
import { ZoneInfo } from "./components/ZoneInfo.js";
import { zoneData } from "./services/ZoneDataService.js";

export class NeighbourhoodLayer extends BaseLayer {
    constructor(mapManager, backend) {
        super();

        this.name = "neighbourhoods";
        this.mapManager = mapManager;

        // Add map layer
        const mapController = new NeighbourhoodZoneController(backend);
        mapManager.addLayer(this.name, mapController);

        this.zoneInfoDiv = document.createElement("zone-info");

        this.unsubscribeSelectZone = null;
    }

    bind() {
        document.body.appendChild(this.zoneInfoDiv);
        this.unsubscribeSelectZone = filterService.addListener("zones", (_) => { this.onSelectedZone(filterService.lastZone); })
        filterService.selectZone(filterService.lastZone, true, true);

        const layerTitleElement = document.querySelector('.layer-title-text');
        if (layerTitleElement) {
            layerTitleElement.textContent = `• Neighbourhoods`; // Uses a bullet/dot
        }

        this.mapManager.toggleLayer(this.name, true);
        this.onSelectedZone(filterService.lastZone);
    }

    unbind() {
        this.mapManager.toggleLayer(this.name, false);
        this.zoneInfoDiv.remove();
        if(this.unsubscribeSelectZone) {
            this.unsubscribeSelectZone();
            this.unsubscribeSelectZone = null;
        }

        const layerTitleElement = document.querySelector('.layer-title-text');
        if (layerTitleElement) {
            layerTitleElement.textContent = ""; 
        }
    }

    onSelectedZone(zone) {
        if(!zone) {
            this.zoneInfoDiv.visible = false;
            return;
        }
        
        this.zoneInfoDiv.visible = true;

        const name = zoneData.getName(zone);
        const borough = zoneData.getBorough(zone);

        this.zoneInfoDiv.heading = name;
        this.zoneInfoDiv.data = {
            "Zone ID": zone,
            "Borough": borough
        };
    }
}
