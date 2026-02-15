import { BaseLayer } from "./BaseLayer.js";
import { NeighbourhoodZoneController } from "./NeighbourhoodZoneController.js";
import { filterService } from "./services/FilterService.js";
import { ZoneInfo } from "./components/ZoneInfo.js";

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

        this.mapManager.toggleLayer(this.name, true);
    }

    unbind() {
        this.mapManager.toggleLayer(this.name, false);
        this.zoneInfoDiv.remove();
        if(this.unsubscribeSelectZone) {
            this.unsubscribeSelectZone();
            this.unsubscribeSelectZone = null;
        }
    }

    onSelectedZone(zone) {
        this.zoneInfoDiv.zone = zone;
    }
}
