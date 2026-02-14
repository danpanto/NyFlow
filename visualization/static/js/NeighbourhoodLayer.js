import { BaseLayer } from "./BaseLayer.js";
import { NeighbourhoodZoneController } from "./NeighbourhoodZoneController.js";
import { filterService } from "./services/FilterService.js";
import { ZoneInfo } from "./ZoneInfo.js";

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
        this.mapManager.toggleLayer(this.name, true);
        document.body.appendChild(this.zoneInfoDiv);
        this.unsubscribeSelectZone = filterService.addListener("zones", (_) => { this.onSelectedZone(filterService.lastZone); })
    }

    unbind() {
        this.mapManager.toggleLayer(this.name, false);
        this.zoneInfoDiv.remove();

        if(this.unsubscribeSelectZone()) {
            this.unsubscribeSelectZone();
            this.unsubscribeSelectZone = null;
        }
    }

    onSelectedZone(zone) {
        console.log(zone);
        this.zoneInfoDiv.zone = zone;
    }
}
