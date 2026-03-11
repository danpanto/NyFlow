import { DataQueryLayer } from "./DataQueryLayer.js";
import { filterService } from "./services/FilterService.js";

export class LandmarkLayer extends DataQueryLayer {
    constructor(mapManager, backend, tipLayer) {
        super(mapManager, backend, "landmarks");
        this.tipLayer = tipLayer;
        this.pointsLayerGroup = L.featureGroup();
        this.pointsData = [];
    }

    async bind() {
        this.tipLayer.bind();
        const layerTitleElement = document.querySelector('.layer-title-text');
        if (layerTitleElement) {
            layerTitleElement.textContent = `• Touristic Landmarks`;
        }
        document.title = `${this.baseAppName} • Landmarks`;

        this.pointsLayerGroup.addTo(this.mapManager.map);

        await this._loadStaticData();

        this.unsubscribeSelectZone = filterService.addListener("zones", () => {
            this.tipLayer.onSelectedZone(filterService.lastZone);
        });
    }

    async _loadStaticData() {
        try {
            // Load points
            const responsePoints = await fetch('/api/landmark-points', {
                method: 'POST'
            });
            const resultPoints = await responsePoints.json();

            if (resultPoints.status === "ok") {
                this.pointsData = resultPoints.data;
                this._renderPoints();
            }
        } catch (e) {
            console.error("Error loading restaurant data", e);
        }
    }

    _renderPoints() {
        this.pointsLayerGroup.clearLayers();

        for (const pt of this.pointsData) {
            if (pt.lat == null || pt.lng == null) continue;

            const landmarkColor = "#FFD700";

            const marker = L.circleMarker([pt.lat, pt.lng], {
                radius: 6,
                fillColor: landmarkColor,
                color: "#000",
                weight: 1,
                opacity: 1,
                fillOpacity: 0.8
            });

            marker.bindPopup(`
                <div style="font-family: sans-serif;">
                    <strong style="font-size: 14px;">${pt.name}</strong><br>
                    <span style="color: #666;">Historical Landmark</span><br>
                </div>
            `);

            marker.addTo(this.pointsLayerGroup);
        }
    }

    unbind() {
        this.tipLayer.unbind();
        this.pointsLayerGroup.removeFrom(this.mapManager.map);
        if (this.unsubscribeSelectZone) this.unsubscribeSelectZone();
        document.title = this.baseAppName;
    }
}
