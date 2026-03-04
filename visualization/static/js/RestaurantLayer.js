import { DataQueryLayer } from "./DataQueryLayer.js";
import { filterService } from "./services/FilterService.js";

export class RestaurantRatingsLayer extends DataQueryLayer {
    constructor(mapManager, backend) {
        // Pass a dummy variable name for config purposes if needed, or register 'restaurant_ratings' in queryVariables.js
        super(mapManager, backend, "restaurant_ratings");
        this.pointsLayerGroup = L.markerClusterGroup({
            chunkedLoading: true,
            maxClusterRadius: (zoom) => {
                if (zoom <= 13) return 80;
                if (zoom <= 15) return 50;
                return 40;
            },
            iconCreateFunction: (cluster) => {
                const count = cluster.getChildCount();
                let size = 'small';
                if (count > 20) size = 'large';
                else if (count > 5) size = 'medium';

                return L.divIcon({
                    html: `<div><span>${count}</span></div>`,
                    className: `marker-cluster marker-cluster-${size}`,
                    iconSize: L.point(40, 40)
                });
            }
        });
        this.pointsData = [];
    }

    async bind() {
        document.body.appendChild(this.drawer);
        document.body.appendChild(this.uiWrapper);

        this.mapManager.toggleLayer(this.variable, true);
        this.pointsLayerGroup.addTo(this.mapManager.map);

        await this._loadStaticData();

        this.unsubscribeSelectZone = filterService.addListener("zones", () => {
            this.onSelectedZone(filterService.lastZone);
        });
    }

    async _loadStaticData() {
        try {
            // Load ratings for zones
            const responseRatings = await fetch('/api/restaurant-ratings', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ variables: [], vendors: [], date: { min: "2021-01-01T00:00:00", max: "2025-12-31T23:00:00" } })
            });
            const resultRatings = await responseRatings.json();

            if (resultRatings.status === "ok") {
                this.data = resultRatings.data;
                this.mapController.update({ query: this.data });
                const { min, max, absoluteMax } = this.mapController.dataBounds;
                this.legend.update(this.gradient, min, max, absoluteMax);
            }

            // Load points
            const responsePoints = await fetch('/api/restaurant-points', {
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
        let min, max;
        if (this.mapController && this.mapController.dataBounds) {
            min = this.mapController.dataBounds.min;
            max = this.mapController.dataBounds.max;
        }

        if (max === undefined || max === null || max === min) {
            min = 0; max = 100; // Fallback if data bounds are not well-defined
        }

        const markers = [];

        for (const pt of this.pointsData) {
            if (pt.lat == null || pt.lng == null) continue;

            let color = '#777';
            if (pt.score != null && max !== min) {
                const p = Math.max(0, Math.min(1, (pt.score - min) / (max - min)));
                color = this.gradient.get(p);
            } else if (pt.score != null) {
                color = this.gradient.get(0.5);
            }

            const marker = L.circleMarker([pt.lat, pt.lng], {
                radius: 4,
                fillColor: color,
                color: "#000",
                weight: 1,
                opacity: 1,
                fillOpacity: 0.8
            });

            marker.bindPopup(`<b>${pt.name}</b><br>Score: ${pt.score != null ? pt.score.toFixed(1) : 'N/A'}`);
            markers.push(marker);
        }

        this.pointsLayerGroup.addLayers(markers);
    }

    unbind() {
        this.drawer.remove();
        this.uiWrapper.remove();
        this.mapManager.toggleLayer(this.variable, false);
        this.pointsLayerGroup.removeFrom(this.mapManager.map);
        if (this.unsubscribeSelectZone) this.unsubscribeSelectZone();
    }
}
