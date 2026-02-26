import { LayerInterface } from "./LayerInterface.js";
import { filterService } from "./services/FilterService.js";

delete L.Icon.Default.prototype._getIconUrl;

L.Icon.Default.mergeOptions({
    iconRetinaUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/images/marker-icon-2x.png',
    iconUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/images/marker-icon.png',
    shadowUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/images/marker-shadow.png',
});

export class RouteController extends LayerInterface {
    constructor() {
        super();
        this._visible = false;
        this.routingControl = null;
        this.map = null;
        this.lastClickPos = null; // Store last clicked position

        this._onFilterChange = this._onFilterChange.bind(this);
        filterService.addListener("date", this._onFilterChange);
        filterService.addListener("vendors", this._onFilterChange);
        filterService.addListener("zones", this._onFilterChange);
    }

    async _onFilterChange() {
        if (!this._visible) return;
        await this.fetchAndDrawRoute();
    }

    async fetchAndDrawRoute() {
        const payload = {
            date: filterService.dateRange || { min: "2021-01-01T00:00:00", max: "2025-12-31T23:00:00" },
            vendors: Array.from(filterService.vendors).sort(),
            zones: Array.from(filterService.zones).sort((a, b) => a - b),
            variables: ["route"],
            click_pos: this.lastClickPos ? { lat: this.lastClickPos.lat, lng: this.lastClickPos.lng } : null
        };

        try {
            console.log("Fetching route with payload:", payload);
            const response = await fetch('/api/route', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });

            const result = await response.json();

            if (result.status === "ok" && result.data) {
                console.log("Route received from backend:", result.data);
                const waypoints = result.data.map(coord => L.latLng(coord[0], coord[1]));
                if (this.routingControl) {
                    this.routingControl.setWaypoints(waypoints);
                }
            } else {
                console.error("API Error fetching route:", result.msg);
            }
        } catch (err) {
            console.error("Fetch route failed:", err);
        }
    }

    mount(map) {
        this.map = map;

        this.routingControl = L.Routing.control({
            waypoints: [],
            routeWhileDragging: false,
            showAlternatives: true,
            show: true,
            router: L.Routing.osrmv1({
                serviceUrl: 'https://router.project-osrm.org/route/v1',
                profile: 'driving',
                routingOptions: {
                    alternatives: true
                }
            })
        });

        // Register event listener once
        this.routingControl.on('routesfound', (e) => {
            const routes = e.routes;
            const mainRoute = routes[0];
            console.log(`Route visualization: Found ${routes.length} paths. Main path distance: ${mainRoute.summary.totalDistance / 1000} km`);
        });

        this.routingControl.on('routingerror', (e) => {
            console.error("OSRM Routing Error:", e.error);
        });

        if (this._visible) {
            this.routingControl.addTo(this.map);
            this.fetchAndDrawRoute();
        }
    }

    unmount() {
        if (this.routingControl && this.map) {
            this.map.removeControl(this.routingControl);
        }
        this.map = null;
    }

    update(config) {
        // Handle map clicks
        if (config.click !== undefined && this._visible) {
            this.lastClickPos = config.click;
            this.fetchAndDrawRoute();
            return;
        }

        if (config.visible !== undefined) {
            this._visible = config.visible;
            if (!this.map || !this.routingControl) return;

            if (this._visible) {
                this.routingControl.addTo(this.map);
                this.fetchAndDrawRoute();
            } else {
                this.map.removeControl(this.routingControl);
            }
        }
    }

    get visible() {
        return this._visible;
    }
}
