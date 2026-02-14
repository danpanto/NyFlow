import { themeService } from "./services/ThemeService.js";

export class MapManager {
    constructor(domElement, mapConfig = {}) {

    const defaults = {
            zoomControl: false,
            lat: 40.70,
            lon: -73.95,
            zoom: 13,
            minZoom: 11,
            maxZoom: 18,
            minLat: 40.35,
            maxLat: 41.05,
            minLng: -74.95,
            maxLng: -73.00
        };

        const config = { ...defaults, ...mapConfig };

        const leafletOptions = {
            zoomControl: config.zoomControl,
            center: [config.lat, config.lon],
            zoom: config.zoom,
            minZoom: config.minZoom,
            maxZoom: config.maxZoom,
            maxBounds: [
                [config.minLat, config.minLng], 
                [config.maxLat, config.maxLng]
            ],
            maxBoundsViscosity: 1.0 
        };

        this.map = L.map(domElement, leafletOptions);
        this.map.on("click", (e) => this._onMapClick(e));

        this.tileProviders = {
            "light": {
                url: 'https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png',
                attribution: '&copy; CartoDB'
            },
            "dark": {
                url: 'https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png',
                attribution: '&copy; CartoDB'
            },
        };

        this.layers = new Map();

        this.tileLayer = null;
        this._theme = null;
        themeService.addListener((theme) => this.setTheme(theme));
    }

    setTheme(theme) {
        let selectedTheme = theme;
        if (!this.tileProviders[selectedTheme]) {
            console.warn(`Invalid map theme '${theme}', defaulting to light theme.`)
            selectedTheme = "light";
        }

        if(this._theme == selectedTheme) return;
        this._theme = selectedTheme;

        // Change tile provider
        if (this.tileLayer) this.map.removeLayer(this.tileLayer);
        const provider = this.tileProviders[selectedTheme];
        this.tileLayer = L.tileLayer(provider.url, { attribution: provider.attribution });
        this.map.addLayer(this.tileLayer);

        // Notify all layers about the theme change
        this.layers.forEach(layer => layer.update({ theme: selectedTheme }));
    }

    _onMapClick(e) {
        this.layers.forEach(layer => {
            layer.update({ 
                click: {
                    lat: e.latlng["lat"],
                    lng: e.latlng["lng"],
                    event: e
                }
            });
        });
    }

    addLayer(id, layerInstance) {
        if (this.layers.has(id)) {
            console.warn(`Layer ${id} already exists. Removing old one.`);
            this.removeLayer(id);
        }

        this.layers.set(id, layerInstance);
        layerInstance.mount(this.map);
    }

    toggleLayer(id, visibility = null) {
        const layer = this.layers.get(id);
        if(!layer) {
            console.warn(`Layer ${id} doesn't exists. Can't toggle visibility.`);
            return;
        }

        const visible = (visibility !== null) ? visibility : !layer.visible;
        layer.update({ visible : visible });
    }

    removeLayer(id) {
        const layer = this.layers.get(id);
        if (layer) {
            layer.unmount(); 
            this.layers.delete(id);
        } else {
            console.warn(`Layer ${id} doesn't exists. Can't be removed.`);
        }
    }

    updateLayer(id, config) {
        const layer = this.layers.get(id);
        if (!layer) {
            console.warn(`Layer ${id} doesn't exists. Can't be updated.`);
            return;
        }

        layer.update(config);
    }

    setView(lat, lng, zoom) {
        this.map.flyTo([lat, lng], zoom);
    }
}
