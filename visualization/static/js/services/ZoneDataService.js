export class ZoneData {
    constructor() {
        if (ZoneData.instance) {
            return ZoneData.instance;
        }
        ZoneData.instance = this;

        this.geoJson = null;
        this.lookupMap = new Map();
        this.boroughs = new Set();

        this.loadingPromise = this.load(); 
    }

    async load() {
        if (this.geoJson) return this.geoJson;

        if (this.loadingPromise) return this.loadingPromise;

        this.loadingPromise = (async () => {
            try {
                const response = await fetch('/api/taxi_zones');
                const data = await response.json();
                
                this.geoJson = data;
                
                data.features.forEach(f => {
                   this.lookupMap.set(String(f.properties.locationid), f.properties);
                   if (f.properties.borough) this.boroughs.add(f.properties.borough);
                });

                return this.geoJson;
            } catch (e) {
                console.error("Failed to load zones", e);
                throw e;
            }
        })();

        return this.loadingPromise;
    }

    getGeoJson() {
        return this.geoJson;
    }

    getFeature(id) {
        return this.lookupMap.get(String(id));
    }

    getName(id) {
        const props = this.lookupMap.get(String(id));
        return props ? props.zone : null;
    }

    getBorough(id) {
        const props = this.lookupMap.get(String(id));
        return props ? props.borough : null;
    }

    getAllBoroughs() {
        return Array.from(this.boroughs).sort();
    }

    get isReady() {
        return !!this.geoJson;
    }
}

export const zoneData = new ZoneData();
