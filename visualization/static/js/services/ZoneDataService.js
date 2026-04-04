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
                const [zonesResponse, distancesResponse] = await Promise.all([
                    fetch('/api/taxi_zones'),
                    fetch('/api/zone-distances')
                ]);
                if (!zonesResponse.ok) throw new Error(`Error en zones: ${zonesResponse.status}`);
                if (!distancesResponse.ok) throw new Error(`Error en distances: ${distancesResponse.status}`);

                const [zonesData, distancesData] = await Promise.all([
                    zonesResponse.json(),
                    distancesResponse.json()
                ]);

                this.distances = distancesData;
                
                this.geoJson = zonesData;
                
                zonesData.features.forEach(f => {
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

    getIds() {
        return Array.from(this.lookupMap.keys());
    }

    getDistance(idA, idB) {
        const a = String(idA);
        const b = String(idB);

        if (a === b) return 0;

        const dist = this.distances[a]?.[b] ?? this.distances[b]?.[a];

        if (dist === undefined) {
            console.warn(`Distancia no encontrada entre ${a} y ${b}`);
            return null; 
        }

        return dist;
    }

    get isReady() {
        return !!this.geoJson;
    }
}

export const zoneData = new ZoneData();
