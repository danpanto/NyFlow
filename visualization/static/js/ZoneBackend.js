import { zoneData } from "./services/ZoneDataService.js";

export class ZoneBackend {
    constructor() {
        this.layer = null;
        this.map = null;
        this.activeController = null;
        this._initPromise = null;
    }

    async bind(map, controller) {
        this.map = map;
        this.activeController = controller;

        if (!this.layer) {
            await this._initLayer();
        }

        this.refresh();

        if (!this.map.hasLayer(this.layer)) {
            this.layer.addTo(this.map);
        }
    }

    unbind() {
        if (this.layer) {
            this.layer.remove();
        }
        this.activeController = null;
        this.map = null;
        this.refresh();
    }

    refresh() {
        if (!this.layer) return;

        if (this.activeController) {
            this.layer.setStyle((feature) => this.activeController.getStyle(this._getId(feature)));
            this.layer.eachLayer((l) => {
                this.layer.resetStyle(l);
            });
        } else {
            this.layer.setStyle(() => ({ 
                opacity: 0, 
                fillOpacity: 0, 
                interactive: false
            }));
        }
    }

    _getId(feature) { return feature.properties.locationid; }

    async _initLayer() {
        if (this._initPromise) return this._initPromise;

        this._initPromise = (async () => {
            await zoneData.load();

            const geoJson = zoneData.getGeoJson();

            this.layer = L.geoJson(geoJson, {
                style: (feature) => {
                    return this.activeController 
                        ? this.activeController.getStyle(this._getId(feature))
                        : { opacity: 0 };
                },
                onEachFeature: (feature, layer) => {
                    layer.on('click', (e) => {
                        L.DomEvent.stopPropagation(e);
                        this.activeController?.onClick(e, this._getId(feature));
                    });
                    layer.on('mouseover', (e) => this.activeController?.onHover(e, this._getId(feature)));
                    layer.on('mouseout', (e) => this.activeController?.onUnhover(e, this._getId(feature)));
                }
            });
        })();

        return this._initPromise;
    }
}
