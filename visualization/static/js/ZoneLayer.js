import { ZoneData } from "./ZoneData.js";

export class ZoneLayer {
    constructor(map) {
        this.zoneData = new ZoneData();
        this.layer = null;
        this.map = map;
        this.visible = false;

        this.clickCallback = null;
        this.clickDestroyer = null;

        this.hoverCallback = null;
        this.hoverDestroyer = null;

        this.unhoverCallback = null;
        this.unhoverDestroyer = null;

        this.styleCallback = null;
        this.styleDestroyer = null;

        this.init();
    }

    async init() {
        if(this.layer) return;

        const geoJson = await this.zoneData.getGeoJson();
        this.layer = L.geoJson(geoJson, {
            style: (feature) => {
                if (this.styleCallback) return this.styleCallback(feature);
                return { fillColor: '#3388ff', weight: 1, color: 'white', fillOpacity: 0.1 };
            },
            onEachFeature: (feature, layer) => {
                layer.on('click', (e) => {
                    if(this.clickCallback) {

                    }
                });

                layer.on('mouseover', (e) => {
                    if(this.hoverCallback) {

                    }
                });

                layer.on('mouseout', (e) => {
                    if(this.unhoverCallback) {

                    }
                });
            },
        });

        if(this.visible) {
            this.layer.addTo(this.map);
        }
    }

    hide() {
        this.visible = false;
        if (this.layer) {
            this.layer.remove();
        }
    }

    show() {
        this.visible = true;
        if (this.layer) {
            this.layer.addTo(this.map);
        }
    }

    toggle() {
        if(this.visible) this.hide();
        else this.show();
    }

    setClickEvent(callback, destroyer = null) {
        if(this.clickDestroyer) this.clickDestroyer();
        this.clickDestroyer = destroyer;
        this.clickCallback = callback;
    }

    setHoverEvent(callback, destroyer = null) {
        if(this.hoverDestroyer) this.hoverDestroyer();
        this.hoverDestroyer = destroyer;
        this.hoverCallback = callback;
    }

    setUnhoverEvent(callback, destroyer = null) {
        if(this.unhoverDestroyer) this.unhoverDestroyer();
        this.unhoverDestroyer = destroyer;
        this.unhoverCallback = callback;
    }

    setStyle(callback, destroyer = null) {
        if (this.styleDestroyer) this.styleDestroyer();

        this.styleCallback = callback;
        this.styleDestroyer = destroyer;

        if (this.layer) {
            this.layer.setStyle(callback);
        }
    }
}
