class FilterService extends EventTarget {

    _sendEvent(name, value) {
        this.dispatchEvent(new CustomEvent(`filter-${name}`, { 
            detail: { value: value }
        }));
    }

    addListener(name, callback) {
        const internalWrapper = (e) => callback(e.detail.value);
        this.addEventListener(`filter-${name}`, internalWrapper);

        // Callback to unsubcribe
        return () => this.removeEventListener(`filter-${name}`, internalWrapper);
    }

    constructor() {
        super();
        this._lastZone = null;
        this._zones = new Set();
        this._layer = null;
    }

    selectLayer(layer) {
        this._layer = layer;
        this._sendEvent("layer", layer);
    }

    selectZone(zone, select=true, unique=false) {
        let changed = false;

        if (unique) {
            // If it's already the ONLY selected zone, we don't need to clear and trigger a repaint
            const isAlreadyOnlySelection = this._zones.size === 1 && this._zones.has(zone) && select;

            if (this._zones.size > 0 && !isAlreadyOnlySelection) {
                this._zones.clear();
                changed = true;
            }
        }

        if (select) {
            if (zone !== null && !this._zones.has(zone)) {
                this._zones.add(zone);
                changed = true;
            }
        } else {
            if (this._zones.has(zone)) {
                this._zones.delete(zone);
                changed = true;
            }
        }

        if(zone === null && this._zones.size != 0) {
            this._zones.clear();
            changed = true;
        }

        if (changed) {
            this._lastZone = zone;
            this._sendEvent("zones", Array.from(this._zones)); 
        }
    }

    isSelectedZone(zone) {
        return this._zones.has(zone);
    }

    get lastZone() { return this._lastZone; }
    get zones() { return this._zones; }
    get layer() { return this._layer; }
}

export const filterService = new FilterService();
