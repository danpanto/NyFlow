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
        this._dateRange = null;
        this._vendors = new Set();
        this._daysOfWeek = new Set();
    }

    selectMaxDateRange(date) {
        if(this._dateRange["max"] == date) return;
        this._dateRange["max"] = date;
        this._sendEvent("date", this._dateRange);
    }

    selectMinDateRange(date) {
        if(this._dateRange["min"] == date) return;
        this._dateRange["min"] = date;
        this._sendEvent("date", this._dateRange);
    }

    selectDateRange(date) {
        if(this._dateRange == date) return;
        this._dateRange = date;
        this._sendEvent("date", this._dateRange);
    }

    selectLayer(layer) {
        if (this._layer !== layer) {
            this.selectVendor(null);
            this.selectZone(null);
            this.selectDayOfWeek(null);
        }
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

    selectVendor(vendorId, unique = false) {
        let changed = false;
        if (vendorId === undefined) return;
        if (vendorId !== null) vendorId = String(vendorId);
        if (unique) {
            // If it's already the ONLY selected vendor, don't clear and trigger repaint
            const isAlreadyOnlySelection = this._vendors.size === 1 && this._vendors.has(vendorId);

            if (this._vendors.size > 0 && !isAlreadyOnlySelection) {
                this._vendors.clear();
                changed = true;
            }
        }

        if (vendorId !== null && !this._vendors.has(vendorId)) {
            this._vendors.add(vendorId);
            changed = true;
        } else if(this._vendors.has(vendorId))  {
            this._vendors.delete(vendorId);
            changed = true;
        }

        // Passing null clears all vendors
        if (vendorId === null && this._vendors.size !== 0) {
            this._vendors.clear();
            changed = true;
        }

        if (changed) {
            // Send out an array of all currently selected vendors
            this._sendEvent("vendors", this._vendors); 
        }
    }

    isSelectedVendor(vendorId) {
        return this._vendors.has(vendorId);
    }

    selectDayOfWeek(day, unique = false) {
        let changed = false;
        if (day === undefined) return;
        if (day !== null) day = Number(day);
        if (unique) {
            const isAlreadyOnlySelection = this._daysOfWeek.size === 1 && this._daysOfWeek.has(day);

            if (this._daysOfWeek.size > 0 && !isAlreadyOnlySelection) {
                this._daysOfWeek.clear();
                changed = true;
            }
        }

        if (day !== null && !this._daysOfWeek.has(day)) {
            this._daysOfWeek.add(day);
            changed = true;
        } else if (this._daysOfWeek.has(day)) {
            this._daysOfWeek.delete(day);
            changed = true;
        }

        if (day === null && this._daysOfWeek.size !== 0) {
            this._daysOfWeek.clear();
            changed = true;
        }

        if (changed) {
            this._sendEvent("days", this._daysOfWeek);
        }
    }

    isSelectedDayOfWeek(day) {
        return this._daysOfWeek.has(Number(day));
    }

    get lastZone() { return this._lastZone; }
    get zones() { return this._zones; }
    get layer() { return this._layer; }
    get minDateRange() { return this._dateRange["min"]; }
    get maxDateRange() { return this._dateRange["max"]; }
    get dateRange() { return this._dateRange; }
    get vendors() { return this._vendors; }
    get daysOfWeek() { return this._daysOfWeek; }
}

export const filterService = new FilterService();
