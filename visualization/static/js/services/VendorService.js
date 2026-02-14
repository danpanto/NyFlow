class VendorService {
    constructor() {
        if (VendorService.instance) {
            return VendorService.instance;
        }
        VendorService.instance = this;

        this._data = null;

        this.loadingPromise = this.load(); 
    }

    async load() {
        if (this._data) return this._data;

        if (this.loadingPromise) return this.loadingPromise;

        this.loadingPromise = (async () => {
            try {
                const response = await fetch('/api/vendor');
                this._data = await response.json();
                return this._data;
            } catch (e) {
                console.error("Failed to load data range", e);
                throw e;
            }
        })();

        return this.loadingPromise;
    }

    get range() {
        return this._range;
    }

    get max() {
        return this._range["max"];
    }

    get min() {
        return this._range["min"];
    }
}

export const vendorService = new VendorService();
