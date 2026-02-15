export class DataRangeService {
    constructor() {
        if (DataRangeService.instance) {
            return DataRangeService.instance;
        }
        DataRangeService.instance = this;

        this._range = null;

        this.loadingPromise = this.load(); 
    }

    async load() {
        if (this._range) return this._range;

        if (this.loadingPromise) return this.loadingPromise;

        this.loadingPromise = (async () => {
            try {
                const response = await fetch('/api/date_range');
                const data = await response.json();

                this._range = {
                    max: new Date(data["max"]),
                    min: new Date(data["min"]),
                };

                return this._range;
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

export const dataRangeservice = new DataRangeService();
