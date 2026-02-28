import { DataQueryLayer } from "./DataQueryLayer.js";
import { filterService } from "./services/FilterService.js";

export class StaticDataLayer extends DataQueryLayer {
    
    async bind() {
        document.body.appendChild(this.drawer);
        document.body.appendChild(this.uiWrapper);

        this.mapManager.toggleLayer(this.variable, true);

        await this._loadStaticData();

        // 3. Solo permitimos que se actualice la tarjeta de info al hacer click/hover
        this.unsubscribeSelectZone = filterService.addListener("zones", () => { 
            this.onSelectedZone(filterService.lastZone);
        });
    }

    async _loadStaticData() {
        try {
            const response = await fetch('/api/restaurant-ratings', { 
                method: 'POST',
                headers: { 'Content-Type': 'application/json' }
            });
            const result = await response.json();

            if (result.status === "ok") {
                this.data = result.data;
                
                this.mapController.update({ query: this.data });
                const { min, max, absoluteMax } = this.mapController.dataBounds;
                this.legend.update(this.gradient, min, max, absoluteMax);
            }
        } catch (e) {
            console.error("Error cargando capa estática", e);
        }
    }

    unbind() {
        this.drawer.remove();
        this.uiWrapper.remove();
        this.mapManager.toggleLayer(this.variable, false);
        if (this.unsubscribeSelectZone) this.unsubscribeSelectZone();
    }
}