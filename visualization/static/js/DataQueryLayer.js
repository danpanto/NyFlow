import { BaseLayer } from "./BaseLayer.js";
import { DataQueryController } from "./DataQueryController.js";
import { queryService } from "./services/QueryService.js";
import { Gradient } from "./color.js";
import { GradientLegend } from "./components/GradientLegend.js";

export class DataQueryLayer extends BaseLayer {
    constructor(mapManager, backend, variable) {
        super();

        this.variable = variable;
        this.mapManager = mapManager;
        this.unsubscribe = null;

        this.gradient = new Gradient('magma', 12);

        this.mapController = new DataQueryController(backend, this.gradient);

        mapManager.addLayer(this.variable, this.mapController);
        this.legend = document.createElement("gradient-legend");
        this.legend.setAttribute("title", this.variable);

        this.controlPanel = document.createElement("control-panel");
    }

    bind() {
        document.body.appendChild(this.controlPanel);
        document.body.appendChild(this.legend);
        
        this.mapManager.toggleLayer(this.variable, true);
        
        this.unsubscribe = queryService.addListener(this.variable, (data, loading) => {
            if (!loading && data) {
                this.mapController.update({ query: data });

                const { min, max, absoluteMax } = this.mapController.dataBounds;
                this.legend.update(this.gradient, min, max, absoluteMax);
            }
        });
    }

    unbind() {
        this.controlPanel.remove();
        this.legend.remove();

        this.mapManager.toggleLayer(this.variable, false);

        if (this.unsubscribe) {
            this.unsubscribe();
            this.unsubscribe = null;
        }
    }
}
