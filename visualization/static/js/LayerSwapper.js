import { filterService } from "./services/FilterService.js";

export class LayerSwapper {
    constructor(layerDict) {
        this.layerDict = layerDict;
        this.selectedLayer = null;

        filterService.addListener("layer", (layer) => {
            this.swap(layer);
        });
        this.swap(filterService.layer);
    }

    swap(layer) {
        if(layer === this.selectedLayer) return;

        if(!this.layerDict[layer]) {
            console.warn(`Trying to set invalid layer '${layer}'.`)
        }
    
        const newLayer = this.layerDict[layer];
        const oldLayer = this.layerDict[this.selectedLayer];

        if(oldLayer) oldLayer.unbind();
        this.selectedLayer = layer;
        if(newLayer) newLayer.bind();
    }
};
