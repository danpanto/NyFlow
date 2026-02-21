import { BaseLayer } from "./BaseLayer.js";
import { RouteController } from "./RouteController.js";
import { DataQueryLayer } from "./DataQueryLayer.js";

export class RouteLayer extends BaseLayer {
    constructor(mapManager, tripsLayer) {
        super();
        this.name = "routes"; // Coincide con el <button layer="routes">
        this.mapManager = mapManager;
        this.baseAppName = "NyFlow";

        // Usamos la capa visual de viajes ya existente
        this.tripsLayer = tripsLayer;

        // Registrar el controlador visual en el gestor del mapa
        const mapController = new RouteController();
        mapManager.addLayer(this.name, mapController);
    }

    bind() {
        // Vinculamos la interfaz y los colores de la capa interna de DataQueryLayer
        this.tripsLayer.bind();

        // Sobrescribimos el título para que ponga Optimal Routes en vez de Total Trips
        const layerTitleElement = document.querySelector('.layer-title-text');
        if (layerTitleElement) {
            layerTitleElement.textContent = `• Optimal Routes`;
        }
        document.title = `${this.baseAppName} • Routes`;

        // Mostramos la línea de la ruta
        this.mapManager.toggleLayer(this.name, true);
    }

    unbind() {
        // Desvinculamos la interfaz interna
        this.tripsLayer.unbind();

        // Ocultamos la línea de la ruta
        this.mapManager.toggleLayer(this.name, false);

        const layerTitleElement = document.querySelector('.layer-title-text');
        if (layerTitleElement) {
            layerTitleElement.textContent = "";
        }
        document.title = this.baseAppName;
    }
}
