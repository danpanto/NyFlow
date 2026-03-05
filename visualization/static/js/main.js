import { themeService } from "./services/ThemeService.js";
import { LayerSwapper } from "./LayerSwapper.js";
import { MapManager } from "./MapManager.js";
import { ZoneBackend } from "./ZoneBackend.js";
import { NeighbourhoodLayer } from "./NeighbourhoodLayer.js";
import { DataQueryLayer } from "./DataQueryLayer.js";
import { RouteLayer } from "./RouteLayer.js";
import { RestaurantRatingsLayer } from "./RestaurantLayer.js";
import { AskingRentLayer } from "./AskingRentLayer.js";
import { queryService } from "./services/QueryService.js";
import { SUPPORTED_VARIABLES } from "./queryVariables.js";

import { ControlPanel } from "./components/ControlPanel.js";
import { ThemeButton } from "./components/ThemeButton.js";
import { BottomDrawer } from "./components/BottomDrawer.js";
import { RightDrawer } from "./components/RightDrawer.js";
import { LayerSelector } from "./components/LayerSelector.js";

document.addEventListener('DOMContentLoaded', async () => {

    // Initialize map
    const mapContainer = document.getElementById("map-view");
    if (!mapContainer) {
        console.error("Couldn't find map element with id 'map-view'");
        return;
    }

    const mapManager = new MapManager(mapContainer);

    // Layer selector
    const sharedBackend = new ZoneBackend();
    const neighbourhoodLayer = new NeighbourhoodLayer(mapManager, sharedBackend);

    let layerSwapperData = new Map();
    layerSwapperData[neighbourhoodLayer.name] = neighbourhoodLayer;

    const queryLayers = {};
    SUPPORTED_VARIABLES.forEach((v) => {
        const layer = new DataQueryLayer(mapManager, sharedBackend, v);
        queryLayers[v] = layer;
        layerSwapperData[v] = layer;
    });

    const routeLayer = new RouteLayer(mapManager, queryLayers["total_trips"]);
    layerSwapperData[routeLayer.name] = routeLayer;

    const restaurantRatingsLayer = new RestaurantRatingsLayer(mapManager, sharedBackend);
    layerSwapperData["restaurant_ratings"] = restaurantRatingsLayer;

    const askingRentLayer = new AskingRentLayer(mapManager, sharedBackend);
    layerSwapperData["asking_rent"] = askingRentLayer;

    const layerSwapper = new LayerSwapper(layerSwapperData);

    // DEBUG
    window.themeService = themeService;
    window.selectLayer = (name) => mapManager.toggleLayer(name, true);

});
