import { themeService } from "./services/ThemeService.js";
import { LayerSwapper } from "./LayerSwapper.js";
import { MapManager } from "./MapManager.js";
import { ZoneBackend } from "./ZoneBackend.js";
import { NeighbourhoodLayer } from "./NeighbourhoodLayer.js";

import { ControlPanel } from "./components/ControlPanel.js";
import { ThemeButton } from "./components/ThemeButton.js";
import { BottomDrawer } from "./components/BottomDrawer.js";
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

    const layerSwapper = new LayerSwapper(layerSwapperData);

    // DEBUG
    window.themeService = themeService;
    window.selectLayer = (name) => mapManager.toggleLayer(name, true);

});
