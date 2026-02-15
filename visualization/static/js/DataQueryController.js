import { ZoneController } from "./ZoneController.js";
import { paletteFromList } from "./color.js";
import { zoneData } from "./services/ZoneDataService.js";
import { filterService } from "./services/FilterService.js";

export class DataQueryController extends ZoneController {
    constructor(backend, gradient) {
        super(backend);
        this.selectedId = null;

        this.normalOpacity = 0.6; // Increased slightly for better visibility
        this.hoverOpacity = 0.9;
        this.selectOpacity = 1.0;
        
        this.currentData = null;
        this.dataBounds = { min: 0, max: 1 };
        
        // Initialize Gradient (e.g., 'magma' with 7 quantization steps)
        this.gradient = gradient;

        this._init();
    }

    async _init() {
        await zoneData.load();
        const boroughs = zoneData.getAllBoroughs();
        this.palette = paletteFromList(boroughs);

        if(this._visible) {
            this.backend.refresh();
        }
    }

    update(data) {
        super.update(data);
        let changed = false;

        if(data.click !== undefined) {
            filterService.selectZone(null);
            changed = true;
        }

        if(data.query !== undefined) {
            this.currentData = data.query;
            this._calculateBounds(); // Update min/max for the color scale
            changed = true;
        }

        if(changed) this.backend.refresh();
    }

    _calculateBounds() {
        if (!this.currentData || Object.keys(this.currentData).length === 0) return;
        
        // Sort values ascending
        const values = Object.values(this.currentData).sort((a, b) => a - b);
        
        // Calculate the 95th Percentile
        const p95Index = Math.floor(values.length * 0.95);
        const p95Value = values[p95Index];

        this.dataBounds = {
            min: values[0],
            max: p95Value, // We use the 95th percentile as our "100%" color
            absoluteMax: values[values.length - 1] // Keep this just in case
        };
    }

    _getNormalizedValue(id) {
        if (!this.currentData || this.currentData[id] === undefined) return null;
        
        const val = this.currentData[id];
        const { min, max } = this.dataBounds;

        if (max === min) return 0.5;

        // If a value is above our 95th percentile, it just gets the max color (1.0)
        if (val >= max) return 1.0;

        // Standard linear scale for the remaining 95% of the data
        return (val - min) / (max - min);
    }
    getStyle(id) {
        if(!this.currentData || this.currentData[id] === undefined) {
            return { 
                color: "transparent", 
                fillColor: "transparent",
                opacity: 0,
                fillOpacity: 0, 
                interactive: true,
                weight: 1
            };
        }

        const isSelected = filterService.isSelectedZone(id);
        const normalized = this._getNormalizedValue(id);
        
        return {
            color: "white",
            fillColor: this.gradient.get(normalized),
            fillOpacity: isSelected ? this.selectOpacity : this.normalOpacity,
            weight: 1,
        };
    }

    onHover(e, id) {
        const target = e.target;
        const isSelected = filterService.isSelectedZone(id);
        if(isSelected) return;
        target.setStyle({
            fillOpacity: this.hoverOpacity,
            weight: 1
        });
    }

    onUnhover(e, id) {
        const target = e.target;
        const isSelected = filterService.isSelectedZone(id);
        if(isSelected) return;

        // Reset to the base style defined in getStyle
        target.setStyle(this.getStyle(id));
    }

    onClick(e, id) {
        const isMultiSelect = e.originalEvent.ctrlKey || e.originalEvent.metaKey;
        const isCurrentlySelected = filterService.isSelectedZone(id);
        const isSelecting = isMultiSelect ? !isCurrentlySelected : true;

        filterService.selectZone(id, isSelecting, !isMultiSelect);
        this.backend.refresh(); 
    }
}
