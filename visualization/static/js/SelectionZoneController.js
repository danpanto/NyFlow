import { ZoneController } from "./ZoneController.js";

export class SelectionZoneController extends ZoneController {
    constructor(backend, color) {
        super(backend);
        this.selectedId = null;
        this.color = color;
    }

    getStyle(id) {
        const isSelected = id === this.selectedId;
        return { 
            fillColor: isSelected ? this.color : 'gray',
            fillOpacity: isSelected ? 0.6 : 0.1,
            weight: isSelected ? 3 : 1
        };
    }

    onClick(e, id) {
        console.log(id);
        this.selectedId = id;
        this.backend.refresh(); 
    }
}
