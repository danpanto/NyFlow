import { ZoneController } from "./ZoneController.js";
import { paletteFromList } from "./color.js";
import { zoneData } from "./services/ZoneDataService.js";
import { filterService } from "./services/FilterService.js";

export class NeighbourhoodZoneController extends ZoneController {
    constructor(backend) {
        super(backend);
        this.selectedId = null;

        this.normalOpacity = 0.3;
        this.hoverOpacity = 0.4;
        this.selectOpacity = 0.8;

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

        if(data.click !== undefined) {
            filterService.selectZone(null);
            this.backend.refresh();
        }
    }

    getStyle(id) {
        if(!this.palette) return { opacity: 0, fillOpacity: 0 };

        const isSelected = filterService.isSelectedZone(id);
        const opacity = isSelected ? this.selectOpacity : this.normalOpacity;

        const borough = zoneData.getBorough(id); 
        return {
            color: "white",
            fillColor: this.palette.get(borough),
            fillOpacity: opacity,
            weight: 1,
        };
    }

    onHover(e, id) {
        const target = e.target;
        const isSelected = filterService.isSelectedZone(id);

        if(isSelected) return;

        target.setStyle({
            fillOpacity: this.hoverOpacity,
        });
    }

    onUnhover(e, id) {
        const target = e.target;
        const isSelected = filterService.isSelectedZone(id);

        if(isSelected) return;

        target.setStyle({
            fillOpacity: this.normalOpacity,
        });
    }

    onClick(e, id) {
        const isMultiSelect = e.originalEvent.ctrlKey || e.originalEvent.metaKey;
        const isCurrentlySelected = filterService.isSelectedZone(id);
        const isSelecting = isMultiSelect ? !isCurrentlySelected : true;

        filterService.selectZone(id, isSelecting, !isMultiSelect);

        this.backend.refresh(); 
    }
}
