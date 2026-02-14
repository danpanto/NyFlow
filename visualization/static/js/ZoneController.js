import { LayerInterface } from "./LayerInterface.js";

export class ZoneController extends LayerInterface {
    constructor(backend) {
        super();
        this.backend = backend;
        this._visible = false;
        this.map = null;
    }

    mount(map) {
        this.map = map;
        this._visible = false;
    }

    unmount() {
        this.map = null;
        this._visible = false;
    }


    update(data) {
        if (data.visible !== undefined) {
            this.visible = data.visible;
        }

        if (data.theme !== undefined) {
             this.backend.refresh(); 
        }
    }

    get visible() {
        return this._visible;
    }

    set visible(value) {
        this._visible = value;
        if(this._visible) {
            this.backend.bind(this.map, this);
        } else {
            this.backend.unbind();
        }
    }

    getStyle(feature) {
        return { color: '#3388ff', weight: 1, fillOpacity: 0.1 };
    }

    onClick(e, feature) {}
    onHover(e, feature) {}
    onUnhover(e, feature) {}
}
