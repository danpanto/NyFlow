export class LayerInterface {
    mount(map) {
        throw new Error("Method 'mount(map)' must be implemented.");
    }

    unmount() {
        throw new Error("Method 'unmount()' must be implemented.");
    }

    update() {
        throw new Error("Method 'update(data)' must be implemented.");
    }

    get visible() {
        throw new Error("Getter 'visible' must be implemented.");
    }
}
