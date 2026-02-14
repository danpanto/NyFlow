class ThemeService extends EventTarget {
    constructor() {
        super();
        this._theme = localStorage.getItem('app-theme') || 
                      (window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light');

        this._updateDOM();
    }

    get theme() {
        return this._theme;
    }

    set theme(value) {
        if (value !== 'light' && value !== 'dark') return; // Invalid theme
        if(this._theme === value) return; // Theme is already set

        this._theme = value;
        localStorage.setItem('app-theme', value);
        this._updateDOM();

        this.dispatchEvent(new CustomEvent('theme-changed', { 
            detail: { theme: this._theme } 
        }));
    }

    addListener(callback, doFirstCall = true) {
        const internalWrapper = (e) => callback(e.detail.theme);
        this.addEventListener('theme-changed', internalWrapper);

        if(doFirstCall) callback(this._theme);

        // Callback to unsubcribe
        return () => this.removeEventListener('theme-changed', internalWrapper);
    }

    toggle() {
        this.theme = (this._theme === 'light') ? 'dark' : 'light';
    }

    applyThemeToDocument() {
        // This handles standard CSS changes via a class on <body>
        document.documentElement.setAttribute('data-theme', this._theme);
        document.body.className = this._theme;
    }

    _updateDOM() {
        const html = document.documentElement;
        if (this._theme === 'dark') {
            html.classList.add('dark');
        } else {
            html.classList.remove('dark');
        }
    }
}

export const themeService = new ThemeService();
