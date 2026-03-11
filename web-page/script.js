const toggleButton = document.getElementById('theme-toggle');
const currentTheme = localStorage.getItem('theme');

if (currentTheme) {
    document.documentElement.setAttribute('data-theme', currentTheme);
    if (currentTheme === 'dark') {
        toggleButton.textContent = '☀️';
    }


    setTimeout(function(){
        let theme = document.documentElement.getAttribute('data-theme');
        const iframe = document.querySelector('#mapa iframe') || document.getElementById('map-iframe');
        if (iframe) {
            iframe.contentWindow.postMessage({ action: 'data-theme', value: theme }, '*');
        }

    }, 2000);

}

toggleButton.addEventListener('click', () => {
    let theme = document.documentElement.getAttribute('data-theme');
    
    let mensaje; 

    if (theme === 'dark') {
        document.documentElement.setAttribute('data-theme', 'light');
        localStorage.setItem('theme', 'light');
        toggleButton.textContent = '🌙';
        
        mensaje = { action: 'data-theme', value: 'light' };
    } else {
        document.documentElement.setAttribute('data-theme', 'dark');
        localStorage.setItem('theme', 'dark');
        toggleButton.textContent = '☀️';
        
        mensaje = { action: 'data-theme', value: 'dark' };
    }

    const iframe = document.querySelector('#mapa iframe') || document.getElementById('map-iframe');
    if (iframe) {
        iframe.contentWindow.postMessage(mensaje, '*');
    }
});

const layerButtons = document.querySelectorAll('.layer-btn');
const mapIframe = document.getElementById('map-iframe');

layerButtons.forEach(button => {
    button.addEventListener('click', () => {
        layerButtons.forEach(btn => btn.classList.remove('active'));
        
        button.classList.add('active');
        
        const layerId = button.getAttribute('data-layer');
        
        if (mapIframe && mapIframe.contentWindow) {
            mapIframe.contentWindow.postMessage({ action: 'change-layer', value: layerId }, '*');
        }
    });
});

const faders = document.querySelectorAll('.fade-in');

const appearOptions = {
    threshold: 0.15, 
    rootMargin: "0px 0px -50px 0px"
};

const appearOnScroll = new IntersectionObserver(function(entries, appearOnScroll) {
    entries.forEach(entry => {
        if (!entry.isIntersecting) {
            return;
        } else {
            entry.target.classList.add('visible');
            appearOnScroll.unobserve(entry.target);
        }
    });
}, appearOptions);

faders.forEach(fader => {
    appearOnScroll.observe(fader);
});