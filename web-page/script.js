const toggleButton = document.getElementById('theme-toggle');
const currentTheme = localStorage.getItem('theme');

if (currentTheme) {
    document.documentElement.setAttribute('data-theme', currentTheme);
    if (currentTheme === 'dark') {
        toggleButton.textContent = '☀️ Modo Claro';
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
        toggleButton.textContent = '🌙 Modo Oscuro';
        
        mensaje = { action: 'data-theme', value: 'light' };
    } else {
        document.documentElement.setAttribute('data-theme', 'dark');
        localStorage.setItem('theme', 'dark');
        toggleButton.textContent = '☀️ Modo Claro';
        
        mensaje = { action: 'data-theme', value: 'dark' };
    }

    const iframe = document.querySelector('#mapa iframe') || document.getElementById('map-iframe');
    if (iframe) {
        iframe.contentWindow.postMessage(mensaje, '*');
    }
});