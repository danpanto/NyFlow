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

// --- LÓGICA DEL FORMULARIO DE CONTACTO ---
const contactForm = document.getElementById('nyflow-contact-form');
const formStatus = document.getElementById('form-status');

if (contactForm) {
    contactForm.addEventListener('submit', async function(event) {
        // 1. Evitamos que la página se recargue o cambie a Formspree
        event.preventDefault(); 
        
        // 2. Cambiamos el texto del botón mientras carga
        const submitBtn = document.getElementById('submit-btn');
        const originalBtnText = submitBtn.innerText;
        submitBtn.innerText = "Enviando...";
        submitBtn.disabled = true;

        // 3. Recogemos los datos y los enviamos
        const formData = new FormData(contactForm);
        const endpointUrl = contactForm.action;

        try {
            const response = await fetch(endpointUrl, {
                method: 'POST',
                body: formData,
                headers: {
                    'Accept': 'application/json'
                }
            });

            formStatus.style.display = "block"; // Mostramos el mensaje oculto

            if (response.ok) {
                formStatus.innerHTML = "✅ ¡Mensaje enviado con éxito! Te responderemos pronto.";
                formStatus.style.color = "#28a745"; // Verde
                contactForm.reset(); // Vaciamos los campos
            } else {
                formStatus.innerHTML = "❌ Hubo un problema al enviar. Inténtalo de nuevo.";
                formStatus.style.color = "#dc3545"; // Rojo
            }
        } catch (error) {
            formStatus.style.display = "block";
            formStatus.innerHTML = "❌ Error de red. Comprueba tu conexión.";
            formStatus.style.color = "#dc3545";
        } finally {
            // Restauramos el botón
            submitBtn.innerText = originalBtnText;
            submitBtn.disabled = false;
        }
    });
}