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

const contactForm = document.getElementById('nyflow-contact-form');
const formStatus = document.getElementById('form-status');

if (contactForm) {
    contactForm.addEventListener('submit', async function(event) {
        event.preventDefault(); 
        
        const submitBtn = document.getElementById('submit-btn');
        const originalBtnText = submitBtn.innerText;
        submitBtn.innerText = "Enviando...";
        submitBtn.disabled = true;

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

            formStatus.style.display = "block"; 

            if (response.ok) {
                formStatus.innerHTML = "✅ ¡Mensaje enviado con éxito! Te responderemos pronto.";
                formStatus.style.color = "#28a745"; 
                contactForm.reset(); 
            } else {
                formStatus.innerHTML = "❌ Hubo un problema al enviar. Inténtalo de nuevo.";
                formStatus.style.color = "#dc3545";
            }
        } catch (error) {
            formStatus.style.display = "block";
            formStatus.innerHTML = "❌ Error de red. Comprueba tu conexión.";
            formStatus.style.color = "#dc3545";
        } finally {
            submitBtn.innerText = originalBtnText;
            submitBtn.disabled = false;
        }
    });
}

const faqQuestions = document.querySelectorAll('.faq-question');

faqQuestions.forEach(question => {
    question.addEventListener('click', () => {
        question.classList.toggle('active');
        
        const answer = question.nextElementSibling;
        
        if (question.classList.contains('active')) {
            answer.style.maxHeight = answer.scrollHeight + "px";
        } else {
            answer.style.maxHeight = null;
        }
    });
});

// const mobileMenu = document.getElementById('mobile-menu');
// const navLinks = document.querySelector('.nav-links');

// if (mobileMenu && navLinks) {
//     mobileMenu.addEventListener('click', () => {
//         navLinks.classList.toggle('active');
        
//         const icon = mobileMenu.querySelector('i');
//         if (navLinks.classList.contains('active')) {
//             icon.classList.remove('ph-list');
//             icon.classList.add('ph-x');
//         } else {
//             icon.classList.remove('ph-x');
//             icon.classList.add('ph-list');
//         }
//     });
// }