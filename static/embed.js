/**
 * Embeddable Chatbot Widget Loader
 * Usage: <script src="https://your-domain.com/static/embed.js?client_id=demo"></script>
 */

(function() {
    'use strict';
    
    // Prevent multiple loads
    if (window.ChatbotEmbedLoaded) {
        console.warn('Chatbot embed script already loaded');
        return;
    }
    window.ChatbotEmbedLoaded = true;
    
    // Get the script tag that loaded this file
    const currentScript = document.currentScript || document.querySelector('script[src*="embed.js"]');
    
    if (!currentScript) {
        console.error('Chatbot: Could not find embed script');
        return;
    }
    
    // Extract configuration from script URL
    const scriptSrc = currentScript.src;
    const url = new URL(scriptSrc);
    const clientId = url.searchParams.get('client_id') || 'default';
    const apiUrl = url.origin;
    
    // Wait for DOM to be ready
    function initChatbot() {
        // Create container
        const container = document.createElement('div');
        container.id = 'chatbot-container';
        document.body.appendChild(container);
        
        // Load CSS
        const link = document.createElement('link');
        link.rel = 'stylesheet';
        link.href = `${apiUrl}/static/widget.css`;
        document.head.appendChild(link);
        
        // Load widget script
        const script = document.createElement('script');
        script.src = `${apiUrl}/static/widget.js`;
        script.onload = function() {
            // Initialize the widget after script loads
            if (window.ChatbotWidget) {
                window.ChatbotWidget.init({
                    clientId: clientId,
                    apiUrl: apiUrl
                });
            } else {
                console.error('ChatbotWidget not found after script load');
            }
        };
        script.onerror = function() {
            console.error('Failed to load chatbot widget script');
        };
        document.body.appendChild(script);
    }
    
    // Initialize when DOM is ready
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', initChatbot);
    } else {
        initChatbot();
    }
})();

