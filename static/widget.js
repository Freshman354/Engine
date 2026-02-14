/**
 * Lumvi Chatbot Widget - Embeddable Script
 * Creates a floating chat button and iframe
 */

(function() {
    'use strict';
    
    // Get the client ID from the script tag
    const scripts = document.getElementsByTagName('script');
    let clientId = 'demo';
    let apiUrl = '';
    
    for (let script of scripts) {
        const src = script.getAttribute('src');
        if (src && src.includes('widget.js')) {
            clientId = script.getAttribute('data-client-id') || 'demo';
            
            // Extract base URL from script src
            try {
                const url = new URL(src);
                apiUrl = url.origin;
            } catch (e) {
                apiUrl = window.location.origin;
            }
            break;
        }
    }
    
    // Prevent double-loading
    if (window.LumviChatbotLoaded) {
        console.warn('Lumvi chatbot already loaded');
        return;
    }
    window.LumviChatbotLoaded = true;
    
    // Create toggle button
    const toggleBtn = document.createElement('button');
    toggleBtn.id = 'lumvi-chatbot-toggle';
    toggleBtn.setAttribute('aria-label', 'Open chat');
    toggleBtn.innerHTML = `
        <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"></path>
        </svg>
    `;
    toggleBtn.style.cssText = `
        position: fixed;
        bottom: 20px;
        right: 20px;
        width: 60px;
        height: 60px;
        border-radius: 50%;
        background: linear-gradient(135deg, #6366f1 0%, #06b6d4 100%);
        border: none;
        color: white;
        cursor: pointer;
        box-shadow: 0 4px 16px rgba(99, 102, 241, 0.4);
        z-index: 999998;
        display: flex;
        align-items: center;
        justify-content: center;
        transition: all 0.3s ease;
    `;
    
    toggleBtn.onmouseover = function() {
        this.style.transform = 'scale(1.1)';
        this.style.boxShadow = '0 6px 24px rgba(99, 102, 241, 0.6)';
    };
    
    toggleBtn.onmouseout = function() {
        this.style.transform = 'scale(1)';
        this.style.boxShadow = '0 4px 16px rgba(99, 102, 241, 0.4)';
    };
    
    // Create iframe container
    const container = document.createElement('div');
    container.id = 'lumvi-chatbot-container';
    container.style.cssText = `
        position: fixed;
        bottom: 90px;
        right: 20px;
        width: 400px;
        height: 600px;
        max-height: calc(100vh - 120px);
        border-radius: 16px;
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);
        z-index: 999999;
        overflow: hidden;
        transition: all 0.3s ease;
        display: none;
    `;
    
    // Create iframe
    const iframe = document.createElement('iframe');
    iframe.src = `${apiUrl}/widget?client_id=${clientId}`;
    iframe.setAttribute('allow', 'microphone');
    iframe.setAttribute('title', 'Lumvi Chatbot');
    iframe.style.cssText = `
        width: 100%;
        height: 100%;
        border: none;
        border-radius: 16px;
    `;
    
    container.appendChild(iframe);
    
    // Toggle functionality
    let isOpen = false;
    
    toggleBtn.onclick = function() {
        isOpen = !isOpen;
        
        if (isOpen) {
            container.style.display = 'block';
            toggleBtn.innerHTML = `
                <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <line x1="18" y1="6" x2="6" y2="18"></line>
                    <line x1="6" y1="6" x2="18" y2="18"></line>
                </svg>
            `;
            toggleBtn.setAttribute('aria-label', 'Close chat');
        } else {
            container.style.display = 'none';
            toggleBtn.innerHTML = `
                <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"></path>
                </svg>
            `;
            toggleBtn.setAttribute('aria-label', 'Open chat');
        }
    };
    
    // Mobile responsiveness
    function adjustForMobile() {
        if (window.innerWidth <= 480) {
            container.style.width = 'calc(100vw - 40px)';
            container.style.right = '20px';
            container.style.left = '20px';
        } else {
            container.style.width = '400px';
            container.style.left = 'auto';
        }
    }
    
    window.addEventListener('resize', adjustForMobile);
    
    // Add to page when DOM is ready
    function insertWidget() {
        document.body.appendChild(toggleBtn);
        document.body.appendChild(container);
        adjustForMobile();
        console.log('✅ Lumvi chatbot loaded successfully');
    }
    
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', insertWidget);
    } else {
        insertWidget();
    }
})();