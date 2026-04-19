/**
 * Lumvi Chatbot Widget - Simple Iframe Embed
 * Version: 2.0
 */

(function() {
    'use strict';
    
    // Prevent double-loading
    if (window.LumviChatbotLoaded) {
        console.warn('Lumvi chatbot already loaded');
        return;
    }
    window.LumviChatbotLoaded = true;
    
    // Get client ID from script tag
    const currentScript = document.currentScript || document.querySelector('script[data-client-id]');
    // ALWAYS use demo on landing page, or use specified client ID
    const clientId = window.LUMVI_CLIENT_ID || 
                    (currentScript ? currentScript.getAttribute('data-client-id') : null) || 
                    'demo';

    console.log('🎯 Widget initializing for client:', clientId);
    
    // Get base URL
    const baseUrl = 'https://lumvi.net';
    
    console.log('🚀 Loading Lumvi chatbot for client:', clientId);
    
    // Create toggle button
    const button = document.createElement('button');
    button.id = 'lumvi-chat-button';
    button.setAttribute('aria-label', 'Open chat');
    button.innerHTML = `
        <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"></path>
        </svg>
    `;
    
    // Button styles — hidden until brand color is loaded to prevent flash
    Object.assign(button.style, {
        position: 'fixed',
        bottom: '20px',
        right: '20px',
        width: '60px',
        height: '60px',
        borderRadius: '50%',
        background: '#B8924A',   // neutral default — overwritten before visible
        border: 'none',
        color: 'white',
        cursor: 'pointer',
        boxShadow: '0 4px 16px rgba(0,0,0,0.2)',
        zIndex: '999998',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        transition: 'opacity 0.25s ease, transform 0.2s ease, box-shadow 0.2s ease',
        opacity: '0',           // invisible until color is ready
        pointerEvents: 'none'   // not clickable while invisible
    });

    // Fetch brand color BEFORE showing the button — eliminates the blue→gold flash
    fetch(`${baseUrl}/api/config?client_id=${clientId}`)
        .then(function(r) { return r.json(); })
        .then(function(data) {
            var color = '#B8924A';  // Lumvi gold fallback
            if (data.success && data.config && data.config.branding && data.config.branding.primary_color) {
                color = data.config.branding.primary_color;
            }
            button.style.background  = color;
            button.style.boxShadow   = '0 4px 16px ' + color + '66';
            button._brandColor       = color;
            // Now reveal — user sees the button exactly once, already in the right color
            button.style.opacity       = '1';
            button.style.pointerEvents = 'auto';
        })
        .catch(function() {
            // Network error — still show the button with the default color
            button.style.opacity       = '1';
            button.style.pointerEvents = 'auto';
        });

    
    // Create chat container
    const container = document.createElement('div');
    container.id = 'lumvi-chat-container';
    
    // Container styles
    Object.assign(container.style, {
        position: 'fixed',
        bottom: '90px',
        right: '20px',
        width: '400px',
        height: '600px',
        maxHeight: 'calc(100vh - 120px)',
        borderRadius: '16px',
        boxShadow: '0 8px 32px rgba(0, 0, 0, 0.3)',
        zIndex: '999999',
        overflow: 'hidden',
        display: 'none',
        transition: 'all 0.3s ease'
    });
    
    // Create iframe
    const iframe = document.createElement('iframe');
    iframe.src = `${baseUrl}/widget?client_id=${clientId}`;
    iframe.setAttribute('allow', 'microphone');
    iframe.setAttribute('title', 'Lumvi Chatbot');
    
    // Iframe styles
    Object.assign(iframe.style, {
        width: '100%',
        height: '100%',
        border: 'none',
        borderRadius: '16px'
    });
    
    container.appendChild(iframe);
    
    // Button hover effects
    button.addEventListener('mouseenter', function() {
        this.style.transform = 'scale(1.1)';
        var c = this._brandColor || '#6366f1';
        this.style.boxShadow = '0 6px 24px ' + c + '99';
    });
    
    button.addEventListener('mouseleave', function() {
        this.style.transform = 'scale(1)';
        var c = this._brandColor || '#6366f1';
        this.style.boxShadow = '0 4px 16px ' + c + '66';
    });
    
    // Toggle chat
    let isOpen = false;
    
    button.addEventListener('click', function() {
        isOpen = !isOpen;
        
        if (isOpen) {
            container.style.display = 'block';
            button.innerHTML = `
                <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <line x1="18" y1="6" x2="6" y2="18"></line>
                    <line x1="6" y1="6" x2="18" y2="18"></line>
                </svg>
            `;
            button.setAttribute('aria-label', 'Close chat');
            console.log('✅ Chat opened');
        } else {
            container.style.display = 'none';
            button.innerHTML = `
                <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"></path>
                </svg>
            `;
            button.setAttribute('aria-label', 'Open chat');
            console.log('❌ Chat closed');
        }
    });
    
    // Mobile responsive
    function adjustMobile() {
        if (window.innerWidth <= 480) {
            container.style.width = 'calc(100vw - 40px)';
            container.style.left = '20px';
            container.style.right = '20px';
        } else {
            container.style.width = '400px';
            container.style.left = 'auto';
            container.style.right = '20px';
        }
    }
    
    window.addEventListener('resize', adjustMobile);
    
    // Insert into page
    function init() {
        document.body.appendChild(button);
        document.body.appendChild(container);
        adjustMobile();
        console.log('✅ Lumvi chatbot loaded successfully');
    }
    
    // Wait for DOM
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
    
})();