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
    
    // Base URL
    const baseUrl = 'https://lumvi.net';
    
    console.log('🚀 Loading Lumvi chatbot for client:', clientId);

    // Icon paths — kept identical to customize.html's ICON_PATHS so the
    // launcher matches whatever the admin picked in "Floating Button Icon".
    const ICON_PATHS = {
        chat:  '<path d="M21 11.5a8.38 8.38 0 0 1-.9 3.8 8.5 8.5 0 0 1-7.6 4.7 8.38 8.38 0 0 1-3.8-.9L3 21l1.9-5.7a8.38 8.38 0 0 1-.9-3.8 8.5 8.5 0 0 1 4.7-7.6 8.38 8.38 0 0 1 3.8-.9h.5a8.48 8.48 0 0 1 8 8v.5z"/>',
        spark: '<path d="M12 2l1.8 6.2L20 10l-6.2 1.8L12 18l-1.8-6.2L4 10l6.2-1.8L12 2z"/>',
        help:  '<circle cx="12" cy="12" r="10"/><path d="M9.09 9a3 3 0 0 1 5.83 1c0 2-3 3-3 3"/><line x1="12" y1="17" x2="12.01" y2="17"/>',
        smile: '<circle cx="12" cy="12" r="10"/><path d="M8 14s1.5 2 4 2 4-2 4-2"/><line x1="9" y1="9" x2="9.01" y2="9"/><line x1="15" y1="9" x2="15.01" y2="9"/>'
    };
    function closedIconSvg(key) {
        return '<svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">' +
               (ICON_PATHS[key] || ICON_PATHS.chat) + '</svg>';
    }
    let launcherIcon   = 'chat';   // updated from /api/config below
    let widgetPosition = 'right';  // updated from /api/config below
    
    // Create toggle button
    const button = document.createElement('button');
    button.id = 'lumvi-chat-button';
    button.setAttribute('aria-label', 'Open chat');
    button.innerHTML = closedIconSvg(launcherIcon);
    
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

            // Widget Position — left/right, matches customize.html's picker
            var pos = (data.config && data.config.branding && data.config.branding.widget_position === 'left') ? 'left' : 'right';
            widgetPosition = pos;
            if (pos === 'left') {
                button.style.left = '20px';
                button.style.right = 'auto';
                container.style.left = '20px';
                container.style.right = 'auto';
            } else {
                button.style.left = 'auto';
                button.style.right = '20px';
                container.style.left = 'auto';
                container.style.right = '20px';
            }
            adjustMobile(); // re-apply responsive layout now that position is known

            // Floating Button Icon
            var iconKey = (data.config && data.config.branding && data.config.branding.launcher_icon) || 'chat';
            launcherIcon = ICON_PATHS[iconKey] ? iconKey : 'chat';
            if (!isOpen) { button.innerHTML = closedIconSvg(launcherIcon); }

            // T1: offline badge — grey dot on the button when outside hours
            if (data.success && data.config && data.config.is_online === false) {
                var badge = document.createElement('span');
                Object.assign(badge.style, {
                    position: 'absolute', top: '-2px', right: '-2px',
                    width: '12px', height: '12px',
                    borderRadius: '50%',
                    background: '#9ca3af',
                    border: '2px solid white',
                    display: 'block',
                });
                button.style.position = 'relative';
                button.style.overflow = 'visible';
                button.appendChild(badge);
            }

            // T1: proactive triggers — evaluate rules, fire into iframe on match
            var triggers = (data.config && data.config.triggers) ? data.config.triggers : [];
            if (triggers.length > 0) {
                _setupProactiveTriggers(triggers, iframe, button, function() { isOpen; });
            }

            // Now reveal — user sees the button exactly once, already in the right color
            button.style.opacity       = '1';
            button.style.pointerEvents = 'auto';
        })
        .catch(function() {
            // Network error — still show the button with the default color
            button.style.opacity       = '1';
            button.style.pointerEvents = 'auto';
        });

    function _setupProactiveTriggers(triggers, targetIframe, toggleBtn, getIsOpen) {
        var _firedKey = 'lumvi_fired_' + clientId;
        var _fired    = {};
        try { _fired = JSON.parse(sessionStorage.getItem(_firedKey) || '{}'); } catch(_) {}

        triggers.forEach(function(t) {
            if (_fired[t.id]) return;  // already fired this browser session

            if (t.trigger_type === 'time_on_page') {
                var delaySecs = parseInt(t.trigger_value, 10) || 10;
                setTimeout(function() {
                    if (_fired[t.id]) return;
                    _fireTrigger(t, targetIframe, toggleBtn);
                    _fired[t.id] = true;
                    try { sessionStorage.setItem(_firedKey, JSON.stringify(_fired)); } catch(_) {}
                }, delaySecs * 1000);

            } else if (t.trigger_type === 'url_match') {
                // Match against the current page URL
                var currentUrl = '';
                try { currentUrl = window.location.href; } catch(_) {}
                if (currentUrl.indexOf(t.trigger_value) !== -1) {
                    setTimeout(function() {
                        if (_fired[t.id]) return;
                        _fireTrigger(t, targetIframe, toggleBtn);
                        _fired[t.id] = true;
                        try { sessionStorage.setItem(_firedKey, JSON.stringify(_fired)); } catch(_) {}
                    }, 1500);
                }
            }
        });
    }

    function _fireTrigger(trigger, targetIframe, toggleBtn) {
        // Open widget if closed
        if (!isOpen) {
            toggleBtn.click();
        }
        // Give iframe a moment to render, then postMessage the trigger
        setTimeout(function() {
            try {
                targetIframe.contentWindow.postMessage(
                    { type: 'lumvi:proactive', message: trigger.message },
                    '*'
                );
            } catch(_) {}
        }, isOpen ? 200 : 600);
    }

    
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
            button.innerHTML = closedIconSvg(launcherIcon);
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
        } else if (widgetPosition === 'left') {
            container.style.width = '400px';
            container.style.left = '20px';
            container.style.right = 'auto';
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