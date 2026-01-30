/**
 * FAQ Chatbot Widget - Embeddable Chat Interface
 * Supports white-labeling and lead collection
 */

(function() {
    'use strict';

    const ChatbotWidget = {
        config: null,
        clientId: 'default',
        apiUrl: '',
        isOpen: false,
        conversationHistory: [],
        leadCollectionMode: false,
        leadData: {},
        
        /**
         * Initialize the chatbot widget
         */
        init: function(options) {
            this.clientId = options.clientId || 'default';
            this.apiUrl = options.apiUrl || '';
            
            // Load client configuration
            this.loadConfig().then(() => {
                this.render();
                this.attachEventListeners();
            });
        },
        
        /**
         * Load client configuration from API
         */
        loadConfig: async function() {
            try {
                const response = await fetch(`${this.apiUrl}/api/config?client_id=${this.clientId}`);
                const data = await response.json();
                
                if (data.success) {
                    this.config = data.config;
                    this.applyBranding();
                } else {
                    console.error('Failed to load config');
                }
            } catch (error) {
                console.error('Error loading config:', error);
            }
        },
        
        /**
         * Apply client branding
         */
        applyBranding: function() {
            if (!this.config) return;
            
            const root = document.documentElement;
            root.style.setProperty('--primary-color', this.config.branding.primary_color);
            root.style.setProperty('--secondary-color', this.config.branding.secondary_color);
        },
        
        /**
         * Render the chatbot UI
         */
        render: function() {
            const container = document.getElementById('chatbot-container');
            
            const html = `
                <button class="chatbot-button" id="chatbot-toggle" style="background: ${this.config?.branding.primary_color || '#4F46E5'}">
                    <svg viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
                        <path d="M12 2C6.48 2 2 6.48 2 12c0 1.54.36 3 .97 4.29L2 22l5.71-.97C9 21.64 10.46 22 12 22c5.52 0 10-4.48 10-10S17.52 2 12 2zm0 18c-1.38 0-2.68-.31-3.86-.86l-.28-.15-2.9.49.49-2.9-.15-.28C4.31 14.68 4 13.38 4 12c0-4.41 3.59-8 8-8s8 3.59 8 8-3.59 8-8 8z"/>
                    </svg>
                </button>
                
                <div class="chatbot-window" id="chatbot-window" style="display: none;">
                    <div class="chatbot-header" style="background: ${this.config?.branding.primary_color || '#4F46E5'}">
                        <div class="chatbot-header-content">
                            <img src="${this.config?.branding.bot_avatar || ''}" alt="Bot" class="chatbot-avatar" onerror="this.style.display='none'">
                            <div class="chatbot-title">
                                <h3>${this.config?.bot_settings.bot_name || 'Support Bot'}</h3>
                                <div class="chatbot-status">● Online</div>
                            </div>
                        </div>
                        <button class="chatbot-close" id="chatbot-close">×</button>
                    </div>
                    
                    <div class="chatbot-messages" id="chatbot-messages">
                        <!-- Messages will be inserted here -->
                    </div>
                    
                    <div class="chatbot-input">
                        <input type="text" id="chatbot-input-field" placeholder="Type your message..." autocomplete="off">
                        <button id="chatbot-send">Send</button>
                    </div>
                    
                    <div class="chatbot-footer">
                        Powered by Your Company
                    </div>
                </div>
            `;
            
            container.innerHTML = html;
            
            // Send welcome message
            this.addBotMessage(this.config?.bot_settings.welcome_message || 'Hi! How can I help you?');
        },
        
        /**
         * Attach event listeners
         */
        attachEventListeners: function() {
            const toggleBtn = document.getElementById('chatbot-toggle');
            const closeBtn = document.getElementById('chatbot-close');
            const sendBtn = document.getElementById('chatbot-send');
            const inputField = document.getElementById('chatbot-input-field');
            
            toggleBtn.addEventListener('click', () => this.toggleChat());
            closeBtn.addEventListener('click', () => this.closeChat());
            sendBtn.addEventListener('click', () => this.sendMessage());
            
            inputField.addEventListener('keypress', (e) => {
                if (e.key === 'Enter') {
                    this.sendMessage();
                }
            });
        },
        
        /**
         * Toggle chat window
         */
        toggleChat: function() {
            const window = document.getElementById('chatbot-window');
            this.isOpen = !this.isOpen;
            window.style.display = this.isOpen ? 'flex' : 'none';
            
            if (this.isOpen) {
                document.getElementById('chatbot-input-field').focus();
                this.scrollToBottom();
            }
        },
        
        /**
         * Close chat window
         */
        closeChat: function() {
            const window = document.getElementById('chatbot-window');
            this.isOpen = false;
            window.style.display = 'none';
        },
        
        /**
         * Send user message
         */
        sendMessage: async function() {
            const inputField = document.getElementById('chatbot-input-field');
            const message = inputField.value.trim();
            
            if (!message) return;
            
            // Add user message to UI
            this.addUserMessage(message);
            this.conversationHistory.push({ role: 'user', content: message });
            
            // Clear input
            inputField.value = '';
            
            // If in lead collection mode, handle lead form input
            if (this.leadCollectionMode) {
                this.handleLeadInput(message);
                return;
            }
            
            // Show typing indicator
            this.showTyping();
            
            // Send to API
            try {
                const response = await fetch(`${this.apiUrl}/api/chat`, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify({
                        message: message,
                        client_id: this.clientId,
                        context: {
                            conversation_history: this.conversationHistory
                        }
                    })
                });
                
                const data = await response.json();
                
                this.hideTyping();
                
                if (data.success) {
                    // Check if lead collection should be triggered
                    if (data.trigger_lead_collection) {
                        this.startLeadCollection(data.extracted_email);
                    } else {
                        this.addBotMessage(data.response);
                        this.conversationHistory.push({ role: 'bot', content: data.response });
                    }
                } else {
                    this.addBotMessage('Sorry, something went wrong. Please try again.');
                }
            } catch (error) {
                this.hideTyping();
                console.error('Error sending message:', error);
                this.addBotMessage('Sorry, I\'m having trouble connecting. Please try again.');
            }
        },
        
        /**
 * Handle lead input during collection
 */
handleLeadInput: async function(message) {
    const messageLower = message.toLowerCase().trim();
    
    // Check if user wants to cancel lead collection
    if (messageLower === 'cancel' || messageLower === 'nevermind' || messageLower === 'skip' || messageLower === 'back') {
        this.leadCollectionMode = false;
        this.leadData = {};
        this.addBotMessage("No problem! Lead collection cancelled. Feel free to ask me any questions about our services!");
        return;
    }
    
    // Check if user is asking a question instead of providing lead info
    const questionIndicators = ['what', 'how', 'when', 'where', 'why', 'who', 'can', 'do you', 'tell me', '?'];
    const isQuestion = questionIndicators.some(indicator => messageLower.includes(indicator));
    
    if (isQuestion && !this.leadData.name) {
        // User is asking a question at the start of lead collection
        this.leadCollectionMode = false;
        this.leadData = {};
        
        // Forward the question to the normal chat handler
        this.conversationHistory.push({ role: 'user', content: message });
        this.showTyping();
        
        try {
            const response = await fetch(`${this.apiUrl}/api/chat`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    message: message,
                    client_id: this.clientId,
                    context: {
                        conversation_history: this.conversationHistory
                    }
                })
            });
            
            const data = await response.json();
            this.hideTyping();
            
            if (data.success) {
                if (data.trigger_lead_collection) {
                    this.startLeadCollection(data.extracted_email);
                } else {
                    this.addBotMessage(data.response);
                    this.conversationHistory.push({ role: 'bot', content: data.response });
                }
            }
        } catch (error) {
            this.hideTyping();
            this.addBotMessage('Sorry, something went wrong. Please try again.');
        }
        return;
    }
    
    // Collect name
    if (!this.leadData.name) {
        this.leadData.name = message;
        
        if (this.leadData.email) {
            this.addBotMessage("Great! And your phone number? (Type 'skip' if you'd prefer not to share)");
        } else {
            this.addBotMessage("Thanks! What's your email address?");
        }
        return;
    }
    
    // Collect email
    if (!this.leadData.email) {
        if (this.isValidEmail(message)) {
            this.leadData.email = message;
            this.addBotMessage("Perfect! And your phone number? (Type 'skip' to leave blank)");
        } else {
            this.addBotMessage("That doesn't look like a valid email. Can you try again? (Or type 'cancel' to go back to chatting)");
        }
        return;
    }
    
    // Collect phone
    if (!this.leadData.phone && this.leadData.phone !== 'skipped') {
        if (messageLower === 'skip') {
            this.leadData.phone = 'skipped';
            this.addBotMessage("No worries! What company do you represent? (Type 'skip' to leave blank)");
        } else {
            this.leadData.phone = message;
            this.addBotMessage("Thanks! What company do you represent? (Type 'skip' to leave blank)");
        }
        return;
    }
    
    // Collect company
    if (!this.leadData.company && this.leadData.company !== 'skipped') {
        if (messageLower === 'skip') {
            this.leadData.company = 'skipped';
            this.addBotMessage("Got it! Anything else you'd like to tell us? (Or type 'skip' to finish)");
        } else {
            this.leadData.company = message;
            this.addBotMessage("Excellent! Anything else you'd like to tell us? (Or type 'skip' to finish)");
        }
        return;
    }
    
    // Collect final message and submit
    if (messageLower === 'skip') {
        this.leadData.message = '';
    } else {
        this.leadData.message = message;
    }
    
    await this.submitLead();
},
        
        /**
         * Submit lead to API
         */
        submitLead: async function() {
            this.showTyping();
            
            try {
                const response = await fetch(`${this.apiUrl}/api/lead`, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify({
                        client_id: this.clientId,
                        name: this.leadData.name,
                        email: this.leadData.email,
                        phone: (this.leadData.phone && this.leadData.phone !== 'skipped') ? this.leadData.phone : '',
                        company: (this.leadData.company && this.leadData.company !== 'skipped') ? this.leadData.company : '',
                        message: this.leadData.message || '',
                        conversation_snippet: this.conversationHistory.slice(-5).map(m => m.content).join(' | '),
                        source_url: window.location.href
                    })
                });
                
                const data = await response.json();
                
                this.hideTyping();
                
                if (data.success) {
                    this.addBotMessage(data.message);
                    
                    // Show contact info
                    if (data.contact_info) {
                        setTimeout(() => {
                            const contactMsg = `You can also reach us at:\n📧 ${data.contact_info.email}\n📞 ${data.contact_info.phone}`;
                            this.addBotMessage(contactMsg);
                        }, 1000);
                    }
                    
                    // Reset lead collection mode
                    this.leadCollectionMode = false;
                    this.leadData = {};
                } else {
                    this.addBotMessage('Sorry, there was an error saving your information. Please try again or email us directly.');
                }
            } catch (error) {
                this.hideTyping();
                console.error('Error submitting lead:', error);
                this.addBotMessage('Sorry, something went wrong. Please try emailing us directly.');
            }
        },
        
        /**
         * Validate email format
         */
        isValidEmail: function(email) {
            const re = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
            return re.test(email);
        },
        
        /**
         * Add user message to chat
         */
        addUserMessage: function(message) {
            const messagesContainer = document.getElementById('chatbot-messages');
            
            const messageEl = document.createElement('div');
            messageEl.className = 'message user';
            messageEl.innerHTML = `
                <div class="message-content">${this.escapeHtml(message)}</div>
            `;
            
            messagesContainer.appendChild(messageEl);
            this.scrollToBottom();
        },
        
        /**
         * Add bot message to chat
         */
        addBotMessage: function(message) {
            const messagesContainer = document.getElementById('chatbot-messages');
            
            const messageEl = document.createElement('div');
            messageEl.className = 'message bot';
            messageEl.innerHTML = `
                <img src="${this.config?.branding.bot_avatar || ''}" alt="Bot" class="message-avatar" onerror="this.style.display='none'">
                <div class="message-content">${this.escapeHtml(message).replace(/\n/g, '<br>')}</div>
            `;
            
            messagesContainer.appendChild(messageEl);
            this.scrollToBottom();
        },
        
        /**
         * Show typing indicator
         */
        showTyping: function() {
            const messagesContainer = document.getElementById('chatbot-messages');
            
            const typingEl = document.createElement('div');
            typingEl.className = 'message bot';
            typingEl.id = 'typing-indicator';
            typingEl.innerHTML = `
                <img src="${this.config?.branding.bot_avatar || ''}" alt="Bot" class="message-avatar" onerror="this.style.display='none'">
                <div class="message-content">
                    <div class="typing-indicator">
                        <span></span>
                        <span></span>
                        <span></span>
                    </div>
                </div>
            `;
            
            messagesContainer.appendChild(typingEl);
            this.scrollToBottom();
        },
        
        /**
         * Hide typing indicator
         */
        hideTyping: function() {
            const typingEl = document.getElementById('typing-indicator');
            if (typingEl) {
                typingEl.remove();
            }
        },
        
        /**
         * Scroll chat to bottom
         */
        scrollToBottom: function() {
            const messagesContainer = document.getElementById('chatbot-messages');
            messagesContainer.scrollTop = messagesContainer.scrollHeight;
        },
        
        /**
         * Escape HTML to prevent XSS
         */
        escapeHtml: function(text) {
            const div = document.createElement('div');
            div.textContent = text;
            return div.innerHTML;
        }
    };
    
    // Expose to global scope
    window.ChatbotWidget = ChatbotWidget;
})();