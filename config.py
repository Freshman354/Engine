"""
Production configuration settings
"""

import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    """Base configuration"""
    SECRET_KEY = os.getenv('SECRET_KEY', 'dev-secret-key-change-me')
    DEBUG = os.getenv('DEBUG', 'False').lower() == 'true'
    DATABASE = 'chatbot.db'
    
    # Gemini API Configuration - ACTUALLY WORKS
    GEMINI_API_KEY = os.getenv('GEMINI_API_KEY', '')
    GEMINI_MODEL = 'gemini-2.0-flash'  # Updated to the latest model
    USE_AI = os.getenv('USE_AI', 'True').lower() == 'true'
    
    # Plan limits...
    
    # Plan limits
    PLAN_LIMITS = {
        'free': {
            'clients': 1,
            'faqs_per_client': 5,
            'messages_per_day': 50,
            'analytics': False,
            'customization': False
        },
        'starter': {
            'clients': 5,
            'faqs_per_client': 999999,
            'messages_per_day': 999999,
            'analytics': True,
            'customization': False
        },
        'agency': {
            'clients': 15,
            'faqs_per_client': 999999,
            'messages_per_day': 999999,
            'analytics': True,
            'customization': True
        },
        'enterprise': {
            'clients': 999999,
            'faqs_per_client': 999999,
            'messages_per_day': 999999,
            'analytics': True,
            'customization': True
        }
    }

class ProductionConfig(Config):
    """Production configuration"""
    DEBUG = False
    TESTING = False

class DevelopmentConfig(Config):
    """Development configuration"""
    DEBUG = True
    TESTING = False

class TestingConfig(Config):
    """Testing configuration"""
    DEBUG = True
    TESTING = True

# Configuration dictionary
config = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'testing': TestingConfig,
    'default': DevelopmentConfig
}