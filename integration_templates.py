"""
integration_templates.py
=========================
Pre-built configs for common platforms an agency's clients already use.
Picking a template in the Agent Actions dashboard pre-fills the
integration's base_url pattern, auth type, and a starter set of actions —
instead of an agency hand-typing param_mapping/response_mapping from
scratch for every single client on the same platform.

This is Lumvi-curated content (like vertical_prompts.py), not
agency-editable — agencies can still edit/add/remove actions after
applying a template, same as any other integration.

Structure per template:
  name             — display name shown in the dashboard picker
  platform         — short key, used as the option value
  base_url         — real API base, or a {slug}-style placeholder the
                     agency fills in during setup if the client's URL varies
  auth_type        — 'api_key' | 'bearer' | 'basic'
  auth_hint        — short help text shown next to the credential field
  actions          — list of starter action dicts (same shape add_action()
                     expects). Endpoint paths and field names are the
                     platform's REAL, documented API shape as of this
                     writing — agencies should still verify against their
                     client's actual API version before going live; API
                     shapes drift and are not guaranteed to be current.
"""
from typing import Dict, List

INTEGRATION_TEMPLATES: Dict[str, Dict] = {

    'calendly': {
        'name': 'Calendly',
        'platform': 'calendly',
        'base_url': 'https://api.calendly.com',
        'auth_type': 'bearer',
        'auth_hint': 'Personal Access Token from Calendly Integrations settings.',
        'actions': [
            {
                'action_name': 'check_availability',
                'description': 'Checks available time slots for a given event type.',
                'http_method': 'GET',
                'endpoint_path': '/event_type_available_times',
                'param_mapping': {'event_type_uri': 'event_type', 'start_time': 'start_time', 'end_time': 'end_time'},
                'response_mapping': {'slots': 'collection'},
                'requires_confirmation': False,
            },
            {
                'action_name': 'cancel_booking',
                'description': "Cancels a customer's scheduled event.",
                'http_method': 'POST',
                'endpoint_path': '/scheduled_events/{uuid}/cancellation',
                'param_mapping': {'event_uuid': 'uuid', 'reason': 'reason'},
                'response_mapping': {'status': 'resource.status'},
                'requires_confirmation': True,
            },
        ],
    },

    'acuity': {
        'name': 'Acuity Scheduling',
        'platform': 'acuity',
        'base_url': 'https://acuityscheduling.com/api/v1',
        'auth_type': 'basic',
        'auth_hint': 'User ID as username, API Key as password (Acuity Integrations > API).',
        'actions': [
            {
                'action_name': 'check_availability',
                'description': 'Lists open appointment slots for a date and appointment type.',
                'http_method': 'GET',
                'endpoint_path': '/availability/times',
                'param_mapping': {'date': 'date', 'appointment_type_id': 'appointmentTypeID'},
                'response_mapping': {'slots': 'time'},
                'requires_confirmation': False,
            },
            {
                'action_name': 'book_appointment',
                'description': "Books a new appointment on the client's calendar.",
                'http_method': 'POST',
                'endpoint_path': '/appointments',
                'param_mapping': {
                    'date_time': 'datetime', 'appointment_type_id': 'appointmentTypeID',
                    'first_name': 'firstName', 'last_name': 'lastName', 'email': 'email',
                },
                'response_mapping': {'confirmation_id': 'id'},
                'requires_confirmation': True,
            },
            {
                'action_name': 'cancel_appointment',
                'description': "Cancels a customer's appointment.",
                'http_method': 'PUT',
                'endpoint_path': '/appointments/{id}/cancel',
                'param_mapping': {'appointment_id': 'id'},
                'response_mapping': {'status': 'status'},
                'requires_confirmation': True,
            },
        ],
    },

    'shopify': {
        'name': 'Shopify',
        'platform': 'shopify',
        'base_url': 'https://{store}.myshopify.com/admin/api/2024-10',
        'auth_type': 'api_key',
        'auth_hint': "Admin API access token. Header name should stay 'X-Shopify-Access-Token'.",
        'actions': [
            {
                'action_name': 'check_order_status',
                'description': "Looks up a customer's order status by order number.",
                'http_method': 'GET',
                'endpoint_path': '/orders.json',
                'param_mapping': {'order_number': 'name'},
                'response_mapping': {'orders': 'orders'},
                'requires_confirmation': False,
            },
            {
                'action_name': 'initiate_return',
                'description': 'Starts a return request for an order.',
                'http_method': 'POST',
                'endpoint_path': '/orders/{order_id}/return.json',
                'param_mapping': {'order_id': 'order_id', 'reason': 'note'},
                'response_mapping': {'return_id': 'return.id'},
                'requires_confirmation': True,
            },
        ],
    },

    'square': {
        'name': 'Square Appointments',
        'platform': 'square',
        'base_url': 'https://connect.squareup.com/v2',
        'auth_type': 'bearer',
        'auth_hint': 'Access token from Square Developer Dashboard.',
        'actions': [
            {
                'action_name': 'check_availability',
                'description': 'Searches for open booking slots.',
                'http_method': 'POST',
                'endpoint_path': '/bookings/availability/search',
                'param_mapping': {'start_at': 'start_at', 'service_variation_id': 'service_variation_id'},
                'response_mapping': {'slots': 'availabilities'},
                'requires_confirmation': False,
            },
            {
                'action_name': 'book_appointment',
                'description': 'Creates a new booking.',
                'http_method': 'POST',
                'endpoint_path': '/bookings',
                'param_mapping': {
                    'start_at': 'start_at', 'customer_id': 'customer_id',
                    'service_variation_id': 'service_variation_id',
                },
                'response_mapping': {'booking_id': 'booking.id'},
                'requires_confirmation': True,
            },
        ],
    },

}


def list_templates() -> List[Dict]:
    """Summary list for the template picker dropdown — no action detail."""
    return [
        {'platform': t['platform'], 'name': t['name'], 'auth_type': t['auth_type'],
         'auth_hint': t['auth_hint'], 'action_count': len(t['actions']),
         'base_url': t['base_url']}
        for t in INTEGRATION_TEMPLATES.values()
    ]


def get_template(platform: str) -> Dict:
    """Full template detail (base_url, auth_type, actions) for one platform key."""
    return INTEGRATION_TEMPLATES.get(platform, {})
