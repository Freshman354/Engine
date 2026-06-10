"""
models/__init__.py
------------------
Re-exports every public function from every sub-module so that all
existing call sites (blueprints, app.py, admin_routes, ai_helper, etc.)
continue to work without any changes.

    import models
    models.get_faqs(client_id)      # ✓ unchanged
    models.get_db()                 # ✓ unchanged

The split is purely internal to this package.
"""

# ── Database connection ───────────────────────────────────────────────────────
from .db import (
    get_db,
    get_db_connection,
)

# ── Schema migrations ─────────────────────────────────────────────────────────
from .migrations import (
    init_db,
    migrate_clients_table,
    migrate_clients_active,
    migrate_white_label,
    migrate_client_status,
    migrate_cron_tables,
    migrate_onboarding,
    migrate_faqs_table,
    migrate_faq_to_knowledge_base,
    migrate_google_oauth,
    migrate_password_reset_tokens,
    migrate_lead_custom_fields,
    migrate_lead_pipeline,
    migrate_admin_columns,
    migrate_payments_and_events,
    migrate_agency_seat_billing,
    migrate_conversation_features,
    migrate_poor_answers,
    migrate_chat_sessions,
    migrate_kb_gaps,
    migrate_kb_gap_status,
    migrate_knowledge_base,
    migrate_webhooks,
    migrate_api_usage_log,
    migrate_subscription_expiry,
    migrate_to_recurring_subscriptions,
)

# ── Cron infrastructure ───────────────────────────────────────────────────────
from .cron import (
    log_cron_run,
    get_cron_last_run,
    get_cron_history,
    prune_old_logs,
    get_clients_for_weekly_digest_due,
    mark_digest_sent,
    upsert_usage_warning,
    get_usage_warning,
)

# ── Users ─────────────────────────────────────────────────────────────────────
from .users import (
    create_user,
    verify_user,
    get_user_by_id,
    get_user_by_email,
    get_user_by_google_id,
    create_or_link_google_user,
    save_password_reset_token,
    get_password_reset_token,
    delete_password_reset_token,
    update_user_password,
    mark_onboarding_complete,
)

# ── Billing & subscriptions ───────────────────────────────────────────────────
from .billing import (
    update_user_subscription,
    cancel_user_subscription,
    set_subscription_expiry,
    downgrade_expired_users,
    downgrade_single_user,
    track_event,
    record_payment,
    get_all_payments,
    get_mrr,
    get_total_revenue,
    get_revenue_by_month,
    get_all_users,
    record_agency_overage_seat,
    get_agency_users_with_overage,
    get_agency_overage_summary,
)

# ── Clients ───────────────────────────────────────────────────────────────────
from .clients import (
    create_client,
    get_user_clients,
    get_client_by_id,
    verify_client_ownership,
    delete_client,
    toggle_client_suspended,
    clone_client,
    is_valid_domain,
    check_domain_dns,
    get_client_by_custom_domain,
    save_white_label_settings,
    get_email_from_for_client,
    save_agency_branding,
    get_agency_branding,
    get_clients_enriched_stats,
    get_all_clients,
)

# ── Conversations ─────────────────────────────────────────────────────────────
from .conversations import (
    get_daily_message_count,
    get_client_owner,
    get_conversation_message_count,
    save_conversation_summary,
    get_latest_conversation_summary,
    get_recent_conversations,
    get_conversations,
)

# ── FAQs ──────────────────────────────────────────────────────────────────────
from .faqs import (
    validate_and_enrich_faqs,
    save_faqs,
    get_faqs,
    delete_all_faqs,
    get_leads_this_month_bulk,
    get_unanswered_questions_for_email,
    get_clients_for_weekly_digest,
)

# ── Leads ─────────────────────────────────────────────────────────────────────
from .leads import (
    save_lead,
    get_lead_by_id,
    update_lead,
    delete_lead_by_client,
    bulk_update_leads,
    get_leads,
    get_all_leads_admin,
    admin_delete_lead,
)

# ── Knowledge base ────────────────────────────────────────────────────────────
from .knowledge import (
    store_faq_embedding,
    get_faq_embeddings,
    get_knowledge_chunks,
    get_knowledge_chunks_raw,
    get_relevant_knowledge,
    store_embedding,
    get_embeddings_for_client,
    delete_knowledge_chunks,
    delete_knowledge_base,
    save_knowledge_chunks,
)

# ── KB gaps & poor answers ────────────────────────────────────────────────────
from .kb_gaps import (
    record_kb_gap,
    get_kb_gaps,
    get_kb_gap_digest_last_sent,
    set_kb_gap_digest_last_sent,
    record_poor_answer,
    get_poor_answers,
    mark_kb_gap_resolved,
)

# ── Chat sessions ─────────────────────────────────────────────────────────────
from .sessions import (
    load_session,
    upsert_session,
    delete_session,
)

# ── Webhooks ──────────────────────────────────────────────────────────────────
from .webhooks import (
    get_webhooks,
    save_webhooks,
    get_signing_secret,
    regenerate_signing_secret,
    log_webhook_delivery,
    get_webhook_logs,
)

# ── Articles ──────────────────────────────────────────────────────────────────
from .articles import (
    get_articles,
    get_article_by_id,
    create_article,
    update_article,
    delete_article,
)

# ── Client portal users ───────────────────────────────────────────────────────
from .client_users import (
    create_client_user,
    verify_client_user,
    get_client_users,
    get_client_user_by_id,
    delete_client_user,
    update_client_user_password,
)

# ── Affiliate programme ───────────────────────────────────────────────────────
from .affiliate import (
    create_affiliate,
    get_affiliate_by_user_id,
    get_affiliate_by_code,
    create_referral,
    create_commission,
    get_affiliate_stats,
    get_affiliate_commissions,
)

# ── Analytics & admin reporting ───────────────────────────────────────────────
from .analytics import (
    get_user_count_by_plan,
    get_new_users_this_month,
    get_user_growth_by_month,
    admin_update_user,
    admin_delete_user,
    log_api_usage,
    get_api_cost_summary,
    get_top_chatbots_by_cost,
    get_user_cost_breakdown,
    get_user_ai_costs_dict,
    get_cost_revenue_by_month,
    get_daily_burn_last_30,
    purge_old_api_logs,
    get_db_stats,
    get_churn_this_week,
    get_past_due_count,
    get_active_subscription_count,
    get_paid_user_count,
    get_free_user_count,
    get_total_client_count,
    get_analytics_events,
    get_event_counts,
    get_conversion_funnel,
)

# ── Tags (conversation tagging) ───────────────────────────────────────────────
from .tags import (
    get_client_tags,
    create_tag,
    delete_tag,
    apply_tag,
    remove_tag,
    get_session_tags,
)

# ── Tier 1 — session extensions ───────────────────────────────────────────────
from .sessions import (
    submit_csat,
    set_session_status,
    get_session_status,
    set_agent_typing,
    get_agent_typing,
)

# ── Tier 1 — client extensions ────────────────────────────────────────────────
from .clients import (
    check_business_hours,
    get_proactive_triggers,
    save_proactive_trigger,
    delete_proactive_trigger,
)

# ── Tier 1 — migrations ───────────────────────────────────────────────────────
from .migrations import (
    migrate_page_context,
    migrate_csat,
    migrate_conversation_status,
    migrate_conversation_tags,
    migrate_proactive_triggers,
)
