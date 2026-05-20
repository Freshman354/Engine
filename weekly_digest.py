"""
weekly_digest.py — Lumvi Weekly Agency Digest
==============================================

Sends each agency owner a personalised weekly email covering every
chatbot they manage:

  • Total conversations this week vs last week (+ % change)
  • Leads captured this week
  • Answer match rate (matched / total × 100)
  • Top 5 unanswered questions (from kb_gaps) with suggested FAQ copy
  • A plain-English "one thing to do this week" recommendation

Triggered two ways:
  1. External cron — GET/POST /cron/weekly-digest  (secured by CRON_SECRET)
  2. Admin panel  — POST /api/admin/send-weekly-digest

The digest is built entirely from existing tables:
  conversations  → volume, match rate
  kb_gaps        → unanswered questions
  leads          → lead count
  clients        → client names
  users          → agency email + plan

Email is sent via Flask-Mail (the same mail object used everywhere else).
The function is designed to be called from a background thread so it
never blocks a request.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# DATA LAYER
# ─────────────────────────────────────────────────────────────────────────────

def _get_agency_clients(user_id: int, conn, cursor) -> list[dict]:
    """All active (non-suspended) clients belonging to this user."""
    cursor.execute(
        """SELECT client_id, company_name
           FROM clients
           WHERE user_id = %s
             AND (is_suspended IS NULL OR is_suspended = FALSE)
           ORDER BY company_name""",
        (user_id,)
    )
    return [dict(r) for r in cursor.fetchall()]


def _conversation_stats(client_ids: list[str], conn, cursor) -> dict:
    """
    Returns per-client dicts with:
      this_week   — conversations in the last 7 days
      last_week   — conversations in the 7 days before that
      matched     — matched conversations this week
      total       — total conversations this week (for match rate)
    """
    if not client_ids:
        return {}

    now   = datetime.now(timezone.utc)
    w0    = now - timedelta(days=7)   # start of this week window
    w1    = now - timedelta(days=14)  # start of last week window

    result = {cid: {'this_week': 0, 'last_week': 0, 'matched': 0, 'total': 0}
              for cid in client_ids}

    # This week
    cursor.execute(
        """SELECT client_id,
                  COUNT(*)                              AS total,
                  SUM(CASE WHEN matched THEN 1 ELSE 0 END) AS matched_cnt
           FROM conversations
           WHERE client_id = ANY(%s)
             AND timestamp  >= %s
           GROUP BY client_id""",
        (client_ids, w0)
    )
    for r in cursor.fetchall():
        cid = r['client_id']
        result[cid]['this_week'] = int(r['total'])
        result[cid]['matched']   = int(r['matched_cnt'] or 0)
        result[cid]['total']     = int(r['total'])

    # Last week
    cursor.execute(
        """SELECT client_id, COUNT(*) AS total
           FROM conversations
           WHERE client_id = ANY(%s)
             AND timestamp  >= %s
             AND timestamp  <  %s
           GROUP BY client_id""",
        (client_ids, w1, w0)
    )
    for r in cursor.fetchall():
        result[r['client_id']]['last_week'] = int(r['total'])

    return result


def _lead_stats(client_ids: list[str], conn, cursor) -> dict:
    """Leads captured this week per client."""
    if not client_ids:
        return {}
    now = datetime.now(timezone.utc)
    w0  = now - timedelta(days=7)
    cursor.execute(
        """SELECT client_id, COUNT(*) AS cnt
           FROM leads
           WHERE client_id = ANY(%s)
             AND created_at >= %s
           GROUP BY client_id""",
        (client_ids, w0)
    )
    base   = {cid: 0 for cid in client_ids}
    for r in cursor.fetchall():
        base[r['client_id']] = int(r['cnt'])
    return base


def _top_gaps(client_id: str, limit: int, conn, cursor) -> list[dict]:
    """Top unanswered questions for a client this week, ordered by hit count."""
    now = datetime.now(timezone.utc)
    w0  = now - timedelta(days=7)
    cursor.execute(
        """SELECT question, count, confidence
           FROM kb_gaps
           WHERE client_id = %s
             AND last_seen >= %s
           ORDER BY count DESC, last_seen DESC
           LIMIT %s""",
        (client_id, w0, limit)
    )
    return [dict(r) for r in cursor.fetchall()]


def _get_active_users_for_digest(conn, cursor) -> list[dict]:
    """
    Agency users who should receive a digest:
      - plan is not 'free'
      - not an admin account
      - not cancelled / churned
    """
    cursor.execute(
        """SELECT id, email
           FROM users
           WHERE plan_type     != 'free'
             AND (is_admin      IS NULL OR is_admin = FALSE)
             AND (subscription_status IS NULL
                  OR subscription_status NOT IN ('cancelled'))
           ORDER BY id"""
    )
    return [dict(r) for r in cursor.fetchall()]


# ─────────────────────────────────────────────────────────────────────────────
# RECOMMENDATION ENGINE
# ─────────────────────────────────────────────────────────────────────────────

def _pick_recommendation(stats: dict, gap_count: int, leads: int) -> str:
    """
    Plain-English 'one thing to do this week' based on the agency's data.
    Returns a single sentence — no AI cost.
    """
    match_rate  = 0.0
    this_week   = stats.get('this_week', 0)
    last_week   = stats.get('last_week', 0)

    if stats.get('total', 0) > 0:
        match_rate = stats['matched'] / stats['total'] * 100

    if gap_count >= 5 and match_rate < 70:
        return (
            "Your bot is struggling to answer questions in a few key areas. "
            "Adding FAQ answers for the top 5 gaps listed below could push "
            "your match rate above 80% this week."
        )
    if leads == 0 and this_week > 10:
        return (
            "Your bot is getting conversations but no leads this week. "
            "Check that your lead-capture trigger is enabled in the chatbot settings — "
            "it may have been turned off accidentally."
        )
    if this_week < 5 and last_week < 5:
        return (
            "Traffic is very low on this bot. Consider sharing the chat widget link "
            "directly on your client's social profiles or email footer to drive "
            "more visitors to it."
        )
    if match_rate >= 85 and leads > 0:
        return (
            "Great week — this bot is performing well. Consider showing your client "
            "a screenshot of this report as a quick win to strengthen the relationship."
        )
    return (
        "Keep an eye on the unanswered questions below. Adding just 2–3 new FAQs "
        "each week compounds quickly and keeps the match rate climbing."
    )


# ─────────────────────────────────────────────────────────────────────────────
# HTML EMAIL BUILDER
# ─────────────────────────────────────────────────────────────────────────────

_BRAND_GRADIENT = "linear-gradient(135deg,#6366f1 0%,#7c3aed 50%,#a78bfa 100%)"
_BG_DARK        = "#0f172a"
_BG_CARD        = "#1e293b"
_BORDER         = "rgba(99,102,241,0.2)"
_TEXT_MUTED     = "#94a3b8"
_TEXT_DIM       = "#64748b"
_ACCENT         = "#818cf8"
_GREEN          = "#4ade80"
_RED            = "#f87171"
_YELLOW         = "#fbbf24"


def _pct_badge(this_week: int, last_week: int) -> str:
    """Return a coloured +/-% HTML badge comparing two values."""
    if last_week == 0:
        if this_week == 0:
            return f'<span style="color:{_TEXT_DIM};font-size:12px;">No data</span>'
        return f'<span style="color:{_GREEN};font-size:12px;">▲ New activity</span>'
    change = (this_week - last_week) / last_week * 100
    if change >= 0:
        colour = _GREEN
        arrow  = "▲"
    else:
        colour = _RED
        arrow  = "▼"
    return (
        f'<span style="color:{colour};font-size:12px;font-weight:700;">'
        f'{arrow} {abs(change):.0f}% vs last week</span>'
    )


def _stat_cell(label: str, value: str, badge: str = "") -> str:
    return f"""
      <td style="padding:16px;text-align:center;background:{_BG_DARK};
                 border-radius:10px;width:33%;">
        <div style="font-size:22px;font-weight:800;color:#e2e8f0;">{value}</div>
        <div style="font-size:11px;color:{_TEXT_DIM};margin-top:4px;text-transform:uppercase;
                    letter-spacing:0.5px;">{label}</div>
        {f'<div style="margin-top:6px;">{badge}</div>' if badge else ''}
      </td>"""


def _gap_rows(gaps: list[dict]) -> str:
    """HTML rows for the top unanswered questions table."""
    if not gaps:
        return f"""
        <tr><td colspan="2" style="padding:16px;color:{_TEXT_DIM};font-size:13px;text-align:center;">
          No unanswered questions this week — great job! 🎉
        </td></tr>"""

    rows = []
    for i, gap in enumerate(gaps, 1):
        q          = gap['question']
        count      = gap['count']
        confidence = gap.get('confidence', 0.0)

        # Suggest a short FAQ answer hint — directional, not authoritative
        if confidence < 0.3:
            hint = "Bot had no matching answer — add a dedicated FAQ entry."
        elif confidence < 0.6:
            hint = "Partial match found — consider expanding the existing answer."
        else:
            hint = "Near-match found — tune the FAQ trigger keywords."

        rows.append(f"""
        <tr>
          <td style="padding:12px 8px;border-bottom:1px solid rgba(255,255,255,0.05);">
            <div style="display:flex;align-items:flex-start;gap:10px;">
              <span style="background:{_BRAND_GRADIENT};color:#fff;border-radius:50%;
                           width:22px;height:22px;min-width:22px;display:inline-flex;
                           align-items:center;justify-content:center;
                           font-size:11px;font-weight:700;margin-top:1px;">{i}</span>
              <div>
                <div style="font-size:13px;font-weight:600;color:#e2e8f0;
                            line-height:1.5;">{q}</div>
                <div style="font-size:11px;color:{_TEXT_DIM};margin-top:3px;">{hint}</div>
              </div>
            </div>
          </td>
          <td style="padding:12px 8px;text-align:right;white-space:nowrap;
                     border-bottom:1px solid rgba(255,255,255,0.05);
                     vertical-align:top;">
            <span style="background:rgba(99,102,241,0.1);color:{_ACCENT};
                         border-radius:20px;padding:3px 10px;font-size:12px;
                         font-weight:700;">{count}×</span>
          </td>
        </tr>""")
    return "".join(rows)


def _client_section(client_name: str, client_id: str,
                    conv: dict, leads: int, gaps: list[dict],
                    dashboard_url: str) -> str:
    """Full HTML block for one client chatbot."""
    this_week  = conv.get('this_week', 0)
    last_week  = conv.get('last_week', 0)
    matched    = conv.get('matched', 0)
    total      = conv.get('total', 0)
    match_rate = round(matched / total * 100) if total > 0 else 0

    rate_colour = _GREEN if match_rate >= 75 else (_YELLOW if match_rate >= 50 else _RED)

    badge     = _pct_badge(this_week, last_week)
    rec       = _pick_recommendation(conv, len(gaps), leads)
    gap_html  = _gap_rows(gaps)

    return f"""
    <!-- ── CLIENT: {client_name} ── -->
    <tr><td style="padding:0 0 32px;">
      <table width="100%" cellpadding="0" cellspacing="0"
             style="background:{_BG_CARD};border-radius:14px;overflow:hidden;">

        <!-- Client header -->
        <tr><td style="background:{_BRAND_GRADIENT};padding:18px 24px;">
          <table width="100%" cellpadding="0" cellspacing="0">
            <tr>
              <td>
                <div style="font-size:16px;font-weight:800;color:#fff;">{client_name}</div>
                <div style="font-size:11px;color:rgba(255,255,255,0.7);margin-top:2px;">
                  ID: {client_id}
                </div>
              </td>
              <td align="right">
                <a href="{dashboard_url}/clients/{client_id}/analytics"
                   style="background:rgba(255,255,255,0.15);color:#fff;
                          text-decoration:none;padding:7px 16px;border-radius:8px;
                          font-size:12px;font-weight:700;">
                  View Analytics →
                </a>
              </td>
            </tr>
          </table>
        </td></tr>

        <!-- Stats row -->
        <tr><td style="padding:20px 24px;">
          <table width="100%" cellpadding="0" cellspacing="0">
            <tr>
              {_stat_cell("Conversations this week", str(this_week), badge)}
              <td style="width:12px;"></td>
              {_stat_cell("Leads captured", str(leads))}
              <td style="width:12px;"></td>
              {_stat_cell("Match rate",
                          f'<span style="color:{rate_colour};">{match_rate}%</span>')}
            </tr>
          </table>
        </td></tr>

        <!-- Recommendation -->
        <tr><td style="padding:0 24px 20px;">
          <div style="background:rgba(99,102,241,0.08);border:1px solid {_BORDER};
                      border-radius:10px;padding:14px 16px;">
            <div style="font-size:11px;font-weight:700;color:{_ACCENT};
                        text-transform:uppercase;letter-spacing:0.5px;margin-bottom:6px;">
              💡 One thing to do this week
            </div>
            <div style="font-size:13px;color:{_TEXT_MUTED};line-height:1.7;">
              {rec}
            </div>
          </div>
        </td></tr>

        <!-- Top unanswered questions -->
        <tr><td style="padding:0 24px 24px;">
          <div style="font-size:12px;font-weight:700;color:{_ACCENT};
                      text-transform:uppercase;letter-spacing:0.5px;margin-bottom:12px;">
            Top unanswered questions this week
          </div>
          <table width="100%" cellpadding="0" cellspacing="0"
                 style="background:{_BG_DARK};border-radius:10px;overflow:hidden;">
            {gap_html}
          </table>
          {'<div style="margin-top:10px;text-align:right;"><a href="' + dashboard_url + '/clients/' + client_id + '/knowledge-base" style="font-size:12px;color:' + _ACCENT + ';text-decoration:none;font-weight:600;">Add answers in FAQ Manager →</a></div>' if gaps else ''}
        </td></tr>

      </table>
    </td></tr>"""


def build_digest_html(user_email: str, clients_data: list[dict],
                      week_str: str, dashboard_url: str) -> str:
    """
    Assemble the full weekly digest HTML email for one agency owner.

    clients_data: list of dicts, each with keys:
      name, client_id, conv (dict), leads (int), gaps (list)
    """
    client_sections = "".join(
        _client_section(
            client_name=c['name'],
            client_id=c['client_id'],
            conv=c['conv'],
            leads=c['leads'],
            gaps=c['gaps'],
            dashboard_url=dashboard_url,
        )
        for c in clients_data
    )

    no_clients_msg = ""
    if not clients_data:
        no_clients_msg = f"""
        <tr><td style="padding:40px;text-align:center;color:{_TEXT_DIM};font-size:14px;">
          You don't have any active chatbots yet.
          <br><br>
          <a href="{dashboard_url}/create-client"
             style="background:{_BRAND_GRADIENT};color:#fff;text-decoration:none;
                    padding:12px 28px;border-radius:8px;font-weight:700;font-size:14px;">
            Create Your First Chatbot →
          </a>
        </td></tr>"""

    total_convos = sum(c['conv'].get('this_week', 0) for c in clients_data)
    total_leads  = sum(c['leads'] for c in clients_data)
    total_bots   = len(clients_data)

    return f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Your Lumvi Weekly Digest</title>
</head>
<body style="margin:0;padding:0;background:{_BG_DARK};
             font-family:'Inter',Arial,sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0"
       style="background:{_BG_DARK};padding:40px 0;">
<tr><td align="center">
<table width="600" cellpadding="0" cellspacing="0"
       style="max-width:600px;width:100%;">

  <!-- ── HEADER ── -->
  <tr><td style="background:{_BRAND_GRADIENT};border-radius:16px 16px 0 0;
                 padding:32px 40px;text-align:center;">
    <div style="display:inline-block;background:rgba(255,255,255,0.15);
                border-radius:10px;padding:8px 18px;margin-bottom:14px;">
      <span style="font-size:22px;font-weight:900;color:#fff;
                   letter-spacing:-0.5px;">⚡ Lumvi</span>
    </div>
    <h1 style="margin:0;font-size:22px;font-weight:800;color:#fff;line-height:1.3;">
      Your Weekly Chatbot Report
    </h1>
    <p style="margin:8px 0 0;font-size:13px;color:rgba(255,255,255,0.75);">
      Week of {week_str}
    </p>
  </td></tr>

  <!-- ── SUMMARY BAR ── -->
  <tr><td style="background:#1a2744;padding:20px 40px;">
    <table width="100%" cellpadding="0" cellspacing="0">
      <tr>
        <td style="text-align:center;border-right:1px solid rgba(255,255,255,0.08);
                   padding:0 16px;">
          <div style="font-size:24px;font-weight:900;color:#e2e8f0;">{total_bots}</div>
          <div style="font-size:11px;color:{_TEXT_DIM};text-transform:uppercase;
                      letter-spacing:0.5px;margin-top:2px;">Active bots</div>
        </td>
        <td style="text-align:center;border-right:1px solid rgba(255,255,255,0.08);
                   padding:0 16px;">
          <div style="font-size:24px;font-weight:900;color:#e2e8f0;">{total_convos}</div>
          <div style="font-size:11px;color:{_TEXT_DIM};text-transform:uppercase;
                      letter-spacing:0.5px;margin-top:2px;">Conversations</div>
        </td>
        <td style="text-align:center;padding:0 16px;">
          <div style="font-size:24px;font-weight:900;color:{_GREEN};">{total_leads}</div>
          <div style="font-size:11px;color:{_TEXT_DIM};text-transform:uppercase;
                      letter-spacing:0.5px;margin-top:2px;">Leads captured</div>
        </td>
      </tr>
    </table>
  </td></tr>

  <!-- ── BODY ── -->
  <tr><td style="background:{_BG_DARK};padding:32px 40px;">
    <table width="100%" cellpadding="0" cellspacing="0">
      {client_sections}
      {no_clients_msg}
    </table>
  </td></tr>

  <!-- ── CTA ── -->
  <tr><td style="background:{_BG_CARD};padding:28px 40px;text-align:center;">
    <a href="{dashboard_url}/dashboard"
       style="display:inline-block;background:{_BRAND_GRADIENT};color:#fff;
              text-decoration:none;padding:14px 36px;border-radius:10px;
              font-weight:800;font-size:14px;">
      Open My Dashboard →
    </a>
    <p style="margin:16px 0 0;font-size:12px;color:{_TEXT_DIM};line-height:1.7;">
      Reply to this email any time — I read every response.<br>
      <a href="{dashboard_url}/settings/notifications"
         style="color:{_TEXT_DIM};text-decoration:underline;">
        Manage digest preferences
      </a>
    </p>
  </td></tr>

  <!-- ── FOOTER ── -->
  <tr><td style="background:{_BG_DARK};border-radius:0 0 16px 16px;
                 padding:20px 40px;text-align:center;">
    <p style="margin:0;color:#334155;font-size:12px;">
      © {datetime.now().year} Lumvi &middot;
      <a href="https://lumvi.net" style="color:#475569;text-decoration:none;">lumvi.net</a>
      &middot;
      <a href="https://lumvi.net/privacy-policy"
         style="color:#475569;text-decoration:none;">Privacy</a>
    </p>
  </td></tr>

</table>
</td></tr>
</table>
</body>
</html>"""


# ─────────────────────────────────────────────────────────────────────────────
# MAIN ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def send_weekly_digests(mail, app_context, dashboard_url: Optional[str] = None) -> dict:
    """
    Build and send a weekly digest email to every qualifying agency user.

    Parameters
    ----------
    mail          : Flask-Mail `mail` object (imported from app.py)
    app_context   : the Flask `app` instance (used for app_context + logger)
    dashboard_url : Base URL of the app, e.g. "https://lumvi.net"
                    Falls back to APP_URL env var, then "https://lumvi.net".

    Returns a summary dict: {sent, skipped, errors, total_users}
    """
    import models
    from flask_mail import Message

    base_url = (
        dashboard_url
        or os.environ.get('APP_URL', 'https://lumvi.net')
    ).rstrip('/')

    week_str = datetime.now().strftime('%B %d, %Y')

    summary = {'sent': 0, 'skipped': 0, 'errors': 0, 'total_users': 0}

    try:
        conn, cursor = models.get_db()
    except Exception as e:
        log.error(f"[WeeklyDigest] DB connection failed: {e}")
        summary['errors'] += 1
        return summary

    try:
        users = _get_active_users_for_digest(conn, cursor)
        summary['total_users'] = len(users)

        for user in users:
            user_id    = user['id']
            user_email = user['email']

            try:
                clients = _get_agency_clients(user_id, conn, cursor)

                if not clients:
                    # User has no bots yet — still send but with empty state
                    # so the email is a gentle nudge to create their first bot
                    clients_data = []
                else:
                    client_ids   = [c['client_id'] for c in clients]
                    conv_stats   = _conversation_stats(client_ids, conn, cursor)
                    lead_stats   = _lead_stats(client_ids, conn, cursor)

                    clients_data = []
                    for c in clients:
                        cid  = c['client_id']
                        gaps = _top_gaps(cid, limit=5, conn=conn, cursor=cursor)
                        clients_data.append({
                            'name':      c['company_name'],
                            'client_id': cid,
                            'conv':      conv_stats.get(cid, {}),
                            'leads':     lead_stats.get(cid, 0),
                            'gaps':      gaps,
                        })

                    # Skip users whose bots had zero activity this week
                    # (avoids spamming agencies with dormant accounts)
                    total_activity = sum(
                        c['conv'].get('this_week', 0) + c['leads']
                        for c in clients_data
                    )
                    if total_activity == 0 and len(clients_data) > 0:
                        log.info(
                            f"[WeeklyDigest] Skipping user {user_id} "
                            f"({user_email}) — no activity this week"
                        )
                        summary['skipped'] += 1
                        continue

                html_body = build_digest_html(
                    user_email=user_email,
                    clients_data=clients_data,
                    week_str=week_str,
                    dashboard_url=base_url,
                )

                with app_context.app_context():
                    msg = Message(
                        subject=f"📊 Your Lumvi weekly report — week of {week_str}",
                        sender="Lumvi <support@lumvi.net>",
                        recipients=[user_email],
                        html=html_body,
                    )
                    mail.send(msg)

                log.info(
                    f"[WeeklyDigest] Sent to user {user_id} ({user_email}) "
                    f"— {len(clients_data)} bot(s)"
                )
                summary['sent'] += 1

            except Exception as user_err:
                log.error(
                    f"[WeeklyDigest] Failed for user {user_id} "
                    f"({user_email}): {user_err}"
                )
                summary['errors'] += 1
                continue

    finally:
        try:
            cursor.close()
            conn.close()
        except Exception:
            pass

    log.info(
        f"[WeeklyDigest] Complete — "
        f"sent={summary['sent']} skipped={summary['skipped']} "
        f"errors={summary['errors']} total={summary['total_users']}"
    )
    return summary


# ─────────────────────────────────────────────────────────────────────────────
# MIGRATION — digest_preferences column
# ─────────────────────────────────────────────────────────────────────────────

def migrate_digest_preferences():
    """
    Adds a `digest_enabled` boolean column to the users table.
    Defaults to TRUE so all existing paid users are opted in automatically.
    Safe to call every startup — fully idempotent (IF NOT EXISTS).

    Call this from app.py alongside the other migrate_* calls.
    """
    import models
    try:
        conn, cursor = models.get_db()
        cursor.execute(
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS "
            "digest_enabled BOOLEAN DEFAULT TRUE"
        )
        conn.commit()
        cursor.close()
        conn.close()
        print("✅ migrate_digest_preferences complete")
    except Exception as e:
        print(f"⚠️  migrate_digest_preferences: {e}")
