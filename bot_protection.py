"""
bot_protection.py
═══════════════════════════════════════════════════════════════════════════
Blocks non-search-engine bots at the application layer.

IMPORTANT — read before relying on this alone: robots.txt is honor-system
only, and this module is a second, real layer of enforcement — but it is
NOT the strongest available layer. Lumvi already sits behind Cloudflare
(see the custom-domain DNS setup); Cloudflare's Bot Fight Mode / Super Bot
Fight Mode operates at the edge with signals this module can't replicate
(behavioral fingerprinting, TLS/JA3 fingerprinting, a constantly-updated
reputation database) and has search-engine allowlisting built in. Turn
that on first, in the Cloudflare dashboard: Security → Bots. This module
is a defense-in-depth layer for anything that gets past the edge, or for
periods where edge protection isn't available.

How it works:
  1. Real browsers (the overwhelming majority of traffic) are recognised
     by their User-Agent and pass straight through — zero added latency,
     no DNS lookups. Only requests that self-identify as a bot (by UA
     string, or by having no UA / a known scraper-library UA) hit any of
     the logic below.
  2. A request whose UA claims to be a known search engine crawler
     (Googlebot, Bingbot, etc.) is verified with Forward-Confirmed
     Reverse DNS (FCrDNS) — the method Google and Bing themselves publish
     as the official way to distinguish real crawler traffic from
     spoofed User-Agent strings:
       a. Reverse DNS (PTR) lookup on the request IP.
       b. Check the hostname ends with that engine's verification domain
          (googlebot.com/google.com/googleusercontent.com for Google,
          search.msn.com for Bing, etc).
       c. Forward DNS (A) lookup on that hostname, confirm it resolves
          back to the SAME IP. This step is what stops an attacker from
          just pointing their own PTR record at a fake "googlebot.com"-
          looking name.
     Verified → allowed. Confirmed spoofed (wrong domain, or forward
     lookup resolves to a different IP) → blocked. DNS lookup itself
     failing/timing out is treated as INCONCLUSIVE, not a failure — some
     legitimate Bingbot IPs are known to have inconsistent PTR records
     (this is a real, documented issue, not a hypothetical), and the cost
     of wrongly blocking real Google/Bing traffic (lost SEO) is much
     higher than the cost of occasionally letting a UA-spoofing bot
     through this one layer — especially with Cloudflare as the primary
     defense in front of this.
  3. Everything else that self-identifies as a bot (scraper libraries,
     generic "bot"/"crawler"/"spider" UAs not on the known-engine list,
     empty UAs) is blocked outright.

Verified results are cached in-process per IP for 1 hour — a crawl
session hits many pages quickly; there's no need to re-run DNS lookups
for every single request from the same already-verified IP.
"""

import re
import socket
import time
from typing import Optional, Tuple

from flask import request, abort

from utils import get_logger

logger = get_logger('lumvi.bot_protection')


# ── Known search engine crawlers ───────────────────────────────────────────
# UA substring (case-insensitive) -> tuple of acceptable reverse-DNS suffixes.
# Add a new engine by adding one line here — nothing else needs to change.
# Verification domains per each engine's own published documentation.

_KNOWN_CRAWLERS = {
    'googlebot':     ('googlebot.com', 'google.com', 'googleusercontent.com'),
    'bingbot':       ('search.msn.com',),
    'slurp':         ('crawl.yahoo.net',),
    'duckduckbot':   ('duckduckgo.com',),
    'baiduspider':   ('baidu.com', 'baidu.jp'),
    'yandexbot':     ('yandex.com', 'yandex.net', 'yandex.ru'),
    'applebot':      ('applebot.apple.com',),
}

# Any UA containing one of these (case-insensitive), that ISN'T also one of
# the known crawlers above, is an unambiguous non-search-engine bot — no DNS
# lookup needed, just block. Not exhaustive (impossible to be); this catches
# common scraper libraries and self-identifying generic bots. Everything
# that doesn't match this list OR the known-crawler list is treated as a
# normal browser and passed through untouched.
_GENERIC_BOT_MARKERS = (
    'bot', 'crawl', 'spider', 'scrape', 'python-requests', 'python-urllib',
    'curl/', 'wget/', 'go-http-client', 'java/', 'libwww-perl', 'httpclient',
    'okhttp', 'axios/', 'node-fetch', 'aiohttp', 'phantomjs', 'headlesschrome',
    'scrapy',
)

_DNS_TIMEOUT_SECONDS = 2.0
_VERIFY_CACHE_TTL_SECONDS = 3600  # 1 hour

# ip -> (verified: bool, expiry_ts: float)
_verify_cache: dict = {}


def _cache_get(ip: str) -> Optional[bool]:
    entry = _verify_cache.get(ip)
    if not entry:
        return None
    verified, expiry = entry
    if time.time() > expiry:
        _verify_cache.pop(ip, None)
        return None
    return verified


def _cache_set(ip: str, verified: bool) -> None:
    _verify_cache[ip] = (verified, time.time() + _VERIFY_CACHE_TTL_SECONDS)


def _match_known_crawler(user_agent: str) -> Optional[Tuple[str, tuple]]:
    ua_lower = user_agent.lower()
    for name, domains in _KNOWN_CRAWLERS.items():
        if name in ua_lower:
            return name, domains
    return None


def _is_generic_bot(user_agent: str) -> bool:
    ua_lower = user_agent.lower()
    return any(marker in ua_lower for marker in _GENERIC_BOT_MARKERS)


def _verify_fcrdns(ip: str, valid_suffixes: tuple) -> Optional[bool]:
    """
    Forward-Confirmed Reverse DNS check. Returns True (verified), False
    (confirmed spoofed), or None (inconclusive — DNS lookup itself failed
    or timed out; treat as "don't block", not as "not verified").
    """
    old_timeout = socket.getdefaulttimeout()
    socket.setdefaulttimeout(_DNS_TIMEOUT_SECONDS)
    try:
        try:
            hostname, _, _ = socket.gethostbyaddr(ip)
        except (socket.herror, socket.gaierror, socket.timeout):
            return None  # no PTR record, or DNS unreachable — inconclusive

        hostname_lower = hostname.lower().rstrip('.')
        if not any(hostname_lower == d or hostname_lower.endswith('.' + d) for d in valid_suffixes):
            logger.info(f'[BotProtection] spoofed crawler: ip={ip} ptr={hostname_lower} (wrong domain)')
            return False

        try:
            forward_ip = socket.gethostbyname(hostname_lower)
        except (socket.gaierror, socket.timeout):
            return None  # forward lookup failed — inconclusive, not proof of spoofing

        if forward_ip != ip:
            logger.info(f'[BotProtection] spoofed crawler: ip={ip} ptr={hostname_lower} forward={forward_ip} (mismatch)')
            return False

        return True
    finally:
        socket.setdefaulttimeout(old_timeout)


def _client_ip() -> str:
    # Cloudflare sets CF-Connecting-IP; fall back to X-Forwarded-For (first
    # hop) then the raw socket address. Adjust if the proxy chain differs.
    return (
        request.headers.get('CF-Connecting-IP')
        or (request.headers.get('X-Forwarded-For', '').split(',')[0].strip())
        or request.remote_addr
        or ''
    )


def check_bot_request() -> None:
    """
    Flask before_request hook. Call abort(403) for anything that
    self-identifies as a bot and isn't a verified search engine crawler.
    Does nothing (returns None) for everything else — i.e. every normal
    browser request is completely unaffected.
    """
    user_agent = request.headers.get('User-Agent', '')

    if not user_agent:
        logger.info(f'[BotProtection] blocked: empty User-Agent ip={_client_ip()}')
        abort(403)

    crawler_match = _match_known_crawler(user_agent)
    if crawler_match is None:
        # Not claiming to be a known search engine. Block only if it looks
        # like a bot at all — real browsers pass through untouched here.
        if _is_generic_bot(user_agent):
            logger.info(f'[BotProtection] blocked: generic bot ua="{user_agent[:80]}" ip={_client_ip()}')
            abort(403)
        return  # normal browser traffic — no further checks

    # Claims to be a known search engine — verify it actually is one.
    name, valid_suffixes = crawler_match
    ip = _client_ip()
    if not ip:
        # Can't verify without an IP to check — fail safe by NOT blocking a
        # self-identified search engine on a missing-IP technicality; this
        # should be rare (Cloudflare always sets CF-Connecting-IP) and the
        # downside of wrongly blocking real Googlebot outweighs this edge
        # case.
        return

    cached = _cache_get(ip)
    if cached is True:
        return
    if cached is False:
        abort(403)

    verified = _verify_fcrdns(ip, valid_suffixes)
    if verified is True:
        _cache_set(ip, True)
        return
    if verified is False:
        _cache_set(ip, False)
        abort(403)

    # verified is None: DNS lookup was inconclusive (timeout / no PTR
    # record). Don't cache an inconclusive result — allow this one through
    # rather than risk blocking real search engine traffic; see module
    # docstring for why (documented Bingbot PTR inconsistencies).
    logger.info(f'[BotProtection] inconclusive DNS check for claimed {name}: ip={ip} — allowing')


def register_bot_protection(app) -> None:
    """
    Wire the bot check into every request. Call this from app.py's
    startup, the same way register_webhook_routes(app) is called.

    Excludes the webhook endpoints — those are hit by Shopify/WooCommerce/
    Acuity/Calendly/Square's own servers (not browsers, not search
    engines), and are already protected by their own HMAC signature
    verification (see webhooks.py). Blocking them here on a User-Agent
    technicality would break real order/appointment sync.
    """
    @app.before_request
    def _bot_protection_hook():
        if request.path.startswith('/webhooks/'):
            return
        check_bot_request()
