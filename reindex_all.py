#!/usr/bin/env python3
"""
reindex_all.py — Re-index all Lumvi clients after an embedding model change.

Run this once after:
  - Switching to Voyage AI (voyage-3-lite, 512-dim)
  - Any future embedding model change
  - Bulk FAQ updates across all tenants

Usage:
    # Re-index every client in your DB:
    python reindex_all.py

    # Re-index specific clients only:
    python reindex_all.py --clients client_abc client_xyz

    # Faster (higher Voyage API concurrency — use on paid plan):
    python reindex_all.py --concurrency 5

    # Dry run — lists clients and FAQ counts, does not write anything:
    python reindex_all.py --dry-run

On Render: add this as a one-off job in the dashboard, or run via:
    render run python reindex_all.py
"""

import argparse
import logging
import os
import sys
import time

# ── Logging setup ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%dT%H:%M:%S',
)
logger = logging.getLogger('reindex_all')


def main():
    parser = argparse.ArgumentParser(description='Re-index all Lumvi client embeddings')
    parser.add_argument(
        '--clients', nargs='*', default=None,
        help='Specific client IDs to re-index. Defaults to all clients in DB.',
    )
    parser.add_argument(
        '--concurrency', type=int, default=3,
        help='Parallel workers (default 3 — safe for Voyage free tier).',
    )
    parser.add_argument(
        '--delay', type=float, default=1.0,
        help='Seconds to wait between clients (default 1.0).',
    )
    parser.add_argument(
        '--dry-run', action='store_true',
        help='List clients and FAQ counts without writing any embeddings.',
    )
    args = parser.parse_args()

    # ── Validate env ──────────────────────────────────────────────────────────
    if not os.environ.get('VOYAGE_API_KEY'):
        logger.error("VOYAGE_API_KEY is not set — aborting.")
        sys.exit(1)

    # ── Import app modules ────────────────────────────────────────────────────
    try:
        import models as _m
        from ai_helper import AIHelper
    except ImportError as e:
        logger.error(f"Import failed: {e}  — run this script from your app root directory.")
        sys.exit(1)

    # ── Dry run: just list clients ────────────────────────────────────────────
    if args.dry_run:
        try:
            if args.clients:
                client_ids = args.clients
            else:
                all_clients = _m.get_all_clients()
                client_ids  = [
                    str(c.get('id') or c.get('client_id', ''))
                    for c in all_clients
                    if c.get('id') or c.get('client_id')
                ]

            logger.info(f"[DryRun] {len(client_ids)} clients would be re-indexed:")
            total_faqs = 0
            for cid in client_ids:
                faqs = _m.get_faqs(cid) or []
                logger.info(f"  {cid}: {len(faqs)} FAQs")
                total_faqs += len(faqs)
            logger.info(
                f"[DryRun] Total: {total_faqs} FAQs across {len(client_ids)} clients "
                f"(~{total_faqs * 5} Voyage API calls including paraphrases)"
            )
        except Exception as e:
            logger.error(f"Dry run failed: {e}")
            sys.exit(1)
        return

    # ── Real run ──────────────────────────────────────────────────────────────
    helper  = AIHelper()
    t0      = time.monotonic()

    results = helper.reindex_all_clients(
        client_ids  = args.clients,
        concurrency = args.concurrency,
        delay_between_clients = args.delay,
    )

    # ── Final report ──────────────────────────────────────────────────────────
    elapsed   = time.monotonic() - t0
    succeeded = {cid: n for cid, n in results.items() if n >= 0}
    failed    = {cid: n for cid, n in results.items() if n  < 0}

    print("\n" + "═" * 55)
    print(f"  Re-index complete — {elapsed:.1f}s")
    print("═" * 55)
    print(f"  Clients OK  : {len(succeeded)}")
    print(f"  Clients FAIL: {len(failed)}")
    print(f"  Total embeds: {sum(succeeded.values())}")
    if failed:
        print(f"\n  Failed clients: {list(failed.keys())}")
        print("  Check logs above for per-client crash details.")
    print("═" * 55 + "\n")

    if failed:
        sys.exit(1)   # non-zero exit so Render marks the job as failed


if __name__ == '__main__':
    main()
