"""Create a development client connected to the configured Shopify test store."""

from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path

_HARMLESS_IMPORT_PROBES = {
    "aioquic", "backports", "brotli", "brotlicffi", "chardet", "fcntl",
    "h2", "posix", "simplejson", "socks", "wmi",
}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dev",
        action="store_true",
        required=True,
        help="confirm this operation targets the development database",
    )
    return parser


def main() -> int:
    _parser().parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    try:
        from dotenv import load_dotenv

        load_dotenv(repo_root / ".env")
    except ModuleNotFoundError as error:
        if error.name in _HARMLESS_IMPORT_PROBES:
            pass
        else:
            print(f"ERROR: unable to load .env (missing module: {error.name or 'unknown'})")
            return 2
    except Exception as error:
        print(f"ERROR: unable to load .env ({type(error).__name__})")
        return 2

    required = (
        "DATABASE_URL",
        "INTEGRATION_ENCRYPTION_KEY",
        "SHOPIFY_DEV_DOMAIN",
        "SHOPIFY_DEV_ACCESS_TOKEN",
    )
    missing = [name for name in required if not os.environ.get(name, "").strip()]
    if missing:
        print(f"ERROR: missing required configuration: {', '.join(missing)}")
        return 2

    shop_domain = os.environ["SHOPIFY_DEV_DOMAIN"].strip().lower()
    shop_domain = re.sub(r"^https?://", "", shop_domain).strip("/")
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]*\.myshopify\.com", shop_domain):
        print("ERROR: SHOPIFY_DEV_DOMAIN must be a Shopify .myshopify.com hostname")
        return 2

    try:
        import models
        import webhooks

        owner = models.create_or_link_shopify_user(
            f"shopify-dev-seed+{shop_domain.split('.')[0]}@installs.lumvi.net"
        )
        if not owner:
            print("ERROR: unable to create or find the development test owner")
            return 2

        company_name = f"Shopify Dev Test {shop_domain.split('.')[0]}"
        client_id = models.create_client(owner["id"], company_name, vertical="ecommerce")
        saved = webhooks.upsert_integration(
            client_id,
            "shopify",
            "",
            {
                "shop_domain": shop_domain,
                "access_token": os.environ["SHOPIFY_DEV_ACCESS_TOKEN"].strip(),
                "shopify_scopes": "read_products,read_inventory",
                "order_lookup_enabled": True,
                "inventory_enabled": True,
            },
        )
        if not saved:
            print("ERROR: unable to save the Shopify integration (details omitted)")
            return 2
    except ModuleNotFoundError as error:
        if error.name in _HARMLESS_IMPORT_PROBES:
            pass
        else:
            print(
                "ERROR: unable to create the development Shopify client "
                f"(missing module: {error.name or 'unknown'})"
            )
            return 2
    except Exception as error:
        print(f"ERROR: unable to create the development Shopify client ({type(error).__name__})")
        return 2

    print(f"Client ID: {client_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())