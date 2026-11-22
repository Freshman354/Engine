# Shopify Development Client Seed Report

## Request

Add a development-only script that reads `SHOPIFY_DEV_DOMAIN` and
`SHOPIFY_DEV_ACCESS_TOKEN` from `.env`, creates a test client and Shopify
integration in the development database, stores the token using the same
encryption as the OAuth flow, refuses to run without `--dev`, and prints the
client ID without printing or logging the token. Then run the script and the
Shopify lookup verifier for the created client.

## Work Completed

Added `scripts/seed_dev_shopify_client.py`.

- Requires the explicit `--dev` argument at argument parsing, before loading
  environment configuration or importing database modules.
- Loads `.env` from the repository root and checks for database, encryption,
  and Shopify development settings.
- Validates that the configured shop is a `.myshopify.com` hostname.
- Creates or finds a dedicated synthetic test owner, creates an ecommerce
  client, and stores the Shopify platform integration through
  `webhooks.upsert_integration()`, the write path used by Shopify OAuth.
- That write path encrypts `access_token` through the shared
  `crypto_utils.encrypt_credentials()` Fernet mechanism.
- The script's output contains only a client ID on success. Error output is
  limited to missing setting names or exception class names; it omits
  exception details that might contain sensitive data.

## Validation and Run Results

- Ran the script without `--dev`: argument parsing refused to proceed.
- Ran the script with `--dev`: its preflight stopped with
  `ERROR: missing required configuration: INTEGRATION_ENCRYPTION_KEY`.
- Checked editor diagnostics for the new script: no errors reported.
- No client or integration rows were created. The preflight happens before
  application/database modules are imported.
- Did not run `scripts/verify_shopify_lookup.py`: the seeder did not create a
  client, so there is no resulting client ID to pass to the verifier.

## Blockers and Safety Note

`SHOPIFY_DEV_DOMAIN` and `SHOPIFY_DEV_ACCESS_TOKEN` are configured, but
`INTEGRATION_ENCRYPTION_KEY` is not present in the inspected `.env` or process
environment. The configured `DATABASE_URL` points to a Render Postgres host
and database named `lumvi`, and the repository has no separate dev database
variable or code-level development-database marker. For that reason, the
database write was not attempted; the target could not be confirmed as the
requested development database.

To complete the requested run safely, configure the development database URL
as `DATABASE_URL` and the matching OAuth-compatible `INTEGRATION_ENCRYPTION_KEY`
in the local environment, then run:

```powershell
& 'c:/Users/DELL/Chatbot-engine/venv/Scripts/python.exe' scripts/seed_dev_shopify_client.py --dev
& 'c:/Users/DELL/Chatbot-engine/venv/Scripts/python.exe' scripts/verify_shopify_lookup.py --client-id <printed-client-id>
```

No Shopify token value is included in this report.