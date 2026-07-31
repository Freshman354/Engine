# Shopify App Embed Activation — Changes

## Files changed
- `app.py` — added `build_embed_activation_urls(shop)` (single canonical source
  for `activate_embed_url` and `manual_activation_url`), called once from
  `connect_shopify_callback`. Removed `SHOPIFY_EXTENSION_UUID`; deep link now
  uses `SHOPIFY_APP_CLIENT_ID` + handle `lumvi-chat-embed`, per Shopify's
  documented `activateAppId={api_key}/{handle}` format.
- `static/js/embed-activation.js` — NEW. Shared front-end module
  (`LumviEmbedActivation.parse/apply/render`) that wires the one-click deep
  link button and injects the manual-fallback instructions into the
  `#activateEmbedBanner` element. Single source of truth for that UI across
  all three pages below.
- `templates/onboarding.html` — includes the shared script; step-5 "ready"
  screen now calls `LumviEmbedActivation.apply(...)`.
- `templates/dashboard_enterprise.html` — includes the shared script; OAuth
  return handler now calls `LumviEmbedActivation.render(params)`.
- `templates/integrations.html` — includes the shared script; OAuth return
  handler now calls `LumviEmbedActivation.apply(...)`.

## Not included
- `diffs/app.py.diff` — left untouched per instruction (historical artifact,
  not part of the deploy/dev workflow).
- `shopify-extension/` — no changes; included here only as reference that
  the deployed extension handle is `lumvi-chat-embed`, matching the code.

## How to apply
Copy `app.py`, `templates/*.html`, and `static/js/embed-activation.js` into
your working repo at the same relative paths (overwrite existing files),
then redeploy. Make sure `SHOPIFY_APP_CLIENT_ID` is set in your environment
(same value used for OAuth already) — no new env var is needed.
