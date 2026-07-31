/**
 * Shared "activate the Shopify app embed" UI logic.
 *
 * Used on every page a merchant can land on right after a Shopify OAuth
 * connect: onboarding.html, dashboard_enterprise.html, integrations.html.
 * All three already render a banner with the same two element IDs
 * (#activateEmbedBanner, #activateEmbedBtn) — this module is the single
 * place that (a) wires the one-click deep link into that banner and
 * (b) injects the manual-fallback instructions into it, so the fallback
 * markup/copy/behavior lives in exactly one file instead of being copied
 * across templates.
 *
 * Backend contract (see app.py, _build_embed_activation): the OAuth
 * callback redirect always includes `manual_activation_url` (a plain
 * theme-editor link, no activateAppId — always valid). It includes
 * `activate_embed_url` (the one-click deep link) only when
 * SHOPIFY_APP_CLIENT_ID is configured. Because Shopify's activateAppId
 * deep link is known to fail intermittently on Shopify's own side
 * (community-reported, unresolved as of this writing) with no
 * client-side way to detect that failure, the manual fallback is always
 * shown whenever it's available — never gated behind "try the button
 * first."
 */
(function (global) {
  const STEPS = [
    'Go to <strong>Online Store &rarr; Themes</strong>',
    'Click <strong>Customize</strong> on your live theme',
    'Click <strong>App embeds</strong> in the left sidebar (puzzle-piece icon)',
    'Toggle <strong>Lumvi</strong> on',
    'Click <strong>Save</strong>',
  ];

  function injectManualFallback(bannerEl, manualUrl) {
    if (!bannerEl || bannerEl.querySelector('#manualActivateFallback')) return;

    const wrap = document.createElement('div');
    wrap.id = 'manualActivateFallback';
    wrap.style.cssText = 'flex-basis:100%;margin-top:10px;font-size:12.5px;opacity:0.9;';
    wrap.innerHTML =
      '<button type="button" id="manualActivateToggle" ' +
      'style="background:none;border:none;padding:0;color:inherit;' +
      'text-decoration:underline;cursor:pointer;font-size:12.5px;font-family:inherit;">' +
      "Button didn't take you to the right place? Show manual steps" +
      '</button>' +
      '<ol id="manualActivateSteps" style="display:none;margin:10px 0 0 0;' +
      'padding-left:18px;line-height:1.7;">' +
      STEPS.map((s) => `<li>${s}</li>`).join('') +
      '</ol>' +
      `<a id="manualActivateLink" href="${manualUrl}" target="_blank" rel="noopener" ` +
      'style="display:none;margin-top:8px;font-weight:600;color:inherit;">' +
      'Open theme editor manually &rarr;</a>';

    bannerEl.appendChild(wrap);

    wrap.querySelector('#manualActivateToggle').addEventListener('click', function () {
      const steps = wrap.querySelector('#manualActivateSteps');
      const link = wrap.querySelector('#manualActivateLink');
      const show = steps.style.display === 'none';
      steps.style.display = show ? 'block' : 'none';
      link.style.display = show ? 'inline-block' : 'none';
    });
  }

  /**
   * Wires the banner/button/fallback given already-extracted values.
   * Use this when the caller needs to hold onto the values before
   * applying them (e.g. onboarding.html, where activation only becomes
   * visible on step 5, not immediately on OAuth return).
   */
  function apply(activateUrl, manualUrl) {
    const banner = document.getElementById('activateEmbedBanner');
    const btn = document.getElementById('activateEmbedBtn');
    if (!banner) return;

    if (activateUrl && btn) {
      btn.href = activateUrl;
      btn.style.display = '';
    } else if (btn) {
      btn.style.display = 'none';
    }

    if (activateUrl || manualUrl) {
      banner.style.display = 'flex';
    }

    if (manualUrl) {
      injectManualFallback(banner, manualUrl);
    }
  }

  /** Reads the two query params without wiring anything yet. */
  function parse(params) {
    return {
      activateUrl: params.get('activate_embed_url') || null,
      manualUrl: params.get('manual_activation_url') || null,
    };
  }

  /** Convenience for pages that show the banner immediately on return. */
  function render(params) {
    const { activateUrl, manualUrl } = parse(params);
    apply(activateUrl, manualUrl);
    return { activateUrl, manualUrl };
  }

  global.LumviEmbedActivation = { parse, apply, render };
})(window);
