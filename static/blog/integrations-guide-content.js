/* ===================================================================
   INTEGRATION GUIDE ARTICLE
   The second fully authored guide, alongside the cornerstone article.
   Grounded in the real Platform Connections flow (webhook URL + secret
   handshake) so this matches the actual product, not a generic
   description of "how integrations work."
=================================================================== */

const INTEGRATION_GUIDE_SLUG = "how-to-connect-a-platform-integration";

const INTEGRATION_GUIDE_ARTICLE = {
  title: "How to Connect Your First Platform Integration",
  category: "Integrations",
  categoryKey: "integrations",
  author: "Priya Nair",
  authorRole: "Solutions Engineer",
  date: "July 2026",
  readTime: 9,
  difficulty: "Beginner",
  updatedRecently: true,
  quickSummary:
    "Every platform integration in Lumvi works the same way underneath: copy a webhook URL from Lumvi, paste it into the platform's webhook settings, then paste the secret the platform gives you back into Lumvi. This walks through that exact process for Shopify, WooCommerce, Acuity, Calendly, and Square, plus how to test a connection before telling a client it's live.",
  keyTakeaways: [
    "Every integration uses the same two-way handshake: a webhook URL from Lumvi, a signing secret from the platform.",
    "The webhook URL is specific to one client. Don't reuse Client A's URL for Client B.",
    "Calendly's webhook feature requires a paid Calendly plan on the client's side. The others don't have that restriction.",
    "Test with a real, live action (a test order, a test booking) before telling the client the integration is live.",
    "If only the secret needs to change, use Update Key rather than disconnecting and reconnecting the whole integration.",
  ],
  toc: [
    { id: "before-you-start", label: "What you'll need" },
    { id: "how-it-works", label: "How the connection works" },
    { id: "connect-platforms", label: "Connecting each platform" },
    { id: "test-connection", label: "Testing the connection" },
    { id: "rotate-keys", label: "Disconnecting or rotating a key" },
  ],
  bodyHTML: `
    <h2 id="before-you-start">What you'll need</h2>
    <p>Before you open the connection modal, have three things ready: the client selected in Lumvi, admin access to the platform you're connecting (Shopify, WooCommerce, Acuity, Calendly, or Square), and about five minutes per platform. That's genuinely most of the setup. The whole flow is designed to be pasted through twice, not configured.</p>
    <p>You'll also see a first step in the onboarding panel confirming you're on a plan that supports integrations. If that step isn't checked off, connecting will be blocked until the account is upgraded, so it's worth confirming before you promise a client a go-live date.</p>

    <h2 id="how-it-works">How the connection works</h2>
    <p>Every platform on Lumvi uses the same underlying pattern, which makes the second and third integration much faster than the first:</p>
    <ol>
      <li>Lumvi generates a webhook URL specific to that client and that platform.</li>
      <li>You paste that URL into the platform's own webhook settings.</li>
      <li>The platform gives you back a secret, a signing key, or a security key (naming varies by platform).</li>
      <li>You paste that secret back into Lumvi and click Connect.</li>
    </ol>
    <div class="callout-tip p-5 my-6">
      <p class="badge bg-signal text-ink mb-2 w-fit">Pro tip</p>
      <p class="text-[14.5px] leading-relaxed">Copy the webhook URL first and paste it into the platform before you go looking for the secret. Most platforms won't show you a signing secret until after a webhook endpoint exists, so doing it in this order avoids a second trip back and forth.</p>
    </div>

    <h2 id="connect-platforms">Connecting each platform</h2>
    <p>The steps below match what you'll see inside each platform's own settings. The exact event names matter: subscribing to the wrong ones is the most common reason a connection looks "successful" but the agent never actually hears about a new order or booking.</p>

    <h3>Shopify</h3>
    <ol>
      <li>In Shopify admin, go to <strong>Settings → Notifications</strong>.</li>
      <li>Scroll to the bottom and click <strong>Create webhook</strong>.</li>
      <li>Paste the Lumvi webhook URL shown in the connection modal.</li>
      <li>Set the format to JSON, and subscribe to <code>orders/created</code>, <code>orders/updated</code>, and <code>orders/cancelled</code>.</li>
      <li>Save, then copy the signing secret Shopify shows you.</li>
      <li>Paste that signing secret into Lumvi and click Connect.</li>
    </ol>

    <h3>WooCommerce</h3>
    <ol>
      <li>In WordPress admin, go to <strong>WooCommerce → Settings → Advanced → Webhooks</strong>.</li>
      <li>Click <strong>Add webhook</strong>.</li>
      <li>Paste the Lumvi webhook URL as the Delivery URL.</li>
      <li>Set the topic to "Order created." Add a second webhook for "Order updated" if the client also wants status changes tracked.</li>
      <li>Choose any secret you like and save it.</li>
      <li>Paste that same secret into Lumvi and click Connect.</li>
    </ol>
    <div class="callout p-5 my-6">
      <p class="badge bg-cobalt text-white mb-2 w-fit">Worth noting</p>
      <p class="text-[14.5px] leading-relaxed">WooCommerce is the one platform on this list where you choose the secret yourself instead of the platform generating one for you. Whatever you type into WooCommerce's secret field has to match, character for character, what you paste into Lumvi.</p>
    </div>

    <h3>Acuity Scheduling</h3>
    <ol>
      <li>In Acuity, go to <strong>Integrations → Webhooks</strong>.</li>
      <li>Click <strong>Add webhook</strong>.</li>
      <li>Paste the Lumvi webhook URL shown in the connection modal.</li>
      <li>Check <code>appointment.scheduled</code>, <code>rescheduled</code>, and <code>cancelled</code>.</li>
      <li>Save, then copy the secret key Acuity shows you.</li>
      <li>Paste that secret key into Lumvi and click Connect.</li>
    </ol>

    <h3>Calendly</h3>
    <ol>
      <li>In Calendly, go to <strong>Integrations → Webhooks</strong>.</li>
      <li>Click <strong>Create Webhook Subscription</strong>.</li>
      <li>Paste the Lumvi webhook URL shown in the connection modal.</li>
      <li>Subscribe to <code>invitee.created</code> and <code>invitee.canceled</code>.</li>
      <li>Save, then copy the signing key Calendly shows you.</li>
      <li>Paste that signing key into Lumvi and click Connect.</li>
    </ol>
    <div class="callout-warn p-5 my-6">
      <p class="badge" style="background:#C15A3B; color:white;">Common mistake</p>
      <p class="text-[14.5px] leading-relaxed mt-2">Calendly's webhook feature is only available on paid Calendly plans. If the "Webhooks" option isn't visible under Integrations, that's almost always why. Check this before promising a client a Calendly-connected agent.</p>
    </div>

    <h3>Square Appointments</h3>
    <ol>
      <li>In the Square Developer Dashboard, open your application.</li>
      <li>Go to <strong>Webhooks → Subscriptions</strong> and click <strong>Add Subscription</strong>.</li>
      <li>Paste the Lumvi webhook URL as the Notification URL. It has to match exactly.</li>
      <li>Subscribe to <code>booking.created</code> and <code>booking.updated</code>.</li>
      <li>Save, then copy the Signature Key Square shows you.</li>
      <li>Paste that signature key into Lumvi and click Connect.</li>
    </ol>
    <div class="callout-warn p-5 my-6">
      <p class="badge" style="background:#C15A3B; color:white;">Common mistake</p>
      <p class="text-[14.5px] leading-relaxed mt-2">Square is strict about the Notification URL matching exactly. A stray trailing slash or a copy-paste that clips the last character will save without an error and then simply never deliver anything. If a Square connection looks fine but nothing's coming through, re-copy the URL and check it character for character before anything else.</p>
    </div>

    <h2 id="test-connection">Testing the connection</h2>
    <p>A connection that saved without an error isn't the same as a connection that works. Before telling a client an integration is live, do one real test:</p>
    <ul>
      <li>Place a real test order (Shopify, WooCommerce) or book a real test appointment (Acuity, Calendly, Square) on the client's actual platform.</li>
      <li>Open the client's widget and ask a question that depends on that new data, like an order status or a booking confirmation.</li>
      <li>Confirm the agent's answer reflects the test order or booking, not a stale or generic response.</li>
    </ul>
    <p>Webhook delivery is close to real time, but not always instant. If the agent doesn't reflect the test action within a minute or two, that's the point to check the platform's webhook logs before assuming Lumvi's side is at fault. Most platforms, including Shopify and Square, keep a delivery log showing whether the webhook fired and what response it got back.</p>

    <h2 id="rotate-keys">Disconnecting or rotating a key</h2>
    <p>If a secret is ever compromised, or a client rotates their own platform credentials, you don't need to tear down the whole integration. Use the platform card's edit action to open <strong>Update Key</strong>, paste the new secret, and save. The webhook URL stays the same, so nothing needs to change on the platform's side.</p>
    <p>Full disconnect and reconnect is only necessary if the webhook URL itself needs to change, which is rare, or if a client is being fully offboarded from that platform.</p>
  `,
  faqs: [
    {
      q: "Do I need a paid plan on the platform itself, not just on Lumvi, to connect it?",
      a: "It depends on the platform. Calendly's webhook feature requires a paid Calendly plan on the client's account. Shopify, WooCommerce, Acuity, and Square don't have that restriction.",
    },
    {
      q: "Can the same platform be connected for more than one client?",
      a: "Yes. Each client gets its own webhook URL, so Shopify for Client A and Shopify for Client B are two separate connections with two separate secrets, even though the setup steps are identical.",
    },
    {
      q: "What if I only need to update the secret, not reconnect the whole integration?",
      a: "Use Update Key from the platform card's edit action instead of disconnecting. The webhook URL stays the same, so there's nothing to change on the platform's side.",
    },
    {
      q: "The connection saved without an error, but the agent still isn't seeing new orders or bookings. What's wrong?",
      a: "Check three things in order: that the webhook URL was pasted exactly (Square in particular fails silently on a mismatch), that the correct events are subscribed to on the platform's side, and the platform's own webhook delivery log to confirm it's actually firing.",
    },
  ],
};
