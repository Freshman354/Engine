/**
 * Lumvi Widget — session_id management
 * =====================================
 * Drop this into your widget's JS file, replacing the existing
 * sendMessage / API call block. The only change from your current
 * widget is:
 *
 *   1. Generate a UUID on first load and store it in localStorage
 *   2. Send it with every /api/chat request
 *   3. On the first response, confirm/store the session_id the
 *      server echoes back (they will always match, but it's good
 *      practice to use the server's value as the source of truth)
 *
 * This ties the conversation, persistent session memory (Phase 3),
 * and any human inbox tickets (Phase 4) to one traceable thread.
 *
 * ── Where to put this ──────────────────────────────────────────
 * In your existing widget JS file, near the top where you define
 * your state variables, add the SESSION_ID block. Then update your
 * sendMessage() function to include session_id in the request body.
 * ───────────────────────────────────────────────────────────────
 */


/* ── 1. SESSION ID — add near your other widget state variables ── */

/**
 * Generate a v4-style UUID without the crypto API so it works in
 * every browser and webview including old Safari versions.
 */
function _lumviUUID() {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, c => {
    const r = Math.random() * 16 | 0;
    return (c === 'x' ? r : (r & 0x3 | 0x8)).toString(16);
  });
}

/**
 * Retrieve the session ID for this widget instance.
 *
 * Key format: lumvi_session_{clientId}
 * Scoped to clientId so a page with multiple Lumvi widgets (different
 * clients) doesn't share the same session.
 *
 * Falls back to an in-memory variable if localStorage is blocked
 * (private browsing mode in some browsers).
 */
let _lumviSessionIdMemory = null;

function getLumviSessionId(clientId) {
  const key = `lumvi_session_${clientId}`;
  try {
    let id = localStorage.getItem(key);
    if (!id) {
      id = _lumviUUID();
      localStorage.setItem(key, id);
    }
    return id;
  } catch (_) {
    // localStorage blocked (private mode / iframe restrictions)
    if (!_lumviSessionIdMemory) {
      _lumviSessionIdMemory = _lumviUUID();
    }
    return _lumviSessionIdMemory;
  }
}

/**
 * Clear the session when the user explicitly resets the chat.
 * Call this from your "Clear conversation" / "Start over" button handler.
 * Also call DELETE /api/chat-sessions/{clientId} if you want to wipe
 * the server-side session memory too (optional).
 */
function clearLumviSession(clientId) {
  const key = `lumvi_session_${clientId}`;
  try {
    localStorage.removeItem(key);
  } catch (_) { /* ignore */ }
  _lumviSessionIdMemory = null;
}


/* ── 2. SEND MESSAGE — update your existing sendMessage() function ── */

/**
 * Replace the fetch() call inside your sendMessage() function with
 * this version. The only addition is `session_id` in the request body.
 *
 * Your existing function likely looks like:
 *
 *   async function sendMessage(message) {
 *     const res = await fetch('/api/chat', {
 *       method: 'POST',
 *       headers: { 'Content-Type': 'application/json' },
 *       body: JSON.stringify({
 *         message:   message,
 *         client_id: CLIENT_ID,
 *         history:   conversationHistory,
 *       }),
 *     });
 *     ...
 *   }
 *
 * Change it to:
 */
async function sendMessage(message) {
  // ── Get (or create) the session ID for this widget instance ──
  const sessionId = getLumviSessionId(CLIENT_ID);  // CLIENT_ID = your existing var

  const res = await fetch('/api/chat', {
    method:  'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      message:    message,
      client_id:  CLIENT_ID,
      history:    conversationHistory,  // your existing history array
      session_id: sessionId,            // ← the only new line
    }),
  });

  const data = await res.json();

  // ── 3. Confirm session_id from server (always matches, good practice) ──
  if (data.session_id && data.session_id !== sessionId) {
    // Server returned a different ID — store it as the authoritative value
    try {
      localStorage.setItem(`lumvi_session_${CLIENT_ID}`, data.session_id);
    } catch (_) {
      _lumviSessionIdMemory = data.session_id;
    }
  }

  return data;
}


/* ── THAT'S IT ─────────────────────────────────────────────────
 *
 * Summary of changes to your widget:
 *
 *   + getLumviSessionId()      — new helper, add near top of widget JS
 *   + clearLumviSession()      — new helper, call on "clear chat" action
 *   + session_id in body       — add to your existing fetch() call
 *   + store server session_id  — add after you read data from the response
 *
 * No changes needed to:
 *   - Your widget HTML / CSS
 *   - Conversation history management
 *   - Lead capture form
 *   - Any other existing behaviour
 *
 * What this unlocks:
 *   ✓ Phase 3 persistent memory works (name, email, frustration
 *     accumulates correctly across turns)
 *   ✓ Human inbox tickets link to the correct transcript
 *   ✓ get_inbox_ticket() returns the full conversation, not an empty array
 *   ✓ Repeat visits within the same browser session are recognised
 *     (frustration_score and turn_count accumulate correctly)
 * ────────────────────────────────────────────────────────────── */
