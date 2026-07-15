/* ===================================================================
   LUMVI KNOWLEDGE HUB — SHARED APP LOGIC
=================================================================== */

/* ---------- Minimal inline icon set (no external icon dependency) --- */
const ICONS = {
  layers: '<path d="M12 2 2 7l10 5 10-5-10-5Z"/><path d="m2 17 10 5 10-5"/><path d="m2 12 10 5 10-5"/>',
  bot: '<rect x="3" y="11" width="18" height="10" rx="2"/><circle cx="12" cy="5" r="2"/><path d="M12 7v4"/><path d="M8 16h.01"/><path d="M16 16h.01"/>',
  trending: '<polyline points="23 6 13.5 15.5 8.5 10.5 1 18"/><polyline points="17 6 23 6 23 12"/>',
  workflow: '<rect x="3" y="3" width="8" height="8" rx="2"/><rect x="13" y="13" width="8" height="8" rx="2"/><path d="M7 11v4a2 2 0 0 0 2 2h4"/>',
  headset: '<path d="M3 12a9 9 0 0 1 18 0"/><path d="M21 15v2a2 2 0 0 1-2 2h-1"/><rect x="17" y="12" width="4" height="6" rx="1"/><rect x="3" y="12" width="4" height="6" rx="1"/>',
  target: '<circle cx="12" cy="12" r="9"/><circle cx="12" cy="12" r="5"/><circle cx="12" cy="12" r="1"/>',
  cart: '<circle cx="9" cy="20" r="1"/><circle cx="18" cy="20" r="1"/><path d="M2 3h2l2.4 12.2a2 2 0 0 0 2 1.8h8.2a2 2 0 0 0 2-1.6L21 8H6"/>',
  plug: '<path d="M12 22v-4"/><path d="M9 8V2"/><path d="M15 8V2"/><path d="M8 8h8a2 2 0 0 1 2 2 6 6 0 0 1-6 6h0a6 6 0 0 1-6-6 2 2 0 0 1 2-2Z"/>',
  chart: '<path d="M3 3v18h18"/><path d="M7 15v3"/><path d="M12 10v8"/><path d="M17 6v12"/>',
  tag: '<path d="M12 2 2 12l10 10 10-10-10-10Z" fill="none"/><path d="M20.6 12.6 12 21.2a2 2 0 0 1-2.83 0l-7.37-7.37a2 2 0 0 1 0-2.83L10.4 2.6A2 2 0 0 1 11.8 2H19a2 2 0 0 1 2 2v7.2a2 2 0 0 1-.4 1.4Z"/><circle cx="15" cy="8" r="1.5"/>',
  search: '<circle cx="11" cy="11" r="7"/><path d="m21 21-4.3-4.3"/>',
  moon: '<path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79Z"/>',
  sun: '<circle cx="12" cy="12" r="4"/><path d="M12 2v2M12 20v2M4.93 4.93l1.41 1.41M17.66 17.66l1.41 1.41M2 12h2M20 12h2M4.93 19.07l1.41-1.41M17.66 6.34l1.41-1.41"/>',
  arrow: '<path d="M5 12h14"/><path d="m12 5 7 7-7 7"/>',
  clock: '<circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 3"/>',
  menu: '<path d="M4 6h16M4 12h16M4 18h16"/>',
  x: '<path d="M18 6 6 18M6 6l12 12"/>',
  chevronDown: '<path d="m6 9 6 6 6-6"/>',
  bookmark: '<path d="M19 21 12 16l-7 5V5a2 2 0 0 1 2-2h10a2 2 0 0 1 2 2Z"/>',
  share: '<circle cx="18" cy="5" r="3"/><circle cx="6" cy="12" r="3"/><circle cx="18" cy="19" r="3"/><path d="m8.6 10.5 6.8-3.9M8.6 13.5l6.8 3.9"/>',
  link: '<path d="M10 13a5 5 0 0 0 7.5.5l2-2a5 5 0 0 0-7-7l-1.5 1.5"/><path d="M14 11a5 5 0 0 0-7.5-.5l-2 2a5 5 0 0 0 7 7l1.5-1.5"/>',
  check: '<path d="M20 6 9 17l-5-5"/>',
};

function icon(name, cls = "w-5 h-5") {
  return `<svg class="${cls}" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">${ICONS[name] || ""}</svg>`;
}

/* ---------- Dark mode ---------- */
function initDarkMode() {
  const root = document.documentElement;
  const stored = sessionStorage.getItem("lumvi-theme");
  if (stored === "dark") root.classList.add("dark");
  const toggles = document.querySelectorAll("[data-theme-toggle]");
  const sync = () => {
    const isDark = root.classList.contains("dark");
    toggles.forEach((t) => (t.innerHTML = icon(isDark ? "sun" : "moon", "w-[18px] h-[18px]")));
  };
  toggles.forEach((t) =>
    t.addEventListener("click", () => {
      root.classList.toggle("dark");
      sessionStorage.setItem("lumvi-theme", root.classList.contains("dark") ? "dark" : "light");
      sync();
    })
  );
  sync();
}

/* ---------- Reveal on scroll ---------- */
function initReveal() {
  const els = document.querySelectorAll(".reveal");
  const io = new IntersectionObserver(
    (entries) => {
      entries.forEach((e) => {
        if (e.isIntersecting) {
          e.target.classList.add("in");
          io.unobserve(e.target);
        }
      });
    },
    { threshold: 0.12 }
  );
  els.forEach((el) => io.observe(el));
}

/* ---------- Mobile nav ---------- */
function initMobileNav() {
  const btn = document.getElementById("mobile-nav-btn");
  const panel = document.getElementById("mobile-nav-panel");
  if (!btn || !panel) return;
  btn.addEventListener("click", () => {
    const open = panel.classList.toggle("hidden");
    btn.innerHTML = icon(open ? "menu" : "x", "w-5 h-5");
  });
}

/* ---------- URL helper (Flask routes, injected by each template) ---------- */
function articleUrl(slug) {
  if (window.BLOG_URLS && window.BLOG_URLS.articleBase) {
    return window.BLOG_URLS.articleBase.replace("__SLUG__", slug);
  }
  return "article.html?slug=" + slug; // fallback if BLOG_URLS wasn't injected
}
function hubUrl() {
  return (window.BLOG_URLS && window.BLOG_URLS.index) || "index.html";
}

/* ---------- Card renderers (used on hub) ---------- */
function articleCardHTML(a, opts = {}) {
  const { dense = false } = opts;
  return `
  <a href="${articleUrl(a.slug)}" class="card group flex flex-col overflow-hidden">
    <div class="h-36 shrink-0 flex items-center justify-center relative" style="background: var(--paper); border-bottom:1px solid var(--line);">
      <div class="absolute inset-0 opacity-[0.5]" style="background: radial-gradient(circle at 30% 20%, var(--signal-dim), transparent 60%), radial-gradient(circle at 80% 80%, var(--cobalt-dim), transparent 55%);"></div>
      <span class="relative badge bg-paper2 border border-line text-ink">${icon(categoryIcon(a.category), "w-3.5 h-3.5")}${a.categoryName}</span>
    </div>
    <div class="p-5 flex flex-col gap-3 flex-1">
      <h3 class="font-display text-[17px] leading-snug font-medium group-hover:text-signal transition-colors">${a.title}</h3>
      ${dense ? "" : `<p class="text-muted text-[13.5px] leading-relaxed">${a.excerpt}</p>`}
      <div class="mt-auto pt-2 flex items-center justify-between text-[12px] font-mono text-muted">
        <span>${a.author}</span>
        <span class="flex items-center gap-1">${icon("clock", "w-3.5 h-3.5")}${a.readTime} min</span>
      </div>
    </div>
  </a>`;
}

function categoryIcon(key) {
  const map = { "white-label": "layers", agents: "bot", agency: "trending", automation: "workflow", support: "headset", leadgen: "target", ecommerce: "cart", integrations: "plug", "case-studies": "chart", pricing: "tag" };
  return map[key] || "layers";
}

function categoryCardHTML(cat) {
  const count = categoryCount(cat.key);
  return `
  <a href="#" data-filter="${cat.key}" class="card p-6 flex flex-col gap-4 group">
    <div class="cat-icon">${icon(cat.icon, "w-5 h-5")}</div>
    <div>
      <h3 class="font-display text-[17px] font-medium">${cat.name}</h3>
      <p class="text-muted text-[13.5px] mt-1 leading-relaxed">${cat.blurb}</p>
    </div>
    <div class="mt-auto pt-3 flex items-center justify-between border-t border-line">
      <span class="font-mono text-[11px] text-muted pt-3">${count} articles</span>
      <span class="pt-3 opacity-0 group-hover:opacity-100 transition-opacity">${icon("arrow", "w-4 h-4")}</span>
    </div>
  </a>`;
}

function popularRowHTML(a, rank) {
  return `
  <a href="${articleUrl(a.slug)}" class="flex items-center gap-5 py-5 border-b border-line group">
    <span class="rank-num w-12 shrink-0">${String(rank).padStart(2, "0")}</span>
    <div class="min-w-0 flex-1">
      <p class="badge bg-cobalt-dim text-cobalt mb-1.5 w-fit">${a.categoryName}</p>
      <h3 class="font-display text-[16px] font-medium truncate group-hover:text-signal transition-colors">${a.title}</h3>
    </div>
    <span class="hidden sm:flex items-center gap-1 font-mono text-[12px] text-muted shrink-0">${icon("clock", "w-3.5 h-3.5")}${a.readTime} min</span>
    <span class="shrink-0 opacity-0 group-hover:opacity-100 transition-opacity">${icon("arrow", "w-4 h-4")}</span>
  </a>`;
}

/* ---------- Search ---------- */
function initSearch(inputSelector, resultsSelector) {
  const input = document.querySelector(inputSelector);
  const results = document.querySelector(resultsSelector);
  if (!input || !results) return;

  const render = (q) => {
    if (!q) {
      results.classList.add("hidden");
      results.innerHTML = "";
      return;
    }
    const matches = ARTICLES.filter((a) => a.title.toLowerCase().includes(q.toLowerCase())).slice(0, 6);
    if (!matches.length) {
      results.innerHTML = `<div class="p-4 text-[13.5px] text-muted">No results for “${q}”. Try “pricing” or “integrations.”</div>`;
    } else {
      results.innerHTML = matches
        .map(
          (a) => `<a href="${articleUrl(a.slug)}" class="flex items-center gap-3 px-4 py-3 hover:bg-paper transition-colors border-b border-line last:border-0">
            <span class="text-muted">${icon("search", "w-4 h-4")}</span>
            <span class="text-[13.5px] flex-1">${a.title}</span>
            <span class="badge bg-paper2 border border-line text-muted">${a.categoryName}</span>
          </a>`
        )
        .join("");
    }
    results.classList.remove("hidden");
  };

  input.addEventListener("input", (e) => render(e.target.value.trim()));
  input.addEventListener("focus", (e) => e.target.value && render(e.target.value.trim()));
  document.addEventListener("click", (e) => {
    if (!input.contains(e.target) && !results.contains(e.target)) {
      results.classList.add("hidden");
    }
  });
  // Keyboard shortcut: "/" focuses search
  document.addEventListener("keydown", (e) => {
    if (e.key === "/" && document.activeElement.tagName !== "INPUT") {
      e.preventDefault();
      input.focus();
    }
  });
}

/* ---------- Rebrand Preview (signature element) ---------- */
const REBRAND_PROFILES = [
  { name: "Lumvi (default)", accent: "#c9962a", bubble: "#10131a", bubbleText: "#f5f5f2", initials: "LM" },
  { name: "Northfield Digital", accent: "#2f5d50", bubble: "#2f5d50", bubbleText: "#ffffff", initials: "ND" },
  { name: "Rally & Co.", accent: "#b5473a", bubble: "#b5473a", bubbleText: "#ffffff", initials: "RC" },
  { name: "Hollow & Vine", accent: "#4a4a6a", bubble: "#4a4a6a", bubbleText: "#ffffff", initials: "HV" },
  { name: "Kestrel Agency", accent: "#1f6f8b", bubble: "#1f6f8b", bubbleText: "#ffffff", initials: "KA" },
];

function initRebrandPreview() {
  const root = document.getElementById("rebrand-preview");
  if (!root) return;
  const swatchWrap = root.querySelector("[data-swatches]");
  const dot = root.querySelector("[data-widget-dot]");
  const bubble = root.querySelector("[data-widget-bubble]");
  const brandName = root.querySelector("[data-brand-name]");
  const initials = root.querySelector("[data-widget-initials]");

  swatchWrap.innerHTML = REBRAND_PROFILES.map(
    (p, i) => `<button class="rebrand-swatch" data-active="${i === 0}" data-i="${i}" style="background:${p.accent}" aria-label="Preview as ${p.name}"></button>`
  ).join("");

  const apply = (i) => {
    const p = REBRAND_PROFILES[i];
    dot.style.background = p.accent;
    bubble.style.background = p.bubble;
    bubble.style.color = p.bubbleText;
    initials.textContent = p.initials;
    brandName.textContent = p.name;
    swatchWrap.querySelectorAll(".rebrand-swatch").forEach((s, si) => s.setAttribute("data-active", si === i));
  };

  swatchWrap.querySelectorAll(".rebrand-swatch").forEach((s) =>
    s.addEventListener("click", () => apply(parseInt(s.dataset.i, 10)))
  );

  apply(0);
}

function findArticleByTitleStart(fragment) {
  return ARTICLES.find((a) => a.title.startsWith(fragment));
}
