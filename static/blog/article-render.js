/* ===================================================================
   ARTICLE PAGE RENDERER
=================================================================== */

initDarkMode();

const params = new URLSearchParams(window.location.search);
const routeSlug = document.body.dataset.slug;
const slug = routeSlug || params.get("slug") || CORNERSTONE_SLUG;
const isCornerstone = slug === CORNERSTONE_SLUG;
const dataArticle = ARTICLES.find((a) => a.slug === slug);

/* ---------- Generic article generator (for the 99 other slugs) ---------- */
const GENERIC_INTROS = {
  "white-label": "White-labeling isn't a single decision, it's a set of smaller ones that compound. This piece looks at one of them in detail.",
  agents: "Understanding how an agent behaves under the hood makes it much easier to explain, sell, and troubleshoot for a client.",
  agency: "Turning any capability into a service line is mostly an operations problem, not a technical one. Here's how it plays out for this one.",
  automation: "Automation earns its keep when it removes a step a human was doing manually — not when it adds a new one to monitor.",
  support: "Support metrics only mean something once you know what a client actually expects the number to change.",
  leadgen: "A chat conversation is a strange place to qualify a lead, but it turns out to work better than most teams expect.",
  ecommerce: "Shoppers ask a small, predictable set of questions — the trick is grounding the agent in the answers, not guessing at them.",
  integrations: "Most integration headaches show up at the edges — auth, retries, and what happens when the other system is down.",
  "case-studies": "Numbers without context don't tell an agency much. Here's the context behind one.",
  pricing: "Pricing AI work is still new enough that most agencies are guessing. This is one place to start instead.",
};

function generateGenericArticle(article) {
  const intro = GENERIC_INTROS[article.category] || GENERIC_INTROS["agents"];
  const bodyHTML = `
    <h2 id="overview">Overview</h2>
    <p>${intro} For agencies working under the <strong>${article.categoryName}</strong> umbrella, this is one of the more common questions that comes up once you're past the first client and into the second or third.</p>
    <p>${article.excerpt} The rest of this guide breaks down what that looks like in practice, where agencies commonly get it wrong, and what a reasonable default looks like if you don't want to overthink it.</p>

    <h2 id="in-practice">In practice</h2>
    <p>The short version: start with the smallest version that could work, ship it to one client, and let their actual usage tell you what to fix. Most of the mistakes agencies make here come from over-engineering the first version before anyone has used it.</p>
    <div class="callout p-5 my-6">
      <p class="badge bg-cobalt text-white mb-2 w-fit">Worth noting</p>
      <p class="text-[14.5px] leading-relaxed">This is one of the areas where a platform's defaults matter more than most agencies expect. Check what your platform does out of the box before building a custom workaround.</p>
    </div>

    <h2 id="checklist">A quick checklist</h2>
    <ul>
      <li>Confirm the default behavior before assuming you need to customize it</li>
      <li>Test with a real client scenario, not a hypothetical one</li>
      <li>Document the decision so the next client's setup is faster</li>
    </ul>
  `;
  return {
    title: article.title,
    category: article.categoryName,
    categoryKey: article.category,
    author: article.author,
    authorRole: "Lumvi",
    date: formatDate(article.date),
    readTime: article.readTime,
    difficulty: article.difficulty,
    updatedRecently: article.updatedRecently,
    quickSummary: article.excerpt + " Here's the practical version, without the fluff.",
    keyTakeaways: [
      `${article.categoryName} decisions compound — get the default right early.`,
      "Ship the smallest version to one real client before customizing further.",
      "Document the decision so it's not re-litigated with the next client.",
    ],
    toc: [
      { id: "overview", label: "Overview" },
      { id: "in-practice", label: "In practice" },
      { id: "checklist", label: "A quick checklist" },
    ],
    bodyHTML,
    faqs: [
      { q: `Does this apply to every client, regardless of vertical?`, a: `Mostly, yes — the underlying pattern holds across verticals, though the specific numbers or copy will differ by client.` },
      { q: `Is this something the platform handles automatically?`, a: `Often partially. It's worth checking your platform's defaults before building a custom process around it.` },
    ],
  };
}

const view = isCornerstone
  ? { ...CORNERSTONE_ARTICLE }
  : dataArticle
  ? generateGenericArticle(dataArticle)
  : generateGenericArticle(ARTICLES[0]);

/* ---------- Populate header ---------- */
document.title = view.title + " — Lumvi Knowledge Hub";
document.getElementById("page-title").textContent = view.title + " — Lumvi Knowledge Hub";
document.getElementById("breadcrumbs").innerHTML = `
  <a href="${hubUrl()}" class="hover:text-signal transition-colors">Hub</a>
  <span>/</span>
  <a href="${hubUrl()}" class="hover:text-signal transition-colors">${view.category}</a>
  <span>/</span>
  <span class="text-ink dark:text-white truncate max-w-[220px] sm:max-w-none">${view.title}</span>
`;
document.getElementById("article-category-badge").innerHTML = `${icon(categoryIcon(view.categoryKey), "w-3.5 h-3.5")}${view.category}`;
document.getElementById("article-title").textContent = view.title;
document.getElementById("article-author").innerHTML = `<span class="w-6 h-6 rounded-full bg-cobalt/20 flex items-center justify-center font-mono text-[10px] text-cobalt">${view.author.split(" ").map(s=>s[0]).join("")}</span> ${view.author}${view.authorRole ? `, ${view.authorRole}` : ""}`;
document.getElementById("article-readtime").textContent = `${view.readTime} min read`;
document.getElementById("article-date").textContent = view.date;
if (view.updatedRecently) document.getElementById("article-updated").classList.remove("hidden");
document.getElementById("hero-icon").innerHTML = icon(categoryIcon(view.categoryKey), "w-16 h-16");
document.getElementById("quick-summary").textContent = view.quickSummary;
document.getElementById("article-body").innerHTML = view.bodyHTML;
document.getElementById("key-takeaways").innerHTML = view.keyTakeaways
  .map((t) => `<li class="flex items-start gap-2.5">${icon("check", "w-4 h-4 text-signal shrink-0 mt-0.5")}<span>${t}</span></li>`)
  .join("");
document.getElementById("difficulty-badge").textContent = view.difficulty;

/* ---------- Icons for header buttons ---------- */
document.getElementById("bookmark-btn").innerHTML = icon("bookmark", "w-4 h-4");
document.getElementById("copy-link-btn").innerHTML = icon("link", "w-4 h-4");
document.getElementById("share-btn").innerHTML = icon("share", "w-4 h-4");

/* ---------- Bookmark (persisted locally) ---------- */
const bookmarkKey = "lumvi-bookmarks";
function getBookmarks() { try { return JSON.parse(localStorage.getItem(bookmarkKey) || "[]"); } catch { return []; } }
function setBookmarks(list) { localStorage.setItem(bookmarkKey, JSON.stringify(list)); }
const bookmarkBtn = document.getElementById("bookmark-btn");
function syncBookmarkUI() {
  const on = getBookmarks().includes(slug);
  bookmarkBtn.style.color = on ? "var(--signal)" : "";
  bookmarkBtn.style.borderColor = on ? "var(--signal)" : "";
}
bookmarkBtn.addEventListener("click", () => {
  const list = getBookmarks();
  const idx = list.indexOf(slug);
  if (idx >= 0) list.splice(idx, 1); else list.push(slug);
  setBookmarks(list);
  syncBookmarkUI();
});
syncBookmarkUI();

/* ---------- Copy link / share ---------- */
document.getElementById("copy-link-btn").addEventListener("click", async (e) => {
  try { await navigator.clipboard.writeText(window.location.href); } catch {}
  const btn = e.currentTarget;
  const original = btn.innerHTML;
  btn.innerHTML = icon("check", "w-4 h-4 text-signal");
  setTimeout(() => (btn.innerHTML = original), 1400);
});
document.getElementById("share-btn").addEventListener("click", async () => {
  if (navigator.share) {
    try { await navigator.share({ title: view.title, url: window.location.href }); } catch {}
  } else {
    try { await navigator.clipboard.writeText(window.location.href); } catch {}
  }
});

/* ---------- TOC ---------- */
const tocEl = document.getElementById("toc");
tocEl.innerHTML = view.toc.map((t) => `<a href="#${t.id}" data-toc="${t.id}" class="toc-link">${t.label}</a>`).join("");

const sectionEls = view.toc.map((t) => document.getElementById(t.id)).filter(Boolean);
const tocLinks = Array.from(document.querySelectorAll("[data-toc]"));
function syncTocActive() {
  let current = sectionEls[0];
  const scrollY = window.scrollY + 120;
  sectionEls.forEach((el) => { if (el.offsetTop <= scrollY) current = el; });
  tocLinks.forEach((l) => l.classList.toggle("active", current && l.dataset.toc === current.id));
}
window.addEventListener("scroll", syncTocActive, { passive: true });
syncTocActive();

/* ---------- Reading progress ---------- */
const progressBar = document.getElementById("reading-progress");
function syncProgress() {
  const h = document.documentElement;
  const scrolled = h.scrollTop;
  const height = h.scrollHeight - h.clientHeight;
  progressBar.style.width = height > 0 ? `${Math.min(100, (scrolled / height) * 100)}%` : "0%";
}
window.addEventListener("scroll", syncProgress, { passive: true });
syncProgress();

/* ---------- Back to top ---------- */
document.getElementById("back-to-top").addEventListener("click", () => window.scrollTo({ top: 0, behavior: "smooth" }));

/* ---------- FAQ accordion ---------- */
document.getElementById("faq-list").innerHTML = view.faqs
  .map(
    (f, i) => `
    <div class="faq-item py-2" data-open="${i === 0}">
      <button class="w-full flex items-center justify-between gap-4 py-3.5 text-left" data-faq-toggle>
        <span class="font-medium text-[15px]">${f.q}</span>
        <span class="faq-chevron shrink-0 text-muted">${icon("chevronDown", "w-4 h-4")}</span>
      </button>
      <div class="faq-answer"><div class="text-muted text-[14.5px] leading-relaxed pb-4 pr-8">${f.a}</div></div>
    </div>`
  )
  .join("");
document.querySelectorAll("[data-faq-toggle]").forEach((btn) => {
  btn.addEventListener("click", () => {
    const item = btn.closest(".faq-item");
    item.dataset.open = item.dataset.open === "true" ? "false" : "true";
  });
});

/* ---------- Continue learning ---------- */
const sameCategory = ARTICLES.filter((a) => a.category === view.categoryKey && a.slug !== slug).slice(0, 3);
document.getElementById("continue-learning").innerHTML = sameCategory
  .map(
    (a, i) => `
    <a href="${articleUrl(a.slug)}" class="flex items-center gap-4 group">
      <span class="path-node flex items-center justify-center text-[12px] shrink-0">${i + 1}</span>
      <span class="text-[14.5px] font-medium group-hover:text-signal transition-colors flex-1">${a.title}</span>
      ${icon("arrow", "w-4 h-4 text-muted shrink-0")}
    </a>`
  )
  .join("");

/* ---------- Related articles ---------- */
const related = ARTICLES.filter((a) => a.category === view.categoryKey && a.slug !== slug)
  .sort((a, b) => b.popularity - a.popularity)
  .slice(0, 3);
document.getElementById("related-grid").innerHTML = (related.length ? related : ARTICLES.filter(a=>a.slug!==slug).slice(0,3))
  .map((a) => articleCardHTML(a, { dense: true }))
  .join("");
