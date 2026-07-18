/* ===================================================================
   LUMVI KNOWLEDGE HUB — ARTICLE DATA
   A single source of truth that powers every dynamic surface on the
   hub: category counts, latest grid, most-popular rail, search index,
   and the "Start Here" path. Swap this for a real API response later
   — every renderer below just expects this shape.
=================================================================== */

const CATEGORIES = [
  { key: "white-label",  name: "White-Label AI",     icon: "layers",     blurb: "Rebrand, price, and ship AI under your own name." },
  { key: "agents",       name: "AI Agents",           icon: "bot",        blurb: "How agents actually work, end to end." },
  { key: "agency",       name: "Agency Growth",       icon: "trending",   blurb: "Turning AI into a repeatable service line." },
  { key: "automation",   name: "AI Automation",       icon: "workflow",   blurb: "Connecting agents to the tools clients already use." },
  { key: "support",      name: "Customer Support",    icon: "headset",    blurb: "Deflection, escalation, and support-desk math." },
  { key: "leadgen",      name: "Lead Generation",     icon: "target",     blurb: "Getting agents to qualify, not just chat." },
  { key: "ecommerce",    name: "E-commerce",          icon: "cart",       blurb: "Product Q&A, cart recovery, and order status." },
  { key: "integrations", name: "Integrations",        icon: "plug",       blurb: "CRMs, calendars, and the plumbing in between." },
  { key: "case-studies", name: "Case Studies",        icon: "chart",      blurb: "What agencies actually shipped, and what it earned." },
  { key: "pricing",      name: "Pricing",              icon: "tag",        blurb: "Packaging AI work so it holds its margin." },
];

const AUTHORS = [
  { name: "Victor Bernard", role: "Founder, Lumvi" },
  { name: "Marcus Oyelaran", role: "Head of Partnerships" },
  { name: "Priya Nair", role: "Solutions Engineer" },
  { name: "Tom Reyes", role: "Agency Success Lead" },
  { name: "Elena Kowalski", role: "Content & Research" },
];

const TITLES = {
  "white-label": [
    "What Is White-Label AI, Really?",
    "White-Label vs. Custom-Built AI Agents: Who Wins",
    "The Agency Checklist for Choosing a White-Label AI Platform",
    "How to Rebrand an AI Widget in Under an Hour",
    "White-Label AI Contracts: The Clauses Agencies Forget",
    "Domain, Logo, Widget: A Full Client Rebrand Walkthrough",
    "Multi-Tenant AI, Explained Without the Jargon",
    "Who Owns the Data in a White-Label AI Deal?",
    "5 Signs Your Agency Is Ready to Resell AI",
    "White-Label AI Support: Who Answers the Client's Ticket",
    "Reseller vs. Referral: Two Ways to Sell AI as an Agency",
    "Naming Your AI Product When It Isn't Really Yours",
    "How to Demo White-Label AI Without Revealing the Platform Behind It",
    "What Happens to a Client's Data When You Switch Platforms",
  ],
  agents: [
    "What Is an AI Agent? A Plain-English Definition",
    "AI Agent vs. Chatbot: The Difference That Actually Matters",
    "Inside a Response: What Happens Between Message and Reply",
    "Retrieval, Reranking, and Why Your Agent Sometimes Guesses",
    "Intent Detection Without Asking the User a Single Question",
    "Fail-Safe Mode: What a Good Agent Does When It's Unsure",
    "Session-Only Memory: Designing for Users Without Accounts",
    "How Much Context Does an Agent Actually Need?",
    "Grounding Answers in a Knowledge Base, Not a Guess",
    "The Anatomy of a Good System Prompt",
    "When to Escalate to a Human, and How to Do It Gracefully",
    "Agentic Actions: Letting Agents Book, Not Just Chat",
    "Choosing a Model Provider Without the Marketing Noise",
  ],
  agency: [
    "Turning a One-Off AI Project Into a Recurring Service Line",
    "How to Pitch AI to a Client Who's Never Used It",
    "Packaging AI as a Retainer Instead of a Project",
    "What to Put in an AI Services One-Pager",
    "The First 90 Days of Selling AI as an Agency",
    "Hiring Your First AI Implementation Specialist",
    "How Many Clients Can One Person Support?",
    "Building a Sales Deck for AI Services That Doesn't Overpromise",
    "Client Discovery Questions for AI Projects",
    "Turning Existing Web Clients Into AI Clients",
    "What Agencies Get Wrong About Scoping AI Work",
    "A Sample 12-Month AI Services Roadmap for Agencies",
    "When to Say No to an AI Project That Isn't a Fit",
  ],
  automation: [
    "Connecting an AI Agent to a Calendar Without Custom Code",
    "Webhooks 101 for Agencies: What They Are, Why You Need Them",
    "Automating Order Status Replies With Shopify and Square",
    "When Automation Should Hand Off to a Human",
    "Building a Booking Flow Inside a Chat Widget",
    "Signature Verification: Why Your Webhooks Need It",
    "Automating Follow-Ups After a Missed Chat",
    "What Belongs in an Automation vs. What Belongs in a Human Workflow",
    "Triggering Slack Alerts When an Agent Can't Answer",
    "Automation Debt: When Too Many Rules Slow an Agent Down",
  ],
  support: [
    "Deflection Rate: The Support Metric Every Client Asks About",
    "Writing a Knowledge Base an AI Agent Can Actually Use",
    "How to Set Expectations With Clients About AI Support Limits",
    "Support Tickets by the Numbers: What AI Actually Reduces",
    "Designing a Graceful Handoff to a Human Agent",
    "Multilingual Support Without Hiring Multilingual Staff",
    "Common Mistakes That Make an AI Support Agent Feel Robotic",
    "Measuring Customer Satisfaction on AI-Handled Conversations",
    "Building a Tone of Voice Guide for a Client's Agent",
    "What to Do When an Agent Gives a Confidently Wrong Answer",
  ],
  leadgen: [
    "Teaching an Agent to Qualify Leads, Not Just Answer Questions",
    "The One-Way Ratchet: How Purchase-Stage Detection Works",
    "Writing Nudges That Don't Feel Like a Popup",
    "From Chat to Calendar: Booking Meetings Inside the Widget",
    "What Counts as a Qualified Lead From a Chat Conversation?",
    "Lead Scoring Signals an AI Agent Can Actually Detect",
    "How to A/B Test a Lead Capture Flow in Chat",
    "Routing Hot Leads to Sales in Real Time",
    "Why Most Chat Widgets Undercount Their Own Leads",
  ],
  ecommerce: [
    "Product Q&A at Scale: Grounding Answers in a Live Catalog",
    "Recovering Abandoned Carts With a Conversational Nudge",
    "Order Status Lookups Without a Support Ticket",
    "Sizing, Returns, and the Questions Shoppers Actually Ask",
    "Connecting an Agent to Inventory Without Overselling",
    "Seasonal Traffic: Prepping an AI Agent for Peak Volume",
    "Upselling Without Sounding Like a Popup",
    "Handling Out-of-Stock Questions Gracefully",
  ],
  integrations: [
    "CRM Sync: What Actually Needs to Flow Both Ways",
    "Calendly vs. Acuity: Which Booking Integration Fits a Client",
    "Encrypting Third-Party Credentials the Right Way",
    "Building an Integration Once, Reusing It Across Clients",
    "What to Do When a Client's Stack Has No API",
    "A Field Guide to Webhook Failures and Retries",
    "Connecting an Agent to a Legacy CRM Without a Rebuild",
    "Zapier or Native Integration: When Each Makes Sense",
  ],
  "case-studies": [
    "How One Agency Added a Recurring AI Line in 60 Days",
    "From Free Trial to 40 Paying Clients: An Agency Breakdown",
    "What Happened When a Client's Chatbot Went Live on Black Friday",
    "Migrating 12 Clients Off a Legacy Chat Tool Without Downtime",
    "The Vertical That Converted Best for One Reseller Agency",
    "A Support Team's Ticket Volume, Before and After AI",
    "Why One Agency Fired Its Lowest-Margin AI Client",
    "Scaling From 5 to 50 Client Chatbots on One Platform",
    "What a Failed AI Rollout Taught One Agency About Scoping",
  ],
  pricing: [
    "Per-Seat vs. Usage-Based: Picking a Billing Model for Resale",
    "How to Price AI Services Without a Race to the Bottom",
    "What Margin Should an Agency Expect on White-Label AI?",
    "Building an AI Pricing Calculator Clients Actually Trust",
    "When to Charge Setup Fees vs. Fold Them Into the Retainer",
    "Renewals and Overages: Designing Billing Clients Understand",
    "Three Pricing Tiers That Actually Map to Client Size",
    "What to Do When a Client Asks for a Discount",
  ],
};

const DIFFICULTIES = ["Beginner", "Intermediate", "Advanced"];

// Deterministic pseudo-random so the "site" looks the same on every load
function seeded(seed) {
  let s = seed;
  return () => {
    s = (s * 9301 + 49297) % 233280;
    return s / 233280;
  };
}

function buildArticles() {
  const rand = seeded(42);
  const articles = [];
  let id = 1;
  const today = new Date("2026-07-15");

  CATEGORIES.forEach((cat) => {
    (TITLES[cat.key] || []).forEach((title) => {
      const author = AUTHORS[Math.floor(rand() * AUTHORS.length)];
      const daysAgo = Math.floor(rand() * 420);
      const date = new Date(today);
      date.setDate(date.getDate() - daysAgo);
      const readTime = 4 + Math.floor(rand() * 11);
      const popularity = Math.floor(rand() * 1000);
      const difficulty = DIFFICULTIES[Math.floor(rand() * DIFFICULTIES.length)];
      const updatedRecently = rand() > 0.8;

      articles.push({
        id: id++,
        slug: title.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/(^-|-$)/g, ""),
        title,
        category: cat.key,
        categoryName: cat.name,
        excerpt: cat.blurb,
        author: author.name,
        authorRole: author.role,
        date,
        readTime,
        popularity,
        difficulty,
        updatedRecently,
      });
    });
  });

  // Hand-authored guides (real content, not generated placeholders).
  // Keep this list in sync with the *_ARTICLE objects in
  // static/blog/cornerstone-content.js and integrations-guide-content.js.
  articles.push({
    id: id++,
    slug: "how-to-connect-a-platform-integration",
    title: "How to Connect Your First Platform Integration",
    category: "integrations",
    categoryName: "Integrations",
    excerpt: "The exact steps to connect Shopify, WooCommerce, Acuity, Calendly, and Square, plus how to test a connection before it goes live.",
    author: "Priya Nair",
    authorRole: "Solutions Engineer",
    date: new Date("2026-07-16"),
    readTime: 9,
    popularity: 940,
    difficulty: "Beginner",
    updatedRecently: true,
  });

  return articles.sort((a, b) => b.date - a.date);
}

const ARTICLES = buildArticles();

function formatDate(d) {
  return d.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
}

function categoryCount(key) {
  return ARTICLES.filter((a) => a.category === key).length;
}

function totalArticleCount() {
  return ARTICLES.length;
}
