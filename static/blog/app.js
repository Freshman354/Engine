/* ===================================================================
   THE CORNERSTONE ARTICLE
   The one fully authored, human-quality guide the rest of the hub is
   built around. Every other article slug falls back to the templated
   generator in article-render.js.
=================================================================== */

const CORNERSTONE_SLUG = "complete-guide-white-label-ai-agents";

const CORNERSTONE_ARTICLE = {
  title: "The Complete Guide to White-Label AI Agents",
  category: "White-Label AI",
  categoryKey: "white-label",
  author: "Victor Bernard",
  authorRole: "Founder, Lumvi",
  date: "July 2026",
  readTime: 22,
  difficulty: "Intermediate",
  updatedRecently: true,
  quickSummary:
    "White-labeling an AI agent means putting your agency's name on a platform someone else built and maintains. Done well, it's a real service line with real margin. Done badly, it's a support liability with your logo on it. This guide covers what to evaluate, how to price it, what belongs in the contract, and the exact steps to get a client's widget live.",
  keyTakeaways: [
    "White-label AI is a resale relationship, not a build. Your job is packaging, pricing, and support, not model training.",
    "Per-seat pricing is easier for clients to understand than usage-based pricing, but usage-based protects your margin on high-volume accounts.",
    "The contract should be explicit about data ownership, especially what happens to a client's conversation history if they leave.",
    "A rebrand is more than a logo swap. Domain, widget copy, and support handoff all need to match the client's brand, not yours.",
    "The most common launch mistake is skipping a real knowledge base review before go-live.",
  ],
  toc: [
    { id: "what-is-white-label-ai", label: "What white-label AI actually means" },
    { id: "build-vs-buy", label: "Build, buy, or white-label?" },
    { id: "pricing", label: "Pricing models that hold their margin" },
    { id: "contracts", label: "What belongs in the contract" },
    { id: "rebrand", label: "The client rebrand walkthrough" },
    { id: "mistakes", label: "Common mistakes to avoid" },
    { id: "launch", label: "The launch checklist" },
  ],
  bodyHTML: `
    <h2 id="what-is-white-label-ai">What white-label AI actually means</h2>
    <p>White-label AI is simple in concept and easy to get wrong in practice. A platform vendor builds and maintains the underlying agent (the model routing, the retrieval pipeline, the uptime) and your agency puts its own name, domain, and support process on top of it. The client never knows the platform exists. As far as they're concerned, they hired your agency to build them an AI chatbot, and that's exactly what they got.</p>
    <p>The distinction that matters isn't technical, it's operational. You are not responsible for whether the model hallucinates on a hard question. You <em>are</em> responsible for whether the knowledge base you loaded is any good, whether the widget matches the client's site, and whether someone answers when a client emails asking why the bot gave a wrong price.</p>
    <div class="callout p-5 my-6">
      <p class="badge bg-cobalt text-white mb-2 w-fit">Founder insight</p>
      <p class="text-[14.5px] leading-relaxed">The agencies that do well with white-label AI stop thinking of themselves as reselling software and start thinking of themselves as running a support desk with a very fast first responder. That mental shift changes how you price it, staff it, and sell it.</p>
    </div>

    <h2 id="build-vs-buy">Build, buy, or white-label?</h2>
    <p>Most agencies land on white-label after trying, or seriously pricing out, the other two options. Here's how the three actually compare once you account for the full cost of ownership, not just the sticker price.</p>
    <table class="comparison-table w-full text-[14px] my-8 border-collapse">
      <thead>
        <tr><th class="text-left">Approach</th><th class="text-left">Time to first client</th><th class="text-left">Who owns uptime</th><th class="text-left">Best for</th></tr>
      </thead>
      <tbody>
        <tr><td>Build in-house</td><td>4–8 months</td><td>You</td><td>Agencies with existing ML engineers to spare</td></tr>
        <tr><td>Buy per-client tools</td><td>1–2 weeks per client</td><td>Vendor, per tool</td><td>One-off projects, no repeat service line</td></tr>
        <tr><td>White-label platform</td><td>Same day</td><td>Platform vendor</td><td>Agencies building a recurring AI service line</td></tr>
      </tbody>
    </table>
    <p>Building in-house makes sense if AI infrastructure is genuinely your differentiator and you already have the team. For most web and marketing agencies, it isn't. The differentiator is the client relationship, the onboarding, and the support, not the retrieval pipeline underneath it.</p>

    <h2 id="pricing">Pricing models that hold their margin</h2>
    <p>Two pricing structures dominate white-label AI resale, and they trade off in predictable ways.</p>
    <h3>Per-seat pricing</h3>
    <p>You pay the platform a flat fee per client account, and you charge the client a markup on that seat. It's easy to explain in a proposal (something like "$X per month per chatbot") and it's easy to forecast. The risk is a high-volume client burning far more in usage than their seat covers, quietly eating your margin.</p>
    <h3>Usage-based pricing</h3>
    <p>You pass through (with markup) whatever the client's agent actually costs to run: conversations, messages, or tokens. It protects your margin at scale, but it's a harder number to put in a sales deck, and clients tend to be wary of anything that sounds like it could spike unexpectedly.</p>
    <div class="callout-tip p-5 my-6">
      <p class="badge bg-signal text-ink mb-2 w-fit">Pro tip</p>
      <p class="text-[14.5px] leading-relaxed">A hybrid works well for most agencies: quote per-seat for the sales conversation, but set a fair-use volume threshold in the contract with a defined overage rate. Clients get the simple number; you get the margin protection.</p>
    </div>

    <h2 id="contracts">What belongs in the contract</h2>
    <p>Three clauses get skipped most often, and they're the three that cause the worst conversations later:</p>
    <ul>
      <li><strong>Data ownership on exit.</strong> If a client cancels, do they get an export of their conversation history and knowledge base? Say so explicitly. Silence here reads as "no" to a client's lawyer.</li>
      <li><strong>Response-time commitments.</strong> If you're promising "AI-powered support," define what happens when the AI can't answer. A vague SLA becomes your problem the first time a client's customer waits four hours for a human reply.</li>
      <li><strong>Who owns the underlying platform relationship.</strong> Make clear that the client's contract is with your agency, not the platform vendor. This protects you from disintermediation and sets expectations about who they call.</li>
    </ul>

    <h2 id="rebrand">The client rebrand walkthrough</h2>
    <p>A rebrand that stops at swapping a logo will feel unfinished to the client the first time they actually use it. A complete rebrand touches four things:</p>
    <ol>
      <li><strong>Widget appearance.</strong> Accent color, avatar, and greeting message should match the client's brand voice, not a generic default.</li>
      <li><strong>Domain and sender identity.</strong> Any emails or links the agent sends should come from the client's domain, not the platform's.</li>
      <li><strong>Escalation copy.</strong> When the agent hands off to a human, the language should sound like the client's support team, not a vendor's canned message.</li>
      <li><strong>Support ownership.</strong> The client's customers should never learn a platform vendor exists. Every support channel routes through your agency first.</li>
    </ol>
    <p>Most white-label platforms let you configure all four without touching code. The work is deciding what the client's brand actually sounds like, which is a conversation, not a settings panel.</p>

    <h2 id="mistakes">Common mistakes to avoid</h2>
    <div class="callout-warn p-5 my-6">
      <p class="badge" style="background:#C15A3B; color:white;" >Common mistake</p>
      <p class="text-[14.5px] leading-relaxed mt-2">Loading a client's old FAQ page into the knowledge base and calling it done. Most FAQ pages were written for a human skimming a page, not for a retrieval system matching a specific question. Rewriting the content as clear question-and-answer pairs takes an afternoon and meaningfully improves accuracy.</p>
    </div>
    <p>Beyond the knowledge base, the two other repeat offenders: launching without testing the agent against the client's actual hardest support questions (not the easy ones), and failing to tell the client's existing support staff that the agent is going live. They end up finding out from a confused customer instead of from you.</p>

    <h2 id="launch">The launch checklist</h2>
    <ul>
      <li>Knowledge base rewritten as clear Q&A pairs, not pasted from an old FAQ page</li>
      <li>Widget tested on the client's actual site, on mobile and desktop</li>
      <li>Escalation path confirmed with a real human on the other end</li>
      <li>Client's support team briefed before go-live, not after</li>
      <li>Ten hardest real support questions tested against the live agent</li>
      <li>Billing and seat details confirmed with the client in writing</li>
    </ul>
  `,
  faqs: [
    {
      q: "How long does it take to launch a white-label AI agent for a new client?",
      a: "Most agencies using a mature white-label platform can go from signed contract to a live, tested widget in about a week. Most of that time goes into building a good knowledge base, not technical setup.",
    },
    {
      q: "Do clients need to know the platform is white-labeled?",
      a: "No, and most agencies keep it that way. The client's contract, support relationship, and billing all run through the agency; the underlying platform stays invisible.",
    },
    {
      q: "What happens if a client wants to leave?",
      a: "This should be defined in your contract before launch. Most platforms support exporting conversation history and knowledge base content, but whether you pass that along (and in what format) is a decision your agency makes, not the platform.",
    },
    {
      q: "Is usage-based or per-seat pricing better for a new agency?",
      a: "Per-seat is usually easier to sell early on because it's a simple number in a proposal. As you take on higher-volume clients, a hybrid model with a fair-use threshold protects margin without complicating the pitch.",
    },
    {
      q: "Can one platform really support 36 different business verticals?",
      a: "Yes, when the knowledge base and integrations are configured per client rather than shared. The underlying agent architecture stays the same; what changes per vertical is the content it's grounded in and which integrations (booking, e-commerce, CRM) are turned on.",
    },
  ],
};
