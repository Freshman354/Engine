# Privacy Policy — Draft Addition (Shopify Protected Customer Data)

**Not legal advice. I'm not a lawyer.** This is a technical description of what the
system now actually does, written to slot into your existing policy — given you
explicitly said the goal here is avoiding lawsuits, have an actual lawyer review this
before it goes live, not just me. I've matched the existing document's tone/numbering
so it's easy to place, not so it's ready to publish as-is.

## Where this fits

Your existing Section 9 ("White-Label Use and Connected Third-Party Platforms")
already covers the general principle correctly — Agency is the data controller,
Lumvi is the processor, Shopify is named as an example of a platform an Agency can
connect. What's below is more specific: exactly which fields, how long, and that
access is now logged. Suggested as a new subsection **9.1**, directly after the
existing Section 9 text.

---

### 9.1 Shopify-Specific Data Handling

When an Agency connects a Shopify store, Lumvi's chatbot may access the following
customer data from that store, solely to answer order-status questions on the
Agency's behalf:

- **Order status and details** (order number, fulfillment status, line items, total)
- **Customer email** (used to verify a chat visitor is asking about their own order)
- **Customer name** (used only for personalized responses, e.g. "Hi [name], your
  order is...")

We do not request or access customer phone numbers or shipping/billing addresses.

**Retention.** Shopify order data is retained only while the Agency's Shopify
integration remains connected. If an Agency disconnects or uninstalls the Shopify
integration, this data is automatically and permanently deleted within 30 days.
[VERIFY: confirm 30 days matches your final decision before publishing —
this description assumes retention tied to disconnection, as built.]

**Access logging.** Access to Shopify customer order data — whether by the chatbot
answering a question or as part of processing a data request from Shopify or a
customer — is logged for security and audit purposes. These logs record when data
was accessed and why, not the data itself.

**Shopify's mandatory data rights processes.** As required by Shopify for any app
accessing its platform, we process the following requests from Shopify on behalf of
connected Agencies:
- A customer's request for the data we hold about them (fulfilled by compiling
  matching order data and providing it to the Agency)
- A customer's request to delete their data (fulfilled by deleting matching order
  records)
- A store's request to delete all data following an app uninstall (fulfilled by
  deleting all data associated with that store)

---

## Also worth a look, not urgent

- **Section 6 (Data Retention)** describes retention for Lumvi accounts generally
  ("Deleted Accounts: data deleted within 30 days"). Worth confirming with whoever
  reviews this that the Shopify-specific language above is consistent with that
  general policy, or cross-referencing one from the other, rather than having two
  separate retention statements that could be read as conflicting.
- **Section 10 (Third-Party Services)** lists Lumvi's own infrastructure vendors
  (Railway, Neon, Google, etc.) but not Shopify/WooCommerce/Acuity/Calendly/Square —
  this appears intentional (those are platforms an *Agency* connects, not vendors
  *Lumvi* chose), consistent with how Section 9 already frames it. Flagging so
  whoever reviews this confirms that distinction is deliberate, not a gap.
