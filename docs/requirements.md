# LinkedIn Outreach Dashboard — Requirements

**Business use case:** Is our **founder-led LinkedIn outreach** working, and where
should we focus to improve it? Founders (Tim, Sascha, Mark) run **Dripify**
campaigns that auto-send connection requests to people at target companies. This
dashboard monitors that outreach — from requests sent, to connections accepted, to
replies — across the founder accounts, over Dripify (activity) + LinkedIn/Unipile
(outcomes) data.

## Questions the dashboard must answer
1. **Volume** — How much outreach are we doing (**connection requests sent**), by
   founder and over time — is activity consistent and at target?
2. **Acceptance** — Are our requests **landing**? What's the **acceptance rate**
   (accepted ÷ sent), and is it improving?
3. **Reply / engagement** — Are accepted connections turning into **conversations**?
   What's the **reply rate** (replied ÷ accepted)?
4. **Founder performance** — How do **Tim / Sascha / Mark** compare on volume,
   acceptance and reply — and what's the **combined** pipeline?
5. **Network growth** — Is the founders' **combined connection base** growing over time?

## Metrics & definitions
- **Requests sent** — connection requests Dripify sent in the period.
- **Accepted** — requests that became connections (new first-degree connections).
- **Acceptance rate** — accepted ÷ sent, for the period.
- **Replied** — accepted connections who sent at least one message back.
- **Reply rate** — replied ÷ accepted.
- **New connections** — connections gained in the period (= accepted).
- **By founder** — each founder is one LinkedIn/Unipile account + one Dripify seat.
- **Change vs prev period** — % (or point) change vs the equivalent period before.

## Data sources (be explicit — this shapes what's real vs pending)
| Metric | Source | Status |
|---|---|---|
| Accepted / new connections / network growth | **Unipile** (connections) | **Live** — pipeline built |
| Replied / reply rate | **Unipile** (messages) | **Live** — pipeline built |
| **Requests sent** | **Dripify** | **Pending** — no public API; via CSV export or Zapier/Make → Koalake |
| **Acceptance rate** (needs *sent*) | Dripify + Unipile | **Pending** on Dripify |

> Unipile alone gives us the *outcome* half (connections + replies). The *sent*
> denominator — and therefore acceptance rate — needs Dripify data landed in Koalake.
> **This mockup uses placeholder numbers throughout** to show the target state.

## Filters
- **Period** — hierarchical **Year › Quarter › Month**, single-select. "Previous
  period" = the equivalent period before; the comparison label names it (e.g. "vs Jun 2026").
- **Founder account** — Tim / Sascha / Mark, **multi-select**. All selected = the
  combined view; isolate one to see that founder alone. A fixed **By Founder** card
  always compares the three side-by-side regardless of the filter.

## Not in v1 (revisit later)
- **Targeting quality** — which target companies / campaigns convert best. The
  highest-value *improvement* lever, but 100% Dripify-dependent — add once Dripify
  data is flowing.
- **HubSpot** opportunity/meeting stage — Phase 2.

## Notes
- Mockup style follows the **Koalake matomo dashboard** house style (light,
  data-dense, Koalake blue, sidebar filters, inline-SVG charts).
- Founder colours (colorblind-safe, validated): Tim `#3b89fa` · Sascha `#eb6834` ·
  Mark `#1baf7a` — always shown with a name label, never colour alone.
- All numbers are **illustrative placeholder data** for design review only.