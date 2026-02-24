import { Resonate } from "@resonatehq/sdk";
import { processEventStream } from "./workflow";
import { makeSampleEvents } from "./events";

// ---------------------------------------------------------------------------
// Resonate setup
// ---------------------------------------------------------------------------

const resonate = new Resonate();
resonate.register(processEventStream);

// ---------------------------------------------------------------------------
// Run the event sourcing demo
// ---------------------------------------------------------------------------

const crashMode = process.argv.includes("--crash");

// In crash mode: the projection store write fails at event 5 (OrderShipped)
// Resonate retries. Events 0-4 are served from cache — not re-applied.
const CRASH_AT_INDEX = crashMode ? 5 : -1;

const userId = `user_${Date.now()}`;
const events = makeSampleEvents(userId);

console.log("=== Event Sourcing / CQRS Projection Demo ===");
console.log(
  `Mode: ${crashMode ? `CRASH (projection store fails at event 05 "OrderShipped", resumes from checkpoint)` : "HAPPY PATH (process all 8 events into account projection)"}`,
);
console.log(`User:   ${userId}`);
console.log(`Events: ${events.length}\n`);

const wallStart = Date.now();

const result = await resonate.run(
  `projection/${userId}`,
  processEventStream,
  userId,
  events,
  CRASH_AT_INDEX,
);

const wallMs = Date.now() - wallStart;

console.log("\n=== Final Projection (Read Model) ===");
const p = result.finalProjection;
console.log(
  JSON.stringify(
    {
      userId: p.userId,
      name: p.name,
      email: p.email,
      subscription: p.subscription,
      subscriptionRenewals: p.subscriptionRenewals,
      totalOrders: p.totalOrders,
      cancelledOrders: p.cancelledOrders,
      activeOrders: p.activeOrders,
      shippedOrders: p.shippedOrders,
      eventsProcessed: p.eventsProcessed,
      wallTimeMs: wallMs,
    },
    null,
    2,
  ),
);

if (crashMode) {
  console.log(
    "\nNotice: events 00-04 each logged once (cached before crash).",
    "\nEvent 05 (OrderShipped) failed → retried → succeeded.",
    "\nThe projection is consistent — no event was applied twice.",
  );
}
