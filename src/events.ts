import type { Context } from "@resonatehq/sdk";

// ---------------------------------------------------------------------------
// Event Sourcing — Domain Events + Projection
// ---------------------------------------------------------------------------
//
// Each UserEvent is an immutable fact about something that happened.
// The current state of the account is derived by applying events in order.
//
// This is the read model (CQRS "Q" side): the projection.
// The write model is just appending events to the sequence.
//
// Resonate's role: process each event durably (exactly once).
// If the processor crashes at event 5, events 1-4 are already checkpointed.
// On resume, those are returned from cache. Processing continues at event 5.
// No event is applied twice — the projection is consistent even after a crash.

// ---------------------------------------------------------------------------
// Event types (the write model / command side produces these)
// ---------------------------------------------------------------------------

export type UserEventType =
  | "UserRegistered"
  | "ProfileUpdated"
  | "SubscriptionActivated"
  | "OrderPlaced"
  | "OrderShipped"
  | "OrderCancelled"
  | "SubscriptionRenewed";

export interface UserEvent {
  eventId: string;
  type: UserEventType;
  timestamp: string;
  payload: Record<string, unknown>;
}

// ---------------------------------------------------------------------------
// Projection (the read model / query side)
// ---------------------------------------------------------------------------

export interface AccountProjection {
  userId: string;
  name: string;
  email: string;
  subscription: "none" | "active" | "cancelled";
  subscriptionRenewals: number;
  totalOrders: number;
  cancelledOrders: number;
  activeOrders: string[];
  shippedOrders: string[];
  eventsProcessed: number;
}

export function initialProjection(userId: string): AccountProjection {
  return {
    userId,
    name: "",
    email: "",
    subscription: "none",
    subscriptionRenewals: 0,
    totalOrders: 0,
    cancelledOrders: 0,
    activeOrders: [],
    shippedOrders: [],
    eventsProcessed: 0,
  };
}

// ---------------------------------------------------------------------------
// applyEvent — pure projection function (no side effects)
// ---------------------------------------------------------------------------
// Takes current state + one event, returns next state.
// This is the core of event sourcing: state = events.reduce(applyEvent, initial)
//
// Each call is wrapped in ctx.run() in the workflow, making it a durable step.
// On crash: completed events return their cached projection. No re-processing.

const attemptMap = new Map<string, number>();

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export async function applyEvent(
  _ctx: Context,
  eventIndex: number,
  event: UserEvent,
  state: AccountProjection,
  crashAtIndex: number,
): Promise<AccountProjection> {
  const key = `event:${eventIndex}`;
  const attempt = (attemptMap.get(key) ?? 0) + 1;
  attemptMap.set(key, attempt);

  // Simulate processing latency (schema validation, enrichment, etc.)
  await sleep(50);

  if (crashAtIndex === eventIndex && attempt === 1) {
    console.log(
      `  [event ${String(eventIndex).padStart(2, "0")}]  ${event.type}  ✗  (projection store write failed)`,
    );
    throw new Error(
      `Projection store write failed for event ${event.eventId}`,
    );
  }

  const retryTag = attempt > 1 ? ` (retry ${attempt})` : "";
  const next = project(state, event);
  console.log(
    `  [event ${String(eventIndex).padStart(2, "0")}]  ${event.type}${retryTag}  →  ${summarize(next)}`,
  );

  return next;
}

// ---------------------------------------------------------------------------
// Pure projection logic — applies one event to produce next state
// ---------------------------------------------------------------------------

function project(state: AccountProjection, event: UserEvent): AccountProjection {
  switch (event.type) {
    case "UserRegistered":
      return {
        ...state,
        name: event.payload["name"] as string,
        email: event.payload["email"] as string,
        eventsProcessed: state.eventsProcessed + 1,
      };

    case "ProfileUpdated":
      return {
        ...state,
        name: (event.payload["name"] as string) ?? state.name,
        email: (event.payload["email"] as string) ?? state.email,
        eventsProcessed: state.eventsProcessed + 1,
      };

    case "SubscriptionActivated":
      return {
        ...state,
        subscription: "active",
        eventsProcessed: state.eventsProcessed + 1,
      };

    case "OrderPlaced": {
      const orderId = event.payload["orderId"] as string;
      return {
        ...state,
        totalOrders: state.totalOrders + 1,
        activeOrders: [...state.activeOrders, orderId],
        eventsProcessed: state.eventsProcessed + 1,
      };
    }

    case "OrderShipped": {
      const orderId = event.payload["orderId"] as string;
      return {
        ...state,
        activeOrders: state.activeOrders.filter((id) => id !== orderId),
        shippedOrders: [...state.shippedOrders, orderId],
        eventsProcessed: state.eventsProcessed + 1,
      };
    }

    case "OrderCancelled": {
      const orderId = event.payload["orderId"] as string;
      return {
        ...state,
        activeOrders: state.activeOrders.filter((id) => id !== orderId),
        cancelledOrders: state.cancelledOrders + 1,
        eventsProcessed: state.eventsProcessed + 1,
      };
    }

    case "SubscriptionRenewed":
      return {
        ...state,
        subscriptionRenewals: state.subscriptionRenewals + 1,
        eventsProcessed: state.eventsProcessed + 1,
      };
  }
}

// ---------------------------------------------------------------------------
// Summarize current projection state for log output
// ---------------------------------------------------------------------------

function summarize(state: AccountProjection): string {
  const parts: string[] = [];
  if (state.name) parts.push(`name="${state.name}"`);
  if (state.subscription !== "none") parts.push(`sub=${state.subscription}`);
  if (state.totalOrders > 0) parts.push(`orders=${state.totalOrders}`);
  if (state.activeOrders.length > 0)
    parts.push(`active=[${state.activeOrders.join(",")}]`);
  if (state.shippedOrders.length > 0)
    parts.push(`shipped=[${state.shippedOrders.join(",")}]`);
  if (state.subscriptionRenewals > 0)
    parts.push(`renewals=${state.subscriptionRenewals}`);
  return parts.join(" ") || "{}";
}

// ---------------------------------------------------------------------------
// Sample event stream for the demo
// ---------------------------------------------------------------------------

export function makeSampleEvents(userId: string): UserEvent[] {
  const ts = (offset: number) =>
    new Date(Date.now() - offset * 60_000).toISOString();

  return [
    {
      eventId: `evt_001`,
      type: "UserRegistered",
      timestamp: ts(60),
      payload: { userId, name: "Alice Chen", email: "alice@example.com" },
    },
    {
      eventId: `evt_002`,
      type: "ProfileUpdated",
      timestamp: ts(55),
      payload: { name: "Alice Chen-Watson" },
    },
    {
      eventId: `evt_003`,
      type: "SubscriptionActivated",
      timestamp: ts(50),
      payload: { plan: "pro", billingCycle: "annual" },
    },
    {
      eventId: `evt_004`,
      type: "OrderPlaced",
      timestamp: ts(40),
      payload: { orderId: "ord_001", items: ["item_a", "item_b"], total: 89.99 },
    },
    {
      eventId: `evt_005`,
      type: "OrderPlaced",
      timestamp: ts(30),
      payload: { orderId: "ord_002", items: ["item_c"], total: 24.99 },
    },
    {
      eventId: `evt_006`,
      type: "OrderShipped",
      timestamp: ts(20),
      payload: { orderId: "ord_001", carrier: "UPS", trackingId: "1Z999" },
    },
    {
      eventId: `evt_007`,
      type: "OrderCancelled",
      timestamp: ts(10),
      payload: { orderId: "ord_002", reason: "changed_mind" },
    },
    {
      eventId: `evt_008`,
      type: "SubscriptionRenewed",
      timestamp: ts(0),
      payload: { period: "2027-annual", amount: 199.00 },
    },
  ];
}
