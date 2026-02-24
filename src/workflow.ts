import type { Context } from "@resonatehq/sdk";
import {
  applyEvent,
  initialProjection,
  type UserEvent,
  type AccountProjection,
} from "./events";

// ---------------------------------------------------------------------------
// Event Sourcing Workflow
// ---------------------------------------------------------------------------
//
// Processes a stream of domain events into a state projection durably.
// Each event application is an independent checkpoint.
//
// The pattern:
//   projection = events.reduce(applyEvent, initialState)
//   ...but with crash recovery at each step.
//
// On crash at event 5:
//   ✓ Events 0-4 return from cache (projection state preserved)
//   ✓ Processing resumes at event 5
//   ✓ Events 0-4 are NOT re-applied (projection is consistent)
//
// This solves the fundamental problem of stateful stream processing:
// how do you resume a reduction without re-processing from the beginning?
//
// Kafka / Flink answer: offset management + checkpointing (complex)
// Resonate answer: yield* ctx.run() per event (one line of code)
//
// CQRS note: this is the "Q" side (query / read model). The events
// themselves (write model) are typically produced by command handlers.
// Resonate durably processes them into a queryable projection.

export interface ProjectionResult {
  userId: string;
  eventsProcessed: number;
  finalProjection: AccountProjection;
}

export function* processEventStream(
  ctx: Context,
  userId: string,
  events: UserEvent[],
  crashAtIndex: number,
): Generator<any, ProjectionResult, any> {
  // Start with empty projection
  let projection = initialProjection(userId);

  // Process events one by one — each is a durable checkpoint.
  //
  // This looks like a simple for-loop, but each yield* is a checkpoint.
  // Resonate stores the projection result of each event in its promise store.
  // On crash: the loop replays, but completed events return their cached
  // projection immediately. Only the failed event retries.
  //
  // The result: exactly-once event processing with zero application code
  // for offset tracking, checkpointing, or deduplication.
  for (let i = 0; i < events.length; i++) {
    const event = events[i]!;
    projection = yield* ctx.run(applyEvent, i, event, projection, crashAtIndex);
  }

  return {
    userId,
    eventsProcessed: projection.eventsProcessed,
    finalProjection: projection,
  };
}
