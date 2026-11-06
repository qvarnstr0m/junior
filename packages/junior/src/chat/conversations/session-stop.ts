import type { StateAdapter } from "chat";
import { and, eq, inArray, lt, lte, sql } from "drizzle-orm";
import { alias } from "drizzle-orm/pg-core";
import { toStoredConversationMessage } from "@/chat/conversations/messages";
import { getConversationEventStore, getSqlExecutor } from "@/chat/db";
import { instructionForInputs } from "@/chat/agent/prompt";
import {
  eventBatchMessageMeta,
  isEventMailboxMetadata,
  renderEventBatchInputs,
} from "@/chat/events/notification";
import { cancelSubscriptions, getWatch } from "@/chat/events/store";
import { getEventBatchDelayMs } from "@/chat/task-execution/mailbox-turn";
import { eventReviewItemInputs } from "@/chat/review-items/prompt-input";
import { normalizeConversationText } from "@/chat/services/conversation-memory";
import type { InboundMessage } from "@/chat/task-execution/store";
import type {
  ConversationWorkerContext,
  ConversationWorkerResult,
} from "@/chat/task-execution/worker";
import { juniorConversations } from "@/db/schema";
import { nextDetailVersion } from "@/chat/conversations/sql/detail-version";
import {
  conversationSubtree,
  conversationSubtreeColumns,
} from "@/chat/conversations/sql/subtree";

/** Skip reason on the Message that saves Events from passive Watches. */
const PASSIVE_WATCH_SKIP_REASON = "passive watch";

/**
 * Record a session stop on each Conversation. Automated input received before
 * it is dropped. With `watches: "passive"`, a person stopped the
 * Conversations: their Watches that exist now become passive until a person
 * sends input. With `watches: "cancel"`, an agent stopped them, so their
 * Watches end.
 */
export async function markConversationsStopped(
  conversationIds: readonly string[],
  options: {
    nowMs?: number;
    state?: StateAdapter;
    watches: "cancel" | "passive";
  },
): Promise<void> {
  if (conversationIds.length === 0) {
    return;
  }
  if (options.watches === "cancel") {
    // Cancel first, so the worker drops every Event those Watches delivered.
    for (const conversationId of conversationIds) {
      await cancelSubscriptions({ conversationId, state: options.state });
    }
  }
  const at = new Date(options.nowMs ?? Date.now());
  await getSqlExecutor()
    .db()
    .update(juniorConversations)
    .set({
      stoppedAt: at,
      watchesPassiveAt: options.watches === "passive" ? at : null,
      detailVersion: nextDetailVersion,
    })
    .where(inArray(juniorConversations.conversationId, [...conversationIds]));
}

/** Return when a Conversation's session was last stopped. */
export async function getConversationStoppedAtMs(
  conversationId: string,
): Promise<number | undefined> {
  return (await readConversationStop(conversationId))?.stoppedAtMs;
}

/** Read when a Conversation last stopped and when its Watches became passive. */
export async function readConversationStop(conversationId: string): Promise<
  | {
      stoppedAtMs?: number;
      watchesPassiveAtMs?: number;
    }
  | undefined
> {
  const [row] = await getSqlExecutor()
    .db()
    .select({
      stoppedAt: juniorConversations.stoppedAt,
      watchesPassiveAt: juniorConversations.watchesPassiveAt,
    })
    .from(juniorConversations)
    .where(eq(juniorConversations.conversationId, conversationId))
    .limit(1);
  if (!row) {
    return undefined;
  }
  return {
    stoppedAtMs: row.stoppedAt?.getTime(),
    watchesPassiveAtMs: row.watchesPassiveAt?.getTime(),
  };
}

/**
 * Make the passive Watches of a stopped Conversation and of every child agent
 * below it active again. Nothing changes when the Conversation itself is not
 * passive. Only Watches made passive before `beforeMs`, and no later than the
 * Conversation's own Watches, change. So a stop that came after the input, or
 * a later stop of one child agent, stays in effect.
 */
export async function reactivateWatchTree(
  conversationId: string,
  beforeMs: number,
): Promise<void> {
  const target = alias(juniorConversations, "target");
  const targetPassiveAt = getSqlExecutor()
    .db()
    .select({ watchesPassiveAt: target.watchesPassiveAt })
    .from(target)
    .where(eq(target.conversationId, conversationId));
  await getSqlExecutor()
    .db()
    .update(juniorConversations)
    .set({ watchesPassiveAt: null, detailVersion: nextDetailVersion })
    .where(
      and(
        inArray(
          juniorConversations.conversationId,
          sql`(select ${conversationSubtreeColumns.conversationId} from ${conversationSubtree([conversationId])})`,
        ),
        lt(juniorConversations.watchesPassiveAt, new Date(beforeMs)),
        lte(juniorConversations.watchesPassiveAt, targetPassiveAt),
      ),
    );
}

/**
 * Apply the last session stop to automated input. Input from a person always
 * runs. When a person sent it after the stop, the Watches of the Conversation
 * and of its child agents become active again. Input from a parent tool call
 * also runs. Events from passive Watches are saved as one Message and start
 * no Turn. Other Events received at or before the stop are dropped. Returns
 * the work to run, with only the Events that are left, or the finished result
 * when nothing is left.
 */
export async function skipInputAfterConversationStop(
  context: ConversationWorkerContext,
  state?: StateAdapter,
): Promise<
  { context: ConversationWorkerContext } | { skipped: ConversationWorkerResult }
> {
  const messages = context.attempt.messages;
  const personInput = messages.filter(isPersonInput);
  if (personInput.length > 0) {
    // The batch can still hold input that the stop cancels. Only input sent
    // after the stop reactivates the Watches.
    await reactivateWatchTree(
      context.conversationId,
      Math.max(...personInput.map((message) => message.receivedAtMs)),
    );
    return { context };
  }
  if (messages.length === 0 || !messages.every(isEventInput)) {
    return { context };
  }
  const stop = await readConversationStop(context.conversationId);
  if (!stop) {
    return { context };
  }
  const passive: InboundMessage[] = [];
  const live: InboundMessage[] = [];
  for (const message of messages) {
    const watchId = eventWatchId(message);
    if (
      watchId !== undefined &&
      (await isPassiveWatch(watchId, stop.watchesPassiveAtMs, state))
    ) {
      passive.push(message);
    } else if (
      stop.stoppedAtMs === undefined ||
      message.receivedAtMs > stop.stoppedAtMs
    ) {
      live.push(message);
    }
  }
  if (live.length === messages.length) {
    return { context };
  }
  if (live.length > 0) {
    // Events of all Watches debounce in one batch. Wait out the window of the
    // whole batch and of the Events that run before any Event is saved, so a
    // later attempt does not save the passive Events again.
    const delayMs = Math.max(
      getEventBatchDelayMs(messages),
      getEventBatchDelayMs(live),
    );
    if (delayMs > 0) {
      return { skipped: { status: "deferred", delayMs } };
    }
  }
  if (passive.length > 0) {
    await savePassiveWatchEvents(context.conversationId, passive);
  }
  if (live.length > 0) {
    // The ack of the attempt still covers the saved and dropped Events.
    return {
      context: { ...context, attempt: { ...context.attempt, messages: live } },
    };
  }
  await context.attempt.ack();
  return { skipped: { status: "completed" } };
}

function isPersonInput(message: InboundMessage): boolean {
  return message.source === "web" || message.source === "slack";
}

function isEventInput(message: InboundMessage): boolean {
  return (
    message.source === "event" && isEventMailboxMetadata(message.input.metadata)
  );
}

/** Return the Watch that delivered this Event. Agent input has none. */
function eventWatchId(message: InboundMessage): string | undefined {
  const metadata = message.input.metadata;
  return isEventMailboxMetadata(metadata)
    ? metadata.event.subscriptionId
    : undefined;
}

/**
 * A Watch is passive when it existed at the last session stop and no person
 * sent input after that stop. A Watch that is created or re-created later
 * stays active.
 *
 * Known gap: the running Turn sees the stop only at its next stop signal
 * check. A Watch that it creates before then stays active.
 */
async function isPassiveWatch(
  watchId: string,
  watchesPassiveAtMs: number | undefined,
  state: StateAdapter | undefined,
): Promise<boolean> {
  if (watchesPassiveAtMs === undefined) {
    return false;
  }
  const watch = await getWatch({ id: watchId, state });
  return !watch || watch.createdAtMs <= watchesPassiveAtMs;
}

/**
 * Save Events as the one Message that a Turn would save for them. The append
 * is idempotent by Message id, so a retried batch saves nothing new.
 */
async function savePassiveWatchEvents(
  conversationId: string,
  messages: readonly InboundMessage[],
): Promise<void> {
  const first = messages[0]!;
  const { text } = instructionForInputs(
    renderEventBatchInputs(messages, await eventReviewItemInputs(messages)).map(
      (input) => ({ kind: "runtime" as const, text: input }),
    ),
  );
  const stored = toStoredConversationMessage({
    id: first.inboundMessageId,
    role: "user",
    text: normalizeConversationText(text),
    createdAtMs: first.createdAtMs,
    ...(first.input.authorIdentityId
      ? { authorIdentityId: first.input.authorIdentityId }
      : undefined),
    meta: {
      replied: false,
      skippedReason: PASSIVE_WATCH_SKIP_REASON,
      ...eventBatchMessageMeta(messages),
    },
  });
  const { createdAtMs, ...data } = stored;
  await getConversationEventStore().append(conversationId, [
    {
      idempotencyKey: `message:${stored.messageId}`,
      createdAtMs,
      data: { type: "message", ...data },
    },
  ]);
}
