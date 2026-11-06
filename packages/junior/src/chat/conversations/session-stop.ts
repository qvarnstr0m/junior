import type { StateAdapter } from "chat";
import { and, eq, inArray, isNotNull } from "drizzle-orm";
import { appendConversationMessages } from "@/chat/conversations/messages";
import { getConversationEventStore, getSqlExecutor } from "@/chat/db";
import {
  eventBatchMessageMeta,
  isEventMailboxMetadata,
  renderEventBatchText,
} from "@/chat/events/notification";
import { getWatch } from "@/chat/events/store";
import {
  normalizeConversationText,
  upsertConversationMessage,
} from "@/chat/services/conversation-memory";
import { coerceThreadConversationState } from "@/chat/state/conversation";
import type { InboundMessage } from "@/chat/task-execution/store";
import type {
  ConversationWorkerContext,
  ConversationWorkerResult,
} from "@/chat/task-execution/worker";
import { juniorConversations } from "@/db/schema";
import { nextDetailVersion } from "@/chat/conversations/sql/detail-version";

/** Skip reason on the Message that saves Events from passive Watches. */
const PASSIVE_WATCH_SKIP_REASON = "passive watch";

/**
 * Record a session stop on each Conversation. Automated input received before
 * it is dropped, and the Watches that exist now become passive until a person
 * sends input.
 */
export async function markConversationsStopped(
  conversationIds: readonly string[],
  nowMs = Date.now(),
): Promise<void> {
  if (conversationIds.length === 0) {
    return;
  }
  const at = new Date(nowMs);
  await getSqlExecutor()
    .db()
    .update(juniorConversations)
    .set({
      stoppedAt: at,
      watchesPassiveAt: at,
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

async function readConversationStop(conversationId: string): Promise<
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

/** Make a Conversation's passive Watches active again. */
async function reactivateWatches(conversationId: string): Promise<void> {
  await getSqlExecutor()
    .db()
    .update(juniorConversations)
    .set({ watchesPassiveAt: null, detailVersion: nextDetailVersion })
    .where(
      and(
        eq(juniorConversations.conversationId, conversationId),
        isNotNull(juniorConversations.watchesPassiveAt),
      ),
    );
}

/**
 * Apply the last session stop to automated input. Input from a person makes
 * the Conversation's Watches active again and always runs, as does input from
 * a parent tool call. Events from passive Watches are saved as one Message
 * and start no Turn. Agent results and agent messages received at or before
 * the stop are dropped. Returns undefined when the work should run.
 */
export async function skipInputAfterConversationStop(
  context: ConversationWorkerContext,
  state?: StateAdapter,
): Promise<ConversationWorkerResult | undefined> {
  const messages = context.attempt.messages;
  // Parent agent input does not reactivate Watches: only a person does.
  if (messages.some(isPersonInput)) {
    await reactivateWatches(context.conversationId);
    return undefined;
  }
  if (messages.length === 0 || !messages.every(isEventInput)) {
    return undefined;
  }
  const stop = await readConversationStop(context.conversationId);
  if (!stop) {
    return undefined;
  }
  const passive: InboundMessage[] = [];
  for (const message of messages) {
    const watchId = eventWatchId(message);
    if (watchId !== undefined) {
      if (!(await isPassiveWatch(watchId, stop.watchesPassiveAtMs, state))) {
        return undefined;
      }
      passive.push(message);
    } else if (
      stop.stoppedAtMs === undefined ||
      message.receivedAtMs > stop.stoppedAtMs
    ) {
      return undefined;
    }
  }
  if (passive.length > 0) {
    await savePassiveWatchEvents(context.conversationId, passive);
  }
  await context.attempt.ack();
  return { status: "completed" };
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
 * sent input after that stop. A Watch that a later Turn created stays active.
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

/** Save Events as the one Message that a Turn would save for them. */
async function savePassiveWatchEvents(
  conversationId: string,
  messages: readonly InboundMessage[],
): Promise<void> {
  const first = messages[0]!;
  const conversation = coerceThreadConversationState(undefined);
  upsertConversationMessage(conversation, {
    id: first.inboundMessageId,
    role: "user",
    text: normalizeConversationText(renderEventBatchText(messages)),
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
  await appendConversationMessages(getConversationEventStore(), {
    conversation,
    conversationId,
  });
}
