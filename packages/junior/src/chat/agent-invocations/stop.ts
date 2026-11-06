import { and, eq, inArray, or } from "drizzle-orm";
import type { StateAdapter } from "chat";
import {
  getConversationStoppedAtMs,
  markConversationsStopped,
} from "@/chat/conversations/session-stop";
import { getSqlExecutor } from "@/chat/db";
import { cancelSubscriptions } from "@/chat/events/store";
import {
  getConversationWorkState,
  stopConversationWork,
} from "@/chat/task-execution/store";
import { juniorAgentInvocations } from "@/db/schema";
import { recordSubagentEnded } from "./markers";
import { isChildAgentSessionStopped } from "./list";
import {
  getChildAgentLockName,
  getLatestAgentInvocationForConversation,
  invocationFromRow,
  isTerminalAgentInvocation,
  NON_TERMINAL_AGENT_INVOCATION_STATUSES,
  parseAgentInvocationResultMessageId,
} from "./store";
import type { TerminalAgentInvocation } from "./types";

/**
 * Stop one child agent of a parent Conversation and every child agent below
 * it. The parent gets no result. SQL stops the work first. Then each
 * Conversation in the tree ends its session, and a Conversation stop request
 * aborts each run that is still working.
 *
 * Returns undefined when the child does not belong to the parent.
 */
export async function stopChildAgent(args: {
  childConversationId: string;
  parentConversationId: string;
  state?: StateAdapter;
}): Promise<{ stopped: boolean } | undefined> {
  const latest = await getLatestAgentInvocationForConversation(
    args.childConversationId,
  );
  const wasStopped =
    latest !== undefined &&
    (latest.status === "stopped" ||
      isChildAgentSessionStopped(
        latest,
        await getConversationStoppedAtMs(args.childConversationId),
      ));
  const tree = await stopChildAgentInvocations(args);
  if (!tree) {
    return undefined;
  }
  await finishStoppedTree(tree, tree.conversationIds, args.state);
  return { stopped: tree.stopped.length > 0 || !wasStopped };
}

/**
 * Stop a root Conversation's session the same way `stopAgent` stops a child:
 * its child agents stop, and it ends its own session. The caller interrupts
 * its own run.
 */
export async function stopConversationSession(args: {
  conversationId: string;
  state?: StateAdapter;
}): Promise<void> {
  await stopChildAgentsOf(args.conversationId, args.state);
  await endSessions([args.conversationId], args.state);
}

/**
 * Stop the child agents of an agent Conversation that a stop interrupted.
 * The worker of that Conversation calls this after it records its own stop,
 * so this sends no stop request to that Conversation.
 */
export async function stopChildAgentsOf(
  conversationId: string,
  state?: StateAdapter,
): Promise<void> {
  const tree = await stopAgentInvocationTree(conversationId);
  await finishStoppedTree(
    tree,
    tree.conversationIds.filter((id) => id !== conversationId),
    state,
  );
}

/**
 * Stop every child agent below a Conversation that a person stops. The
 * Conversation's own agent invocations stay as they are: when it is a child
 * agent, its worker records the stop and reports it to its parent.
 *
 * Known gap: the Conversation's run sees its stop only at its next stop
 * signal check. A child agent that it spawns before then is not stopped.
 */
export async function stopChildAgentsBelow(
  conversationId: string,
  state?: StateAdapter,
): Promise<void> {
  const nowMs = Date.now();
  const tree = await stopAgentInvocationTree(conversationId, nowMs, {
    includeRoot: false,
  });
  await skipUnreadResults(conversationId, nowMs, state);
  await finishStoppedTree(
    tree,
    tree.conversationIds.filter((id) => id !== conversationId),
    state,
  );
}

/**
 * Skip the `appended` results that still wait in a Conversation's mailbox, so
 * they start no Turn. A result that the Conversation already read keeps its
 * status. Call this after the tree stop: it skipped each `pending` result and
 * stopped each child agent, so no result becomes `appended` after this read.
 */
async function skipUnreadResults(
  conversationId: string,
  nowMs: number,
  state: StateAdapter | undefined,
): Promise<void> {
  const messages =
    (await getConversationWorkState({ conversationId, state }))?.messages ?? [];
  const invocationIds = messages.flatMap((message) => {
    const invocationId = parseAgentInvocationResultMessageId(
      message.inboundMessageId,
    );
    return invocationId ? [invocationId] : [];
  });
  if (invocationIds.length === 0) {
    return;
  }
  await getSqlExecutor()
    .db()
    .update(juniorAgentInvocations)
    .set({ parentMailboxStatus: "skipped", updatedAt: new Date(nowMs) })
    .where(
      and(
        eq(juniorAgentInvocations.parentConversationId, conversationId),
        inArray(juniorAgentInvocations.invocationId, invocationIds),
        eq(juniorAgentInvocations.parentMailboxStatus, "appended"),
      ),
    );
}

async function finishStoppedTree(
  tree: StoppedAgentTree,
  conversationIdsToStop: string[],
  state: StateAdapter | undefined,
): Promise<void> {
  for (const invocation of tree.stopped) {
    await recordSubagentEnded(invocation);
  }
  await endSessions(conversationIdsToStop, state);
  for (const conversationId of conversationIdsToStop) {
    await stopConversationWork({ conversationId, state });
  }
}

/**
 * End each Conversation's watches, then record the stop, so the worker drops
 * every event those watches delivered.
 */
async function endSessions(
  conversationIds: string[],
  state: StateAdapter | undefined,
): Promise<void> {
  for (const conversationId of conversationIds) {
    await cancelSubscriptions({ conversationId, state });
  }
  await markConversationsStopped(conversationIds);
}

/**
 * List one child Conversation and every Conversation below it, child first.
 * It takes the create lock of each Conversation before it reads that
 * Conversation's children, so no child agent can start in the tree until the
 * caller's transaction ends.
 */
async function lockAgentConversationTree(
  childConversationId: string,
): Promise<string[]> {
  const executor = getSqlExecutor();
  const conversationIds = [childConversationId];
  let parents = conversationIds;
  while (parents.length > 0) {
    for (const parent of parents) {
      await executor.withLock(
        getChildAgentLockName(parent),
        async () => undefined,
      );
    }
    const rows = await executor
      .db()
      .selectDistinct({
        childConversationId: juniorAgentInvocations.childConversationId,
      })
      .from(juniorAgentInvocations)
      .where(inArray(juniorAgentInvocations.parentConversationId, parents));
    parents = rows
      .map((row) => row.childConversationId)
      .filter((id) => !conversationIds.includes(id));
    conversationIds.push(...parents);
  }
  return conversationIds;
}

/**
 * Stop the agent invocations of one direct child agent and of every child
 * agent below it. Returns undefined when the child does not belong to the
 * parent.
 */
export async function stopChildAgentInvocations(args: {
  childConversationId: string;
  nowMs?: number;
  parentConversationId: string;
}): Promise<StoppedAgentTree | undefined> {
  const owned = await getSqlExecutor()
    .db()
    .select({ invocationId: juniorAgentInvocations.invocationId })
    .from(juniorAgentInvocations)
    .where(
      and(
        eq(
          juniorAgentInvocations.parentConversationId,
          args.parentConversationId,
        ),
        eq(
          juniorAgentInvocations.childConversationId,
          args.childConversationId,
        ),
      ),
    )
    .limit(1);
  if (owned.length === 0) {
    return undefined;
  }
  return await stopAgentInvocationTree(args.childConversationId, args.nowMs);
}

/** The Conversations of a stopped tree and the agent invocations it stopped. */
export type StoppedAgentTree = {
  conversationIds: string[];
  stopped: TerminalAgentInvocation[];
};

/**
 * Stop the agent invocations of one agent Conversation and of every child
 * agent below it. Open agent invocations become `stopped`. Results that would
 * still start or report work in the tree become `skipped`. With
 * `includeRoot: false`, the agent invocations of the Conversation itself stay
 * as they are, and only `pending` results sent to it become `skipped`.
 */
export async function stopAgentInvocationTree(
  conversationId: string,
  nowMs = Date.now(),
  options: { includeRoot: boolean } = { includeRoot: true },
): Promise<StoppedAgentTree> {
  // One transaction holds the create locks of the tree until every write is
  // done. A child agent that starts later finds its parent stopped.
  return await getSqlExecutor().withLock(
    getChildAgentLockName(conversationId),
    () => stopLockedAgentInvocationTree(conversationId, nowMs, options),
  );
}

async function stopLockedAgentInvocationTree(
  conversationId: string,
  nowMs: number,
  options: { includeRoot: boolean },
): Promise<StoppedAgentTree> {
  const db = getSqlExecutor().db();
  const now = new Date(nowMs);
  const conversationIds = await lockAgentConversationTree(conversationId);
  const stoppedChildIds = options.includeRoot
    ? conversationIds
    : conversationIds.filter((id) => id !== conversationId);
  // Stop open work first. After this write no agent invocation in the tree
  // can finish, so no new result can appear.
  const stopped = await db
    .update(juniorAgentInvocations)
    .set({
      errorMessage: "Stopped by the parent conversation",
      parentMailboxStatus: "skipped",
      result: null,
      status: "stopped",
      terminalAt: now,
      updatedAt: now,
    })
    .where(
      and(
        inArray(juniorAgentInvocations.childConversationId, stoppedChildIds),
        inArray(
          juniorAgentInvocations.status,
          NON_TERMINAL_AGENT_INVOCATION_STATUSES,
        ),
      ),
    )
    .returning();
  await db
    .update(juniorAgentInvocations)
    .set({ parentMailboxStatus: "skipped", updatedAt: now })
    .where(
      or(
        // A result sent to the tree would start new work there.
        and(
          inArray(juniorAgentInvocations.parentConversationId, stoppedChildIds),
          inArray(juniorAgentInvocations.parentMailboxStatus, [
            "pending",
            "appended",
          ]),
        ),
        // With `includeRoot: false`, an `appended` result sent to the
        // Conversation can be one that it already read, so only `pending`
        // results are skipped here. See `skipUnreadResults`.
        and(
          eq(juniorAgentInvocations.parentConversationId, conversationId),
          eq(juniorAgentInvocations.parentMailboxStatus, "pending"),
        ),
        // No later agent invocation in the tree sends a `replaced` report.
        and(
          inArray(juniorAgentInvocations.childConversationId, stoppedChildIds),
          eq(juniorAgentInvocations.parentMailboxStatus, "replaced"),
        ),
      ),
    );
  return {
    conversationIds,
    stopped: stopped.map(invocationFromRow).filter(isTerminalAgentInvocation),
  };
}
