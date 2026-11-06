/** Shared parent Conversation setup for agent invocation integration tests. */
import type { StateAdapter } from "chat";
import { expect } from "vitest";
import { createSlackSource } from "@sentry/junior-plugin-api";
import { listChildAgents } from "@/chat/agent-invocations/list";
import { getAgentInvocation } from "@/chat/agent-invocations/store";
import type { AgentInvocation } from "@/chat/agent-invocations/types";
import { createAndEnqueueAgentInvocation } from "@/chat/agent-invocations/work";
import { migrateSchema } from "@/chat/conversations/sql/migrations";
import { createSqlStore } from "@/chat/conversations/sql/store";
import type { ConversationStore } from "@/chat/conversations/store";
import { getConversationEventStore } from "@/chat/db";
import {
  getConversationWorkState,
  hasConversationStop,
  startConversationWork,
} from "@/chat/task-execution/store";
import type { ConversationWorkQueueTestAdapter } from "./conversation-work";
import { createConfiguredJuniorSqlFixture } from "./sql";
export const parentConversationId = "local:test:parent-agent";
export const destination = {
  conversationId: parentConversationId,
  platform: "local",
} as const;
export const invocationInput = {
  actor: { name: "parent-agent", platform: "system" } as const,
  destination,
  input: "Summarize the durable task.",
  parentConversationId,
  reasoningLevel: "medium" as const,
};
export const slackParentConversationId = "slack:C123:1712345.0001";
export const slackDestination = {
  platform: "slack",
  teamId: "T123",
  channelId: "C123",
} as const;
const slackSource = createSlackSource({
  channelId: "C123",
  teamId: "T123",
  threadTs: "1712345.0001",
  visibility: "public",
});
export const slackInvocationInput = {
  ...invocationInput,
  actor: {
    platform: "slack",
    teamId: "T123",
    userId: "U123",
  } as const,
  destination: slackDestination,
  parentConversationId: slackParentConversationId,
};

export async function prepareParentConversation() {
  const fixture = createConfiguredJuniorSqlFixture();
  await migrateSchema(fixture.sql);
  const conversationStore = createSqlStore(fixture.sql);
  await conversationStore.recordActivity({
    conversationId: parentConversationId,
    destination,
    nowMs: 1_000,
    source: "local",
  });
  return { conversationStore, fixture };
}

export async function prepareSlackParentConversation() {
  const fixture = createConfiguredJuniorSqlFixture();
  await migrateSchema(fixture.sql);
  const conversationStore = createSqlStore(fixture.sql);
  await conversationStore.recordActivity({
    conversationId: slackParentConversationId,
    destination: slackDestination,
    nowMs: 1_000,
    sessionSource: slackSource,
    source: "slack",
    visibility: "public",
  });
  return { conversationStore, fixture };
}

export async function loadTurnEvents(conversationId: string) {
  return (await getConversationEventStore().loadHistory(conversationId)).filter(
    (event) =>
      event.data.type === "turn_started" ||
      event.data.type === "turn_completed" ||
      event.data.type === "turn_failed",
  );
}

export async function loadSubagentEvents(conversationId: string) {
  return (await getConversationEventStore().loadHistory(conversationId))
    .map((event) => event.data)
    .filter(
      (data) =>
        data.type === "subagent_started" || data.type === "subagent_ended",
    );
}

export async function loadMessageEvents(conversationId: string) {
  return (await getConversationEventStore().loadHistory(conversationId))
    .map((event) => event.data)
    .filter((data) => data.type === "message");
}

/**
 * Start a child agent of a parent and hold its work lease, like a child
 * worker whose run has not finished.
 */
export async function startRunningChildAgent(args: {
  conversationStore: ConversationStore;
  idempotencyKey: string;
  parentConversationId: string;
  queue: ConversationWorkQueueTestAdapter;
  state: StateAdapter;
}): Promise<{ invocation: AgentInvocation; runId: string }> {
  // A child agent reports to the destination of the parent's run.
  const destination = (
    await getConversationWorkState({
      conversationId: args.parentConversationId,
      state: args.state,
    })
  )?.destination;
  if (!destination) {
    throw new Error("Expected a parent Conversation with a destination");
  }
  const invocation = await createAndEnqueueAgentInvocation(
    {
      ...invocationInput,
      destination,
      idempotencyKey: args.idempotencyKey,
      parentConversationId: args.parentConversationId,
    },
    args,
  );
  // The child worker took its wake, so a queue drain does not run it.
  args.queue.takeMessageFor(invocation.childConversationId);
  const lease = await startConversationWork({
    conversationId: invocation.childConversationId,
    conversationStore: args.conversationStore,
    state: args.state,
  });
  const runId = (
    await getConversationWorkState({
      conversationId: invocation.childConversationId,
      state: args.state,
    })
  )?.execution.runId;
  if (lease.status !== "acquired" || !runId) {
    throw new Error("Expected a running child agent");
  }
  return { invocation, runId };
}

/** Expect a stop of the parent to have stopped its running child agent. */
export async function expectChildAgentStopped(
  child: { invocation: AgentInvocation; runId: string },
  state: StateAdapter,
): Promise<void> {
  const { childConversationId, invocationId, parentConversationId } =
    child.invocation;
  await expect(getAgentInvocation(invocationId)).resolves.toMatchObject({
    errorMessage: "Stopped by the parent conversation",
    parentMailboxStatus: "skipped",
    status: "stopped",
  });
  // The child worker sees this request and aborts its run.
  await expect(
    hasConversationStop({
      conversationId: childConversationId,
      runId: child.runId,
      state,
    }),
  ).resolves.toBe(true);
  await expect(listChildAgents(parentConversationId)).resolves.toEqual([
    expect.objectContaining({ childConversationId, status: "stopped" }),
  ]);
}
