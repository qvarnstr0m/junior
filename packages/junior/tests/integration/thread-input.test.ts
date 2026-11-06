import { createSlackSource, createWebSource } from "@sentry/junior-plugin-api";
import { eq } from "drizzle-orm";
import { afterEach, describe, expect, it } from "vitest";
import { createAcpConversations } from "@/api/acp/conversations";
import { bindMessageAgent } from "@/chat/agent-invocations/message";
import {
  completeAgentInvocation,
  createAgentInvocationForMessage,
  getAgentInvocation,
  getActiveAgentInvocationForConversation,
  getAgentInvocationTurnId,
  markAgentInvocationRunning,
} from "@/chat/agent-invocations/store";
import { createAndEnqueueAgentInvocation } from "@/chat/agent-invocations/work";
import { checkRunConsistency } from "@/chat/agent/run-consistency";
import type { AgentRun } from "@/chat/agent/types";
import type { CreateAgentInvocationInput } from "@/chat/agent-invocations/types";
import {
  appendAndEnqueueWebMessage,
  createAndEnqueueConversation,
  WebMessageRefusedError,
  webActorFromUser,
} from "@/chat/conversations/web-input";
import { openConversationProjection } from "@/chat/conversations/projection";
import { getConversationEventStore, getDb } from "@/chat/db";
import { resolveViewerUser } from "@/chat/plugins/viewer";
import { AuthorizationFlowDisabledError } from "@/chat/services/auth-pause";
import {
  getLocationConfigurationService,
  persistThreadStateById,
} from "@/chat/runtime/thread-state";
import { disconnectStateAdapter } from "@/chat/state/adapter";
import { coerceThreadConversationState } from "@/chat/state/conversation";
import { buildDeterministicTurnId } from "@/chat/state/turn-id";
import {
  getTurnRecord,
  saveTurnCheckpoint,
} from "@/chat/task-execution/checkpoint";
import { recordTurnAuthorization } from "@/chat/task-execution/authorized-turn";
import { getAuthPausedConversationTurnId } from "@/chat/task-execution/mailbox-turn";
import { getConversationWorkState } from "@/chat/task-execution/store";
import { wakePausedTurn } from "@/chat/task-execution/turn-wake";
import { createMessageAgentTool } from "@/chat/tools/runtime/message-agent";
import { juniorConversations } from "@/db/schema";
import {
  prepareSlackParentConversation,
  slackDestination,
  slackInvocationInput,
  slackParentConversationId,
} from "../fixtures/agent-invocations";
import { webTestActor } from "../fixtures/conversation";
import { createConversationWorkHarness } from "../fixtures/conversation-work";

/** Compose production Conversation work with a Slack root. */
async function createHarness() {
  const { conversationStore, fixture } = await prepareSlackParentConversation();
  // Integration tests do not run the agent. The runner checks each run as
  // the agent does first, reads it, and stops it. The test finishes each
  // Agent invocation itself, or makes its runs fail with `invocationError`.
  const runs: AgentRun[] = [];
  let invocationError: Error | undefined;
  const work = await createConversationWorkHarness({
    run: async (run) => {
      await checkRunConsistency(run);
      runs.push(run);
      if (run.agentInvocationId && invocationError) {
        throw invocationError;
      }
      throw new Error("stop after reading the run");
    },
  });
  /** Run the next queued wake of one Conversation. */
  const process = async (conversationId: string) =>
    await work.process(work.queue.takeMessageFor(conversationId));
  return {
    close: async () => await fixture.close(),
    conversationStore,
    /** Run every queued wake of one Conversation, as the queue does. */
    drain: async (conversationId: string) => {
      while (
        work.queue
          .queuedMessages()
          .some((message) => message.conversationId === conversationId)
      ) {
        await process(conversationId);
      }
    },
    /** Make each later Agent invocation run fail with this error. */
    failInvocationsWith: (error: Error) => {
      invocationError = error;
    },
    process,
    queue: work.queue,
    runs,
    state: work.state,
  };
}

type Harness = Awaited<ReturnType<typeof createHarness>>;

/** Finish the open Agent invocation of a child, then report its result. */
async function finishInvocation(harness: Harness, childConversationId: string) {
  const invocation =
    await getActiveAgentInvocationForConversation(childConversationId);
  if (!invocation) throw new Error("Expected an open Agent invocation");
  await completeAgentInvocation({
    invocationId: invocation.invocationId,
    result: "Found it.",
    status: "completed",
  });
  await expect(harness.process(childConversationId)).resolves.toMatchObject({
    status: "completed",
  });
}

/**
 * Start a named child agent and store it as a Thread. Spawn creates only
 * Tasks until Threads have their own entry point.
 */
async function startThread(
  harness: Harness,
  idempotencyKey: string,
  input: Partial<
    Pick<CreateAgentInvocationInput, "destination" | "parentConversationId">
  > = {},
) {
  const created = await createAndEnqueueAgentInvocation(
    {
      ...slackInvocationInput,
      ...input,
      agentName: "researcher",
      idempotencyKey,
    },
    {
      conversationStore: harness.conversationStore,
      queue: harness.queue,
      state: harness.state,
    },
  );
  await getDb()
    .update(juniorConversations)
    .set({ kind: "thread" })
    .where(eq(juniorConversations.conversationId, created.childConversationId));
  return created.childConversationId;
}

async function sendPersonMessage(
  harness: Harness,
  conversationId: string,
  idempotencyKey: string,
) {
  return await appendAndEnqueueWebMessage(
    {
      actor: webTestActor,
      conversationId,
      idempotencyKey,
      message: "What did you find?",
    },
    {
      conversationStore: harness.conversationStore,
      queue: harness.queue,
      state: harness.state,
    },
  );
}

/** Give the Thread more work from its parent agent, like messageAgent. */
async function sendParentMessage(harness: Harness, toolCallId: string) {
  const parentRun: AgentRun = {
    actor: slackInvocationInput.actor,
    conversationId: slackParentConversationId,
    destination: slackDestination,
    instruction: { text: "Coordinate the work." },
    source: createSlackSource({
      channelId: "C123",
      teamId: "T123",
      threadTs: "1712345.0001",
      visibility: "public",
    }),
    turnId: "parent-turn",
  };
  await createMessageAgentTool(
    bindMessageAgent(parentRun, {
      queue: harness.queue,
      state: harness.state,
    })!,
  ).execute!(
    { text: "Also check the retry path.", to: "researcher" },
    {
      toolCallId,
    },
  );
}

/**
 * Save what a sign-in pause leaves: the person's Message and their Turn,
 * which waits for a sign-in callback.
 */
async function savePersonTurnPausedForSignIn(threadId: string) {
  const messageId = "api-msg_person-paused";
  await persistThreadStateById(threadId, {
    conversation: {
      ...coerceThreadConversationState({}),
      messages: [
        {
          author: { userName: webTestActor.userName },
          createdAtMs: 1,
          id: messageId,
          meta: { explicitMention: true, source: "web" },
          role: "user",
          text: "What did you find?",
        },
      ],
    },
  });
  const turnId = buildDeterministicTurnId(messageId);
  await saveTurnCheckpoint({
    mode: "paused",
    conversationId: threadId,
    turnId,
    sliceId: 1,
    reason: "auth",
    messages: [
      {
        role: "user",
        content: [{ type: "text", text: "What did you find?" }],
        timestamp: 1,
      },
    ],
    destination: slackDestination,
    source: createWebSource(threadId, "public"),
    actor: webTestActor,
    surface: "api",
  });
  return turnId;
}

/**
 * Save the slice that a parent's Agent invocation leaves after it took its
 * input: `running` when its worker died, `paused` when it yielded.
 */
async function saveInvocationSlice(
  threadId: string,
  mode: "paused" | "running",
): Promise<string> {
  const { invocation } = await createAgentInvocationForMessage({
    actor: slackInvocationInput.actor,
    childConversationId: threadId,
    destination: slackDestination,
    idempotencyKey: "thread-next",
    input: "Also check the retry path.",
    parentConversationId: slackParentConversationId,
    parentToolCallId: "call-next",
  });
  await markAgentInvocationRunning(invocation.invocationId);
  const history = (
    await openConversationProjection({ conversationId: threadId })
  ).messages;
  const slice = {
    actor: slackInvocationInput.actor,
    conversationId: threadId,
    destination: slackDestination,
    messages: [
      ...history,
      {
        role: "user" as const,
        content: [
          { type: "text" as const, text: "Also check the retry path." },
        ],
        timestamp: Date.now(),
      },
    ],
    sliceId: 1,
    source: { kind: "agent_invocation" as const },
    surface: "internal" as const,
    turnId: getAgentInvocationTurnId(invocation.invocationId),
  };
  await saveTurnCheckpoint(
    mode === "paused"
      ? { ...slice, mode, reason: "yield" }
      : { ...slice, mode },
  );
  await expect(getTurnRecord(threadId, slice.turnId)).resolves.toMatchObject({
    state: mode,
  });
  return invocation.invocationId;
}

/** Record the person's sign-in and send its one empty wake. */
async function signInAndWake(
  harness: Harness,
  threadId: string,
  turnId: string,
) {
  const request = await recordTurnAuthorization({
    actorId: "U123",
    authorizationId: "authorization-1",
    conversationId: threadId,
    destination: slackDestination,
    kind: "plugin",
    provider: "github",
    turnId,
  });
  await wakePausedTurn(request!, {
    conversationStore: harness.conversationStore,
    queue: harness.queue,
    state: harness.state,
  });
}

describe("Thread input", () => {
  afterEach(async () => {
    await disconnectStateAdapter();
  });

  it("runs a person's Turn in a Thread beside its Agent invocations", async () => {
    const harness = await createHarness();
    try {
      await persistThreadStateById(slackParentConversationId, {
        conversation: {
          ...coerceThreadConversationState({}),
          messages: [
            {
              author: { userName: "dana" },
              createdAtMs: 1_000,
              id: "root-message-1",
              role: "user",
              text: "Ship the migration before the backfill.",
            },
          ],
        },
      });
      await getLocationConfigurationService(slackDestination).set({
        key: "jira.project",
        value: "PLAT",
      });
      const threadId = await startThread(harness, "thread-spawn");
      await harness.process(threadId);
      await finishInvocation(harness, threadId);

      await expect(
        sendPersonMessage(harness, threadId, "person-1"),
      ).resolves.toMatchObject({ status: "accepted" });
      await harness.process(threadId);

      const [invocationRun, personRun] = harness.runs;
      expect(invocationRun).toMatchObject({
        actor: slackInvocationInput.actor,
        agentInvocationId: expect.any(String),
      });
      // The person's Turn borrows the invocation Destination and the root's
      // provider defaults, has no Location to post to, and cannot start or
      // message child agents.
      expect(personRun).toMatchObject({
        actor: webTestActor,
        destination: slackDestination,
        disabledFeatures: ["subagents"],
        instruction: {
          context: expect.stringContaining(
            '<root-conversation source="junior">',
          ),
          includeConversationContextWithHistory: true,
        },
      });
      expect(personRun?.instruction.context).toContain(
        "Ship the migration before the backfill.",
      );
      expect(personRun?.agentInvocationId).toBeUndefined();
      expect(personRun?.location).toBeUndefined();
      await expect(
        personRun?.environment?.locationConfiguration?.resolve("jira.project"),
      ).resolves.toBe("PLAT");

      const thread = await harness.conversationStore.get({
        conversationId: threadId,
      });
      expect(thread).toMatchObject({ kind: "thread" });
      expect(thread).not.toHaveProperty("destination");
      expect(thread).not.toHaveProperty("location");
      // Only the Agent invocation reports to the parent.
      const parentWork = await getConversationWorkState({
        conversationId: slackParentConversationId,
        state: harness.state,
      });
      expect(parentWork?.messages).toHaveLength(1);
    } finally {
      await harness.close();
    }
  });

  it("runs a person's Turn in a Thread under a web root", async () => {
    const harness = await createHarness();
    try {
      const viewer = await resolveViewerUser(webTestActor.email);
      if (!viewer) throw new Error("Expected a web test viewer");
      const root = await createAndEnqueueConversation(
        {
          actor: webActorFromUser(viewer),
          idempotencyKey: "web-root",
          message: "Start the work.",
        },
        {
          conversationStore: harness.conversationStore,
          queue: harness.queue,
          state: harness.state,
        },
      );
      const rootDestination = {
        platform: "local",
        conversationId: root.conversationId,
      } as const;
      const threadId = await startThread(harness, "thread-spawn", {
        destination: rootDestination,
        parentConversationId: root.conversationId,
      });
      await harness.process(threadId);
      await finishInvocation(harness, threadId);

      await sendPersonMessage(harness, threadId, "person-1");
      await harness.process(threadId);

      // The Turn replies in the Thread with the Destination of its root.
      expect(harness.runs.at(-1)).toMatchObject({
        actor: webTestActor,
        conversationId: threadId,
        destination: rootDestination,
        source: { conversationId: threadId, kind: "web" },
      });
    } finally {
      await harness.close();
    }
  });

  it("runs Agent invocation input that arrives during a sign-in pause", async () => {
    const harness = await createHarness();
    try {
      const threadId = await startThread(harness, "thread-spawn");
      await harness.process(threadId);
      await finishInvocation(harness, threadId);
      const personTurnId = await savePersonTurnPausedForSignIn(threadId);
      await expect(getAuthPausedConversationTurnId(threadId)).resolves.toBe(
        personTurnId,
      );

      // A paused person's Turn does not hold the Thread. The parent's input
      // runs as soon as it arrives.
      await sendParentMessage(harness, "call-message");
      await harness.process(threadId);
      expect(harness.runs.at(-1)?.agentInvocationId).toEqual(
        expect.any(String),
      );
      await expect(getAuthPausedConversationTurnId(threadId)).resolves.toBe(
        personTurnId,
      );
      await finishInvocation(harness, threadId);

      // Sign-in completes and wakes the Turn, as the OAuth callback does.
      const record = await getTurnRecord(threadId, personTurnId);
      await wakePausedTurn(
        {
          conversationId: threadId,
          destination: slackDestination,
          expectedVersion: record!.version,
          turnId: personTurnId,
        },
        {
          conversationStore: harness.conversationStore,
          queue: harness.queue,
          state: harness.state,
        },
      );
      await harness.process(threadId);

      const resumed = harness.runs.at(-1);
      expect(resumed).toMatchObject({
        actor: { platform: "web" },
        destination: slackDestination,
        turnId: personTurnId,
      });
      expect(resumed?.agentInvocationId).toBeUndefined();
      // The wake does not give the Thread a Destination. The invocation
      // worker refuses a child that has one.
      await expect(
        harness.conversationStore.get({ conversationId: threadId }),
      ).resolves.not.toHaveProperty("destination");
    } finally {
      await harness.close();
    }
  });

  it("recovers a lost Agent invocation before a signed-in person's Turn", async () => {
    const harness = await createHarness();
    try {
      const threadId = await startThread(harness, "thread-spawn");
      await harness.process(threadId);
      await finishInvocation(harness, threadId);
      const personTurnId = await savePersonTurnPausedForSignIn(threadId);
      // The parent's next Agent invocation took its input, then its worker
      // died mid-Turn. Only the heartbeat's empty wake can recover it.
      const invocationId = await saveInvocationSlice(threadId, "running");
      await signInAndWake(harness, threadId, personTurnId);
      const runCount = harness.runs.length;
      await harness.drain(threadId);

      // The wake ends the lost Agent invocation and reports it to the
      // parent. Then the person's Turn resumes.
      await expect(getAgentInvocation(invocationId)).resolves.toMatchObject({
        errorMessage: "Agent invocation lost its worker before it could pause",
        status: "failed",
      });
      expect(
        (
          await getConversationWorkState({
            conversationId: slackParentConversationId,
            state: harness.state,
          })
        )?.messages.map((message) => message.input.metadata?.event),
      ).toContainEqual(
        expect.objectContaining({
          eventType: "agent_invocation.failed",
          identifier: invocationId,
        }),
      );
      expect(harness.runs[runCount]).toMatchObject({
        actor: { platform: "web" },
        turnId: personTurnId,
      });
    } finally {
      await harness.close();
    }
  });

  it("resumes a signed-in person's Turn after the Agent invocation ends", async () => {
    const harness = await createHarness();
    try {
      const threadId = await startThread(harness, "thread-spawn");
      await harness.process(threadId);
      await finishInvocation(harness, threadId);
      const personTurnId = await savePersonTurnPausedForSignIn(threadId);
      // The parent's next Agent invocation yielded between slices. It takes
      // the sign-in wake and ends blocked.
      const invocationId = await saveInvocationSlice(threadId, "paused");
      harness.failInvocationsWith(
        new AuthorizationFlowDisabledError("plugin", "github"),
      );
      await signInAndWake(harness, threadId, personTurnId);
      const runCount = harness.runs.length;
      await harness.drain(threadId);

      await expect(getAgentInvocation(invocationId)).resolves.toMatchObject({
        status: "blocked",
      });
      // The person's Turn gets its own attempt after the Agent invocation.
      // The fake runner stops it, so its later runs are retries.
      expect(harness.runs.slice(runCount, runCount + 2)).toMatchObject([
        { agentInvocationId: invocationId },
        { actor: { platform: "web" }, turnId: personTurnId },
      ]);
    } finally {
      await harness.close();
    }
  });

  it("refuses web and ACP input to a Task", async () => {
    const harness = await createHarness();
    try {
      const viewer = await resolveViewerUser(webTestActor.email);
      if (!viewer) throw new Error("Expected a web test viewer");
      const root = await createAndEnqueueConversation(
        {
          actor: webActorFromUser(viewer),
          idempotencyKey: "web-root",
          message: "Start the work.",
        },
        {
          conversationStore: harness.conversationStore,
          queue: harness.queue,
          state: harness.state,
        },
      );
      const task = await createAndEnqueueAgentInvocation(
        {
          ...slackInvocationInput,
          destination: {
            platform: "local",
            conversationId: root.conversationId,
          },
          idempotencyKey: "task-spawn",
          parentConversationId: root.conversationId,
        },
        {
          conversationStore: harness.conversationStore,
          queue: harness.queue,
          state: harness.state,
        },
      );
      const taskId = task.childConversationId;
      const mailboxBefore = (
        await getConversationWorkState({
          conversationId: taskId,
          state: harness.state,
        })
      )?.messages;

      await expect(
        sendPersonMessage(harness, taskId, "person-1"),
      ).rejects.toBeInstanceOf(WebMessageRefusedError);
      const acp = createAcpConversations({
        conversationStore: harness.conversationStore,
        eventStore: getConversationEventStore(),
        queue: harness.queue,
        state: harness.state,
      });
      await expect(
        acp.prompt({
          conversationId: taskId,
          idempotencyKey: "acp-1",
          text: "Change course.",
          user: viewer,
        }),
      ).resolves.toEqual({ status: "refused" });

      await expect(
        getConversationWorkState({
          conversationId: taskId,
          state: harness.state,
        }),
      ).resolves.toMatchObject({ messages: mailboxBefore });
      await expect(
        harness.conversationStore.get({ conversationId: taskId }),
      ).resolves.not.toHaveProperty("destination");
    } finally {
      await harness.close();
    }
  });
});
