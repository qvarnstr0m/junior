import { afterEach, expect, it, vi } from "vitest";
import { runHeartbeat } from "@/chat/agent-dispatch/heartbeat";
import { stopConversation } from "@/chat/conversations/stop";
import { getEventCatalog } from "@/chat/events/runtime-catalog";
import { enqueueEventNotification } from "@/chat/events/notification";
import { createWatch, takeDueTimerWatches } from "@/chat/events/store";
import { runTimerWatchHeartbeat } from "@/chat/events/timers";
import { createEventTools } from "@/chat/tools/events";
import {
  createConversationId,
  recordWebConversationActivity,
} from "@/chat/conversations/web-input";
import { getConversationWorkState } from "@/chat/task-execution/store";
import {
  closeConversationFixture,
  createConversationWebHarness,
  passiveWatchMessageTexts,
} from "../fixtures/conversation";
import { context, execute } from "../fixtures/event-automations";
import { spawnChildAgent } from "../fixtures/agent-invocations";

// Timer heartbeat coverage lives here because heartbeat.test.ts is at its file limit.
afterEach(async () => {
  vi.restoreAllMocks();
  await closeConversationFixture();
});

async function timerConversation() {
  const fixture = await createConversationWebHarness();
  const conversationId = createConversationId({
    actorEmail: fixture.actor.email,
    idempotencyKey: crypto.randomUUID(),
  });
  await recordWebConversationActivity({
    actor: fixture.actor,
    conversationId,
    conversationStore: fixture.conversationStore,
    nowMs: Date.now(),
  });
  const tools = createEventTools(
    { ...context(), conversationId },
    getEventCatalog(),
  );
  return { ...fixture, conversationId, tools };
}

it("delivers timers once through heartbeat and keeps existing Watch controls", async () => {
  const clock = vi.spyOn(Date, "now").mockReturnValue(1_800_000_000_000);
  const { conversationId, tools, queue, drain, historyTexts, agentRuns } =
    await timerConversation();
  const input = { after_ms: 60_000, intent: "Check the deployment." };
  const timer = (await execute(tools.watchTimer!, input, "timer-1")) as {
    id: string;
    fires_at: string;
  };
  const firesAtMs = clock() + 60_000;
  expect(timer).toEqual({
    id: expect.any(String),
    fires_at: new Date(firesAtMs).toISOString(),
  });
  clock.mockReturnValue(clock() + 10_000);
  expect(await execute(tools.watchTimer!, input, "timer-1")).toEqual(timer);
  const cancelled = (await execute(
    tools.watchTimer!,
    input,
    "timer-cancel",
  )) as { id: string };
  await execute(tools.stopWatchingResources!, { id: cancelled.id });
  expect(await execute(tools.listWatches!, {})).toMatchObject({
    subscriptions: [{ id: timer.id, fires_at: timer.fires_at }],
  });
  await runHeartbeat({
    nowMs: firesAtMs - 1,
    conversationWorkQueue: queue,
  });
  expect(queue.sentRecords()).toHaveLength(0);

  clock.mockReturnValue(firesAtMs);
  await runHeartbeat({ nowMs: clock(), conversationWorkQueue: queue });
  expect(queue.sentRecords()).toEqual([
    expect.objectContaining({ conversationId, delayMs: 30_000 }),
  ]);
  const work = await getConversationWorkState({ conversationId });
  expect(work?.execution.pendingMessages).toHaveLength(1);
  expect(work?.execution.pendingMessages[0]).toMatchObject({
    source: "event",
    delivery: "defer",
    input: {
      text: expect.stringContaining(input.intent),
      metadata: { event: { namespace: "junior", eventType: "timer.fired" } },
    },
  });
  expect(await execute(tools.listWatches!, {})).toEqual({ subscriptions: [] });
  await expect(
    execute(tools.stopWatchingResources!, { id: timer.id }),
  ).rejects.toThrow("already completed");

  // A tool replay does not rearm a completed timer; the cancelled one stays quiet.
  clock.mockReturnValue(clock() + 120_000);
  expect(await execute(tools.watchTimer!, input, "timer-1")).toEqual(timer);
  await runTimerWatchHeartbeat({ nowMs: clock(), queue });
  expect(
    (await getConversationWorkState({ conversationId }))?.execution
      .pendingMessages,
  ).toHaveLength(1);
  expect(await takeDueTimerWatches(clock())).toEqual([]);
  await drain();
  expect(agentRuns).toHaveLength(1);
  // The timer Turn stays system-acted but can use the creator's grants.
  expect(agentRuns[0]!.credentialContext).toEqual({
    actor: { platform: "system", name: "event" },
    subject: expect.objectContaining({
      type: "user",
      userId: "U123",
      allowedWhen: "timer-watch",
      binding: expect.objectContaining({
        conversationId,
        watchId: timer.id,
      }),
    }),
  });
  expect(await historyTexts(conversationId)).toContain(
    "Conversation request complete.",
  );
});

it("recovers an abandoned claim and a failed queue send without duplicating input", async () => {
  const clock = vi.spyOn(Date, "now").mockReturnValue(1_800_000_000_000);
  const { conversationId, tools, queue } = await timerConversation();
  const timer = (await execute(
    tools.watchTimer!,
    { after_ms: 1, intent: "Check once." },
    "timer-retry",
  )) as { id: string };
  clock.mockReturnValue(clock() + 1);
  expect(await takeDueTimerWatches(clock())).toMatchObject([{ id: timer.id }]);
  // A second heartbeat cannot claim work while its lease is live.
  expect(await takeDueTimerWatches(clock())).toEqual([]);
  clock.mockReturnValue(clock() + 120_000);
  queue.rejectSends();
  await expect(
    runTimerWatchHeartbeat({ nowMs: clock(), queue }),
  ).rejects.toThrow("Timer Watch delivery failed");
  expect(
    (await getConversationWorkState({ conversationId }))?.execution
      .pendingMessages,
  ).toHaveLength(1);
  queue.allowSends();
  clock.mockReturnValue(clock() + 120_000);
  await runHeartbeat({ nowMs: clock(), conversationWorkQueue: queue });
  expect(
    (await getConversationWorkState({ conversationId }))?.execution
      .pendingMessages,
  ).toHaveLength(1);
  expect(queue.sentRecords()).toHaveLength(1);
  expect(await execute(tools.listWatches!, {})).toEqual({ subscriptions: [] });

  await execute(
    tools.watchTimer!,
    { after_ms: 1, intent: "Do not deliver stale work." },
    "timer-expired",
  );
  clock.mockReturnValue(clock() + 24 * 60 * 60 * 1000 + 2);
  await runTimerWatchHeartbeat({ nowMs: clock(), queue });
  expect(await takeDueTimerWatches(clock())).toEqual([]);
  expect(
    (await getConversationWorkState({ conversationId }))?.execution
      .pendingMessages,
  ).toHaveLength(1);
});

it("saves timers of a stopped root to its transcript until a person writes", async () => {
  const clock = vi.spyOn(Date, "now").mockReturnValue(1_800_000_000_000);
  const fixture = await timerConversation();
  const { conversationId, tools, queue, drain, agentRuns } = fixture;
  const input = { after_ms: 60_000, intent: "Check the deployment." };
  await execute(tools.watchTimer!, input, "timer-1");
  await execute(
    tools.watchTimer!,
    { after_ms: 600_000, intent: "Check much later." },
    "timer-2",
  );
  await execute(
    tools.watchTimer!,
    { after_ms: 3_600_000, intent: "Check after the reply." },
    "timer-3",
  );
  clock.mockReturnValue(clock() + 60_000);
  await runTimerWatchHeartbeat({ nowMs: clock(), queue });
  expect(
    (await getConversationWorkState({ conversationId }))?.execution
      .pendingMessages,
  ).toHaveLength(1);

  // A person's stop makes the watches passive: an event that already
  // arrived and a later one are saved to the transcript and start no Turn.
  await stopConversation({ conversationId, queue });
  expect(
    ((await execute(tools.listWatches!, {})) as { subscriptions: unknown[] })
      .subscriptions,
  ).toHaveLength(2);
  clock.mockReturnValue(clock() + 600_000);
  await runTimerWatchHeartbeat({ nowMs: clock(), queue });
  clock.mockReturnValue(clock() + 120_000);
  await drain();
  expect(agentRuns).toHaveLength(0);
  const passive = (await passiveWatchMessageTexts(conversationId)).join("\n");
  expect(passive).toContain("Check the deployment.");
  expect(passive).toContain("Check much later.");

  // A watch set after the stop wakes the root again.
  await execute(tools.watchTimer!, input, "timer-4");
  clock.mockReturnValue(clock() + 60_000);
  await runTimerWatchHeartbeat({ nowMs: clock(), queue });
  clock.mockReturnValue(clock() + 120_000);
  await drain();
  expect(agentRuns).toHaveLength(1);

  // A person's message makes the old watches active again.
  await fixture.continue({
    conversationId,
    idempotencyKey: "after-stop",
    message: "Keep watching.",
  });
  await drain();
  expect(agentRuns).toHaveLength(2);
  clock.mockReturnValue(clock() + 3_600_000);
  await runTimerWatchHeartbeat({ nowMs: clock(), queue });
  clock.mockReturnValue(clock() + 120_000);
  await drain();
  expect(agentRuns).toHaveLength(3);
  expect(agentRuns[2]!.source).toMatchObject({ kind: "event" });
});

it("keeps watches passive when the stop drops queued input, but not a re-created watch", async () => {
  const clock = vi.spyOn(Date, "now").mockReturnValue(1_800_000_000_000);
  const fixture = await timerConversation();
  const { conversationId, tools, queue, drain, agentRuns } = fixture;
  await execute(
    tools.watchTimer!,
    { after_ms: 600_000, intent: "Check the deployment." },
    "timer-1",
  );
  await execute(
    tools.watchTimer!,
    { after_ms: 1_200_000, intent: "Check again later." },
    "timer-2",
  );
  const deployWatch = () =>
    createWatch({
      conversationId,
      events: ["deployment.succeeded"],
      expiresAtMs: clock() + 3_600_000,
      identifier: "prj_junior:production",
      intent: "Report deploy results.",
      label: "Vercel deployment junior-prod",
      namespace: "vercel",
      resourceType: "deployment",
    });
  await deployWatch();

  // The stop drops a person's message that is still queued. That message
  // does not make the watches active again.
  await fixture.continue({
    conversationId,
    idempotencyKey: "before-stop",
    message: "Are you there?",
  });
  clock.mockReturnValue(clock() + 1_000);
  await expect(
    stopConversation({ conversationId, queue }),
  ).resolves.toMatchObject({ status: "requested" });
  await drain();
  expect(agentRuns).toHaveLength(0);
  clock.mockReturnValue(clock() + 600_000);
  await runTimerWatchHeartbeat({ nowMs: clock(), queue });
  clock.mockReturnValue(clock() + 120_000);
  await drain();
  expect(agentRuns).toHaveLength(0);
  expect(await passiveWatchMessageTexts(conversationId)).toEqual([
    expect.stringContaining("Check the deployment."),
  ]);

  // Watching the same resource again creates an active watch. A passive
  // timer Event in the same batch is still saved, not run.
  const recreated = await deployWatch();
  await enqueueEventNotification({
    event: {
      eventKey: "vercel:deploy-1:deployment.succeeded",
      eventType: "deployment.succeeded",
      identifier: recreated.identifier,
      namespace: recreated.namespace,
      occurredAtMs: clock(),
      trustedSummary: "Deploy succeeded.",
    },
    queue,
    subscription: recreated,
  });
  clock.mockReturnValue(clock() + 500_000);
  await runTimerWatchHeartbeat({ nowMs: clock(), queue });
  clock.mockReturnValue(clock() + 120_000);
  await drain();
  expect(agentRuns).toHaveLength(1);
  expect(agentRuns[0]!.source).toMatchObject({ kind: "event" });
  // The passive Event reaches the Turn only as transcript context.
  const { text } = agentRuns[0]!.instruction;
  expect(text).toContain("Deploy succeeded.");
  expect(text).not.toContain("Check again later.");
  expect(await passiveWatchMessageTexts(conversationId)).toEqual([
    expect.stringContaining("Check the deployment."),
    expect.stringContaining("Check again later."),
  ]);
});

it("saves a stopped child's timers until a person writes to its root", async () => {
  const clock = vi.spyOn(Date, "now").mockReturnValue(1_800_000_000_000);
  // The root needs a Destination for its child, so start it with a message.
  const fixture = await timerConversation();
  const { agentRuns, conversationStore, drain, queue, state } = fixture;
  const { conversationId } = await fixture.start({
    idempotencyKey: "root-with-child",
    message: "Start a child.",
  });
  await drain();
  const child = await spawnChildAgent({
    conversationStore,
    idempotencyKey: "child-with-timers",
    parentConversationId: conversationId,
    queue,
    state,
  });
  const childTools = createEventTools(
    { ...context(), conversationId: child.childConversationId },
    getEventCatalog(),
  );
  await execute(
    childTools.watchTimer!,
    { after_ms: 60_000, intent: "Check soon." },
    "child-timer-1",
  );
  await execute(
    childTools.watchTimer!,
    { after_ms: 600_000, intent: "Check later." },
    "child-timer-2",
  );
  await execute(
    childTools.watchTimer!,
    { after_ms: 3_600_000, intent: "Check much later." },
    "child-timer-3",
  );

  // A person's stop of the root stops the child and makes its watches
  // passive, so its timer is saved to its transcript and starts no Turn.
  clock.mockReturnValue(clock() + 1_000);
  await stopConversation({ conversationId, queue });
  await drain();
  clock.mockReturnValue(clock() + 120_000);
  await runTimerWatchHeartbeat({ nowMs: clock(), queue });
  clock.mockReturnValue(clock() + 120_000);
  await drain();
  expect(agentRuns).toHaveLength(1);
  expect(await passiveWatchMessageTexts(child.childConversationId)).toEqual([
    expect.stringContaining("Check soon."),
  ]);

  // A person's message to the root makes the child's watches active again,
  // so its next timer starts the stopped child.
  clock.mockReturnValue(clock() + 1_000);
  await fixture.continue({
    conversationId,
    idempotencyKey: "after-root-stop",
    message: "Keep going.",
  });
  await drain();
  clock.mockReturnValue(clock() + 600_000);
  await runTimerWatchHeartbeat({ nowMs: clock(), queue });
  clock.mockReturnValue(clock() + 120_000);
  await drain();
  expect(
    agentRuns
      .filter((run) => run.conversationId === child.childConversationId)
      .map((run) => run.source.kind),
  ).toEqual(["event"]);

  // A person's stop of only the child stays in effect when a person writes
  // to the root, because the root itself was not stopped.
  await stopConversation({ conversationId: child.childConversationId, queue });
  await drain();
  clock.mockReturnValue(clock() + 1_000);
  await fixture.continue({
    conversationId,
    idempotencyKey: "after-child-stop",
    message: "Carry on.",
  });
  await drain();
  clock.mockReturnValue(clock() + 3_600_000);
  await runTimerWatchHeartbeat({ nowMs: clock(), queue });
  clock.mockReturnValue(clock() + 120_000);
  await drain();
  expect(
    agentRuns.filter((run) => run.conversationId === child.childConversationId),
  ).toHaveLength(1);
  expect(await passiveWatchMessageTexts(child.childConversationId)).toEqual([
    expect.stringContaining("Check soon."),
    expect.stringContaining("Check much later."),
  ]);
});
