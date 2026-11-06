import { eq } from "drizzle-orm";
import { getChildAgentLabel } from "@/chat/agent-invocations/store";
import { readAutomationTitlesByConversationId } from "@/chat/automations/execution-stats";
import { getConversationStore, getDb } from "@/chat/db";
import { conversationKindFromRow } from "@/chat/conversations/sql/conversation-row";
import type { ConversationKind } from "@/chat/conversations/store";
import { juniorConversations } from "@/db/schema";
import type { ConversationAgentTreeNode } from "../schema/conversation";
import type { ConversationAccessReader } from "./access";
import { readFirstHumanMessageTexts } from "./first-message";
import { conversationSummaryFromStoredConversation } from "./projection";

/** Bound the parent chain that one agent tree reads. */
const MAX_AGENT_TREE_DEPTH = 16;

/** Name a child the viewer cannot read, or one with no agent label. */
function childLabel(kind: ConversationKind): string {
  return kind === "thread" ? "Thread" : "Task";
}

async function readTreeNode(
  conversationId: string,
): Promise<{ kind: ConversationKind; parentId?: string } | undefined> {
  const [row] = await getDb()
    .select({
      kind: juniorConversations.kind,
      parentConversationId: juniorConversations.parentConversationId,
    })
    .from(juniorConversations)
    .where(eq(juniorConversations.conversationId, conversationId));
  if (!row) return undefined;
  return {
    kind: conversationKindFromRow(row),
    ...(row.parentConversationId
      ? { parentId: row.parentConversationId }
      : undefined),
  };
}

/**
 * Read the agent tree of a child Conversation: its parent chain, root first,
 * with the child last. Returns undefined for a root Conversation. Each
 * Conversation has its own access check, so a viewer sees a label only where
 * they can see private content.
 */
export async function readConversationAgentTree(
  conversationId: string,
  readAccess: ConversationAccessReader,
): Promise<ConversationAgentTreeNode[] | undefined> {
  // Read only parent ids and kinds. Every detail request calls this, and a
  // root Conversation must not need a full Conversation record.
  const ids = [conversationId];
  const kindById = new Map<string, ConversationKind>();
  let node = await readTreeNode(conversationId);
  if (node) kindById.set(conversationId, node.kind);
  let parentId = node?.parentId;
  while (
    parentId &&
    !ids.includes(parentId) &&
    ids.length < MAX_AGENT_TREE_DEPTH
  ) {
    ids.unshift(parentId);
    node = await readTreeNode(parentId);
    if (node) kindById.set(parentId, node.kind);
    parentId = node?.parentId;
  }
  if (ids.length < 2) {
    return undefined;
  }
  // After a cycle or the depth bound, the first id is still a child.
  const root = parentId
    ? undefined
    : await getConversationStore().get({ conversationId: ids[0]! });

  const [
    access,
    automationTitleByConversation,
    firstHumanMessageTextByConversation,
  ] = await Promise.all([
    readAccess(ids),
    readAutomationTitlesByConversationId(root ? [root] : []),
    readFirstHumanMessageTexts(getDb(), root ? [root] : []),
  ]);
  return await Promise.all(
    ids.map(async (id) => {
      const itemAccess = access.get(id);
      if (root?.conversationId === id) {
        // The summary projection owns root titles and their privacy labels.
        const summary = conversationSummaryFromStoredConversation({
          access: itemAccess,
          automationTitle: automationTitleByConversation.get(id),
          conversation: root,
          durationMs: 0,
          firstHumanMessageText: firstHumanMessageTextByConversation.get(id),
        });
        return {
          conversationId: id,
          kind: "root" as const,
          label: summary.displayTitle,
        };
      }
      // The walk above read every id in the chain.
      const kind = kindById.get(id) ?? "task";
      if (!itemAccess?.canViewPrivateContent) {
        return { conversationId: id, kind, label: childLabel(kind) };
      }
      const agent = await getChildAgentLabel(id);
      return {
        conversationId: id,
        kind,
        label: agent?.title ?? agent?.agentName ?? childLabel(kind),
      };
    }),
  );
}
