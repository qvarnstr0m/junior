import type { Location } from "@/chat/conversations/location";
import type { Conversation } from "@/chat/conversations/store";
import { getConversationStore } from "@/chat/db";

/**
 * Read how many parents a child Conversation has, and the root Conversation
 * with its Location. A child of the root has depth 1.
 */
export async function readChildDepthAndRootLocation(
  conversationId: string,
): Promise<{ depth: number; location?: Location; root?: Conversation }> {
  const conversationStore = getConversationStore();
  let depth = 0;
  let conversation = await conversationStore.get({ conversationId });
  while (conversation?.parentConversationId) {
    depth += 1;
    conversation = await conversationStore.get({
      conversationId: conversation.parentConversationId,
    });
  }
  return { depth, location: conversation?.location, root: conversation };
}
