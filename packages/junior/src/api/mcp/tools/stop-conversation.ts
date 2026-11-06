import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import * as z from "zod";
import { stopConversationTurnForViewer } from "@/api/conversations/stop";
import { mcpConversationIdSchema } from "../conversation";
import {
  apiToolError,
  isConversationParticipant,
  mcpToolError,
  mcpToolResult,
  type McpToolContext,
} from "../tool-context";

/**
 * Register `stopConversation`, which stops the Conversation and every child
 * agent below it, and makes their Watches passive.
 */
export function registerStopConversationTool(
  server: McpServer,
  { queue, user }: McpToolContext,
): void {
  server.registerTool(
    "stopConversation",
    {
      title: "Stop conversation",
      description:
        "Stop one of your conversations: its running turn and every child agent below it. Their watches become passive: events are saved to the conversation but start no turn, until you send the conversation a message. Messages that wait for the turn are dropped. It starts no new work.",
      inputSchema: { conversationId: mcpConversationIdSchema },
      outputSchema: {
        status: z
          .enum(["no_work", "requested"])
          .describe(
            "`requested` when the running turn was asked to stop. `no_work` when no turn was running. Child agents are stopped in both cases.",
          ),
        droppedMessages: z
          .number()
          .int()
          .describe("Messages that waited for the turn and were dropped."),
      },
      annotations: {
        destructiveHint: true,
        idempotentHint: true,
        openWorldHint: false,
        readOnlyHint: false,
      },
    },
    async ({ conversationId }) => {
      if (!(await isConversationParticipant(user, conversationId))) {
        return mcpToolError("Conversation not found.");
      }
      try {
        const result = await stopConversationTurnForViewer(
          user,
          conversationId,
          { queue },
        );
        return mcpToolResult({
          status: result.status,
          droppedMessages: result.stoppedInboundMessageIds.length,
        });
      } catch (error) {
        return apiToolError(error);
      }
    },
  );
}
