import { and, asc, eq, ne } from "drizzle-orm";
import type { SlackDestination } from "@sentry/junior-plugin-api";
import { openSlackDirectMessage } from "@/chat/slack/outbound";
import type { JuniorDatabase } from "@/db/db";
import { juniorIdentities } from "@/db/schema";

/**
 * Open Junior's direct message with the Slack Identity of one User. Return
 * undefined when the User has no Slack Identity.
 */
export async function openUserDirectDestination(
  db: JuniorDatabase,
  userId: string,
): Promise<SlackDestination | undefined> {
  const [identity] = await db
    .select({
      slackUserId: juniorIdentities.providerSubjectId,
      teamId: juniorIdentities.providerTenantId,
    })
    .from(juniorIdentities)
    .where(
      and(
        eq(juniorIdentities.userId, userId),
        eq(juniorIdentities.kind, "user"),
        eq(juniorIdentities.provider, "slack"),
        ne(juniorIdentities.providerTenantId, ""),
      ),
    )
    .orderBy(
      asc(juniorIdentities.providerTenantId),
      asc(juniorIdentities.providerSubjectId),
    )
    .limit(1);
  if (!identity) return undefined;
  return {
    platform: "slack",
    teamId: identity.teamId,
    channelId: await openSlackDirectMessage(identity.slackUserId),
  };
}
