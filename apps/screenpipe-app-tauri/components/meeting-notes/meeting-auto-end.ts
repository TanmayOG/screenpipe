// screenpipe — AI that knows everything you've seen, said, or heard
// https://screenpipe.com

export type AutoEndPrompt = "inactivity" | "time-limit";

/**
 * Which "stopped transcribing" banner a live status calls for. Reads the
 * engine's status text because the event carries no separate reason field.
 */
export function autoEndPromptFor(status: {
  active: boolean;
  error?: string | null;
}): AutoEndPrompt | null {
  const error = status.error?.toLowerCase() ?? "";
  if (status.active || !error.includes("auto-ended")) return null;
  return error.includes("inactive") ? "inactivity" : "time-limit";
}
