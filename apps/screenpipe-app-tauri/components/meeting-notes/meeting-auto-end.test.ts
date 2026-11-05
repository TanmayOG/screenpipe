// screenpipe — AI that knows everything you've seen, said, or heard
// https://screenpipe.com

import { describe, expect, it } from "vitest";
import { autoEndPromptFor } from "./meeting-auto-end";

describe("autoEndPromptFor", () => {
  it("offers resume after the inactivity auto-end", () => {
    expect(
      autoEndPromptFor({
        active: false,
        error: "meeting auto-ended after live audio became inactive",
      }),
    ).toBe("inactivity");
  });

  it("offers resume after the time-limit auto-end", () => {
    expect(
      autoEndPromptFor({
        active: false,
        error: "meeting auto-ended at the live time limit",
      }),
    ).toBe("time-limit");
  });

  it("stays quiet for anything that is not an auto-end", () => {
    expect(autoEndPromptFor({ active: false })).toBeNull();
    expect(autoEndPromptFor({ active: false, error: null })).toBeNull();
    expect(
      autoEndPromptFor({ active: false, error: "websocket closed" }),
    ).toBeNull();
    expect(
      autoEndPromptFor({
        active: true,
        error: "meeting auto-ended at the live time limit",
      }),
    ).toBeNull();
  });
});
