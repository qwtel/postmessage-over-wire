import { describe, expect, it } from "bun:test";

import { WireMessageChannel } from "./index";
import { closeAll, createEndpointPair, createTestContext, nextMessage } from "./test-util";

describe("postMessage transfer validation", () => {
  it("rejects transferring the source port", () => {
    const { port1 } = new WireMessageChannel(createTestContext("source"));

    expect(() => port1.postMessage("nope", [port1])).toThrow(DOMException);

    closeAll(port1);
  });

  it("rejects transferring the destination port", () => {
    const { port1, port2 } = new WireMessageChannel(createTestContext("destination"));

    expect(() => port1.postMessage("nope", [port2])).toThrow(DOMException);

    closeAll(port1, port2);
  });

  it("rejects transferring the same port twice in one message", () => {
    const context = createTestContext("duplicate");
    const { port1, port2 } = new WireMessageChannel(context);
    const { port1: transferred } = new WireMessageChannel(context);

    expect(() => port1.postMessage("nope", [transferred, transferred])).toThrow(DOMException);

    closeAll(port1, port2, transferred);
  });

  it("rejects transferring a detached port again", async () => {
    const contextA = createTestContext("left");
    const [endpointA, endpointB] = createEndpointPair(contextA);
    const { port1, port2 } = new WireMessageChannel(contextA);

    const firstTransfer = nextMessage(endpointB);
    endpointA.postMessage("first", [port1]);
    await firstTransfer;

    expect(() => endpointA.postMessage("second", [port1])).toThrow(DOMException);

    closeAll(port2, endpointA, endpointB);
  });
});
