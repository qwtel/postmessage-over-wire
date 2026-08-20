import { describe, expect, it, setDefaultTimeout } from "bun:test";

setDefaultTimeout(50);

import { WireMessageChannel } from "../index";
import { closeAll, createEndpointPair, createTestContext, nextMessage, nextPortMessage, timeout } from "./test-util";

describe("postMessage transfer validation", () => {
  it("rejects transferring the source port", () => {
    const { port1, port2 } = new WireMessageChannel(createTestContext("source"));

    expectDOMException(() => port1.postMessage("nope", [port1]), "DataCloneError");

    closeAll(port1, port2);
  });

  it("rejects transferring the destination port", () => {
    const { port1, port2 } = new WireMessageChannel(createTestContext("destination"));

    expectDOMException(() => port1.postMessage("nope", [port2]), "DataCloneError");

    closeAll(port1, port2);
  });

  it.failing("rejects transferring the same port twice without detaching it", async () => {
    const context = createTestContext("duplicate");
    const { port1, port2 } = new WireMessageChannel(context);
    const { port1: transferred, port2: peer } = new WireMessageChannel(context);

    try {
      expectDOMException(
        () => port1.postMessage("nope", [transferred, transferred]),
        "DataCloneError",
      );

      const received = nextPortMessage(peer);
      transferred.postMessage("still attached");
      expect((await received).data).toBe("still attached");
    } finally {
      closeAll(port1, port2, transferred, peer);
    }
  });

  it("rejects transferring a detached port again", async () => {
    const contextA = createTestContext("left");
    const [endpointA, endpointB] = createEndpointPair(contextA);
    const { port1, port2 } = new WireMessageChannel(contextA);

    const firstTransfer = nextMessage(endpointB);
    endpointA.postMessage("first", [port1]);
    await firstTransfer;

    expectDOMException(() => endpointA.postMessage("second", [port1]), "DataCloneError");

    closeAll(port2, endpointA, endpointB);
  });

  it("rejects transferring the source or destination through options syntax", () => {
    const { port1, port2 } = new WireMessageChannel(createTestContext("options-invalid"));

    expectDOMException(
      () => port1.postMessage("source", { transfer: [port1] }),
      "DataCloneError",
    );
    expectDOMException(
      () => port1.postMessage("destination", { transfer: [port2] }),
      "DataCloneError",
    );

    closeAll(port1, port2);
  });

  it("leaves both channel ends usable after source and destination rejection", async () => {
    const { port1, port2 } = new WireMessageChannel(createTestContext("validation-atomic"));

    expect(() => port1.postMessage("bad source", [port1])).toThrow();
    expect(() => port1.postMessage("bad destination", [port2])).toThrow();

    const at1 = nextPortMessage(port1);
    const at2 = nextPortMessage(port2);
    port1.postMessage("to two");
    port2.postMessage("to one");
    expect((await at1).data).toBe("to one");
    expect((await at2).data).toBe("to two");
    closeAll(port1, port2);
  });

  it("rejects transferring a closed port", () => {
    const context = createTestContext("closed-transfer");
    const carrier = new WireMessageChannel(context);
    const payload = new WireMessageChannel(context);
    payload.port1.close();

    expectDOMException(
      () => carrier.port1.postMessage("closed", [payload.port1]),
      "DataCloneError",
    );

    closeAll(carrier.port1, carrier.port2, payload.port2);
  });

  it("makes the old wrapper unusable immediately after a successful transfer", async () => {
    const context = createTestContext("detach-now");
    const [endpointA, endpointB] = createEndpointPair(context);
    const payload = new WireMessageChannel(context);
    const transferred = nextMessage(endpointB);

    endpointA.postMessage("move", [payload.port1]);

    expectDOMException(() => payload.port1.postMessage("detached"), "InvalidStateError");
    expectDOMException(
      () => endpointA.postMessage("move again", [payload.port1]),
      "DataCloneError",
    );

    const remotePort = (await transferred).ports[0];
    closeAll(payload.port2, remotePort, endpointA, endpointB);
  });

  it.failing("makes close() on a detached wrapper a no-op", async () => {
    const contextA = createTestContext("detached-close-a");
    const [endpointA, endpointB] = createEndpointPair(contextA, createTestContext("detached-close-b"));
    const channel = new WireMessageChannel(contextA);
    const transferred = nextMessage(endpointB);

    endpointA.postMessage("move", [channel.port1]);
    const moved = (await transferred).ports[0];
    channel.port1.close();

    try {
      const received = nextPortMessage(moved);
      channel.port2.postMessage("still routed");
      expect((await timeout(received)).data).toBe("still routed");
    } finally {
      closeAll(channel.port2, moved, endpointA, endpointB);
    }
  });

  it.failing("leaves earlier valid ports attached when a later transfer entry is invalid", async () => {
    const context = createTestContext("all-or-nothing");
    const carrier = new WireMessageChannel(context);
    const valid = new WireMessageChannel(context);
    const invalid = new WireMessageChannel(context);
    invalid.port1.close();

    try {
      expect(() => carrier.port1.postMessage("no partial detach", [valid.port1, invalid.port1])).toThrow();

      const received = nextPortMessage(valid.port2);
      valid.port1.postMessage("still attached");
      expect((await received).data).toBe("still attached");
    } finally {
      closeAll(carrier.port1, carrier.port2, valid.port1, valid.port2, invalid.port1, invalid.port2);
    }
  });

  it.failing("rejects a non-transferable value in the transfer list", () => {
    const { port1, port2 } = new WireMessageChannel(createTestContext("invalid-transferable"));
    try {
      expect(() => port1.postMessage("invalid", [{} as Transferable])).toThrow(DOMException);
    } finally {
      closeAll(port1, port2);
    }
  });

  it("rejects transferring a port that is already exposed through toNative()", () => {
    const context = createTestContext("bridged-transfer");
    const carrier = new WireMessageChannel(context);
    const payload = new WireMessageChannel(context);
    const native = payload.port1.toNative();

    expectDOMException(
      () => carrier.port1.postMessage("cannot move two public handles", [payload.port1]),
      "DataCloneError",
    );

    closeAll(native, carrier.port1, carrier.port2, payload.port1, payload.port2);
  });

  it.failing("rejects transferring a port owned by another WireContext", () => {
    // Moving IDs between independent route tables is not defined. The current
    // implementation accepts this and can leave the source context inconsistent.
    const carrier = new WireMessageChannel(createTestContext("context-a"));
    const payload = new WireMessageChannel(createTestContext("context-b"));
    try {
      expectDOMException(
        () => carrier.port1.postMessage("wrong routing domain", [payload.port1]),
        "DataCloneError",
      );
    } finally {
      closeAll(carrier.port1, carrier.port2, payload.port1, payload.port2);
    }
  });
});

function expectDOMException(callback: () => void, name: string) {
  try {
    callback();
    throw new Error(`Expected ${name}`);
  } catch (error) {
    expect(error).toBeInstanceOf(DOMException);
    expect((error as DOMException).name).toBe(name);
  }
}
