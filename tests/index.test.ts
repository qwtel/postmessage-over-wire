import { describe, expect, it, setDefaultTimeout } from "bun:test";

setDefaultTimeout(50);

import * as wire from "../index";
import { closeAll, createEndpointPair, createTestRouter, nextMessage, nextPortMessage } from "./test-util";

describe("postmessage-over-wire", () => {
  it("keeps the public exports available", () => {
    expect(typeof wire.WireEndpoint).toBe("function");
    expect(typeof wire.WireMessageChannel).toBe("function");
    expect(typeof wire.WireMessageEvent).toBe("function");
    expect(typeof wire.WireMessagePort).toBe("function");
    expect(typeof wire._internals.createRouter).toBe("function");
    expect(typeof wire.isAbortError).toBe("function");
  });

  it("can isolate routes with test routers", () => {
    const routerA = createTestRouter("a");
    const routerB = createTestRouter("b");

    const channelA = new wire.WireMessageChannel(routerA);
    const channelB = new wire.WireMessageChannel(routerB);

    expect(wire._internals.routeIds(routerA).sort()).toEqual(["a-1", "a-2"]);
    expect(wire._internals.routeIds(routerB).sort()).toEqual(["b-1", "b-2"]);

    closeAll(channelA.port1, channelA.port2, channelB.port1, channelB.port2);
  });

  it("generates cryptographically-random 128-bit bigint port IDs", () => {
    const router = wire._internals.createRouter();
    const { port1, port2 } = new wire.WireMessageChannel(router);
    try {
      const [id1, id2] = wire._internals.routeIds(router);

      expect(typeof id1).toBe("bigint");
      expect(typeof id2).toBe("bigint");
      expect(id1).toBeGreaterThan(0xffff_ffff_ffff_ffffn);
      expect(id2).toBeGreaterThan(0xffff_ffff_ffff_ffffn);
      expect(id1).not.toBe(id2);
    } finally {
      closeAll(port1, port2);
    }
  });

  it("supports deterministic IDs and route inspection through test internals", () => {
    let nextId = 10;
    const router = wire._internals.createRouter({ generateId: () => nextId++ });
    const { port1, port2 } = new wire.WireMessageChannel(router);

    expect(wire._internals.routeIds(router)).toEqual([10, 11]);

    closeAll(port1, port2);
    expect(wire._internals.routeCount(router)).toBe(0);
  });

  it("keeps WireMessagePort on the DataView host-object serialization path", () => {
    const { port1, port2 } = new wire.WireMessageChannel(createTestRouter("dataview"));

    expect(port1).toBeInstanceOf(DataView);
    expect(port1.byteLength).toBe(0);

    closeAll(port1, port2);
  });

  it("does not expose a public WireMessagePort constructor", () => {
    expect(() => new (wire.WireMessagePort as any)(Symbol("wrong"))).toThrow(TypeError);
  });

  it("recognizes AbortError without treating arbitrary errors as aborts", () => {
    expect(wire.isAbortError(new DOMException("cancelled", "AbortError"))).toBe(true);
    expect(wire.isAbortError(new Error("ordinary"))).toBe(false);
    expect(wire.isAbortError({ name: "AbortError" })).toBe(false);
  });

  it("passes endpoint messages through a same-process stream pair", async () => {
    const [endpointA, endpointB] = createEndpointPair();

    const received = nextMessage(endpointB);
    endpointA.postMessage({ hello: "world" });

    expect((await received).data).toEqual({ hello: "world" });

    closeAll(endpointA, endpointB);
  });

  it("transfers a wire port over an endpoint and keeps the remote entanglement usable", async () => {
    const routerA = createTestRouter("left");
    const [endpointA, endpointB] = createEndpointPair(routerA);
    const { port1, port2 } = new wire.WireMessageChannel(routerA);

    const transferReceived = nextMessage(endpointB);
    endpointA.postMessage("take this", [port1]);

    const event = await transferReceived;
    expect(event.data).toBe("take this");
    expect(event.ports).toHaveLength(1);

    const replyReceived = nextPortMessage(port2);
    event.ports[0].postMessage("hello over the transferred port");

    expect((await replyReceived).data).toBe("hello over the transferred port");

    closeAll(event.ports[0], port2, endpointA, endpointB);
  });
});
