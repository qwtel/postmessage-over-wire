import { describe, expect, it, setDefaultTimeout } from "bun:test";

setDefaultTimeout(50);

import * as wire from "../index";
import { closeAll, createEndpointPair, createTestContext, nextMessage, nextPortMessage } from "./test-util";

describe("postmessage-over-wire", () => {
  it("keeps the public exports available", () => {
    expect(typeof wire.WireEndpoint).toBe("function");
    expect(typeof wire.WireMessageChannel).toBe("function");
    expect(typeof wire.WireMessageEvent).toBe("function");
    expect(typeof wire.WireMessagePort).toBe("function");
    expect(typeof wire.createWireContext).toBe("function");
    expect(typeof wire.isAbortError).toBe("function");
  });

  it("can isolate route tables with injected contexts", () => {
    const contextA = createTestContext("a");
    const contextB = createTestContext("b");

    const channelA = new wire.WireMessageChannel(contextA);
    const channelB = new wire.WireMessageChannel(contextB);

    expect(Array.from(contextA.routeTable.keys()).sort()).toEqual(["a-1", "a-2"]);
    expect(Array.from(contextB.routeTable.keys()).sort()).toEqual(["b-1", "b-2"]);
    expect(contextA.routeTable).not.toBe(contextB.routeTable);

    closeAll(channelA.port1, channelA.port2, channelB.port1, channelB.port2);
  });

  it.failing("generates cryptographically-random 128-bit bigint port IDs", () => {
    const context = wire.createWireContext({ finalizer: null });
    const { port1, port2 } = new wire.WireMessageChannel(context);
    try {
      const [id1, id2] = Array.from(context.routeTable.keys());

      expect(typeof id1).toBe("bigint");
      expect(typeof id2).toBe("bigint");
      expect(id1).toBeGreaterThan(0xffff_ffff_ffff_ffffn);
      expect(id2).toBeGreaterThan(0xffff_ffff_ffff_ffffn);
      expect(id1).not.toBe(id2);
    } finally {
      closeAll(port1, port2);
    }
  });

  it("uses a supplied route table and ID generator", () => {
    const routeTable = new Map<wire.PortId, unknown>();
    let nextId = 10;
    const context = wire.createWireContext({
      routeTable,
      generateId: () => nextId++,
      finalizer: null,
    });
    const { port1, port2 } = new wire.WireMessageChannel(context);

    expect(context.routeTable).toBe(routeTable);
    expect(Array.from(routeTable.keys())).toEqual([10, 11]);

    closeAll(port1, port2);
    expect(routeTable.size).toBe(0);
  });

  it("keeps WireMessagePort on the DataView host-object serialization path", () => {
    const { port1, port2 } = new wire.WireMessageChannel(createTestContext("dataview"));

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
    const contextA = createTestContext("left");
    const [endpointA, endpointB] = createEndpointPair(contextA);
    const { port1, port2 } = new wire.WireMessageChannel(contextA);

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
