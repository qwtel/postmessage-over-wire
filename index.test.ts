import { describe, expect, it } from "bun:test";

import * as wire from "./index";
import { closeAll, createEndpointPair, createTestContext, nextMessage, nextPortMessage } from "./test-util";

describe("postmessage-over-wire", () => {
  it("keeps the public exports available", () => {
    expect(typeof wire.WireEndpoint).toBe("function");
    expect(typeof wire.WireMessageChannel).toBe("function");
    expect(typeof wire.WireMessageEvent).toBe("function");
    expect(typeof wire.WireMessagePort).toBe("function");
    expect(typeof wire.createWireContext).toBe("function");
    expect(typeof wire.isAbortError).toBe("function");
    expect(typeof wire.__internals).toBe("object");
    expect("globalRouteTable" in wire.__internals).toBe(true);
    expect("unshippedStream" in wire.__internals).toBe(true);
    expect("unshippedWriter" in wire.__internals).toBe(true);
    expect("unshippedPortLoop" in wire.__internals).toBe(true);
  });

  it("can isolate route tables with injected contexts", () => {
    const contextA = createTestContext("a");
    const contextB = createTestContext("b");

    new wire.WireMessageChannel(contextA);
    new wire.WireMessageChannel(contextB);

    expect(Array.from(contextA.routeTable.keys()).sort()).toEqual(["a-1", "a-2"]);
    expect(Array.from(contextB.routeTable.keys()).sort()).toEqual(["b-1", "b-2"]);
    expect(contextA.routeTable).not.toBe(contextB.routeTable);
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
