import { describe, expect, it, setDefaultTimeout } from "bun:test";

setDefaultTimeout(50);

import { WireMessageChannel, WireMessagePort } from "../index";
import {
  closeAll,
  createTestContext,
  nextMessage,
  nextPortMessage,
} from "./test-util";

describe("native MessagePort bridging", () => {
  it("memoizes toNative() so one wire wrapper has one native facade", () => {
    const { port1, port2 } = new WireMessageChannel(createTestContext("native-memo"));

    const first = port1.toNative();
    const second = port1.toNative();

    expect(first).toBe(second);
    closeAll(first, port1, port2);
  });

  it("delivers from the wire peer to a toNative() facade", async () => {
    const { port1, port2 } = new WireMessageChannel(createTestContext("wire-native"));
    const native = port1.toNative();
    const received = nextMessage(native);
    native.start();

    port2.postMessage({ from: "wire" });

    expect((await received).data).toEqual({ from: "wire" });
    closeAll(native, port1, port2);
  });

  it("delivers from a toNative() facade to the wire peer", async () => {
    const { port1, port2 } = new WireMessageChannel(createTestContext("native-wire"));
    const native = port1.toNative();
    const received = nextPortMessage(port2);

    native.postMessage({ from: "native" });

    expect((await received).data).toEqual({ from: "native" });
    closeAll(native, port1, port2);
  });

  it("delivers from a native peer through fromNative()", async () => {
    const native = new MessageChannel();
    const wire = WireMessagePort.fromNative(native.port1, createTestContext("from-native"));
    const received = nextPortMessage(wire);

    native.port2.postMessage("native to wire");

    expect((await received).data).toBe("native to wire");
    closeAll(native.port1, native.port2, wire);
  });

  it("delivers from fromNative() to the native peer", async () => {
    const native = new MessageChannel();
    const wire = WireMessagePort.fromNative(native.port1, createTestContext("to-native-peer"));
    const received = nextMessage(native.port2);
    native.port2.start();

    wire.postMessage("wire to native");

    expect((await received).data).toBe("wire to native");
    closeAll(native.port1, native.port2, wire);
  });

  it("converts a transferred wire port into a native transferred port", async () => {
    const context = createTestContext("wire-port-native");
    const bridge = new WireMessageChannel(context);
    const payload = new WireMessageChannel(context);
    const nativeBridge = bridge.port1.toNative();
    const transferred = nextMessage(nativeBridge);
    nativeBridge.start();

    bridge.port2.postMessage({ nested: payload.port1 }, [payload.port1]);

    const event = await transferred;
    expect(event.ports).toHaveLength(1);
    expect(event.data.nested).toBe(event.ports[0]);

    const reply = nextPortMessage(payload.port2);
    event.ports[0].postMessage("native transferred endpoint works");
    expect((await reply).data).toBe("native transferred endpoint works");
    closeAll(nativeBridge, event.ports[0], bridge.port1, bridge.port2, payload.port2);
  });

  it("converts a transferred native port into a wire transferred port", async () => {
    const context = createTestContext("native-port-wire");
    const bridge = new WireMessageChannel(context);
    const nativeBridge = bridge.port1.toNative();
    const payload = new MessageChannel();
    const transferred = nextPortMessage(bridge.port2);

    nativeBridge.postMessage({ nested: payload.port1 }, [payload.port1]);

    const event = await transferred;
    expect(event.ports).toHaveLength(1);
    expect(event.data.nested).toBe(event.ports[0]);

    const reply = nextMessage(payload.port2);
    payload.port2.start();
    event.ports[0].postMessage("wire transferred endpoint works");
    expect((await reply).data).toBe("wire transferred endpoint works");
    closeAll(nativeBridge, payload.port2, bridge.port1, bridge.port2, event.ports[0]);
  });

  it("replaces transferred ports nested in Map and Set across the bridge", async () => {
    const context = createTestContext("native-collections");
    const bridge = new WireMessageChannel(context);
    const payload = new WireMessageChannel(context);
    const nativeBridge = bridge.port1.toNative();
    const transferred = nextMessage(nativeBridge);
    nativeBridge.start();

    bridge.port2.postMessage({
      map: new Map([["port", payload.port1]]),
      set: new Set([payload.port1]),
    }, [payload.port1]);

    const event = await transferred;
    expect(event.data.map.get("port")).toBe(event.ports[0]);
    expect(Array.from(event.data.set)).toEqual([event.ports[0]]);
    closeAll(nativeBridge, event.ports[0], bridge.port1, bridge.port2, payload.port2);
  });
});
