import { describe, expect, it, setDefaultTimeout } from "bun:test";

setDefaultTimeout(50);

import { WireMessageChannel, WireMessageEvent } from "../index";
import { closeAll, createTestRouter, nextPortMessage, settle } from "./test-util";

describe("structured message data", () => {
  it("clones primitive values, arrays, and plain objects", async () => {
    const { port1, port2 } = new WireMessageChannel(createTestRouter("values"));
    const received = nextPortMessage(port2);

    port1.postMessage({
      string: "hello",
      number: 42,
      boolean: true,
      nil: null,
      missing: undefined,
      bigint: 123n,
      array: [1, "two", false],
    });

    expect((await received).data).toEqual({
      string: "hello",
      number: 42,
      boolean: true,
      nil: null,
      missing: undefined,
      bigint: 123n,
      array: [1, "two", false],
    });
    closeAll(port1, port2);
  });

  it("preserves special numeric values", async () => {
    const { port1, port2 } = new WireMessageChannel(createTestRouter("numbers"));
    const received = nextPortMessage(port2);

    port1.postMessage([NaN, Infinity, -Infinity, -0]);

    const [nan, positive, negative, minusZero] = (await received).data;
    expect(Number.isNaN(nan)).toBe(true);
    expect(positive).toBe(Infinity);
    expect(negative).toBe(-Infinity);
    expect(Object.is(minusZero, -0)).toBe(true);
    closeAll(port1, port2);
  });

  it("clones Date, RegExp, Map, and Set", async () => {
    const { port1, port2 } = new WireMessageChannel(createTestRouter("builtins"));
    const date = new Date("2025-01-02T03:04:05.000Z");
    const expression = /hello/gi;
    const map = new Map<any, any>([["key", { nested: true }]]);
    const set = new Set<any>(["a", 2]);
    const received = nextPortMessage(port2);

    port1.postMessage({ date, expression, map, set });

    const clone = (await received).data;
    expect(clone.date).toEqual(date);
    expect(clone.expression.source).toBe(expression.source);
    expect(clone.expression.flags).toBe(expression.flags);
    expect(clone.map).toEqual(map);
    expect(clone.set).toEqual(set);
    closeAll(port1, port2);
  });

  it("clones ArrayBuffer and typed-array contents", async () => {
    const { port1, port2 } = new WireMessageChannel(createTestRouter("buffers"));
    const buffer = new Uint8Array([1, 2, 3, 255]).buffer;
    const view = new Uint16Array([10, 20, 30]);
    const received = nextPortMessage(port2);

    port1.postMessage({ buffer, view });

    const clone = (await received).data;
    expect(Array.from(new Uint8Array(clone.buffer))).toEqual([1, 2, 3, 255]);
    expect(Array.from(clone.view)).toEqual([10, 20, 30]);
    expect(clone.buffer).not.toBe(buffer);
    expect(clone.view).not.toBe(view);
    closeAll(port1, port2);
  });

  it("preserves cycles and repeated-reference identity within a message", async () => {
    const { port1, port2 } = new WireMessageChannel(createTestRouter("identity"));
    const child = { value: 1 };
    const value: any = { first: child, second: child };
    value.self = value;
    const received = nextPortMessage(port2);

    port1.postMessage(value);

    const clone = (await received).data;
    expect(clone).not.toBe(value);
    expect(clone.self).toBe(clone);
    expect(clone.first).toBe(clone.second);
    expect(clone.first).not.toBe(child);
    closeAll(port1, port2);
  });

  it("takes the data snapshot before postMessage() returns", async () => {
    const { port1, port2 } = new WireMessageChannel(createTestRouter("snapshot"));
    const value = { nested: { count: 1 }, list: ["original"] };
    const received = nextPortMessage(port2);

    port1.postMessage(value);
    value.nested.count = 2;
    value.list.push("late mutation");

    expect((await received).data).toEqual({ nested: { count: 1 }, list: ["original"] });
    closeAll(port1, port2);
  });

  it("throws synchronously for uncloneable data", async () => {
    const { port1, port2 } = new WireMessageChannel(createTestRouter("uncloneable"));
    let messages = 0;
    port2.onmessage = () => messages++;

    expect(() => port1.postMessage({ fn() {} })).toThrow();
    await settle();
    expect(messages).toBe(0);

    closeAll(port1, port2);
  });

  it("requires a port in message data to appear in the transfer list", async () => {
    const router = createTestRouter("unlisted-port");
    const carrier = new WireMessageChannel(router);
    const payload = new WireMessageChannel(router);

    expect(() => carrier.port1.postMessage({ port: payload.port1 })).toThrow();

    const received = nextPortMessage(payload.port2);
    payload.port1.postMessage("still attached");
    expect((await received).data).toBe("still attached");
    closeAll(carrier.port1, carrier.port2, payload.port1, payload.port2);
  });

  it("reports transferred ports even when data does not reference them", async () => {
    const router = createTestRouter("out-of-band-port");
    const carrier = new WireMessageChannel(router);
    const payload = new WireMessageChannel(router);
    const transferred = nextPortMessage(carrier.port2);

    carrier.port1.postMessage("port is out of band", [payload.port1]);

    const event = await transferred;
    expect(event.data).toBe("port is out of band");
    expect(event.ports).toHaveLength(1);

    const reply = nextPortMessage(payload.port2);
    event.ports[0].postMessage("works");
    expect((await reply).data).toBe("works");
    closeAll(carrier.port1, carrier.port2, payload.port2, event.ports[0]);
  });

  it("uses the same received object for event.ports and nested data references", async () => {
    const router = createTestRouter("port-identity");
    const carrier = new WireMessageChannel(router);
    const payload = new WireMessageChannel(router);
    const transferred = nextPortMessage(carrier.port2);

    carrier.port1.postMessage({ deeply: { port: payload.port1 } }, [payload.port1]);

    const event = await transferred;
    expect(event.data.deeply.port).toBe(event.ports[0]);
    closeAll(carrier.port1, carrier.port2, payload.port2, event.ports[0]);
  });

  it("deduplicates repeated references to one transferred port", async () => {
    const router = createTestRouter("repeated-port");
    const carrier = new WireMessageChannel(router);
    const payload = new WireMessageChannel(router);
    const transferred = nextPortMessage(carrier.port2);

    carrier.port1.postMessage({ a: payload.port1, b: payload.port1 }, [payload.port1]);

    const event = await transferred;
    expect(event.ports).toHaveLength(1);
    expect(event.data.a).toBe(event.ports[0]);
    expect(event.data.b).toBe(event.ports[0]);
    closeAll(carrier.port1, carrier.port2, payload.port2, event.ports[0]);
  });

  it("transfers ports nested in Map and Set values", async () => {
    const router = createTestRouter("collection-port");
    const carrier = new WireMessageChannel(router);
    const payload = new WireMessageChannel(router);
    const transferred = nextPortMessage(carrier.port2);

    carrier.port1.postMessage({
      map: new Map([["port", payload.port1]]),
      set: new Set([payload.port1]),
    }, [payload.port1]);

    const event = await transferred;
    expect(event.data.map.get("port")).toBe(event.ports[0]);
    expect(Array.from(event.data.set)).toEqual([event.ports[0]]);
    closeAll(carrier.port1, carrier.port2, payload.port2, event.ports[0]);
  });

  it("accepts StructuredSerializeOptions as well as a transfer array", async () => {
    const router = createTestRouter("options");
    const carrier = new WireMessageChannel(router);
    const first = new WireMessageChannel(router);
    const second = new WireMessageChannel(router);

    const arrayReceived = nextPortMessage(carrier.port2);
    carrier.port1.postMessage("array", [first.port1]);
    const arrayEvent = await arrayReceived;

    const optionsReceived = nextPortMessage(carrier.port2);
    carrier.port1.postMessage("options", { transfer: [second.port1] });
    const optionsEvent = await optionsReceived;

    expect(arrayEvent.data).toBe("array");
    expect(arrayEvent.ports).toHaveLength(1);
    expect(optionsEvent.data).toBe("options");
    expect(optionsEvent.ports).toHaveLength(1);
    closeAll(
      carrier.port1,
      carrier.port2,
      first.port2,
      second.port2,
      arrayEvent.ports[0],
      optionsEvent.ports[0],
    );
  });

  it("leaves every transferred port attached when data serialization fails", async () => {
    const router = createTestRouter("atomic-clone");
    const carrier = new WireMessageChannel(router);
    const first = new WireMessageChannel(router);
    const second = new WireMessageChannel(router);

    try {
      expect(() => carrier.port1.postMessage(
        { uncloneable() {} },
        [first.port1, second.port1],
      )).toThrow();

      const atFirst = nextPortMessage(first.port2);
      const atSecond = nextPortMessage(second.port2);
      first.port1.postMessage("first attached");
      second.port1.postMessage("second attached");
      expect((await atFirst).data).toBe("first attached");
      expect((await atSecond).data).toBe("second attached");
    } finally {
      closeAll(carrier.port1, carrier.port2, first.port1, first.port2, second.port1, second.port2);
    }
  });

  it("constructs MessageEvent-compatible metadata", async () => {
    const { port1, port2 } = new WireMessageChannel(createTestRouter("event-fields"));
    const received = nextPortMessage(port2);

    port1.postMessage("metadata");

    const event = await received;
    expect(event).toBeInstanceOf(WireMessageEvent);
    expect(event.type).toBe("message");
    expect(event.data).toBe("metadata");
    expect(event.ports).toEqual([]);
    expect(event.origin).toBe("");
    expect(event.lastEventId).toBe("");
    expect(event.source).toBeNull();
    closeAll(port1, port2);
  });

  it("defaults WireMessageEvent data and ports", () => {
    const event = new WireMessageEvent("message");

    expect(event.data).toBeNull();
    expect(event.ports).toEqual([]);
    expect(event.bubbles).toBe(false);
    expect(event.cancelable).toBe(false);
  });

  it("honors non-port transferables such as ArrayBuffer", async () => {
    // The public signature accepts Transferable[], but the implementation only
    // acts on WireMessagePort entries today. Preserve this as a conformance gap.
    const { port1, port2 } = new WireMessageChannel(createTestRouter("array-buffer-transfer"));
    try {
      const buffer = new ArrayBuffer(4);
      const received = nextPortMessage(port2);

      port1.postMessage(buffer, [buffer]);

      expect(buffer.byteLength).toBe(0);
      expect((await received).data.byteLength).toBe(4);
    } finally {
      closeAll(port1, port2);
    }
  });
});
