import { describe, expect, it, setDefaultTimeout } from "bun:test";

setDefaultTimeout(50);

import { WireMessageChannel } from "../index";
import {
  closeAll,
  createTestContext,
  nextEvent,
  nextMessage,
  nextPortMessage,
  nextPortMessages,
  settle,
  timeout,
} from "./test-util";

describe("WireMessagePort", () => {
  it("creates two distinct ports and two live routes", () => {
    const context = createTestContext("shape");
    const { port1, port2 } = new WireMessageChannel(context);

    expect(port1).not.toBe(port2);
    expect(context.routeTable.size).toBe(2);

    closeAll(port1, port2);
    expect(context.routeTable.size).toBe(0);
  });

  it("delivers messages across a local channel", async () => {
    const context = createTestContext("local");
    const { port1, port2 } = new WireMessageChannel(context);

    const received = nextPortMessage(port2);
    port1.postMessage({ local: true });

    expect((await received).data).toEqual({ local: true });

    closeAll(port1, port2);
    expect(context.routeTable.size).toBe(0);
  });

  it("does not dispatch queued messages until start() is called", async () => {
    const { port1, port2 } = new WireMessageChannel(createTestContext("start"));
    let dispatched = false;
    const received = nextMessage(port2).then((event) => {
      dispatched = true;
      return event;
    });

    port1.postMessage("waiting");
    await settle();
    expect(dispatched).toBe(false);

    port2.start();
    expect((await received).data).toBe("waiting");
    closeAll(port1, port2);
  });

  it("makes start() idempotent", async () => {
    const { port1, port2 } = new WireMessageChannel(createTestContext("start-twice"));
    let count = 0;
    port2.addEventListener("message", () => count++);

    port2.start();
    port2.start();
    port1.postMessage("once");
    await settle();

    expect(count).toBe(1);
    closeAll(port1, port2);
  });

  it("starts automatically when onmessage is assigned", async () => {
    const { port1, port2 } = new WireMessageChannel(createTestContext("onmessage-start"));
    const received = new Promise<MessageEvent>((resolve) => {
      port2.onmessage = resolve;
    });

    port1.postMessage("automatic");
    expect((await received).data).toBe("automatic");
    closeAll(port1, port2);
  });

  it("delivers asynchronously", async () => {
    const { port1, port2 } = new WireMessageChannel(createTestContext("async"));
    let synchronous = true;
    const received = nextPortMessage(port2).then(() => {
      expect(synchronous).toBe(false);
    });

    port1.postMessage("later");
    synchronous = false;
    await received;
    closeAll(port1, port2);
  });

  it("uses message tasks rather than microtasks for delivery", async () => {
    const { port1, port2 } = new WireMessageChannel(createTestContext("tasks"));
    try {
      const order: string[] = [];
      port2.onmessage = () => order.push("message");

      port1.postMessage("later task");
      queueMicrotask(() => order.push("microtask"));
      await Promise.resolve();

      expect(order).toEqual(["microtask"]);
      await settle();
      expect(order).toEqual(["microtask", "message"]);
    } finally {
      closeAll(port1, port2);
    }
  });

  it("preserves FIFO order for a backlog", async () => {
    const { port1, port2 } = new WireMessageChannel(createTestContext("fifo"));
    const received = nextPortMessages(port2, 50);

    for (let index = 0; index < 50; index++) port1.postMessage(index);

    expect((await received).map((event) => event.data)).toEqual(
      Array.from({ length: 50 }, (_, index) => index),
    );
    closeAll(port1, port2);
  });

  it("preserves independent FIFO order in both directions", async () => {
    const { port1, port2 } = new WireMessageChannel(createTestContext("duplex"));
    const at1 = nextPortMessages(port1, 3);
    const at2 = nextPortMessages(port2, 3);

    port1.postMessage("1-a");
    port2.postMessage("2-a");
    port1.postMessage("1-b");
    port2.postMessage("2-b");
    port1.postMessage("1-c");
    port2.postMessage("2-c");

    expect((await at1).map(({ data }) => data)).toEqual(["2-a", "2-b", "2-c"]);
    expect((await at2).map(({ data }) => data)).toEqual(["1-a", "1-b", "1-c"]);
    closeAll(port1, port2);
  });

  it("supports once listeners without keeping the port strongly referenced", async () => {
    const context = createTestContext("once");
    const { port1, port2 } = new WireMessageChannel(context);
    let count = 0;
    const received = new Promise<MessageEvent>((resolve) => {
      port2.addEventListener("message", (event) => {
        count++;
        resolve(event);
      }, { once: true });
    });

    port2.start();
    port1.postMessage("first");

    expect((await received).data).toBe("first");
    port1.postMessage("second");
    await Promise.resolve();
    expect(count).toBe(1);
    expect(context.nonGCedPorts.has(port2)).toBe(false);

    closeAll(port1, port2);
  });

  it("keeps a port retained until its last message listener is removed", () => {
    const context = createTestContext("listeners");
    const { port1, port2 } = new WireMessageChannel(context);
    const first = () => {};
    const second = { handleEvent() {} };

    port2.addEventListener("message", first);
    port2.addEventListener("message", second);
    expect(context.nonGCedPorts.has(port2)).toBe(true);

    port2.removeEventListener("message", first);
    expect(context.nonGCedPorts.has(port2)).toBe(true);

    port2.removeEventListener("message", second);
    expect(context.nonGCedPorts.has(port2)).toBe(false);
    closeAll(port1, port2);
  });

  it.failing("keeps listener retention when removal uses the wrong capture value", () => {
    const context = createTestContext("listener-capture");
    const { port1, port2 } = new WireMessageChannel(context);
    const listener = () => {};
    try {
      port1.addEventListener("message", listener, true);
      port1.removeEventListener("message", listener, false);

      expect(context.nonGCedPorts.has(port1)).toBe(true);
    } finally {
      closeAll(port1, port2);
    }
  });

  it.failing("returns the callback assigned to onmessage", () => {
    const { port1, port2 } = new WireMessageChannel(createTestContext("onmessage-getter"));
    const listener = () => {};
    try {
      port1.onmessage = listener;

      expect(port1.onmessage).toBe(listener);
    } finally {
      closeAll(port1, port2);
    }
  });

  it.failing("returns null after onmessage is cleared", () => {
    const { port1, port2 } = new WireMessageChannel(createTestContext("onmessage-clear"));
    try {
      port1.onmessage = () => {};

      port1.onmessage = null;

      expect(port1.onmessage).toBeNull();
    } finally {
      closeAll(port1, port2);
    }
  });

  it.failing("exposes the WireMessagePort itself as the event target", async () => {
    // Delegating to a private EventTarget currently leaks that private object
    // through `this`, target, and currentTarget.
    const { port1, port2 } = new WireMessageChannel(createTestContext("event-target"));
    try {
      const received = new Promise<[unknown, EventTarget|null, EventTarget|null]>((resolve) => {
        port2.addEventListener("message", function (event) {
          resolve([this, event.target, event.currentTarget]);
        }, { once: true });
      });
      port2.start();

      port1.postMessage("target");

      const [listenerThis, target, currentTarget] = await received;
      expect(listenerThis).toBe(port2);
      expect(target).toBe(port2);
      expect(currentTarget).toBe(port2);
    } finally {
      closeAll(port1, port2);
    }
  });

  it.failing("releases message-listener retention when its AbortSignal aborts", () => {
    const context = createTestContext("listener-abort");
    const { port1, port2 } = new WireMessageChannel(context);
    const abort = new AbortController();
    try {
      port2.addEventListener("message", () => {}, { signal: abort.signal });
      expect(context.nonGCedPorts.has(port2)).toBe(true);

      abort.abort();

      expect(context.nonGCedPorts.has(port2)).toBe(false);
    } finally {
      closeAll(port1, port2);
    }
  });

  it.failing("does not retain a listener registered with an already-aborted signal", () => {
    const context = createTestContext("listener-already-aborted");
    const { port1, port2 } = new WireMessageChannel(context);
    const abort = new AbortController();
    abort.abort();
    try {
      port2.addEventListener("message", () => {}, { signal: abort.signal });

      expect(context.nonGCedPorts.has(port2)).toBe(false);
    } finally {
      closeAll(port1, port2);
    }
  });

  it("replaces and clears onmessage without retaining old handlers", async () => {
    const context = createTestContext("replace-handler");
    const { port1, port2 } = new WireMessageChannel(context);
    const calls: string[] = [];

    port2.onmessage = () => calls.push("old");
    port2.onmessage = () => calls.push("new");
    port1.postMessage("first");
    await settle();
    expect(calls).toEqual(["new"]);

    port2.onmessage = null;
    port1.postMessage("second");
    await settle();
    expect(calls).toEqual(["new"]);
    expect(context.nonGCedPorts.has(port2)).toBe(false);

    closeAll(port1, port2);
  });

  it("dispatches close on the entangled port", async () => {
    const context = createTestContext("close");
    const { port1, port2 } = new WireMessageChannel(context);

    const closed = nextEvent<CloseEvent>(port2, "close");
    port2.start();
    port1.close();

    const event = await closed;
    expect(event.type).toBe("close");
    expect(event.wasClean).toBe(true);

    closeAll(port2);
    expect(context.routeTable.size).toBe(0);
  });

  it("delivers already-queued messages before close", async () => {
    const { port1, port2 } = new WireMessageChannel(createTestContext("close-order"));
    const order: string[] = [];
    const closed = new Promise<void>((resolve) => {
      port2.addEventListener("message", ({ data }) => order.push(data));
      port2.addEventListener("close", () => {
        order.push("close");
        resolve();
      });
    });
    port2.start();

    port1.postMessage("first");
    port1.postMessage("second");
    port1.close();
    await closed;

    expect(order).toEqual(["first", "second", "close"]);
    closeAll(port2);
  });

  it("queues close until the receiving port is started", async () => {
    const { port1, port2 } = new WireMessageChannel(createTestContext("close-start"));
    let dispatched = false;
    const closed = nextEvent<CloseEvent>(port2, "close").then((event) => {
      dispatched = true;
      return event;
    });

    port1.close();
    await settle();
    expect(dispatched).toBe(false);

    port2.start();
    expect((await closed).wasClean).toBe(true);
    closeAll(port2);
  });

  it("makes close() idempotent and emits one peer close", async () => {
    const { port1, port2 } = new WireMessageChannel(createTestContext("close-twice"));
    let count = 0;
    port2.addEventListener("close", () => count++);
    port2.start();

    port1.close();
    port1.close();
    await settle();

    expect(count).toBe(1);
    closeAll(port2);
  });

  it("rejects postMessage() after close", () => {
    const { port1, port2 } = new WireMessageChannel(createTestContext("closed-send"));
    port1.close();

    expect(() => port1.postMessage("too late")).toThrow(DOMException);

    closeAll(port2);
  });

  it("retains already-queued events when the receiving port closes", async () => {
    // close() disentangles the port; it does not remove tasks already in its
    // port message queue.
    const { port1, port2 } = new WireMessageChannel(createTestContext("queued-on-close"));
    const messages: unknown[] = [];
    port2.addEventListener("message", ({ data }) => messages.push(data));

    port1.postMessage("queued");
    port2.close();
    port2.start();
    await settle();

    expect(messages).toEqual(["queued"]);
    closeAll(port1);
  });

  it.failing("queues messages sent to a port while it is being transferred locally", async () => {
    const context = createTestContext("pending");
    const carrier = new WireMessageChannel(context);
    const payload = new WireMessageChannel(context);

    carrier.port2.addEventListener("messageerror", () => {});
    payload.port1.addEventListener("messageerror", () => {});
    const transferred = nextPortMessage(carrier.port2);
    let receivedPort: MessagePort|undefined;
    try {
      carrier.port1.postMessage("take this", [payload.port1]);
      payload.port2.postMessage("sent before transfer dispatch");

      receivedPort = (await timeout(transferred, 25)).ports[0];
      const received = nextPortMessage(receivedPort);
      expect((await timeout(received, 25)).data).toBe("sent before transfer dispatch");
    } finally {
      closeAll(carrier.port1, carrier.port2, payload.port1, payload.port2, receivedPort);
    }
  });

  it.failing("moves already-queued messages with a transferred port", async () => {
    const context = createTestContext("queued");
    const carrier = new WireMessageChannel(context);
    const payload = new WireMessageChannel(context);

    carrier.port2.addEventListener("messageerror", () => {});
    payload.port1.addEventListener("messageerror", () => {});
    let receivedPort: MessagePort|undefined;
    try {
      payload.port2.postMessage("queued before transfer");
      const transferred = nextPortMessage(carrier.port2);
      carrier.port1.postMessage("take this", [payload.port1]);

      receivedPort = (await timeout(transferred, 25)).ports[0];
      const received = nextPortMessage(receivedPort);
      expect((await timeout(received, 25)).data).toBe("queued before transfer");
    } finally {
      closeAll(carrier.port1, carrier.port2, payload.port1, payload.port2, receivedPort);
    }
  });
});
