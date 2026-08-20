import { describe, expect, it, setDefaultTimeout } from "bun:test";

setDefaultTimeout(50);

import * as Caplink from "../../comlink/src/caplink";
import { WireEndpoint, WireMessageChannel, WireMessagePort } from "../index";
import { closeAll, createEndpointPair, createLinkedStreams, createTestContext, nextEvent, nextMessage, settle, timeout } from "./test-util";

const syntheticClose = (target: EventTarget) => target.dispatchEvent(new Event("close"));

describe("deterministic lifetime management", () => {
  it("supports using to close a port and release both local routes", async () => {
    const context = createTestContext("using-port");
    const channel = new WireMessageChannel(context);
    const closed = nextEvent<CloseEvent>(channel.port2, "close");
    channel.port2.start();

    {
      using port = channel.port1;
      void port;
    }

    expect((await timeout(closed)).wasClean).toBe(true);
    expect(context.routeTable.size).toBe(0);
    closeAll(channel.port2);
  });

  it("supports using to disconnect a link and break every dependent port", async () => {
    const leftContext = createTestContext("using-link-left");
    const rightContext = createTestContext("using-link-right");
    const [left, right] = createEndpointPair(leftContext, rightContext);
    const channel = new WireMessageChannel(leftContext);
    const transferred = nextMessage(right);
    left.postMessage("move", [channel.port1]);
    const moved = (await transferred).ports[0];
    const localClosed = nextEvent<CloseEvent>(channel.port2, "close");
    const remoteClosed = nextEvent<CloseEvent>(moved, "close");
    channel.port2.start();
    moved.start();

    {
      using connection = left;
      void connection;
    }

    await timeout(Promise.all([localClosed, remoteClosed]));
    expect(leftContext.routeTable.size).toBe(0);
    expect(rightContext.routeTable.size).toBe(0);
    closeAll(channel.port2, moved, right);
  });

  it("dispatches exactly one clean close event when using disposes a WireEndpoint", async () => {
    const endpoint = new WireEndpoint({ readable: new ReadableStream<Uint8Array>(), writable: new WritableStream<Uint8Array>() }, "using-close", createTestContext("using-close"));
    const closes: CloseEvent[] = [];
    const closed = nextEvent<CloseEvent>(endpoint, "close");
    endpoint.addEventListener("close", (event) => closes.push(event));

    try {
      {
        using connection = endpoint;
        void connection;
      }
      await timeout(closed);
      await settle();
      expect(closes).toHaveLength(1);
      expect(closes[0].wasClean).toBe(true);
    } finally {
      closeAll(endpoint);
    }
  });

  it("dispatches an unclean close event when the readable reaches EOF", async () => {
    let finish!: () => void;
    const endpoint = new WireEndpoint({
      readable: new ReadableStream<Uint8Array>({ start(controller) { finish = () => controller.close(); } }),
      writable: new WritableStream<Uint8Array>(),
    }, "readable-eof", createTestContext("readable-eof"));
    const closed = nextEvent<CloseEvent>(endpoint, "close");

    try {
      finish();
      expect((await timeout(closed)).wasClean).toBe(false);
    } finally {
      closeAll(endpoint);
    }
  });

  it("dispatches an unclean close but no error for an aborted readable", async () => {
    let abort!: () => void;
    const endpoint = new WireEndpoint({
      readable: new ReadableStream<Uint8Array>({ start(controller) { abort = () => controller.error(new DOMException("aborted", "AbortError")); } }),
      writable: new WritableStream<Uint8Array>(),
    }, "readable-abort", createTestContext("readable-abort"));
    const closed = nextEvent<CloseEvent>(endpoint, "close");
    let errors = 0;
    endpoint.addEventListener("error", () => errors++);

    try {
      abort();
      expect((await timeout(closed)).wasClean).toBe(false);
      expect(errors).toBe(0);
    } finally {
      closeAll(endpoint);
    }
  });

  it("dispatches error and unclean close when the writable fails", async () => {
    const failure = new Error("writer failed");
    const endpoint = new WireEndpoint({
      readable: new ReadableStream<Uint8Array>(),
      writable: new WritableStream<Uint8Array>({ write() { throw failure; } }),
    }, "writer-failure", createTestContext("writer-failure"));
    const errored = nextEvent<ErrorEvent>(endpoint, "error");
    const closed = nextEvent<CloseEvent>(endpoint, "close");

    try {
      endpoint.postMessage("trigger failure");
      expect((await timeout(errored)).error).toBe(failure);
      expect((await timeout(closed)).wasClean).toBe(false);
    } finally {
      closeAll(endpoint);
    }
  });

  it("does not process incoming frames after a WireEndpoint closes", async () => {
    const [leftStream, rightStream] = createLinkedStreams();
    const leftWriter = leftStream.writable.getWriter();
    const allowWriterClose = Promise.withResolvers<void>();
    const left = new WireEndpoint({
      readable: leftStream.readable,
      writable: new WritableStream<Uint8Array>({
        write(chunk) { return leftWriter.write(chunk); },
        async close() {
          await allowWriterClose.promise;
          await leftWriter.close();
        },
        abort(reason) { return leftWriter.abort(reason); },
      }),
    }, "closing-reader-left", createTestContext("closing-reader-left"));
    const right = new WireEndpoint(rightStream, "closing-reader-right", createTestContext("closing-reader-right"));
    const events: string[] = [];
    left.addEventListener("close", () => events.push("close"));
    left.addEventListener("message", ({ data }) => events.push(`message:${data}`));

    try {
      left.terminate();
      right.postMessage("late");
      await settle();
      await settle();

      expect(events).toEqual(["close"]);
    } finally {
      allowWriterClose.resolve();
      closeAll(left, right);
      await settle();
    }
  });

  it("rejects pending Caplink requests when its WireEndpoint is explicitly disconnected", async () => {
    const [server, client] = createEndpointPair();
    Caplink.expose({ wait: () => new Promise<never>(() => {}) }, server);
    const remote = Caplink.wrap<any>(client);
    const rejected = remote.wait().then(() => null, (error: unknown) => error);

    try {
      await settle();
      {
        using connection = client;
        void connection;
      }
      expect(await timeout(rejected)).toBeInstanceOf(Error);
    } finally {
      syntheticClose(client);
      syntheticClose(server);
      closeAll(client, server);
    }
  });

  it("disposes Caplink exports when the peer WireEndpoint disconnects", async () => {
    const [server, client] = createEndpointPair();
    const disposed = Promise.withResolvers<void>();
    let disposals = 0;
    Caplink.expose({
      [Symbol.dispose]() {
        disposals++;
        disposed.resolve();
      },
    }, server);

    try {
      {
        using connection = client;
        void connection;
      }
      await timeout(disposed.promise);
      expect(disposals).toBe(1);
    } finally {
      syntheticClose(server);
      syntheticClose(client);
      closeAll(client, server);
    }
  });

  it("using a carrier port abandons transferred ports that were never delivered", async () => {
    const context = createTestContext("abandoned-transfer");
    const carrier = new WireMessageChannel(context);
    const payload = new WireMessageChannel(context);
    const payloadClosed = nextEvent<CloseEvent>(payload.port2, "close");
    payload.port2.start();
    carrier.port1.postMessage("queued transfer", [payload.port1]);

    try {
      {
        using receiver = carrier.port2;
        void receiver;
      }
      expect((await timeout(payloadClosed)).wasClean).toBe(true);
      expect(context.routeTable.size).toBe(0);
    } finally {
      closeAll(carrier.port1, carrier.port2, payload.port1, payload.port2);
    }
  });

  it("abandons transferred ports when their carrier destination has closed", async () => {
    const leftContext = createTestContext("closed-destination-left");
    const rightContext = createTestContext("closed-destination-right");
    const [left, right] = createEndpointPair(leftContext, rightContext);
    const carrier = new WireMessageChannel(leftContext);
    const carrierMoved = nextMessage(right);
    left.postMessage("carrier", [carrier.port1]);
    const moved = (await carrierMoved).ports[0];
    const payload = new WireMessageChannel(leftContext);
    const payloadClosed = nextEvent<CloseEvent>(payload.port2, "close");
    payload.port2.start();

    try {
      moved.close();
      carrier.port2.postMessage("late transfer", [payload.port1]);

      expect((await timeout(payloadClosed)).wasClean).toBe(true);
      await settle();
      expect(leftContext.routeTable.size).toBe(0);
      expect(rightContext.routeTable.size).toBe(0);
    } finally {
      closeAll(carrier.port1, carrier.port2, payload.port1, payload.port2, moved, left, right);
    }
  });

  it("explicitly closing a fromNative bridge closes the native port", async () => {
    const closed = Promise.withResolvers<void>();
    class ObservedNativePort extends EventTarget {
      closes = 0;
      listeners = 0;
      override addEventListener(type: string, listener: EventListenerOrEventListenerObject | null, options?: boolean | AddEventListenerOptions) {
        if (listener) this.listeners++;
        super.addEventListener(type, listener, options);
      }
      override removeEventListener(type: string, listener: EventListenerOrEventListenerObject | null, options?: boolean | EventListenerOptions) {
        if (listener) this.listeners--;
        super.removeEventListener(type, listener, options);
      }
      postMessage() {}
      start() {}
      close() {
        this.closes++;
        closed.resolve();
      }
    }
    const native = new ObservedNativePort();
    const wire = WireMessagePort.fromNative(native as unknown as MessagePort, createTestContext("native-disposal"));

    try {
      {
        using bridge = wire;
        void bridge;
      }
      await timeout(closed.promise);
      expect(native.closes).toBe(1);
      expect(native.listeners).toBe(0);
    } finally {
      native.dispatchEvent(new Event("close"));
      closeAll(wire);
    }
  });

  it("explicitly closing a toNative bridge closes its native facade", () => {
    const NativeMessageChannel = globalThis.MessageChannel;
    let privatePortListenerRemovals = 0;
    class ObservedMessageChannel extends NativeMessageChannel {
      constructor() {
        super();
        const removeEventListener = this.port2.removeEventListener.bind(this.port2);
        Object.defineProperty(this.port2, "removeEventListener", {
          configurable: true,
          value(type: string, listener: EventListenerOrEventListenerObject | null, options?: boolean | EventListenerOptions) {
            if (type === "message" && listener) privatePortListenerRemovals++;
            removeEventListener(type, listener, options);
          },
        });
      }
    }
    Object.defineProperty(globalThis, "MessageChannel", { configurable: true, writable: true, value: ObservedMessageChannel });
    const channel = new WireMessageChannel(createTestContext("native-facade-disposal"));
    const native = channel.port1.toNative();
    Object.defineProperty(globalThis, "MessageChannel", { configurable: true, writable: true, value: NativeMessageChannel });
    const nativeClose = native.close.bind(native);
    let closes = 0;
    Object.defineProperty(native, "close", { configurable: true, value: () => { closes++; nativeClose(); } });

    try {
      {
        using bridge = channel.port1;
        void bridge;
      }
      channel.port1.close();
      expect(closes).toBe(1);
      expect(privatePortListenerRemovals).toBe(1);
    } finally {
      Object.defineProperty(globalThis, "MessageChannel", { configurable: true, writable: true, value: NativeMessageChannel });
      nativeClose();
      closeAll(channel.port1, channel.port2);
    }
  });
});
