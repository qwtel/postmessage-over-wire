import { describe, expect, it, setDefaultTimeout } from "bun:test";

setDefaultTimeout(50);

import { WireEndpoint, WireMessageChannel } from "../index";
import { deserialize as deserializeFrame } from "@workers/v8-value-serializer/v8";
import {
  closeAll,
  createEndpointPair,
  createFragmentedLinkedStreams,
  createLinkedStreams,
  createTestRouter,
  nextEvent,
  nextMessage,
  nextMessages,
  nextPortMessage,
  settle,
  timeout,
} from "./test-util";

describe("WireEndpoint", () => {
  it("writes protocol frames and shipped inbox envelopes as compact positional tuples", async () => {
    const [leftStream, rightStream] = createLinkedStreams();
    const leftFrames: unknown[][] = [];
    const rightFrames: unknown[][] = [];
    const capture = (stream: typeof leftStream, frames: unknown[][]) => {
      const writer = stream.writable.getWriter();
      return {
        readable: stream.readable,
        writable: new WritableStream<Uint8Array>({
          write(chunk) {
            const length = new DataView(chunk.buffer, chunk.byteOffset, 4).getUint32(0, true);
            expect(length).toBe(chunk.byteLength - 4);
            const frame = deserializeFrame(chunk.subarray(4));
            expect(Array.isArray(frame)).toBe(true);
            frames.push(frame);
            return writer.write(chunk);
          },
          close: () => writer.close(),
          abort: (reason) => writer.abort(reason),
        }),
      };
    };
    const leftRouter = createTestRouter("tuple-left");
    const left = new WireEndpoint(capture(leftStream, leftFrames), "left", leftRouter);
    const right = new WireEndpoint(capture(rightStream, rightFrames), "right", createTestRouter("tuple-right"));
    const channel = new WireMessageChannel(leftRouter);
    const transferred = nextMessage(right);

    channel.port2.postMessage("queued before transfer");
    left.postMessage("move", [channel.port1]);
    const moved = (await transferred).ports[0];
    const closed = nextEvent<CloseEvent>(moved, "close");
    moved.start();
    channel.port2.close();
    await closed;
    await settle();

    expect(leftFrames.map(([type]) => type)).toEqual(["message", "close"]);
    expect(rightFrames.map(([type]) => type)).toEqual(["moved"]);
    const shippedPort = (leftFrames[0]/* .ports */[3] as unknown[][])[0];
    expect(Array.isArray(shippedPort)).toBe(true);
    expect(Array.isArray((shippedPort/* .inbox */[2] as unknown[])[0])).toBe(true);
    closeAll(channel.port1, channel.port2, moved, left, right);
  });

  it("delivers without an explicit start() call", async () => {
    const [left, right] = createEndpointPair();
    const received = nextMessage(right);

    left.postMessage("endpoint message");

    expect((await received).data).toBe("endpoint message");
    closeAll(left, right);
  });

  it("preserves FIFO order across the stream", async () => {
    const [left, right] = createEndpointPair();
    const received = nextMessages(right, 50);

    for (let index = 0; index < 50; index++) left.postMessage(index);

    expect((await received).map(({ data }) => data)).toEqual(
      Array.from({ length: 50 }, (_, index) => index),
    );
    closeAll(left, right);
  });

  it("carries independent ordered traffic in both directions", async () => {
    const [left, right] = createEndpointPair();
    const atLeft = nextMessages(left, 3);
    const atRight = nextMessages(right, 3);

    left.postMessage("l1");
    right.postMessage("r1");
    left.postMessage("l2");
    right.postMessage("r2");
    left.postMessage("l3");
    right.postMessage("r3");

    expect((await atLeft).map(({ data }) => data)).toEqual(["r1", "r2", "r3"]);
    expect((await atRight).map(({ data }) => data)).toEqual(["l1", "l2", "l3"]);
    closeAll(left, right);
  });

  it("treats the transport as a byte stream rather than relying on write boundaries", async () => {
    const [leftStream, rightStream] = createFragmentedLinkedStreams();
    const left = new WireEndpoint(leftStream, "fragmented-left", createTestRouter("fragmented-left"));
    const right = new WireEndpoint(rightStream, "fragmented-right", createTestRouter("fragmented-right"));
    const atLeft = nextMessage(left);
    const atRight = nextMessage(right);

    left.postMessage({ direction: "right", payload: "a".repeat(128) });
    right.postMessage({ direction: "left", payload: [1, 2, 3] });

    expect((await atRight).data).toEqual({ direction: "right", payload: "a".repeat(128) });
    expect((await atLeft).data).toEqual({ direction: "left", payload: [1, 2, 3] });
    closeAll(left, right);
  });

  it("replaces and clears onmessage", async () => {
    const [left, right] = createEndpointPair();
    const calls: string[] = [];

    right.onmessage = () => calls.push("old");
    right.onmessage = () => calls.push("new");
    left.postMessage("first");
    await settle();
    expect(calls).toEqual(["new"]);

    right.onmessage = null;
    left.postMessage("second");
    await settle();
    expect(calls).toEqual(["new"]);
    closeAll(left, right);
  });

  it("flushes an already-scheduled write before terminate() closes the stream", async () => {
    const [left, right] = createEndpointPair();
    const received = nextMessage(right);

    left.postMessage("last message");
    left.terminate();

    expect((await received).data).toBe("last message");
    closeAll(right);
  });

  it("makes terminate() idempotent", async () => {
    const [left, right] = createEndpointPair();

    left.terminate();
    left.terminate();
    right.terminate();
    right.terminate();
    await settle();

    expect(true).toBe(true);
  });

  it("reports postMessage() after termination as messageerror", async () => {
    const [left, right] = createEndpointPair();
    left.terminate();
    await settle();
    const failed = nextEvent<MessageEvent>(left, "messageerror");

    expect(() => left.postMessage("too late")).not.toThrow();

    expect((await failed).data).toBeInstanceOf(Error);
    closeAll(right);
  });

  it("reports a readable-side transport error as an error event", async () => {
    let fail!: (error: unknown) => void;
    const readable = new ReadableStream<Uint8Array>({
      start(controller) {
        fail = (error) => controller.error(error);
      },
    });
    const writable = new WritableStream<Uint8Array>();
    const endpoint = new WireEndpoint({ readable, writable }, "broken-read", createTestRouter("broken-read"));
    const error = new Error("read failed");
    const reported = nextEvent<ErrorEvent>(endpoint, "error");

    fail(error);

    expect((await reported).error).toBe(error);
    closeAll(endpoint);
  });

  it("does not report AbortError as an error event", async () => {
    let fail!: (error: unknown) => void;
    const readable = new ReadableStream<Uint8Array>({
      start(controller) {
        fail = (error) => controller.error(error);
      },
    });
    const writable = new WritableStream<Uint8Array>();
    const endpoint = new WireEndpoint({ readable, writable }, "aborted", createTestRouter("aborted"));
    let errors = 0;
    endpoint.addEventListener("error", () => errors++);

    fail(new DOMException("cancelled", "AbortError"));
    await settle();

    expect(errors).toBe(0);
    closeAll(endpoint);
  });

  it("terminates after an AbortError closes the readable side", async () => {
    let fail!: (error: unknown) => void;
    let closes = 0;
    const readable = new ReadableStream<Uint8Array>({
      start(controller) {
        fail = (error) => controller.error(error);
      },
    });
    const writable = new WritableStream<Uint8Array>({ close() { closes++; } });
    const endpoint = new WireEndpoint({ readable, writable }, "aborted-close", createTestRouter("aborted-close"));

    try {
      fail(new DOMException("cancelled", "AbortError"));
      await settle();

      expect(closes).toBe(1);
    } finally {
      closeAll(endpoint);
    }
  });

  it("reports malformed stream bytes and terminates the endpoint", async () => {
    // DeserializerStream currently treats this truncated frame as an ordinary
    // end-of-stream instead of surfacing an error event.
    let push!: (chunk: Uint8Array) => void;
    let finish!: () => void;
    const readable = new ReadableStream<Uint8Array>({
      start(controller) {
        push = (chunk) => controller.enqueue(chunk);
        finish = () => controller.close();
      },
    });
    const writable = new WritableStream<Uint8Array>();
    const endpoint = new WireEndpoint({ readable, writable }, "malformed", createTestRouter("malformed"));
    const reported = nextEvent<ErrorEvent>(endpoint, "error");

    push(new Uint8Array([0xff, 0x00, 0x01]));
    finish();

    try {
      expect((await timeout(reported, 25)).error).toBeInstanceOf(Error);
    } finally {
      closeAll(endpoint);
    }
  });

  it("does not close unrelated local channels when its link terminates", async () => {
    const routerA = createTestRouter("isolated-link-a");
    const routerB = createTestRouter("isolated-link-b");
    const [leftStream, rightStream] = createLinkedStreams();
    const left = new WireEndpoint(leftStream, "left", routerA);
    const right = new WireEndpoint(rightStream, "right", routerB);
    const local = new WireMessageChannel(routerA);
    const remote = new WireMessageChannel(routerA);
    const transferred = nextMessage(right);
    left.postMessage("move", [remote.port1]);
    const remotePort = (await transferred).ports[0];

    left.terminate();
    await settle();

    const localMessage = nextPortMessage(local.port2);
    local.port1.postMessage("unrelated channel survives");
    expect((await localMessage).data).toBe("unrelated channel survives");
    closeAll(local.port1, local.port2, remote.port2, remotePort, right);
  });

  it("does not close routes owned by another link in the same router", async () => {
    const central = createTestRouter("central");
    const [left1, remote1] = createEndpointPair(central, createTestRouter("remote-1"));
    const [left2, remote2] = createEndpointPair(central, createTestRouter("remote-2"));
    const channel1 = new WireMessageChannel(central);
    const channel2 = new WireMessageChannel(central);
    const transfer1 = nextMessage(remote1);
    const transfer2 = nextMessage(remote2);
    left1.postMessage("one", [channel1.port1]);
    left2.postMessage("two", [channel2.port1]);
    const port1 = (await transfer1).ports[0];
    const port2 = (await transfer2).ports[0];

    left1.terminate();
    await settle();

    const survives = nextPortMessage(channel2.port2);
    port2.postMessage("link two survives");
    expect((await survives).data).toBe("link two survives");
    closeAll(channel1.port2, channel2.port2, port1, port2, remote1, left2, remote2);
  });
});
