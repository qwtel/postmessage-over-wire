import { describe, expect, it, setDefaultTimeout } from "bun:test";

setDefaultTimeout(50);

import { WireEndpoint, WireMessageChannel } from "../index";
import {
  closeAll,
  createLinkedStreams,
  createTestContext,
  nextEvent,
  nextMessage,
  settle,
  timeout,
} from "./test-util";

describe("transport lifecycle and failures", () => {
  it("closes the underlying writable once when terminate() is repeated", async () => {
    let closes = 0;
    const endpoint = new WireEndpoint({
      readable: new ReadableStream<Uint8Array>(),
      writable: new WritableStream<Uint8Array>({ close() { closes++; } }),
    }, "close-once", createTestContext("close-once"));

    endpoint.terminate();
    endpoint.terminate();
    await settle();

    expect(closes).toBe(1);
  });

  it("reports an asynchronous write failure as messageerror", async () => {
    // The serializer pipe currently accepts the RPC write before its downstream
    // byte sink rejects, so the failed post is not associated with an event.
    const failure = new Error("write failed");
    const endpoint = new WireEndpoint({
      readable: new ReadableStream<Uint8Array>(),
      writable: new WritableStream<Uint8Array>({ write() { throw failure; } }),
    }, "write-failure", createTestContext("write-failure"));
    const reported = nextEvent<MessageEvent>(endpoint, "messageerror");

    expect(() => endpoint.postMessage("cannot write")).not.toThrow();

    try {
      expect((await timeout(reported, 25)).data).toBe(failure);
    } finally {
      closeAll(endpoint);
    }
  });

  it("closes the local peer of every channel routed through a failed writer", async () => {
    const contextA = createTestContext("fail-routes-a");
    const contextB = createTestContext("fail-routes-b");
    const [leftBase, rightStream] = createLinkedStreams();
    const baseWriter = leftBase.writable.getWriter();
    let failWrites = false;
    const failure = new Error("link write failed");
    const controlledLeft = {
      readable: leftBase.readable,
      writable: new WritableStream<Uint8Array>({
        write(chunk) {
          if (failWrites) throw failure;
          return baseWriter.write(chunk);
        },
        close() { return baseWriter.close(); },
        abort(reason) { return baseWriter.abort(reason); },
      }),
    };
    const endpointA = new WireEndpoint(controlledLeft, "controlled", contextA);
    const endpointB = new WireEndpoint(rightStream, "remote", contextB);
    const channel = new WireMessageChannel(contextA);
    let moved: MessagePort|undefined;
    try {
      const movedEvent = nextMessage(endpointB);
      endpointA.postMessage("move", [channel.port1]);
      moved = (await timeout(movedEvent, 25)).ports[0];
      const localClosed = nextEvent<CloseEvent>(channel.port2, "close");
      channel.port2.start();
      failWrites = true;

      endpointA.postMessage("trigger failure");

      await timeout(localClosed, 25);
      await settle();
      expect(contextA.routeTable.size).toBe(0);
    } finally {
      closeAll(channel.port1, channel.port2, moved, endpointA, endpointB);
    }
  });

  it("turns readable EOF into endpoint termination", async () => {
    let finish!: () => void;
    let closes = 0;
    const readable = new ReadableStream<Uint8Array>({
      start(controller) { finish = () => controller.close(); },
    });
    const writable = new WritableStream<Uint8Array>({ close() { closes++; } });
    const endpoint = new WireEndpoint({ readable, writable }, "eof", createTestContext("eof"));

    try {
      finish();
      await settle();

      expect(closes).toBe(1);
    } finally {
      closeAll(endpoint);
    }
  });

  it("propagates a one-direction write failure to the remote endpoint", async () => {
    // A general readable/writable pair does not couple failure of one direction
    // to the opposite reader. Solving this requires a transport-level failure
    // signal or a liveness protocol; another frame cannot cross the broken path.
    const contextA = createTestContext("half-open-a");
    const contextB = createTestContext("half-open-b");
    const [leftBase, rightStream] = createLinkedStreams();
    const baseWriter = leftBase.writable.getWriter();
    let failWrites = false;
    const controlledLeft = {
      readable: leftBase.readable,
      writable: new WritableStream<Uint8Array>({
        write(chunk) {
          if (failWrites) throw new Error("one-way write failure");
          return baseWriter.write(chunk);
        },
        close() { return baseWriter.close(); },
        abort(reason) { return baseWriter.abort(reason); },
      }),
    };
    const endpointA = new WireEndpoint(controlledLeft, "half-open", contextA);
    const endpointB = new WireEndpoint(rightStream, "remote-half", contextB);
    const channel = new WireMessageChannel(contextA);
    const movedEvent = nextMessage(endpointB);
    endpointA.postMessage("move", [channel.port1]);
    const moved = (await movedEvent).ports[0];

    try {
      const remoteClosed = nextEvent<CloseEvent>(moved, "close");
      moved.start();
      failWrites = true;
      endpointA.postMessage("trigger one-way failure");

      expect((await timeout(remoteClosed, 25)).wasClean).toBe(false);
    } finally {
      closeAll(channel.port2, moved, endpointA, endpointB);
    }
  });
});
