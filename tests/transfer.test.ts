import { describe, expect, it, setDefaultTimeout } from "bun:test";

setDefaultTimeout(50);

import { WireMessageChannel } from "../index";
import {
  closeAll,
  createEndpointPair,
  createTestRouter,
  nextEvent,
  nextMessage,
  nextMessages,
  nextPortMessage,
  nextPortMessages,
  settle,
  timeout,
} from "./test-util";

describe("moving MessagePort endpoints", () => {
  it("keeps messages sent before and after a local transfer in FIFO order", async () => {
    const router = createTestRouter("local-order");
    const carrier = new WireMessageChannel(router);
    const payload = new WireMessageChannel(router);

    carrier.port2.addEventListener("messageerror", () => {});
    payload.port1.addEventListener("messageerror", () => {});
    let moved: MessagePort|undefined;
    try {
      payload.port2.postMessage("before transfer");
      const transfer = nextPortMessage(carrier.port2);
      carrier.port1.postMessage("move", [payload.port1]);
      payload.port2.postMessage("after transfer");

      moved = (await timeout(transfer, 25)).ports[0];
      const messages = nextPortMessages(moved, 2);
      expect((await timeout(messages, 25)).map(({ data }) => data)).toEqual([
        "before transfer",
        "after transfer",
      ]);
    } finally {
      closeAll(carrier.port1, carrier.port2, payload.port1, payload.port2, moved);
    }
  });

  it("moves a queued inbox over a wire link", async () => {
    const routerA = createTestRouter("remote-queue-a");
    const [endpointA, endpointB] = createEndpointPair(routerA, createTestRouter("remote-queue-b"));
    const payload = new WireMessageChannel(routerA);

    let moved: MessagePort|undefined;
    try {
      payload.port2.postMessage("already queued");
      const transfer = nextMessage(endpointB);
      endpointA.postMessage("move", [payload.port1]);

      moved = (await timeout(transfer, 25)).ports[0];
      expect((await timeout(nextPortMessage(moved), 25)).data).toBe("already queued");
    } finally {
      closeAll(payload.port1, payload.port2, moved, endpointA, endpointB);
    }
  });

  it("routes messages sent immediately after an outbound transfer", async () => {
    const routerA = createTestRouter("remote-race-a");
    const [endpointA, endpointB] = createEndpointPair(routerA, createTestRouter("remote-race-b"));
    const payload = new WireMessageChannel(routerA);
    const transfer = nextMessage(endpointB);

    endpointA.postMessage("move", [payload.port1]);
    payload.port2.postMessage("followed the move");

    const moved = (await transfer).ports[0];
    expect((await nextPortMessage(moved)).data).toBe("followed the move");
    closeAll(payload.port2, moved, endpointA, endpointB);
  });

  it("preserves the transfer frame before later endpoint messages", async () => {
    const routerA = createTestRouter("carrier-order-a");
    const [endpointA, endpointB] = createEndpointPair(routerA, createTestRouter("carrier-order-b"));
    const payload = new WireMessageChannel(routerA);
    const received = nextMessages(endpointB, 2);

    endpointA.postMessage("with port", [payload.port1]);
    endpointA.postMessage("after port");

    const [first, second] = await received;
    expect(first.data).toBe("with port");
    expect(first.ports).toHaveLength(1);
    expect(second.data).toBe("after port");
    closeAll(payload.port2, first.ports[0], endpointA, endpointB);
  });

  it("moves two independent ports in one message and preserves transfer-list order", async () => {
    const routerA = createTestRouter("two-a");
    const [endpointA, endpointB] = createEndpointPair(routerA, createTestRouter("two-b"));
    const first = new WireMessageChannel(routerA);
    const second = new WireMessageChannel(routerA);
    const transfer = nextMessage(endpointB);

    endpointA.postMessage("two", [first.port1, second.port1]);

    const event = await transfer;
    expect(event.ports).toHaveLength(2);
    const firstReply = nextPortMessage(first.port2);
    const secondReply = nextPortMessage(second.port2);
    event.ports[0].postMessage("first");
    event.ports[1].postMessage("second");
    expect((await firstReply).data).toBe("first");
    expect((await secondReply).data).toBe("second");
    closeAll(first.port2, second.port2, ...event.ports, endpointA, endpointB);
  });

  it("can move both ends of one channel to the same remote router", async () => {
    const routerA = createTestRouter("pair-a");
    const routerB = createTestRouter("pair-b");
    const [endpointA, endpointB] = createEndpointPair(routerA, routerB);
    const pair = new WireMessageChannel(routerA);
    const transfer = nextMessage(endpointB);

    endpointA.postMessage("whole channel", [pair.port1, pair.port2]);

    const [left, right] = (await transfer).ports;
    const atRight = nextPortMessage(right);
    left.postMessage("now local on B");
    expect((await atRight).data).toBe("now local on B");

    const atLeft = nextPortMessage(left);
    right.postMessage("and back");
    expect((await atLeft).data).toBe("and back");
    closeAll(left, right, endpointA, endpointB);
  });

  it("can return a port to its origin repeatedly", async () => {
    const routerA = createTestRouter("bounce-a");
    const routerB = createTestRouter("bounce-b");
    const [endpointA, endpointB] = createEndpointPair(routerA, routerB);
    const payload = new WireMessageChannel(routerA);

    let atB = nextMessage(endpointB);
    endpointA.postMessage("out one", [payload.port1]);
    let moving: MessagePort = (await atB).ports[0];

    let atA = nextMessage(endpointA);
    endpointB.postMessage("back one", [moving]);
    moving = (await atA).ports[0];

    atB = nextMessage(endpointB);
    endpointA.postMessage("out two", [moving]);
    moving = (await atB).ports[0];

    atA = nextMessage(endpointA);
    endpointB.postMessage("back two", [moving]);
    moving = (await atA).ports[0];

    const received = nextPortMessage(payload.port2);
    moving.postMessage("still one endpoint");
    expect((await received).data).toBe("still one endpoint");
    closeAll(payload.port2, moving, endpointA, endpointB);
  });

  it("can transfer another port through an endpoint that is itself in flight", async () => {
    const routerA = createTestRouter("nested-a");
    const [endpointA, endpointB] = createEndpointPair(routerA, createTestRouter("nested-b"));
    const carrier = new WireMessageChannel(routerA);
    const nested = new WireMessageChannel(routerA);
    const movedCarrierEvent = nextMessage(endpointB);

    endpointA.postMessage("move carrier", [carrier.port1]);
    carrier.port2.postMessage({ nested: nested.port1 }, [nested.port1]);

    const movedCarrier = (await movedCarrierEvent).ports[0];
    const nestedEvent = await nextPortMessage(movedCarrier);
    expect(nestedEvent.data.nested).toBe(nestedEvent.ports[0]);

    const reply = nextPortMessage(nested.port2);
    nestedEvent.ports[0].postMessage("nested path works");
    expect((await reply).data).toBe("nested path works");
    closeAll(carrier.port2, movedCarrier, nested.port2, nestedEvent.ports[0], endpointA, endpointB);
  });

  it("delivers close sent immediately after a remote transfer", async () => {
    const routerA = createTestRouter("remote-close-a");
    const [endpointA, endpointB] = createEndpointPair(routerA, createTestRouter("remote-close-b"));
    const payload = new WireMessageChannel(routerA);
    const transfer = nextMessage(endpointB);

    endpointA.postMessage("move then close", [payload.port1]);
    payload.port2.close();

    const moved = (await transfer).ports[0];
    const closed = nextEvent<CloseEvent>(moved, "close");
    moved.start();
    expect((await closed).wasClean).toBe(true);
    closeAll(moved, endpointA, endpointB);
  });

  it("does not dispatch queued messages on the detached wrapper", async () => {
    const routerA = createTestRouter("detached-events-a");
    const [endpointA, endpointB] = createEndpointPair(routerA, createTestRouter("detached-events-b"));
    const payload = new WireMessageChannel(routerA);
    let oldWrapperEvents = 0;
    let moved: MessagePort|undefined;
    try {
      payload.port1.onmessage = () => oldWrapperEvents++;
      payload.port2.postMessage("queued");
      const transfer = nextMessage(endpointB);

      endpointA.postMessage("move", [payload.port1]);

      moved = (await timeout(transfer, 25)).ports[0];
      expect((await timeout(nextPortMessage(moved), 25)).data).toBe("queued");
      await settle();
      expect(oldWrapperEvents).toBe(0);
    } finally {
      closeAll(payload.port1, payload.port2, moved, endpointA, endpointB);
    }
  });

  it("does not lose close while a local transfer event is pending", async () => {
    // Closing the peer currently deletes the pending route before the receiving
    // wrapper is installed, so the close event is lost.
    const router = createTestRouter("local-close-race");
    const carrier = new WireMessageChannel(router);
    const payload = new WireMessageChannel(router);
    carrier.port2.addEventListener("messageerror", () => {});
    const transfer = nextPortMessage(carrier.port2);

    carrier.port1.postMessage("move then close", [payload.port1]);
    payload.port2.close();

    let moved: MessagePort|undefined;
    try {
      moved = (await timeout(transfer, 25)).ports[0];
      const closed = nextEvent<CloseEvent>(moved, "close");
      moved.start();
      expect((await timeout(closed, 25)).wasClean).toBe(true);
    } finally {
      closeAll(carrier.port1, carrier.port2, moved, payload.port1, payload.port2);
    }
  });
});
