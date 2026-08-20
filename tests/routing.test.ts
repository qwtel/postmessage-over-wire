import { describe, expect, it, setDefaultTimeout } from "bun:test";

setDefaultTimeout(50);

import { WireEndpoint, WireMessageChannel } from "../index";
import {
  closeAll,
  createLinkedStreams,
  createTestRouter,
  nextEvent,
  nextMessage,
  nextPortMessage,
  nextPortMessages,
  routeCount,
  settle,
  timeout,
} from "./test-util";

describe("postmessage-over-wire routing", () => {
  it("keeps a port usable after it is sent out and then returned", async () => {
    const routerA = createTestRouter("left");
    const [endpointA, endpointB] = (() => {
      const [left, right] = createLinkedStreams();
      return [
        new WireEndpoint(left, "left", routerA),
        new WireEndpoint(right, "right", createTestRouter("right")),
      ] as const;
    })();
    const { port1, port2 } = new WireMessageChannel(routerA);

    const sentToB = nextMessage(endpointB);
    endpointA.postMessage("out", [port1]);
    const remotePort = (await sentToB).ports[0];
    await settle();
    expect(routeCount(routerA)).toBe(2);

    const returnedToA = nextMessage(endpointA);
    endpointB.postMessage("back", [remotePort]);
    const returnedPort = (await returnedToA).ports[0];
    await settle();
    expect(routeCount(routerA)).toBe(2);

    const received = nextPortMessage(port2);
    returnedPort.postMessage("still entangled");

    expect((await received).data).toBe("still entangled");

    closeAll(returnedPort, port2, endpointA, endpointB);
  });

  it("routes a transferred port through an intermediate router", async () => {
    const routerA = createTestRouter("a");
    const routerB = createTestRouter("b");
    const routerC = createTestRouter("c");
    const [aSide, bFromA] = createLinkedStreams();
    const [bToC, cSide] = createLinkedStreams();
    const endpointA = new WireEndpoint(aSide, "a", routerA);
    const endpointBFromA = new WireEndpoint(bFromA, "b-from-a", routerB);
    const endpointBToC = new WireEndpoint(bToC, "b-to-c", routerB);
    const endpointC = new WireEndpoint(cSide, "c", routerC);
    const { port1, port2 } = new WireMessageChannel(routerA);

    const receivedByB = nextMessage(endpointBFromA);
    endpointA.postMessage("to-b", [port1]);
    const portAtB = (await receivedByB).ports[0];

    const receivedByC = nextMessage(endpointC);
    endpointBToC.postMessage("to-c", [portAtB]);
    const portAtC = (await receivedByC).ports[0];

    const receivedByA = nextPortMessage(port2);
    portAtC.postMessage("through-b");

    expect((await receivedByA).data).toBe("through-b");

    closeAll(portAtC, port2, endpointA, endpointBFromA, endpointBToC, endpointC);
  });

  it("closes both sides of transferred channels when their wire link closes", async () => {
    const routerA = createTestRouter("left");
    const routerB = createTestRouter("right");
    const [left, right] = createLinkedStreams();
    const endpointA = new WireEndpoint(left, "left", routerA);
    const endpointB = new WireEndpoint(right, "right", routerB);
    const { port1, port2 } = new WireMessageChannel(routerA);

    let remotePort: MessagePort|undefined;
    try {
      const transferred = nextMessage(endpointB);
      endpointA.postMessage("transfer", [port1]);
      remotePort = (await timeout(transferred, 25)).ports[0];

      const localClose = nextEvent<CloseEvent>(port2, "close");
      const remoteClose = nextEvent<CloseEvent>(remotePort, "close");
      port2.start();
      remotePort.start();
      endpointA.terminate();

      await timeout(Promise.all([localClose, remoteClose]), 25);
      await settle();
      expect(routeCount(routerA)).toBe(0);
      expect(routeCount(routerB)).toBe(0);
    } finally {
      closeAll(port1, port2, remotePort, endpointA, endpointB);
    }
  });

  it("cleans intermediate routes after a multi-hop transferred port returns to origin", async () => {
    const routerA = createTestRouter("a");
    const routerB = createTestRouter("b");
    const routerC = createTestRouter("c");
    const [aSide, bFromA] = createLinkedStreams();
    const [bToC, cSide] = createLinkedStreams();
    const endpointA = new WireEndpoint(aSide, "a", routerA);
    const endpointBFromA = new WireEndpoint(bFromA, "b-from-a", routerB);
    const endpointBToC = new WireEndpoint(bToC, "b-to-c", routerB);
    const endpointC = new WireEndpoint(cSide, "c", routerC);
    const { port1, port2 } = new WireMessageChannel(routerA);
    let returnedPort: MessagePort|undefined;

    try {
      let receivedByB = nextMessage(endpointBFromA);
      endpointA.postMessage("to-b", [port1]);
      let portAtB = (await receivedByB).ports[0];

      const receivedByC = nextMessage(endpointC);
      endpointBToC.postMessage("to-c", [portAtB]);
      const portAtC = (await receivedByC).ports[0];

      receivedByB = nextMessage(endpointBToC);
      endpointC.postMessage("back-to-b", [portAtC]);
      portAtB = (await receivedByB).ports[0];

      const receivedByA = nextMessage(endpointA);
      endpointBFromA.postMessage("back-to-a", [portAtB]);
      returnedPort = (await receivedByA).ports[0];
      await settle();

      const receivedByLocalPort = nextPortMessage(port2);
      returnedPort.postMessage("home");

      expect((await receivedByLocalPort).data).toBe("home");
      expect(routeCount(routerB)).toBe(0);
      expect(routeCount(routerC)).toBe(0);
    } finally {
      closeAll(returnedPort, port2, endpointA, endpointBFromA, endpointBToC, endpointC);
    }
  });

  it("keeps a multi-hop channel bidirectional", async () => {
    const routerA = createTestRouter("duplex-a");
    const routerB = createTestRouter("duplex-b");
    const routerC = createTestRouter("duplex-c");
    const [aSide, bFromA] = createLinkedStreams();
    const [bToC, cSide] = createLinkedStreams();
    const endpointA = new WireEndpoint(aSide, "a", routerA);
    const endpointBFromA = new WireEndpoint(bFromA, "b-from-a", routerB);
    const endpointBToC = new WireEndpoint(bToC, "b-to-c", routerB);
    const endpointC = new WireEndpoint(cSide, "c", routerC);
    const channel = new WireMessageChannel(routerA);

    const atB = nextMessage(endpointBFromA);
    endpointA.postMessage("to b", [channel.port1]);
    const portAtB = (await atB).ports[0];
    const atC = nextMessage(endpointC);
    endpointBToC.postMessage("to c", [portAtB]);
    const portAtC = (await atC).ports[0];

    const atA = nextPortMessage(channel.port2);
    portAtC.postMessage("c to a");
    expect((await atA).data).toBe("c to a");

    const backAtC = nextPortMessage(portAtC);
    channel.port2.postMessage("a to c");
    expect((await backAtC).data).toBe("a to c");
    closeAll(portAtC, channel.port2, endpointA, endpointBFromA, endpointBToC, endpointC);
  });

  it("forwards messages that arrive at an intermediate node during the next move", async () => {
    const routerA = createTestRouter("race-a");
    const routerB = createTestRouter("race-b");
    const routerC = createTestRouter("race-c");
    const [aSide, bFromA] = createLinkedStreams();
    const [bToC, cSide] = createLinkedStreams();
    const endpointA = new WireEndpoint(aSide, "a", routerA);
    const endpointBFromA = new WireEndpoint(bFromA, "b-from-a", routerB);
    const endpointBToC = new WireEndpoint(bToC, "b-to-c", routerB);
    const endpointC = new WireEndpoint(cSide, "c", routerC);
    const channel = new WireMessageChannel(routerA);

    const atB = nextMessage(endpointBFromA);
    endpointA.postMessage("to b", [channel.port1]);
    const portAtB = (await atB).ports[0];
    const atC = nextMessage(endpointC);
    endpointBToC.postMessage("to c", [portAtB]);
    channel.port2.postMessage("sent while moving onward");

    const portAtC = (await atC).ports[0];
    expect((await nextPortMessage(portAtC)).data).toBe("sent while moving onward");
    closeAll(portAtC, channel.port2, endpointA, endpointBFromA, endpointBToC, endpointC);
  });

  it("preserves a larger ordered backlog across an intermediate router", async () => {
    const routerA = createTestRouter("backlog-a");
    const routerB = createTestRouter("backlog-b");
    const routerC = createTestRouter("backlog-c");
    const [aSide, bFromA] = createLinkedStreams();
    const [bToC, cSide] = createLinkedStreams();
    const endpointA = new WireEndpoint(aSide, "a", routerA);
    const endpointBFromA = new WireEndpoint(bFromA, "b-from-a", routerB);
    const endpointBToC = new WireEndpoint(bToC, "b-to-c", routerB);
    const endpointC = new WireEndpoint(cSide, "c", routerC);
    const channel = new WireMessageChannel(routerA);

    const atB = nextMessage(endpointBFromA);
    endpointA.postMessage("to b", [channel.port1]);
    const portAtB = (await atB).ports[0];
    const atC = nextMessage(endpointC);
    endpointBToC.postMessage("to c", [portAtB]);
    const portAtC = (await atC).ports[0];
    const received = nextPortMessages(portAtC, 40);

    for (let index = 0; index < 40; index++) channel.port2.postMessage(index);

    expect((await received).map(({ data }) => data)).toEqual(
      Array.from({ length: 40 }, (_, index) => index),
    );
    closeAll(portAtC, channel.port2, endpointA, endpointBFromA, endpointBToC, endpointC);
  });

  it("propagates an intermediate link termination to both channel endpoints", async () => {
    const routerA = createTestRouter("failure-a");
    const routerB = createTestRouter("failure-b");
    const routerC = createTestRouter("failure-c");
    const [aSide, bFromA] = createLinkedStreams();
    const [bToC, cSide] = createLinkedStreams();
    const endpointA = new WireEndpoint(aSide, "a", routerA);
    const endpointBFromA = new WireEndpoint(bFromA, "b-from-a", routerB);
    const endpointBToC = new WireEndpoint(bToC, "b-to-c", routerB);
    const endpointC = new WireEndpoint(cSide, "c", routerC);
    const channel = new WireMessageChannel(routerA);

    let portAtB: MessagePort|undefined;
    let portAtC: MessagePort|undefined;
    try {
      const atB = nextMessage(endpointBFromA);
      endpointA.postMessage("to b", [channel.port1]);
      portAtB = (await timeout(atB, 25)).ports[0];
      const atC = nextMessage(endpointC);
      endpointBToC.postMessage("to c", [portAtB]);
      portAtC = (await timeout(atC, 25)).ports[0];
      const closedAtA = nextEvent<CloseEvent>(channel.port2, "close");
      const closedAtC = nextEvent<CloseEvent>(portAtC, "close");
      channel.port2.start();
      portAtC.start();

      endpointBToC.terminate();

      await timeout(Promise.all([closedAtA, closedAtC]), 25);
      await settle();
      expect(routeCount(routerA)).toBe(0);
      expect(routeCount(routerB)).toBe(0);
      expect(routeCount(routerC)).toBe(0);
    } finally {
      closeAll(
        channel.port1,
        channel.port2,
        portAtB,
        portAtC,
        endpointA,
        endpointBFromA,
        endpointBToC,
        endpointC,
      );
    }
  });
});
