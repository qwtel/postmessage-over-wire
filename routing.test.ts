import { describe, expect, it } from "bun:test";

import { WireContext, WireEndpoint, WireMessageChannel } from "./index";
import {
  closeAll,
  createLinkedStreams,
  createTestContext,
  nextMessage,
  nextPortMessage,
  settle,
} from "./test-util";

function routeSnapshot(context: WireContext) {
  return Array.from(context.routeTable, ([id, writer]) => [id, (writer as { identifier: unknown }).identifier]);
}

describe("postmessage-over-wire routing", () => {
  it("keeps a port usable after it is sent out and then returned", async () => {
    const contextA = createTestContext("left");
    const [endpointA, endpointB] = (() => {
      const [left, right] = createLinkedStreams();
      return [
        new WireEndpoint(left, "left", contextA),
        new WireEndpoint(right, "right", createTestContext("right")),
      ] as const;
    })();
    const { port1, port2 } = new WireMessageChannel(contextA);

    const sentToB = nextMessage(endpointB);
    endpointA.postMessage("out", [port1]);
    const remotePort = (await sentToB).ports[0];
    await settle();
    expect(routeSnapshot(contextA)).toEqual([
      ["left-1", "left"],
      ["left-2", "left-2"],
    ]);

    const returnedToA = nextMessage(endpointA);
    endpointB.postMessage("back", [remotePort]);
    const returnedPort = (await returnedToA).ports[0];
    await settle();
    expect(routeSnapshot(contextA)).toEqual([
      ["left-1", "left-1"],
      ["left-2", "left-2"],
    ]);

    const received = nextPortMessage(port2);
    returnedPort.postMessage("still entangled");

    expect((await received).data).toBe("still entangled");

    closeAll(returnedPort, port2, endpointA, endpointB);
  });

  it("routes a transferred port through an intermediate endpoint context", async () => {
    const contextA = createTestContext("a");
    const contextB = createTestContext("b");
    const contextC = createTestContext("c");
    const [aSide, bFromA] = createLinkedStreams();
    const [bToC, cSide] = createLinkedStreams();
    const endpointA = new WireEndpoint(aSide, "a", contextA);
    const endpointBFromA = new WireEndpoint(bFromA, "b-from-a", contextB);
    const endpointBToC = new WireEndpoint(bToC, "b-to-c", contextB);
    const endpointC = new WireEndpoint(cSide, "c", contextC);
    const { port1, port2 } = new WireMessageChannel(contextA);

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

  it("cleans intermediate routes after a multi-hop transferred port returns to origin", async () => {
    const contextA = createTestContext("a");
    const contextB = createTestContext("b");
    const contextC = createTestContext("c");
    const [aSide, bFromA] = createLinkedStreams();
    const [bToC, cSide] = createLinkedStreams();
    const endpointA = new WireEndpoint(aSide, "a", contextA);
    const endpointBFromA = new WireEndpoint(bFromA, "b-from-a", contextB);
    const endpointBToC = new WireEndpoint(bToC, "b-to-c", contextB);
    const endpointC = new WireEndpoint(cSide, "c", contextC);
    const { port1, port2 } = new WireMessageChannel(contextA);
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
      expect(routeSnapshot(contextB)).toEqual([]);
      expect(routeSnapshot(contextC)).toEqual([]);
    } finally {
      closeAll(returnedPort, port2, endpointA, endpointBFromA, endpointBToC, endpointC);
    }
  });
});
