import { describe, expect, it } from "bun:test";

import { WireMessageChannel } from "./index";
import { closeAll, createTestContext, nextEvent, nextPortMessage } from "./test-util";

describe("WireMessagePort", () => {
  it("delivers messages across a local channel", async () => {
    const context = createTestContext("local");
    const { port1, port2 } = new WireMessageChannel(context);

    const received = nextPortMessage(port2);
    port1.postMessage({ local: true });

    expect((await received).data).toEqual({ local: true });

    closeAll(port1, port2);
    expect(context.routeTable.size).toBe(0);
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

  it("dispatches close on the entangled port", async () => {
    const context = createTestContext("close");
    const { port1, port2 } = new WireMessageChannel(context);

    const closed = nextEvent<CloseEvent>(port2, "close");
    port2.start();
    port1.close();

    expect((await closed).type).toBe("close");

    closeAll(port2);
    expect(context.routeTable.size).toBe(0);
  });
});
