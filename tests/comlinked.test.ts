import { describe, expect, it, setDefaultTimeout } from "bun:test";

setDefaultTimeout(50);

import * as Caplink from "@workers/caplink";
import "../comlinked";
import { closeAll, createEndpointPair, createTestContext } from "./test-util";

describe("Caplink integration", () => {
  it("creates callback channels in a custom WireContext", async () => {
    const [client, server] = createEndpointPair(createTestContext("callback-client"), createTestContext("callback-server"));
    Caplink.expose({ invoke: async (callback: (value: number) => Promise<number>) => callback(41) }, server);
    const remote = Caplink.wrap<{ invoke(callback: (value: number) => number): Promise<number> }>(client);

    try {
      expect(await remote.invoke((value) => value + 1)).toBe(42);
    } finally {
      closeAll(client, server);
    }
  });

  it("adopts native capability ports into a custom WireContext", async () => {
    const native = new MessageChannel();
    Caplink.expose({ value: 42 }, native.port1);
    const nativeRemote = Caplink.wrap<{ value: number }>(native.port2);
    const [client, server] = createEndpointPair(createTestContext("adopt-client"), createTestContext("adopt-server"));
    Caplink.expose({ read: async (remote: { value: Promise<number> }) => remote.value }, server);
    const remote = Caplink.wrap<{ read(remote: Caplink.Remote<{ value: number }>): Promise<number> }>(client);

    try {
      expect(await remote.read(nativeRemote)).toBe(42);
    } finally {
      closeAll(client, server, native.port1, native.port2);
    }
  });
});
