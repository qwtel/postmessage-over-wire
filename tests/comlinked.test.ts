import { describe, expect, it, setDefaultTimeout } from "bun:test";

setDefaultTimeout(50);

import * as Caplink from "@workers/caplink";
import { WireEndpoint, WireMessageChannel } from "../comlinked";
import { closeAll, createLinkedStreams } from "./test-util";

function createDefaultEndpointPair(): [WireEndpoint, WireEndpoint] {
  const [left, right] = createLinkedStreams();
  return [new WireEndpoint(left), new WireEndpoint(right)];
}

describe("Caplink integration", () => {
  it("creates callback channels for WireEndpoint carriers", async () => {
    const [client, server] = createDefaultEndpointPair();
    Caplink.expose({ invoke: async (callback: (value: number) => Promise<number>) => callback(41) }, server);
    const remote = Caplink.wrap<{ invoke(callback: (value: number) => number): Promise<number> }>(client);

    try {
      expect(await remote.invoke((value) => value + 1)).toBe(42);
    } finally {
      closeAll(client, server);
    }
  });

  it("adopts native capability ports for WireMessagePort carriers", async () => {
    const native = new MessageChannel();
    Caplink.expose({ value: 42 }, native.port1);
    const nativeRemote = Caplink.wrap<{ value: number }>(native.port2);
    const { port1: client, port2: server } = new WireMessageChannel();
    Caplink.expose({ read: async (remote: { value: Promise<number> }) => remote.value }, server);
    const remote = Caplink.wrap<{ read(remote: Caplink.Remote<{ value: number }>): Promise<number> }>(client);

    try {
      expect(await remote.read(nativeRemote)).toBe(42);
    } finally {
      closeAll(client, server, native.port1, native.port2);
    }
  });
});
