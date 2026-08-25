# PostMessage over Wire

An implementation of the Web Message Passing API over a readable/writable stream pair. 

The library provides `WireMessageChannel` and `WireMessagePort`, which follow the familiar `MessageChannel`/`MessagePort` API, plus `WireEndpoint`, which carries messages and transferred ports across a stream connection.

**Status**: Experimental.

## Example

Each `WireEndpoint` receives one duplex stream end: its `writable` sends bytes to the other endpoint, and its `readable` receives bytes written by the other endpoint.

```ts
const endpointA = new WireEndpoint(streamPairA);
const endpointB = new WireEndpoint(streamPairB);
const { port1, port2 } = new WireMessageChannel();

endpointB.addEventListener("message", ({ ports: [remotePort] }) => {
  remotePort.postMessage("hello through the moved port");
});

port2.onmessage = ({ data }) => console.log(data);
endpointA.postMessage("take this port", [port1]);
```

As with native Web Messaging, a successful transfer immediately detaches the sending wrapper. Messages already queued for the port, and messages sent while it is moving, follow the transferred endpoint.

## How it works

Each port end has a stable, random address. A routing table maps that address either to local port state or to the next `WireEndpoint`. When a port moves across a connection, its address, peer address, and queued inbox move with it while the old location becomes a forwarding route.

All frames on a link share one ordered write chain. Consequently, the frame introducing a transferred port is written before later messages addressed to it. A small movement acknowledgement allows obsolete forwarding routes to be removed after the destination has installed the port.

## Transport requirements

`WireEndpoint` accepts:

```ts
{
  readable: ReadableStream<Uint8Array>;
  writable: WritableStream<Uint8Array>;
}
```

The transport must be ordered and reliable, and must expose EOF, errors, and cancellation. The protocol does not add encryption, peer authentication, version negotiation, or heartbeats for otherwise undetectable half-open connections. Both peers should therefore run a compatible protocol version over a trusted or separately secured transport.

## Caplink integration

Importing `comlinked.ts` installs the adapter symbols used by [Caplink](https://github.com/qwtel/caplink), allowing Caplink callbacks and capability endpoints to use wire ports, including conversion to and from native `MessagePort`s.

## Wire format

The public surface intentionally resembles Web Messaging, but the wire format is implementation-specific and depends on a Node/V8-compatible serializer.

The wire format is specifically tied to the encoding and host-object hooks provided by [@workers/v8-value-serializer/v8](https://jsr.io/@workers/v8-value-serializer), a JavaScript implementation of Node/V8's serialization format. Peers may use different serializer implementations only when those implementations are byte-compatible and expose compatible `DefaultSerializer`/`DefaultDeserializer` behavior.

For a Node-targeted build, the JavaScript implementation can be replaced with Node's native V8 serializer at bundle time. For example, with esbuild:

```ts
await esbuild.build({
  // ...
  platform: "node",
  alias: {
    "@workers/v8-value-serializer/v8": "node:v8",
  },
});
```

The module name alone is not evidence of wire compatibility. In particular, as of this writing, Bun provides a `node:v8` compatibility module, but its serialization encoding does not follow Node.js's V8 serialization format.

