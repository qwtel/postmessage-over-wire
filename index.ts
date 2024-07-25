import { TypedEventListenerOrEventListenerObject, TypedEventTarget } from "@workers/typed-event-target";
import { Serializer, SerializerStream, Deserializer, DeserializerStream } from '@workers/v8-value-serializer';
import { streamToAsyncIter } from 'whatwg-stream-to-async-iter'

//#region Library functions
const ensureAsyncIter = <T>(stream: ReadableStream<T>): AsyncIterable<T> => Symbol.asyncIterator in stream 
  ? stream as AsyncIterable<T> 
  : streamToAsyncIter(stream);

/** It's like `pipeThrough`, but for `WritableStream`s. It ensures that every chunk written to `dest` is transformed by `transform`. */
function pipeFrom<T, U>(dest: WritableStream<U>, transform: TransformStream<T, U>): WritableStream<T> {
  const { writable, readable } = transform;
  readable.pipeTo(dest);
  return writable;
}

/** A function that traverses `data` in a "structured clone"-like fashion, replacing values that match keys in `dict` with their corresponding values. */
function structuredReplace(data: any, dict: Map<any, any>): any {
  if (dict.has(data)) return dict.get(data);
  if (Array.isArray(data)) return data.map(x => structuredReplace(x, dict));
  if (data instanceof Map) return new Map(Array.from(data, ([k, v]) => [structuredReplace(k, dict), structuredReplace(v, dict)]));
  if (data instanceof Set) return new Set(Array.from(data, x => structuredReplace(x, dict)));
  if (typeof data === 'object') { for (const k in data) data[k] = structuredReplace(data[k], dict); return data }
  return data;
}

const isReceiver = (val: unknown): val is {} => (typeof val === "object" && val !== null) || typeof val === "function";

if (!('CloseEvent' in globalThis)) {
  Object.defineProperty(globalThis, 'CloseEvent', {
    value: class CloseEvent extends Event {
      wasClean; code; reason;
      constructor(type: string, init?: CloseEventInit) {
        super(type, init);
        this.wasClean = init?.wasClean ?? false;
        this.code = init?.code ?? 0;
        this.reason = init?.reason ?? "";
      }
    }
  });
}

if (!('ErrorEvent' in globalThis)) {
  Object.defineProperty(globalThis, 'ErrorEvent', {
    value: class ErrorEvent extends Event {
      filename; lineno; colno; error;
      constructor(type: string, init?: ErrorEventInit) {
        super(type, init);
        this.filename = init?.filename ?? "";
        this.lineno = init?.lineno ?? 0;
        this.colno = init?.colno ?? 0;
        this.error = init?.error ?? null;
      }
    }
  });
}

// const loggingFinalizer = new FinalizationRegistry((heldValue: any[]) => console.log('Finalizing...', ...heldValue));
//#endregion

const MaxUint32 = 0xffff_ffff;
const MaxUint64n = 0xffff_ffff_ffff_ffffn;

function generateId() {
  const random32Upper = BigInt((Math.random() * MaxUint32) >>> 0);
  const random32Lower = BigInt((Math.random() * MaxUint32) >>> 0);
  return ((random32Upper << 32n) | random32Lower)
}

const DefaultPortId = MaxUint64n;

const Header = "pM" as const; type Header = typeof Header;

export type WireMessagePortEventMap = MessagePortEventMap & { close: CloseEvent };

type PortId = number | bigint | string

type TransferResult = readonly [id: PortId, remoteId: PortId|null];
type SerializedWithTransferResult = { serialized: Uint8Array, transferResult: TransferResult[] };

enum MsgCode { Close = 0, Message = 1, Ack = 2 }

type RPCData  = [header: Header, type: MsgCode.Message, destId: PortId, srcId: PortId,      transfer: TransferResult[], data: Uint8Array];
type RPCAck   = [header: Header, type: MsgCode.Ack,     destId: PortId, srcId: PortId,      transfer: TransferResult[], _unused1: null  ];
type RPCClose = [header: Header, type: MsgCode.Close,   destId: PortId, srcId: PortId|null, _unused0: null,             _unused1: null  ];

type RPCMessage = RPCData | RPCClose | RPCAck;

type RPCWriter = WritableStreamDefaultWriter<RPCMessage> & { identifier: any };

const tagWriter = (writer: WritableStreamDefaultWriter<RPCMessage>, identifier: any): RPCWriter => Object.assign(writer, { identifier });

const kGlobalRouteTable = Symbol.for('pM-globalRouteTable');
const globalRouteTable: Map<PortId, RPCWriter> = ((globalThis as any)[kGlobalRouteTable] ||= new Map());

export type EndpointLike = { dispatchEvent(ev: Event): void }
const _writer = new WeakMap<EndpointLike, RPCWriter>();
const _id = new WeakMap<EndpointLike, PortId>();
const _remoteId = new WeakMap<EndpointLike, PortId|null>();
const _detached = new WeakMap<EndpointLike, boolean>();
const _shipped = new WeakMap<RPCWriter, boolean>();

// FIXME: Must the unshipped event loop be a super global as well?
const unshippedStream = new TransformStream<RPCMessage, RPCMessage>();
const unshippedWriter = tagWriter(unshippedStream.writable.getWriter(), '[Unshipped]');
const unshippedPortLoop = { dispatchEvent() { throw Error("Unreachable") } } satisfies EndpointLike;
_writer.set(unshippedPortLoop, unshippedWriter);
startReceiverLoop.call(unshippedPortLoop, unshippedStream.readable);

const kMessagePortConstructor = Symbol('MessagePortConstructor');

export class WireMessageChannel implements MessageChannel {
  readonly port1;
  readonly port2;
  constructor() {
    this.port1 = new WireMessagePort(kMessagePortConstructor);
    this.port2 = new WireMessagePort(kMessagePortConstructor);
    _remoteIdSetter(this.port1, _id.get(this.port2)!);
    _remoteIdSetter(this.port2, _id.get(this.port1)!);
  }
}

export class WireMessageEvent<T = any> extends Event implements MessageEvent<T|null> {
  readonly data: T|null;
  readonly ports: WireMessagePort[];
  constructor(type: string, eventInitDict: Omit<MessageEventInit<T>, 'ports'> & { ports?: WireMessagePort[] }) {
    super(type, eventInitDict);
    this.data = eventInitDict.data ?? null;
    this.ports = eventInitDict.ports ?? [];
  }
  //#region Boilerplate
  readonly origin = ''; 
  readonly lastEventId = ''; 
  readonly source = null;
  // @ts-ignore
  initMessageEvent(type: string, bubbles?: boolean | undefined, cancelable?: boolean | undefined, data?: any, origin?: string | undefined, lastEventId?: string | undefined, source?: MessageEventSource | null | undefined, ports?: MessagePort[] | undefined): void {
    throw new Error("Method not implemented.");
  }
  //#endregion
}

function acknowledgeTransfer(this: EndpointLike, destId: PortId, srcId: PortId, transferResult: TransferResult[]) {
  if (transferResult.length > 0) {
    const writer = this instanceof WireEndpoint ? _writer.get(this)! : globalRouteTable.get(destId)!;
    // Need to send Ack if message contained transferred ports.
    // We attach a copy of the transfer results and the original port id, s.t. intermediate nodes can potentially clean up their routing tables.
    // This happens when a port was sent in the direction it came from. Note that we can only clean up routing tables after receiving Ack,
    // since in-flight messages from the other side could still arrive and need to be forwarded (returned) to avoid loss of messages.
    writer.write([Header, MsgCode.Ack, destId, srcId, transferResult, null])//.catch(console.warn);
  }
}

function dispatchAsEvent(this: EndpointLike, transferResult: TransferResult[], serialized: Uint8Array) {
  let data, ports;
  try {
    [data, ports] = deserializeWithTransfer({ serialized, transferResult });
  } catch (data) {
    return this.dispatchEvent(new WireMessageEvent('messageerror', { data }));
  }
  const event = new WireMessageEvent('message', { data, ports });
  return this.dispatchEvent(event);
}

async function startReceiverLoop(this: EndpointLike, readable: ReadableStream<RPCMessage>) {
  for await (const rpcMessage of ensureAsyncIter(readable)) {
    try {
      const [, opCode] = rpcMessage;
      switch (opCode) {
        case MsgCode.Message: {
          const [, , portId, , transferResult, buffer] = rpcMessage;

          for (const [,remoteId] of transferResult) {
            if (remoteId && !globalRouteTable.has(remoteId)) {
              // The direction to reach the other end for any port coming through, even if it's dispatched as a local event below, 
              // must be the endpoint at which it arrived at.
              globalRouteTable.set(remoteId, _writer.get(this)!);
            }
          }

          if (portId === _id.get(this)) {
            acknowledgeTransfer.call(this, DefaultPortId, DefaultPortId, transferResult);
            dispatchAsEvent.call(this, transferResult, buffer);
            continue;
          }

          // Forwarding a message
          if (globalRouteTable.has(portId)) {
            const writer = globalRouteTable.get(portId);
            if (!writer) throw Error("No writer found for portId")

            for (const [id] of transferResult) {
              // When forwarding a message, we need to update the route table for all transferred ports to point to the same direction the message went.
              globalRouteTable.set(id, writer);
            }

            await writer.write(rpcMessage);
          }
          // Note: Messages can get dropped here if a close message is traveling the other direction, which is fine.
          break;
        }
        case MsgCode.Ack: {
          const [, , portId, sourceId, transferResult] = rpcMessage;

          // XXX: Extremely sussy. What if the sourceId is the DefaultAddress??
          if (globalRouteTable.has(portId)) {
            const writer = globalRouteTable.get(portId)!;
            const backwardWriter = globalRouteTable.get(sourceId);

            for (const [id, remoteId] of transferResult) {
              // If we've previously sent the other side of the port in the same direction as this acknowledgement is coming from,
              // it is now closer to the remote port than we are, and we can delete it from our routing table.
              if (remoteId && globalRouteTable.get(remoteId) === backwardWriter) {
                globalRouteTable.delete(remoteId);
                // If the port we've just transferred also points that direction, we can delete it from our routing table as well. XXX: Chat, is this real?
                if (globalRouteTable.get(id) === backwardWriter) {
                  globalRouteTable.delete(id);
                }
              }
            }

            // Forwarding the Ack message
            await writer.write(rpcMessage);
          }

          break;
        }
        case MsgCode.Close: {
          const [, , portId, initPortId] = rpcMessage;

          const writer = globalRouteTable.get(portId);

          // It seems like it is not ok to delete the route table entry immediately, since there might be messages in flight,
          // however the source has already stopped dispatching events, so we might as well drop them where they are found.
          globalRouteTable.delete(portId);
          initPortId && globalRouteTable.delete(initPortId);

          // Forward the close message if we haven't reached the destination yet
          await writer?.write(rpcMessage);

          break;
        }
        default: {
          throw Error(`Unknown OpCode: ${opCode}`);
        }
      }
    } catch (err) {
      // TODO: what do here??
      console.error(err);
      this.dispatchEvent(new WireMessageEvent('messageerror', { data: err }));
      continue;
    }
  }
}

const getTransfer = (x?: Transferable[] | StructuredSerializeOptions) => x != null && 'transfer' in x ? x.transfer : Array.isArray(x) ? x : undefined;
const isWireMessagePort = (x: unknown): x is WireMessagePort => x instanceof WireMessagePort;

function postMessage(this: WireEndpoint|WireMessagePort, destId: PortId|null, srcId: PortId, message: any, transfer?: Transferable[] | StructuredSerializeOptions) {
  const ports = getTransfer(transfer)?.filter(isWireMessagePort) ?? [];
  if (ports.some(port => port === this)) {
    throw new DOMException('Cannot transfer source port', 'DataCloneError');
  }
  const doomed = destId != null && ports.find(port => _id.get(port) === destId);
  const { serialized, transferResult } = serializeWithTransferResult(message, ports);
  if (destId == null || doomed) return; // TODO: print warning?

  const writer = this instanceof WireEndpoint ? _writer.get(this)! : globalRouteTable.get(destId)!;

  // For each transferred port, we need to update the global routing table to point the same direction as the message went.
  for (const [id] of transferResult) {
    // The only exception are unshipped ports, which should point to the unshipped event loop instead, where messages are dispatched as local events.
    const remoteWriter = _shipped.has(writer) ? writer : unshippedWriter;
    globalRouteTable.set(id, remoteWriter);
  }

  // Keep the shipped status updated
  if (_shipped.has(writer)) {
    for (const port of ports) {
      const portWriter = globalRouteTable.get(_remoteId.get(port)!);
      portWriter && _shipped.set(portWriter, true);
    }
  }

  // FIXME: What do when write fails??
  // UPDATE: When writing fails, the stream is errored and all future writes will fail as well. There is no recovering from this.
  // In that case, we actually have to send a message in other direction to clean up routing tables along the way and error the original sender.
  writer.write([Header, MsgCode.Message, destId, srcId, transferResult, serialized])//.catch(console.warn);
}

// Temporary storage for deduplication
const serializeMemory = new Map<MessagePort, TransferResult>();

function serializeWithTransferResult(value: any, ports: WireMessagePort[]): SerializedWithTransferResult {
  try {
    const transferResult = ports.map((port) => {
      if (_detached.get(port)) throw new DOMException('Cannot transfer detached port', 'DataCloneError');
      if (serializeMemory.has(port)) throw new DOMException('Cannot transfer port more than once', 'DataCloneError');
      const id = _id.get(port)!;
      const remoteId = _remoteId.get(port)!;
      serializeMemory.set(port, [id, remoteId]);

      _detached.set(port, true);
      _remoteIdSetter(port, null);
      _writer.get(port)!.close()//.catch(console.warn); // FIXME

      return [id, remoteId] as const;
    }) ?? [];
    const serialized = new WireSerializer({ forceUtf8: true }).serialize(value);
    return { serialized, transferResult };
  } finally {
    serializeMemory.clear();
  }
}

// Temporary storage for deduplication
const deserializeMemory = new Map<PortId, WireMessagePort>();

function deserializeWithTransfer(value: SerializedWithTransferResult): [any, WireMessagePort[]] {
  try {
    const { serialized, transferResult } = value;
    const ports = transferResult.map(([id, remoteId]) => {
      const port = new WireMessagePort(kMessagePortConstructor, id, remoteId);
      deserializeMemory.set(id, port);
      return port;
    });
    const data = new WireDeserializer(serialized).deserialize();
    return [data, ports];
  } finally {
    deserializeMemory.clear();
  }
}

async function finalizeMessagePort([id, remoteId]: TransferResult) {
  if (remoteId) {
    // TODO: what do when write fails?
    await globalRouteTable.get(remoteId)?.write([Header, MsgCode.Close, remoteId, id, null, null])//.catch(console.warn)
    globalRouteTable.delete(remoteId); // ensure close op isn't sent twice
  }
  globalRouteTable.delete(id);
}

const portFinalizer = new FinalizationRegistry<TransferResult>((port: TransferResult) => {
  finalizeMessagePort(port);
});

function _remoteIdSetter(that: WireMessagePort, remoteId: PortId|null) {
  const id = _id.get(that)!;
  const currRemoteId = _remoteId.get(that);
  if (remoteId && !currRemoteId) {
    // Once we have a remoteId, we can register cleanup for the global route table
    portFinalizer.register(that, [id, remoteId], that);
    _remoteId.set(that, remoteId);
  } else if (!remoteId && currRemoteId) {
    // When the remoteId is cleared, we MUST unregister the cleanup, otherwise it will mess with the global route table
    portFinalizer.unregister(that);
    _remoteId.set(that, remoteId);
  }
}

/** Holds strong references to message ports with active `message` listeners to prevent them from being GCed. This is to match spec behavior. */
const globalNonGCedPorts = new Set<WireMessagePort>();

export class WireMessagePort extends TypedEventTarget<WireMessagePortEventMap> implements MessagePort {
  #enabled?: Promise<void>
  #readable;

  constructor(key: symbol);
  constructor(key: symbol, designatedId: PortId, remoteId: PortId|null);
  constructor(key: symbol, designatedId?: PortId, remoteId?: PortId|null) {
    if (key !== kMessagePortConstructor) throw new TypeError("Illegal constructor");

    super();

    const id = designatedId ?? generateId();
    _id.set(this, id);
    _remoteIdSetter(this, remoteId ?? null);

    const { readable, writable } = new TransformStream<RPCMessage, RPCMessage>();
    this.#readable = readable;
    const writer = tagWriter(writable.getWriter(), id);
    _writer.set(this, writer);

    _shipped.set(writer, !!remoteId); // if the port has a remote id it was shipped
    _detached.set(this, false);

    globalRouteTable.set(id, writer);
  }

  get #id() { return _id.get(this)! }
  get #remoteId() { return _remoteId.get(this) ?? null }
  get #writer() { return _writer.get(this)! }
  get #detached() { return _detached.get(this)! }

  #updateOnceListenerCount() {
    for (const [listener, { once }] of this.#messageHandlers) {
      if (once) this.#messageHandlers.delete(listener);
    }
    if (this.#messageHandlers.size === 0) {
      globalNonGCedPorts.delete(this);
    }
  }

  async #startReceiverLoop(readable: ReadableStream<RPCMessage>) {
    for await (const rpcMessage of ensureAsyncIter(readable)) {
      try {
        const [, opCode, portId] = rpcMessage;
        switch (opCode) {
          case MsgCode.Message:
            const [, , , srcId, transferResult, buffer] = rpcMessage;
            if (portId === this.#id) {
              this.#updateOnceListenerCount();
              acknowledgeTransfer.call(this, srcId, portId, transferResult);
              dispatchAsEvent.call(this, transferResult, buffer);
              continue;
            }
            throw Error("Message sent to wrong port")
          case MsgCode.Ack:
            if (portId === this.#id) continue;
            throw Error("Message sent to wrong port")
          case MsgCode.Close:
            const [, , , initPortId] = rpcMessage;
            if (portId === this.#id) {
              // No `finalizeMessagePort` here because already cleaned up in main receiver loop.
              this.#cleanup();
              this.dispatchEvent(new CloseEvent('close', { wasClean: !!initPortId }));
              continue;
            }
            throw Error("Message sent to wrong port")
          default:
            throw Error(`Unknown OpCode: ${opCode}`);
        }
      } catch (err) {
        // TODO: what do here??
        console.error(err);
        this.dispatchEvent(new WireMessageEvent('messageerror', { data: err }));
        continue;
      }
    }
  }

  #cleanup() {
    _detached.set(this, true);
    _remoteIdSetter(this, null);
    this.#messageHandlers.clear();
    globalNonGCedPorts.delete(this);
    this.#writer.close()//.catch(console.warn); // FIXME
  }

  postMessage(message: any, transfer?: Transferable[] | StructuredSerializeOptions): void {
    postMessage.call(this, this.#remoteId, this.#id, message, transfer);
  }

  start(): void {
    this.#enabled ||= this.#startReceiverLoop(this.#readable);
  }

  close(): void {
    finalizeMessagePort([this.#id, this.#remoteId]);
    this.#cleanup();
  }

  [Symbol.dispose]() {
    this.close();
  }

  static fromNative(port: MessagePort): WireMessagePort {
    const { port1: publicPort, port2: privatePort } = new WireMessageChannel();
    port.onmessage = nativeToWrite.bind(privatePort);
    privatePort.onmessage = wireToNative.bind(port);
    port.addEventListener('close', () => privatePort.close(), { once: true }); // NOTE: This is not well supported, most implementations don't fire this event.
    return publicPort;
  }

  #nativePort?: MessagePort;
  toNative(): MessagePort {
    if (this.#nativePort) return this.#nativePort;
    _detached.set(this, true);
    const { port1: publicPort, port2: privatePort } = new MessageChannel();
    this.onmessage = wireToNative.bind(privatePort)
    privatePort.onmessage = nativeToWrite.bind(this);
    this.addEventListener('close', () => privatePort.close(), { once: true });
    return this.#nativePort = publicPort;
  }

  //#region Boilerplate
  #messageHandlers: Map<EventListenerOrEventListenerObject, { once: boolean }> = new Map();
  addEventListener<K extends keyof WireMessagePortEventMap>(type: K, listener: TypedEventListenerOrEventListenerObject<WireMessagePortEventMap[K]>|null, options?: boolean|AddEventListenerOptions): void;
  addEventListener(type: string, listener: EventListenerOrEventListenerObject|null, options?: boolean|AddEventListenerOptions|undefined): void;
  addEventListener(type: any, listener: any, options?: any): void {
    super.addEventListener(type, listener, options);
    if (type === 'message' && isReceiver(listener)) {
      this.#messageHandlers.set(listener as any, { once: options?.once === true });
      globalNonGCedPorts.add(this);
    }
  }

  removeEventListener<K extends keyof WireMessagePortEventMap>(type: K, listener: TypedEventListenerOrEventListenerObject<WireMessagePortEventMap[K]>|null, options?: boolean|EventListenerOptions): void;
  removeEventListener(type: string, listener: EventListenerOrEventListenerObject | null, options?: boolean | EventListenerOptions | undefined): void;
  removeEventListener(type: any, listener: any, options?: any): void {
    super.removeEventListener(type, listener, options);
    if (type === 'message' && isReceiver(listener)) {
      this.#messageHandlers.delete(listener as any);
      if (this.#messageHandlers.size === 0) {
        globalNonGCedPorts.delete(this);
      }
    }
  }

  #onmessage: ((this: MessagePort, ev: MessageEvent<any>) => any)|null = null;
  set onmessage(handler: ((this: MessagePort, ev: MessageEvent<any>) => any)|null) { 
    if (this.#onmessage) this.removeEventListener('message', this.#onmessage);
    if (handler) this.addEventListener('message', this.#onmessage = handler.bind(this));
    this.start();
  }
  get onmessage() { return this.#onmessage }

  #onmessageerror: ((this: MessagePort, ev: MessageEvent<any>) => any)|null = null;
  set onmessageerror(handler: ((this: MessagePort, ev: MessageEvent<any>) => any)|null) { 
    if (this.#onmessageerror) this.removeEventListener('messageerror', this.#onmessageerror);
    if (handler) this.addEventListener('messageerror', this.#onmessageerror = handler.bind(this));
  }
  get onmessageerror() { return this.#onmessageerror }
  // #endregion
}

function wireToNative(this: MessagePort, { data, ports }: MessageEvent) {
  // console.log("Forwarding message to native port", data, ports.map(x => x.constructor.name))
  const portDict = new Map(ports.map(p => [p, (p as WireMessagePort).toNative()]));
  if (portDict.size) data = structuredReplace(data, portDict);
  this.postMessage(data, Array.from(portDict.values()));
}

function nativeToWrite(this: WireMessagePort, { data, ports}: MessageEvent) {
  // console.log("Forwarding message to wire port", data, ports.map(x => x.constructor.name))
  const portDict = new Map(ports.map(p => [p, WireMessagePort.fromNative(p)]));
  if (portDict.size) data = structuredReplace(data, portDict);
  this.postMessage(data, Array.from(portDict.values()));
}

export class WireEndpoint extends TypedEventTarget<WorkerEventMap> {
  constructor(
    stream: { 
      readable: ReadableStream<Uint8Array>, 
      writable: WritableStream<Uint8Array>,
    },
    identifier?: any,
  ) {
    super();

    _id.set(this, DefaultPortId); 
    _remoteId.set(this, DefaultPortId);

    const writable: WritableStream<RPCMessage> = pipeFrom(stream.writable, new SerializerStream());
    const readable: ReadableStream<RPCMessage> = stream.readable.pipeThrough(new DeserializerStream());
    const writer = tagWriter(writable.getWriter(), identifier);
    _writer.set(this, writer);

    _shipped.set(writer, true); // Endpoints are "shipped" by definition.

    startReceiverLoop.call(this, readable).catch(error => {
      this.dispatchEvent(new ErrorEvent('error', { error }));
    });
  }

  get #writer() { return _writer.get(this)! }

  postMessage(message: any, transfer?: StructuredSerializeOptions | Transferable[]): void {
    postMessage.call(this, DefaultPortId, DefaultPortId, message, transfer);
  }

  terminate(): void {
    // If a port is referencing us as a gateway, we have to forcefully close the port:
    // XXX: What about the other direction??
    for (const [portId, writer] of globalRouteTable.entries()) {
      if (writer === this.#writer) {
        // The writer might already be in an errored state, nothing we can do about that:
        this.#writer.write([Header, MsgCode.Close, portId, null, null, null]).catch(() => {});
        globalRouteTable.delete(portId);
      }
    }
    // Might already be closed, nothing we can do about that:
    this.#writer.close().catch(() => {});
  }

  [Symbol.dispose]() {
    this.terminate();
  }

  //#region Boilerplate
  #onmessage: ((this: WireEndpoint, ev: MessageEvent<any>) => any)|null = null;
  set onmessage(handler: ((this: WireEndpoint, ev: MessageEvent<any>) => any)|null) { 
    if (this.#onmessage) this.removeEventListener('message', this.#onmessage);
    if (handler) this.addEventListener('message', this.#onmessage = handler.bind(this));
  }
  get onmessage() { return this.#onmessage }

  #onmessageerror: ((this: WireEndpoint, ev: MessageEvent<any>) => any)|null = null;
  set onmessageerror(handler: ((this: WireEndpoint, ev: MessageEvent<any>) => any)|null) { 
    if (this.#onmessageerror) this.removeEventListener('messageerror', this.#onmessageerror);
    if (handler) this.addEventListener('messageerror', this.#onmessageerror = handler.bind(this))
  }
  get onmessageerror() { return this.#onmessageerror }

  #onerror: ((this: WireEndpoint, ev: ErrorEvent) => any)|null = null;
  set onerror(handler: ((this: WireEndpoint, ev: ErrorEvent) => any)|null) { 
    if (this.#onerror) this.removeEventListener('error', this.#onerror);
    if (handler) this.addEventListener('error', this.#onerror = handler.bind(this))
  }
  get onerror() { return this.#onerror }
  // #endregion
}

const kMessagePortTag = 77;

class WireSerializer extends Serializer {
  get hasCustomHostObjects() { return true }
  isHostObject(object: unknown) {
    return object instanceof WireMessagePort;
  }
  writeHostObject(object: object) {
    if (object instanceof WireMessagePort) {
      this.serializer.writeUint32(kMessagePortTag); // tag
      const transferResult = serializeMemory.get(object);
      return !!transferResult && this.serializer.writeObject(transferResult);
    }
    return super.writeHostObject(object);
  }
}

class WireDeserializer extends Deserializer {
  readHostObjectForTag(tag: number) {
    if (tag === kMessagePortTag) {
      const value = this.deserializer.readObjectWrapper() as TransferResult|null;
      const port = value && deserializeMemory.get(value[0]);
      return port ?? null;
    }
    return super.readHostObjectForTag(tag);
  }
};

/** @deprecated For testing only! */
export const __internals = {
  globalRouteTable,
  _writer,
  _id,
  _remoteId,
  _detached,
  _shipped,
  unshippedStream,
  unshippedWriter,
  unshippedPortLoop,
};
