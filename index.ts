/**
 * Web Messaging over an ordered, full-duplex pair of byte streams.
 *
 * This module implements `MessageChannel`-style movable port ends. A
 * `WireMessagePort` can be sent through another port or through a
 * `WireEndpoint`; its entanglement, queued messages, and future traffic move
 * with it. `WireEndpoint` is the carrier for a stream pair, not itself a
 * routed port end.
 *
 * ## The model
 *
 * Each port end has a stable address and a peer address. By default an address
 * is a uniformly random 128-bit bigint produced by `crypto.getRandomValues`,
 * which avoids coordinating allocation between contexts and makes collisions
 * negligible. Applications may inject another `generateId`; correctness only
 * requires its results to be unique across the connected routing domain.
 *
 * The `WireMessagePort` object is the transferable capability. Its address is
 * only the protocol's opaque routing label, not a Caplink-style application
 * capability ID or an authentication boundary. Random addresses also make
 * blind targeting impractical, but a stream peer still has to be trusted or
 * authenticated separately.
 *
 * A context contains one next-hop table. There are only two kinds of route:
 *
 * ```text
 * address ── local ──> port state { peer, inbox, owner }
 * address ── link  ──> ordered writer for another context
 * ```
 *
 * A newly-created local channel therefore looks like this:
 *
 * ```text
 * route table
 *
 *   A ──local──> state A { peer: B, inbox: [] }
 *   B ──local──> state B { peer: A, inbox: [] }
 *
 *   wrapper A                                   wrapper B
 *       │                                           │
 *       └── owns state A     A <──entangled──> B    └── owns state B
 * ```
 *
 * Posting on A looks up A's peer, B, and follows B's current route. The port
 * state owns the inbox independently of its JavaScript wrapper. This is what
 * makes messages queued before or during a move follow the port naturally.
 *
 * ## Moving a port
 *
 * A local move detaches the old wrapper and gives the same state to the
 * receiving wrapper. No protocol or forwarding route is needed.
 *
 * A move across a link performs three operations synchronously, before the
 * transfer frame is queued:
 *
 * 1. Detach the old wrapper.
 * 2. Put the port's state and complete inbox in the transfer frame.
 * 3. Change the local route for that address to the outbound link.
 *
 * ```text
 * before moving B                 after B arrives
 *
 * context L                       context L          context R
 * A ──local──> state A            A ──local──> A     A ──link──> L
 * B ──local──> state B            B ──link───> R     B ──local─> state B
 *
 *                    transfer B { inbox... }
 *              L =============================> R
 * ```
 *
 * Because every write on a link shares one promise chain, the transfer frame
 * precedes messages which follow the newly-installed route. At intermediate
 * contexts, the same rule yields ordinary next-hop forwarding:
 *
 * ```text
 * A -> B:
 * state A --peer B--> [B: link 0->1] --> [B: link 1->2] --> [B: local state B]
 *
 * B -> A:
 * state B --peer A--> [A: link 2->1] --> [A: link 1->0] --> [A: local state A]
 * ```
 *
 * A move carries a random token. Once the destination has installed the
 * state, a small `moved` frame retraces the path:
 *
 * ```text
 * state + move token  ───────────────────────────────>
 * moved(token)        <───────────────────────────────
 *                         prune stale forwarding routes
 * ```
 *
 * This is only a safe route-cleanup barrier. It is not acknowledgement that a
 * posted application message was handled, and it does not change the
 * synchronous `postMessage(): void` API.
 *
 * ## The byte stream
 *
 * Stream chunk boundaries have no protocol meaning. Frames are length-prefixed
 * and may be split or combined by the transport:
 *
 * ```text
 * +----------------------+--------------------------+
 * | uint32 LE byte count | V8-serialized frame body |
 * +----------------------+--------------------------+
 * ```
 *
 * Reads reassemble complete frames; EOF with a partial frame is an error.
 * Writes are serialized and wait for `writer.ready`. The stream pair must be
 * ordered and reliable. EOF, stream errors, and propagated cancellation tear
 * down only routes using the failed link. A transport which hides a half-open
 * failure would require its own heartbeat; this protocol does not add one.
 *
 * ## Web-platform behavior
 *
 * Payloads use V8 structured serialization. `ArrayBuffer` transfer uses
 * `structuredClone`, delivery is scheduled as a MessageChannel task, and
 * ownership uses `WeakRef` plus `FinalizationRegistry` when available. The
 * public classes provide the Web Messaging surface; routing and movement are
 * plain records and procedures below it.
 *
 * @module
 */
import {
  DefaultDeserializer,
  DefaultSerializer,
  deserialize as deserializeFrame,
  serialize as serializeFrame,
} from "@workers/v8-value-serializer/v8";

export type PortId = number | bigint | string;
export type EndpointLike = { dispatchEvent(event: Event): void };
export type WireMessagePortEventMap = MessagePortEventMap & { close: CloseEvent; error: ErrorEvent };

type HeldPort = readonly [id: PortId, peer: PortId];
type DuplexStream = { readable: ReadableStream<Uint8Array>; writable: WritableStream<Uint8Array> };

type LocalEnvelope =
  | { type: "message"; data: Uint8Array; ports: PortState[] }
  | { type: "close"; clean: boolean };

type ShippedPort = { id: PortId; peer: PortId; inbox: ShippedEnvelope[] };

type ShippedEnvelope =
  | { type: "message"; data: Uint8Array; ports: ShippedPort[] }
  | { type: "close"; clean: boolean };

type MessageFrame = { type: "message"; to: PortId | null; data: Uint8Array; ports: ShippedPort[]; move: string | null };

type Frame =
  | MessageFrame
  | { type: "close"; to: PortId; from: PortId; clean: boolean }
  | { type: "moved"; move: string };

type LocalRoute = { type: "local"; port: PortState; peer: PortId };
type LinkRoute = { type: "link"; link: Link; peer: PortId };
type Route = LocalRoute | LinkRoute;

type PendingMove = { back: Link | null; forward: Link; ports: HeldPort[] };

const kPending = Symbol("pendingMoves");

export type WireContext = {
  routeTable: Map<PortId, unknown>;
  nonGCedPorts: Set<WireMessagePort>;
  generateId(): PortId;
  finalizer?: FinalizationRegistry<HeldPort> | null;
};

type InternalContext = WireContext & { [kPending]: Map<string, PendingMove> };

type PortState = {
  context: WireContext; id: PortId; peer: PortId; inbox: LocalEnvelope[];
  owner: WeakRef<WireMessagePort> | null; scheduled: boolean; closed: boolean;
};

type Link = {
  context: WireContext; endpoint: WireEndpoint; reader: ReadableStreamDefaultReader<Uint8Array>;
  writer: WritableStreamDefaultWriter<Uint8Array>; writes: Promise<void>;
  status: "open" | "closing" | "closed"; writerDone?: Promise<void>;
};

const emptyBuffer = new ArrayBuffer(0);
const kConstruct = Symbol("constructWireMessagePort");
const kState = Symbol("state");
const kDetach = Symbol("detach");
const kSchedule = Symbol("schedule");
const kDispose: symbol = (Symbol as any).dispose ?? Symbol.for("Symbol.dispose");
const replacementTag = 77;
const globalRoutes = Symbol.for("postmessage-over-wire.routes.v2");

const routes = (context: WireContext) => context.routeTable as Map<PortId, Route>;
const pendingMoves = (context: WireContext) => (context as InternalContext)[kPending];
const ownerOf = (state: PortState) => state.owner?.deref();
const dataCloneError = (message: string) => new DOMException(message, "DataCloneError");
const invalidStateError = (message: string) => new DOMException(message, "InvalidStateError");

export const isAbortError = (error: unknown): error is Error => error instanceof Error && error.name === "AbortError";

function randomId(): bigint {
  const words = crypto.getRandomValues(new Uint32Array(4));
  return words.reduce((id, word) => (id << 32n) | BigInt(word), 0n);
}

const moveId = () => crypto.randomUUID?.() ?? randomId().toString(16);

export function createWireContext(options: {
  routeTable?: Map<PortId, unknown>;
  generateId?: () => PortId;
  finalizer?: FinalizationRegistry<HeldPort> | null;
} = {}): WireContext {
  const context: InternalContext = {
    routeTable: options.routeTable ?? new Map(),
    nonGCedPorts: new Set<WireMessagePort>(),
    generateId: options.generateId ?? randomId,
    finalizer: null,
    [kPending]: new Map<string, PendingMove>(),
  };

  context.finalizer = options.finalizer === undefined && typeof FinalizationRegistry === "function"
    ? new FinalizationRegistry<HeldPort>((held) => closeAddress(context, held[0], held[1], true))
    : options.finalizer ?? null;
  return context;
}

const defaultContext = createWireContext({
  routeTable: ((globalThis as any)[globalRoutes] ??= new Map()),
});

class PayloadSerializer extends DefaultSerializer {
  constructor(private readonly replacements: Map<object, number>) { super(); }

  _writeHostObject(object: object): void {
    const replacement = this.replacements.get(object);
    if (replacement !== undefined) {
      this.writeUint32(replacementTag);
      this.writeUint32(replacement);
    } else if (object instanceof WireMessagePort) {
      throw dataCloneError("A MessagePort in the message must be listed in transfer");
    } else {
      super._writeHostObject(object as ArrayBufferView);
    }
  }

  serialize(value: unknown): Uint8Array { this.writeHeader(); this.writeValue(value); return this.releaseBuffer(); }

  get _getDataCloneError(): typeof Error { return Error; }
}

class PayloadDeserializer extends DefaultDeserializer {
  #tag: number | null = null;

  constructor(buffer: Uint8Array, private readonly replacements: unknown[]) { super(buffer); }

  _readHostObject(): unknown {
    const tag = super.readUint32();
    if (tag === replacementTag) return this.replacements[super.readUint32()];
    this.#tag = tag;
    return super._readHostObject();
  }

  readUint32(): number {
    if (this.#tag === null) return super.readUint32();
    const tag = this.#tag;
    this.#tag = null;
    return tag;
  }

  deserialize(): unknown { this.readHeader(); return this.readValue(); }
}

function encodePayload(value: unknown, replacements: object[]): Uint8Array {
  return new PayloadSerializer(new Map(replacements.map((object, index) => [object, index]))).serialize(value);
}

const decodePayload = (data: Uint8Array, replacements: unknown[]): any => new PayloadDeserializer(data, replacements).deserialize();

function replaceReferences(value: any, replacements: Map<object, unknown>, seen = new WeakSet<object>()): any {
  if ((typeof value !== "object" && typeof value !== "function") || value === null) return value;
  if (replacements.has(value)) return replacements.get(value);
  if (seen.has(value)) return value;
  seen.add(value);

  if (value instanceof Map) {
    const entries = [...value].map(([key, item]) => [
      replaceReferences(key, replacements, seen),
      replaceReferences(item, replacements, seen),
    ]);
    value.clear();
    for (const [key, item] of entries) value.set(key, item);
  } else if (value instanceof Set) {
    const items = [...value].map((item) => replaceReferences(item, replacements, seen));
    value.clear();
    for (const item of items) value.add(item);
  } else if (!ArrayBuffer.isView(value) && !(value instanceof ArrayBuffer) && !(value instanceof Date) && !(value instanceof RegExp)) {
    for (const key of Object.keys(value)) value[key] = replaceReferences(value[key], replacements, seen);
  }
  return value;
}

function encodeFrame(frame: Frame): Uint8Array {
  const body = serializeFrame(frame);
  const result = new Uint8Array(body.byteLength + 4);
  new DataView(result.buffer).setUint32(0, body.byteLength, true);
  result.set(body, 4);
  return result;
}

function append(left: Uint8Array, right: Uint8Array): Uint8Array {
  if (left.byteLength === 0) return right;
  const joined = new Uint8Array(left.byteLength + right.byteLength);
  joined.set(left);
  joined.set(right, left.byteLength);
  return joined;
}

const tasks: Array<() => void> = [];
const taskChannel = typeof globalThis.MessageChannel === "function" ? new globalThis.MessageChannel() : null;
if (taskChannel) {
  taskChannel.port1.onmessage = () => tasks.shift()?.();
  taskChannel.port1.start();
  (taskChannel.port1 as any).unref?.();
  (taskChannel.port2 as any).unref?.();
}

function postTask(task: () => void): void {
  if (!taskChannel) {
    setTimeout(task, 0);
    return;
  }
  tasks.push(task);
  taskChannel.port2.postMessage(null);
}

type Listener = EventListenerOrEventListenerObject;
type TypedListener<Target, EventType extends Event> =
  | ((this: Target, event: EventType) => any)
  | { handleEvent(event: EventType): any };
type ListenerRecord = {
  type: string;
  listener: Listener;
  capture: boolean;
  wrapped: EventListener;
  signal?: AbortSignal;
  abort?: () => void;
};

function eventFacade(owner: EventTarget, changed: (records: ListenerRecord[]) => void) {
  const target = new EventTarget();
  const records: ListenerRecord[] = [];
  let dispatching = false;

  const removeRecord = (record: ListenerRecord) => {
    const index = records.indexOf(record);
    if (index < 0) return;
    records.splice(index, 1);
    target.removeEventListener(record.type, record.wrapped, record.capture);
    if (record.signal && record.abort) record.signal.removeEventListener("abort", record.abort);
    changed(records);
  };

  return {
    add(type: string, listener: Listener | null, options?: boolean | AddEventListenerOptions) {
      if (!listener) return;
      const capture = typeof options === "boolean" ? options : !!options?.capture;
      const signal = typeof options === "object" ? options.signal : undefined;
      if (signal?.aborted || records.some((item) => item.type === type && item.listener === listener && item.capture === capture)) return;

      const record = {} as ListenerRecord;
      record.type = type;
      record.listener = listener;
      record.capture = capture;
      record.signal = signal;
      record.wrapped = (event) => {
        if (typeof options === "object" && options.once) removeRecord(record);
        if (typeof listener === "function") listener.call(owner, event);
        else listener.handleEvent(event);
      };
      record.abort = signal ? () => removeRecord(record) : undefined;
      records.push(record);
      target.addEventListener(type, record.wrapped, capture);
      if (signal && record.abort) signal.addEventListener("abort", record.abort, { once: true });
      changed(records);
    },
    remove(type: string, listener: Listener | null, options?: boolean | EventListenerOptions) {
      if (!listener) return;
      const capture = typeof options === "boolean" ? options : !!options?.capture;
      const record = records.find((item) => item.type === type && item.listener === listener && item.capture === capture);
      if (record) removeRecord(record);
    },
    dispatch(event: Event) {
      Object.defineProperties(event, {
        target: { configurable: true, get: () => owner },
        currentTarget: { configurable: true, get: () => dispatching ? owner : null },
      });
      dispatching = true;
      try {
        return target.dispatchEvent(event);
      } finally {
        dispatching = false;
      }
    },
    clear() {
      for (const record of [...records]) removeRecord(record);
    },
  };
}

function closeEvent(clean: boolean): CloseEvent {
  if (typeof CloseEvent === "function") return new CloseEvent("close", { wasClean: clean });
  return Object.assign(new Event("close"), { wasClean: clean, code: 0, reason: "" }) as CloseEvent;
}

function errorEvent(error: unknown): ErrorEvent {
  if (typeof ErrorEvent === "function") return new ErrorEvent("error", { error });
  return Object.assign(new Event("error"), { error, message: String(error), filename: "", lineno: 0, colno: 0 }) as ErrorEvent;
}

type MessageEventShape<T> = Omit<MessageEvent<T>, "ports" | "prototype">;

export class WireMessageEvent<T = any> extends Event implements MessageEventShape<T | null> {
  readonly data: T | null;
  readonly ports: WireMessagePort[];
  readonly origin: string;
  readonly lastEventId: string;
  readonly source: MessageEventSource | null;

  constructor(type: string, init: Omit<MessageEventInit<T>, "ports"> & { ports?: WireMessagePort[] } = {}) {
    super(type, init);
    this.data = init.data ?? null;
    this.ports = init.ports ?? [];
    this.origin = init.origin ?? "";
    this.lastEventId = init.lastEventId ?? "";
    this.source = init.source ?? null;
  }

  initMessageEvent(
    _type: string, _bubbles?: boolean, _cancelable?: boolean, _data?: any,
    _origin?: string, _lastEventId?: string, _source?: MessageEventSource | null,
    _ports?: MessagePort[],
  ): void {
    throw new Error("Cannot reinitialize WireMessageEvent");
  }
}

function localState(context: WireContext, id: PortId, peer: PortId, inbox: LocalEnvelope[] = []): PortState {
  const state: PortState = { context, id, peer, inbox, owner: null, scheduled: false, closed: inbox.some((item) => item.type === "close") };
  routes(context).set(id, { type: "local", port: state, peer });
  return state;
}

const attach = (state: PortState) => ownerOf(state) ?? new WireMessagePort(kConstruct, state);

function enqueue(state: PortState, envelope: LocalEnvelope): void {
  state.inbox.push(envelope);
  if (envelope.type === "close") state.closed = true;
  ownerOf(state)?.[kSchedule]();
}

function shippedPairs(ports: ShippedPort[], result: HeldPort[] = []): HeldPort[] {
  for (const port of ports) {
    result.push([port.id, port.peer]);
    for (const envelope of port.inbox) if (envelope.type === "message") shippedPairs(envelope.ports, result);
  }
  return result;
}

function ship(state: PortState, link: Link): ShippedPort {
  const inbox = state.inbox.map<ShippedEnvelope>((envelope) => envelope.type === "close"
    ? envelope
    : { type: "message", data: envelope.data, ports: envelope.ports.map((port) => ship(port, link)) });
  state.inbox.length = 0;
  routes(state.context).set(state.id, { type: "link", link, peer: state.peer });
  return { id: state.id, peer: state.peer, inbox };
}

function pointShipped(context: WireContext, ports: ShippedPort[], link: Link): void {
  for (const port of ports) {
    routes(context).set(port.id, { type: "link", link, peer: port.peer });
    for (const envelope of port.inbox) if (envelope.type === "message") pointShipped(context, envelope.ports, link);
  }
}

function learnPeers(context: WireContext, ports: ShippedPort[], incoming: Link): void {
  for (const port of ports) {
    if (!routes(context).has(port.peer)) routes(context).set(port.peer, { type: "link", link: incoming, peer: port.id });
    for (const envelope of port.inbox) if (envelope.type === "message") learnPeers(context, envelope.ports, incoming);
  }
}

function importShipped(context: WireContext, port: ShippedPort, incoming: Link): PortState {
  const inbox = port.inbox.map<LocalEnvelope>((envelope) => envelope.type === "close"
    ? envelope
    : { type: "message", data: envelope.data, ports: envelope.ports.map((nested) => importShipped(context, nested, incoming)) });
  return localState(context, port.id, port.peer, inbox);
}

const transferList = (value?: Transferable[] | StructuredSerializeOptions) => Array.isArray(value) ? value : value?.transfer ?? [];

function prepareMessage(
  context: WireContext,
  source: PortId | null,
  destination: PortId | null,
  destinationRoute: Route,
  value: unknown,
  transfer?: Transferable[] | StructuredSerializeOptions,
): { data: Uint8Array; local: PortState[]; shipped: ShippedPort[] } {
  const items = transferList(transfer);
  if (new Set(items).size !== items.length) throw dataCloneError("Transfer list contains duplicate values");

  const ports: WireMessagePort[] = [];
  const buffers: ArrayBuffer[] = [];
  for (const item of items) {
    if (item instanceof WireMessagePort) {
      const state = item[kState]();
      if (!item.transferable || state.context !== context || state.closed || ownerOf(state) !== item) {
        throw dataCloneError("Cannot transfer this MessagePort");
      }
      if (state.id === source) throw dataCloneError("Cannot transfer source port");
      if (state.id === destination) throw dataCloneError("Cannot transfer destination port");
      ports.push(item);
    } else if (item instanceof ArrayBuffer) {
      buffers.push(item);
    } else {
      throw dataCloneError("Value is not transferable");
    }
  }

  const data = encodePayload(value, ports);
  if (buffers.length) structuredClone(null, { transfer: buffers });

  const states = ports.map((port) => port[kDetach]());
  if (destinationRoute.type === "local") return { data, local: states, shipped: [] };
  return { data, local: [], shipped: states.map((state) => ship(state, destinationRoute.link)) };
}

function registerMove(context: WireContext, back: Link | null, forward: Link, frame: MessageFrame): void {
  if (!frame.move) return;
  pendingMoves(context).set(frame.move, { back, forward, ports: shippedPairs(frame.ports) });
}

function pruneMove(context: WireContext, move: PendingMove): void {
  const table = routes(context);
  for (const [id, peer] of move.ports) {
    const route = table.get(id);
    const peerRoute = table.get(peer);
    if (route?.type === "link" && peerRoute?.type === "link" && route.link === move.forward && peerRoute.link === move.forward) {
      table.delete(id);
      table.delete(peer);
    }
  }
}

function acknowledge(link: Link, move: string | null): void {
  if (move) void writeFrame(link, { type: "moved", move }).catch(() => {});
}

function handleMoved(link: Link, token: string): void {
  const pending = pendingMoves(link.context).get(token);
  if (!pending || pending.forward !== link) return;
  pendingMoves(link.context).delete(token);
  const forwarded = pending.back ? writeFrame(pending.back, { type: "moved", move: token }) : Promise.resolve();
  void forwarded.then(() => pruneMove(link.context, pending)).catch(() => {});
}

function sendPrepared(
  context: WireContext,
  destination: PortId | null,
  route: Route,
  prepared: ReturnType<typeof prepareMessage>,
  report: EventTarget,
): void {
  if (route.type === "local") {
    enqueue(route.port, { type: "message", data: prepared.data, ports: prepared.local });
    return;
  }

  const token = prepared.shipped.length ? moveId() : null;
  const frame: MessageFrame = { type: "message", to: destination, data: prepared.data, ports: prepared.shipped, move: token };
  registerMove(context, null, route.link, frame);
  void writeFrame(route.link, frame).catch((error) => report.dispatchEvent(new WireMessageEvent("messageerror", { data: error })));
}

function postPort(port: WireMessagePort, value: unknown, transfer?: Transferable[] | StructuredSerializeOptions): void {
  const state = port[kState]();
  if (state.closed || ownerOf(state) !== port) throw invalidStateError("Port is not entangled");
  const route = routes(state.context).get(state.peer);
  if (!route) throw invalidStateError("Port is not entangled");
  sendPrepared(state.context, state.peer, route, prepareMessage(state.context, state.id, state.peer, route, value, transfer), port);
}

function dispatchEndpoint(endpoint: WireEndpoint, data: Uint8Array, states: PortState[]): void {
  postTask(() => {
    const ports = states.map(attach);
    try {
      endpoint.dispatchEvent(new WireMessageEvent("message", { data: decodePayload(data, ports), ports }));
    } catch (error) {
      endpoint.dispatchEvent(new WireMessageEvent("messageerror", { data: error }));
    }
  });
}

function receiveMessage(link: Link, frame: MessageFrame): void {
  const context = link.context;
  learnPeers(context, frame.ports, link);

  if (frame.to === null) {
    const states = frame.ports.map((port) => importShipped(context, port, link));
    acknowledge(link, frame.move);
    dispatchEndpoint(link.endpoint, frame.data, states);
    return;
  }

  const route = routes(context).get(frame.to);
  if (!route) return;
  if (route.type === "local") {
    const states = frame.ports.map((port) => importShipped(context, port, link));
    acknowledge(link, frame.move);
    enqueue(route.port, { type: "message", data: frame.data, ports: states });
  } else {
    pointShipped(context, frame.ports, route.link);
    registerMove(context, link, route.link, frame);
    void writeFrame(route.link, frame).catch(() => {});
  }
}

function receiveClose(link: Link, frame: Extract<Frame, { type: "close" }>): void {
  const table = routes(link.context);
  const route = table.get(frame.to);
  table.delete(frame.to);
  table.delete(frame.from);
  if (route?.type === "local") enqueue(route.port, { type: "close", clean: frame.clean });
  else if (route?.type === "link") void writeFrame(route.link, frame).catch(() => {});
}

const isFrame = (value: unknown): value is Frame => !!value && typeof value === "object"
  && ["message", "close", "moved"].includes((value as any).type);

function receiveFrame(link: Link, value: unknown): void {
  if (!isFrame(value)) throw new Error("Malformed wire frame");
  if (value.type === "message") receiveMessage(link, value);
  else if (value.type === "close") receiveClose(link, value);
  else handleMoved(link, value.move);
}

async function readLink(link: Link): Promise<void> {
  let buffered: Uint8Array<ArrayBufferLike> = new Uint8Array();
  try {
    while (true) {
      const { done, value } = await link.reader.read();
      if (done) {
        if (buffered.byteLength) throw new Error("Truncated wire frame");
        disconnect(link, false, false);
        return;
      }
      buffered = append(buffered, value);
      while (buffered.byteLength >= 4) {
        const length = new DataView(buffered.buffer, buffered.byteOffset, buffered.byteLength).getUint32(0, true);
        if (buffered.byteLength < length + 4) break;
        const body = buffered.slice(4, length + 4);
        buffered = buffered.slice(length + 4);
        receiveFrame(link, deserializeFrame(body));
      }
    }
  } catch (error) {
    if (link.status === "open" && !isAbortError(error)) link.endpoint.dispatchEvent(errorEvent(error));
    disconnect(link, false, false);
  }
}

function writeFrame(link: Link, frame: Frame): Promise<void> {
  if (link.status !== "open") return Promise.reject(new Error("Transport is closed"));
  const bytes = encodeFrame(frame);
  const write = link.writes.then(async () => {
    await link.writer.ready;
    await link.writer.write(bytes);
  });
  link.writes = write.catch((error) => disconnect(link, false, false, error));
  return write;
}

const finishWriter = (link: Link) => link.writerDone ??= link.writes.catch(() => {}).then(() => link.writer.close()).catch(() => {});

function notifyRoute(route: Route | undefined, to: PortId, from: PortId, clean: boolean): void {
  if (route?.type === "local") enqueue(route.port, { type: "close", clean });
  else if (route?.type === "link") void writeFrame(route.link, { type: "close", to, from, clean }).catch(() => {});
}

function disconnect(link: Link, clean: boolean, notifyRemote: boolean, _error?: unknown): void {
  if (link.status !== "open") return;
  const table = routes(link.context);
  const lost = [...table].filter(([, route]) => route.type === "link" && route.link === link);
  const handled = new Set<PortId>();

  for (const [id, route] of lost) {
    if (handled.has(id)) continue;
    handled.add(id);
    handled.add(route.peer);
    const peerRoute = table.get(route.peer);
    table.delete(id);
    table.delete(route.peer);
    if (peerRoute?.type === "link" && peerRoute.link === link) continue;
    if (notifyRemote) void writeFrame(link, { type: "close", to: id, from: route.peer, clean }).catch(() => {});
    notifyRoute(peerRoute, route.peer, id, clean);
  }

  for (const [token, move] of pendingMoves(link.context)) {
    if (move.back === link || move.forward === link) pendingMoves(link.context).delete(token);
  }

  link.status = "closing";
  void finishWriter(link).finally(() => {
    link.status = "closed";
    void link.reader.cancel().catch(() => {});
  });
}

function closeAddress(context: WireContext, id: PortId, peer: PortId, clean: boolean): void {
  const table = routes(context);
  const route = table.get(peer);
  table.delete(id);
  table.delete(peer);
  notifyRoute(route, peer, id, clean);
}

function transcodeToNative(event: MessageEvent, post: (data: unknown, ports: MessagePort[]) => void): void {
  const wirePorts = event.ports as WireMessagePort[];
  const nativePorts = wirePorts.map((port) => port.toNative());
  post(replaceReferences(event.data, new Map(wirePorts.map((port, index) => [port, nativePorts[index]]))), nativePorts);
}

function transcodeFromNative(event: MessageEvent, context: WireContext, post: (data: unknown, ports: WireMessagePort[]) => void): void {
  const nativePorts = [...event.ports];
  const wirePorts = nativePorts.map((port) => WireMessagePort.fromNative(port, context));
  post(replaceReferences(event.data, new Map(nativePorts.map((port, index) => [port, wirePorts[index]]))), wirePorts);
}

function setHandler<T extends (this: any, event: any) => any>(target: EventTarget, type: string, old: T | null, value: T | null): T | null {
  if (old) target.removeEventListener(type, old as EventListener);
  if (value) target.addEventListener(type, value as EventListener);
  return value;
}

export class WireMessagePort extends DataView<ArrayBuffer> implements MessagePort {
  #state: PortState;
  #status: "active" | "closed" | "detached" = "active";
  #events: ReturnType<typeof eventFacade>;
  #started = false;
  #onmessage: ((this: MessagePort, event: MessageEvent) => any) | null = null;
  #onmessageerror: ((this: MessagePort, event: MessageEvent) => any) | null = null;
  #native?: MessagePort;
  transferable = true;

  constructor(key: symbol, state?: PortState) {
    if (key !== kConstruct || !state) throw new TypeError("Illegal constructor");
    super(emptyBuffer);
    this.#state = state;
    this.#events = eventFacade(this as unknown as EventTarget, (listeners) => {
      if (this.#status === "active" && listeners.some((item) => item.type === "message")) state.context.nonGCedPorts.add(this);
      else state.context.nonGCedPorts.delete(this);
    });
    state.owner = new WeakRef(this);
    state.context.finalizer?.register(this, [state.id, state.peer], this);
  }

  [kState](): PortState { return this.#state; }

  [kDetach](): PortState {
    if (this.#status !== "active") throw dataCloneError("Cannot transfer detached port");
    this.#status = "detached";
    this.#events.clear();
    this.#state.context.nonGCedPorts.delete(this);
    this.#state.context.finalizer?.unregister(this);
    if (ownerOf(this.#state) === this) this.#state.owner = null;
    return this.#state;
  }

  [kSchedule](): void {
    const state = this.#state;
    if (!this.#started || state.scheduled || state.inbox.length === 0) return;
    state.scheduled = true;
    postTask(() => {
      state.scheduled = false;
      if (ownerOf(state) !== this || this.#status === "detached") {
        ownerOf(state)?.[kSchedule]();
        return;
      }
      const envelope = state.inbox.shift();
      if (!envelope) return;
      if (envelope.type === "close") {
        this.#status = "closed";
        this.#events.dispatch(closeEvent(envelope.clean));
      } else {
        const ports = envelope.ports.map(attach);
        try {
          this.#events.dispatch(new WireMessageEvent("message", { data: decodePayload(envelope.data, ports), ports }));
        } catch (error) {
          this.#events.dispatch(new WireMessageEvent("messageerror", { data: error }));
        }
      }
      if (state.inbox.length) this[kSchedule]();
      else if (state.closed) state.context.nonGCedPorts.delete(this);
    });
  }

  postMessage(message: any, transfer?: Transferable[] | StructuredSerializeOptions): void { postPort(this, message, transfer); }

  start(): void {
    this.#started = true;
    this[kSchedule]();
  }

  close(): void {
    if (this.#status !== "active") return;
    this.#status = "closed";
    this.#state.closed = true;
    this.#state.context.finalizer?.unregister(this);
    closeAddress(this.#state.context, this.#state.id, this.#state.peer, true);
    if (this.#state.inbox.length === 0) this.#state.context.nonGCedPorts.delete(this);
  }

  [kDispose](): void { this.close(); }

  static fromNative(port: MessagePort, context: WireContext = defaultContext): WireMessagePort {
    const channel = new WireMessageChannel(context);
    port.addEventListener("message", (event) => transcodeFromNative(event, context, (data, ports) => channel.port2.postMessage(data, ports)));
    channel.port2.addEventListener("message", (event) => transcodeToNative(event, (data, ports) => port.postMessage(data, ports)));
    port.addEventListener("close", () => channel.port2.close(), { once: true });
    port.start();
    channel.port2.start();
    return channel.port1;
  }

  toNative(): MessagePort {
    if (this.#native) return this.#native;
    this.transferable = false;
    const channel = new globalThis.MessageChannel();
    this.addEventListener("message", (event) => transcodeToNative(event, (data, ports) => channel.port2.postMessage(data, ports)));
    channel.port2.addEventListener("message", (event) => transcodeFromNative(event, this.#state.context, (data, ports) => this.postMessage(data, ports)));
    this.addEventListener("close", () => channel.port2.close(), { once: true });
    this.start();
    channel.port2.start();
    return this.#native = channel.port1;
  }

  addEventListener<K extends keyof WireMessagePortEventMap>(type: K, listener: TypedListener<WireMessagePort, WireMessagePortEventMap[K]> | null, options?: boolean | AddEventListenerOptions): void;
  addEventListener(type: string, listener: Listener | null, options?: boolean | AddEventListenerOptions): void;
  addEventListener(type: string, listener: Listener | null, options?: boolean | AddEventListenerOptions): void {
    this.#events.add(type, listener, options);
  }

  removeEventListener<K extends keyof WireMessagePortEventMap>(type: K, listener: TypedListener<WireMessagePort, WireMessagePortEventMap[K]> | null, options?: boolean | EventListenerOptions): void;
  removeEventListener(type: string, listener: Listener | null, options?: boolean | EventListenerOptions): void;
  removeEventListener(type: string, listener: Listener | null, options?: boolean | EventListenerOptions): void {
    this.#events.remove(type, listener, options);
  }

  dispatchEvent(event: Event): boolean { return this.#events.dispatch(event); }

  get onmessage() { return this.#onmessage; }

  set onmessage(listener: ((this: MessagePort, event: MessageEvent) => any) | null) {
    this.#onmessage = setHandler(this as unknown as EventTarget, "message", this.#onmessage, listener);
    if (listener) this.start();
  }

  get onmessageerror() { return this.#onmessageerror; }

  set onmessageerror(listener: ((this: MessagePort, event: MessageEvent) => any) | null) {
    this.#onmessageerror = setHandler(this as unknown as EventTarget, "messageerror", this.#onmessageerror, listener);
  }
}

export class WireMessageChannel implements MessageChannel {
  readonly port1: WireMessagePort;
  readonly port2: WireMessagePort;

  constructor(context: WireContext = defaultContext) {
    const id1 = context.generateId();
    const id2 = context.generateId();
    this.port1 = attach(localState(context, id1, id2));
    this.port2 = attach(localState(context, id2, id1));
  }
}

export class WireEndpoint extends EventTarget {
  #link: Link;
  #onmessage: ((this: WireEndpoint, event: MessageEvent) => any) | null = null;
  #onmessageerror: ((this: WireEndpoint, event: MessageEvent) => any) | null = null;
  #onerror: ((this: WireEndpoint, event: ErrorEvent) => any) | null = null;

  constructor(stream: DuplexStream, _identifier?: unknown, context: WireContext = defaultContext) {
    super();
    const link = {
      context,
      endpoint: this,
      reader: stream.readable.getReader(),
      writer: stream.writable.getWriter(),
      writes: Promise.resolve(),
      status: "open",
    } satisfies Link;
    this.#link = link;
    void link.writer.closed.catch((error) => disconnect(link, false, false, error));
    void readLink(link);
  }

  postMessage(message: any, transfer?: Transferable[] | StructuredSerializeOptions): void {
    const link = this.#link;
    if (link.status !== "open") {
      postTask(() => this.dispatchEvent(new WireMessageEvent("messageerror", { data: new Error("Transport is closed") })));
      return;
    }
    const route: LinkRoute = { type: "link", link, peer: "" };
    sendPrepared(link.context, null, route, prepareMessage(link.context, null, null, route, message, transfer), this);
  }

  terminate(): void { disconnect(this.#link, true, true); }

  [kDispose](): void { this.terminate(); }

  addEventListener<K extends keyof WireMessagePortEventMap>(type: K, listener: TypedListener<WireEndpoint, WireMessagePortEventMap[K]> | null, options?: boolean | AddEventListenerOptions): void;
  addEventListener(type: string, listener: Listener | null, options?: boolean | AddEventListenerOptions): void;
  addEventListener(type: string, listener: Listener | null, options?: boolean | AddEventListenerOptions): void {
    super.addEventListener(type, listener, options);
  }

  removeEventListener<K extends keyof WireMessagePortEventMap>(type: K, listener: TypedListener<WireEndpoint, WireMessagePortEventMap[K]> | null, options?: boolean | EventListenerOptions): void;
  removeEventListener(type: string, listener: Listener | null, options?: boolean | EventListenerOptions): void;
  removeEventListener(type: string, listener: Listener | null, options?: boolean | EventListenerOptions): void {
    super.removeEventListener(type, listener, options);
  }

  get onmessage() { return this.#onmessage; }

  set onmessage(listener: ((this: WireEndpoint, event: MessageEvent) => any) | null) {
    this.#onmessage = setHandler(this, "message", this.#onmessage, listener);
  }

  get onmessageerror() { return this.#onmessageerror; }

  set onmessageerror(listener: ((this: WireEndpoint, event: MessageEvent) => any) | null) {
    this.#onmessageerror = setHandler(this, "messageerror", this.#onmessageerror, listener);
  }

  get onerror() { return this.#onerror; }

  set onerror(listener: ((this: WireEndpoint, event: ErrorEvent) => any) | null) {
    this.#onerror = setHandler(this, "error", this.#onerror, listener);
  }
}
