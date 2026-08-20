/**
 * Movable Web Messaging ports over an ordered, full-duplex byte stream pair.
 *
 * `WireMessageChannel` creates an entangled pair of `WireMessagePort`s.
 * `WireMessagePort` implements the `MessagePort` API and is transferable.
 * `WireEndpoint` attaches the local router to one full-duplex transport link;
 * unlike a port, the endpoint is a carrier and has no routed address.
 *
 * ```text
 * router L                     transport                     router R
 *
 * ports + routes   <── WireEndpoint ═════════ WireEndpoint ──> ports + routes
 *                         writable  ──────>  readable
 *                         readable  <──────  writable
 * ```
 *
 * The transport must preserve byte order and reliably surface EOF, errors,
 * and cancellation. It need not preserve write boundaries. The protocol does
 * not provide encryption, peer authentication, or a heartbeat for otherwise
 * undetectable half-open links.
 *
 * ## Routing
 *
 * Every port end has a stable address and the address of its entangled peer.
 * A router maps each known address to exactly one next hop:
 *
 * ```text
 * address ── local ──> port state { peer, inbox, owner }
 * address ── link  ──> another router through a WireEndpoint
 * ```
 *
 * Posting on A looks up A's peer address B and follows B's route. Intermediate
 * routers perform the same lookup, so a moved port can be reached through any
 * number of forwarding routers.
 *
 * ```text
 * state A --peer B--> [B: link 0->1] --> [B: link 1->2] --> [B: local state B]
 * state B --peer A--> [A: link 2->1] --> [A: link 1->0] --> [A: local state A]
 * ```
 *
 * The default address generator returns a cryptographically random 128-bit
 * bigint. A custom `generateId` may be supplied; its values must be unique
 * across every router that can exchange routes. Addresses are opaque protocol
 * labels. The transferable `WireMessagePort` object is the public capability.
 *
 * ## Moving a port
 *
 * Port state owns its inbox independently of the JavaScript wrapper. A local
 * transfer detaches the source wrapper and attaches the same state at delivery.
 * Messages posted before the receiving wrapper exists remain in that inbox.
 *
 * A remote transfer:
 *
 * 1. validates and serializes the complete message without side effects;
 * 2. detaches each transferred wrapper exactly once;
 * 3. snapshots each port's address, peer, and recursively nested inbox;
 * 4. changes its route to the outbound link; and
 * 5. queues the transfer frame on that link's ordered write chain.
 *
 * ```text
 * before moving B                 after B arrives
 *
 * router L                        router L           router R
 * A ──local──> state A            A ──local──> A     A ──link──> L
 * B ──local──> state B            B ──link───> R     B ──local─> state B
 *
 *                    message { ports: [B] }
 *              L =============================> R
 * ```
 *
 * Installing the outbound route before queueing the frame makes later traffic
 * use the same ordered writer after the transfer descriptor. On arrival, the
 * destination installs the port state before acknowledging the move or
 * dispatching the carrier message.
 *
 * ## Wire format
 *
 * The stream is a sequence of independently V8-serialized frames:
 *
 * ```text
 * +----------------------+--------------------------+
 * | uint32 LE byte count | V8-serialized frame body |
 * +----------------------+--------------------------+
 * ```
 *
 * The byte count covers only the frame body. Readers accept arbitrary splitting
 * and coalescing of frames. EOF with an incomplete prefix or body is an error.
 * There are three frame bodies. Top-level frames are positional tuples to keep
 * their serialized representation compact; the labels below document each
 * position and are not written to the wire:
 *
 * ```ts
 * type MessageFrame = [
 *   type:  "message",
 *   to:    PortId | null,      // null dispatches on the WireEndpoint
 *   data:  Uint8Array,         // separately serialized application payload
 *   ports: ShippedPort[],      // transferred ports, in transfer-list order
 *   move:  string | null,      // route-cleanup token when ports are present
 * ];
 *
 * type CloseFrame = [
 *   type:  "close",
 *   to:    PortId,
 *   from:  PortId,
 *   clean: boolean,
 * ];
 *
 * type MovedFrame = [type: "moved", move: string];
 * ```
 *
 * A transferred port includes messages and closes that were already waiting in
 * its inbox. Transferred ports inside those messages are represented recursively:
 *
 * ```ts
 * type ShippedPort = [
 *   id:    PortId,
 *   peer:  PortId,
 *   inbox: Array<
 *     | [type: "message", data: Uint8Array, ports: ShippedPort[]]
 *     | [type: "close", clean: boolean]
 *   >,
 * ];
 * ```
 *
 * Application payloads use a separate V8 serialization so transferred port
 * references can be replaced by indices into `ports`. Deserialization restores
 * those references to the same objects exposed through `MessageEvent.ports`.
 * Non-port transferables such as `ArrayBuffer` are detached with
 * `structuredClone` after successful payload serialization.
 *
 * `moved` is a routing-lifetime acknowledgement. It retraces a transfer's path
 * and lets intermediate contexts discard obsolete forwarding entries only
 * after the destination has installed the moved state. It does not acknowledge
 * application handling or change the synchronous `postMessage(): void` API.
 *
 * Frame writes are serialized and wait for `writer.ready`. Link EOF or failure
 * removes routes using that link and propagates an unclean close toward their
 * remaining local or remote peers. A `WireEndpoint` dispatches `error` for a
 * detected non-abort failure, followed by exactly one `close`; explicit
 * termination is clean, while EOF and failure are unclean. Frame bodies
 * currently have no magic, version negotiation, authentication, or size limit;
 * both peers must use the same trusted protocol version.
 *
 * ## Web-platform behavior
 *
 * Message delivery uses task rather than microtask scheduling. `WeakRef` keeps
 * port state independent of wrappers, while `FinalizationRegistry` performs
 * best-effort cleanup when available. `WireMessagePort` remains a zero-length
 * `DataView` so V8 serializers route it through their host-object hooks.
 *
 * @module
 */
import {
  DefaultDeserializer,
  DefaultSerializer,
  deserialize as deserializeFrame,
} from "@workers/v8-value-serializer/v8";

export type PortId = number | bigint | string;
export type EndpointLike = { dispatchEvent(event: Event): void };
export type WireMessagePortEventMap = MessagePortEventMap & { close: CloseEvent; error: ErrorEvent };

type HeldPort = readonly [id: PortId, peer: PortId];
type DuplexStream = { readable: ReadableStream<Uint8Array>; writable: WritableStream<Uint8Array> };

type LocalEnvelope =
  | [type: "message", data: Uint8Array, ports: PortState[]]
  | [type: "close",   clean: boolean                      ];

type ShippedPort = [id: PortId, peer: PortId, inbox: ShippedEnvelope[]];

type ShippedEnvelope =
  | [type: "message", data: Uint8Array, ports: ShippedPort[]]
  | [type: "close",   clean: boolean                        ];

type MessageFrame = [type: "message", to: PortId | null, data: Uint8Array, ports: ShippedPort[], move: string | null];
type CloseFrame   = [type: "close",   to: PortId,        from: PortId,     clean: boolean                           ];
type MovedFrame   = [type: "moved",   move: string                                                                  ];
type Frame = MessageFrame | CloseFrame | MovedFrame;

type LocalRoute = { type: "local"; port: PortState; peer: PortId };
type LinkRoute = { type: "link"; link: Link; peer: PortId };
type Route = LocalRoute | LinkRoute;

type PendingMove = { back: Link | null; forward: Link; ports: HeldPort[] };

type Router = {
  routes: Map<PortId, Route>;
  retainedPorts: Set<WireMessagePort>;
  pendingMoves: Map<string, PendingMove>;
  generateId(): PortId;
  finalizer: FinalizationRegistry<HeldPort> | null;
};

type PortState = {
  router: Router; id: PortId; peer: PortId; inbox: LocalEnvelope[];
  owner: WeakRef<WireMessagePort> | null; scheduled: boolean; closed: boolean;
};

type Link = {
  router: Router; endpoint: WireEndpoint; reader: ReadableStreamDefaultReader<Uint8Array>;
  writer: WritableStreamDefaultWriter<Uint8Array>; writes: Promise<void>;
  status: "open" | "closing" | "closed"; writerDone?: Promise<void>;
};

const emptyBuffer = new ArrayBuffer(0);
const emptyFramePrefix = new Uint8Array(4);

const kConstruct = Symbol("constructWireMessagePort");
const kState = Symbol("state");
const kDetach = Symbol("detach");
const kSchedule = Symbol("schedule");
const kDispose: typeof Symbol.dispose = ((Symbol as any).dispose ?? Symbol.for("Symbol.dispose")) as typeof Symbol.dispose;

const replacementTag = 77;
const globalRoutes = Symbol.for("postmessage-over-wire.routes.v2");
const ownerOf = (state: PortState) => state.owner?.deref();
const dataCloneError = (message: string) => new DOMException(message, "DataCloneError");
const invalidStateError = (message: string) => new DOMException(message, "InvalidStateError");

export const isAbortError = (error: unknown): error is Error => error instanceof Error && error.name === "AbortError";

function randomId(): bigint {
  const words = crypto.getRandomValues(new Uint32Array(4));
  return words.reduce((id, word) => (id << 32n) | BigInt(word), 0n);
}

const moveId = () => crypto.randomUUID?.() ?? randomId().toString(16);

function createRouter(options: {
  routes?: Map<PortId, Route>;
  generateId?: () => PortId;
} = {}): Router {
  const router: Router = {
    routes: options.routes ?? new Map(),
    retainedPorts: new Set<WireMessagePort>(),
    pendingMoves: new Map<string, PendingMove>(),
    generateId: options.generateId ?? randomId,
    finalizer: null,
  };

  router.finalizer = typeof FinalizationRegistry === "function"
    ? new FinalizationRegistry<HeldPort>((held) => closeAddress(router, held[/* .id */ 0], held[/* .peer */ 1], true))
    : null;
  return router;
}

const defaultRouter = createRouter({
  routes: ((globalThis as any)[globalRoutes] ??= new Map()),
});

/** Test support for simulating independent routing nodes in one JavaScript realm. */
export const _internals = {
  createRouter: (options: { generateId?: () => PortId } = {}) => createRouter(options),
  routeCount: (router: Router) => router.routes.size,
  routeIds: (router: Router) => [...router.routes.keys()],
};

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
  const serializer = new DefaultSerializer();
  serializer.writeRawBytes(emptyFramePrefix);
  serializer.writeHeader();
  serializer.writeValue(frame);
  const result = serializer.releaseBuffer();
  new DataView(result.buffer, result.byteOffset, 4).setUint32(0, result.byteLength - 4, true);
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

function localState(router: Router, id: PortId, peer: PortId, inbox: LocalEnvelope[] = []): PortState {
  const state: PortState = { router, id, peer, inbox, owner: null, scheduled: false, closed: inbox.some(([type]) => type === "close") };
  router.routes.set(id, { type: "local", port: state, peer });
  return state;
}

const attach = (state: PortState) => ownerOf(state) ?? new WireMessagePort(kConstruct, state);

function enqueue(state: PortState, envelope: LocalEnvelope): void {
  state.inbox.push(envelope);
  if (envelope[0] === "close") state.closed = true;
  ownerOf(state)?.[kSchedule]();
}

function shippedPairs(ports: ShippedPort[], result: HeldPort[] = []): HeldPort[] {
  for (const [id, peer, inbox] of ports) {
    result.push([/* id: */ id, /* peer: */ peer]);
    for (const envelope of inbox) if (envelope[0] === "message") shippedPairs(envelope[/* .ports */ 2], result);
  }
  return result;
}

function ship(state: PortState, link: Link): ShippedPort {
  const inbox = state.inbox.map<ShippedEnvelope>((envelope) => envelope[0] === "close"
    ? envelope
    : ["message", /* data: */ envelope[1], /* ports: */ envelope[2].map((port) => ship(port, link))]);
  state.inbox.length = 0;
  state.router.routes.set(state.id, { type: "link", link, peer: state.peer });
  return [/* id: */ state.id, /* peer: */ state.peer, /* inbox: */ inbox];
}

function pointShipped(router: Router, ports: ShippedPort[], link: Link): void {
  for (const [id, peer, inbox] of ports) {
    router.routes.set(id, { type: "link", link, peer });
    for (const envelope of inbox) if (envelope[0] === "message") pointShipped(router, envelope[/* .ports */ 2], link);
  }
}

function learnPeers(router: Router, ports: ShippedPort[], incoming: Link): void {
  for (const [id, peer, inbox] of ports) {
    if (!router.routes.has(peer)) router.routes.set(peer, { type: "link", link: incoming, peer: id });
    for (const envelope of inbox) if (envelope[0] === "message") learnPeers(router, envelope[/* .ports */ 2], incoming);
  }
}

function importShipped(router: Router, port: ShippedPort, incoming: Link): PortState {
  const [id, peer, shippedInbox] = port;
  const inbox = shippedInbox.map<LocalEnvelope>((envelope) => envelope[0] === "close"
    ? envelope
    : ["message", /* data: */ envelope[1], /* ports: */ envelope[2].map((nested) => importShipped(router, nested, incoming))]);
  return localState(router, id, peer, inbox);
}

const transferList = (value?: Transferable[] | StructuredSerializeOptions) => Array.isArray(value) ? value : value?.transfer ?? [];

function prepareMessage(
  router: Router,
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
      if (!item.transferable || state.router !== router || state.closed || ownerOf(state) !== item) {
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

function registerMove(router: Router, back: Link | null, forward: Link, frame: MessageFrame): void {
  const [, , , ports, move] = frame;
  if (!move) return;
  router.pendingMoves.set(move, { back, forward, ports: shippedPairs(ports) });
}

function pruneMove(router: Router, move: PendingMove): void {
  const table = router.routes;
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
  if (move) void writeFrame(link, ["moved", /* move: */ move]).catch(() => {});
}

function handleMoved(link: Link, token: string): void {
  const pending = link.router.pendingMoves.get(token);
  if (!pending || pending.forward !== link) return;
  link.router.pendingMoves.delete(token);
  const forwarded = pending.back ? writeFrame(pending.back, ["moved", /* move: */ token]) : Promise.resolve();
  void forwarded.then(() => pruneMove(link.router, pending)).catch(() => {});
}

function sendPrepared(
  router: Router,
  destination: PortId | null,
  route: Route,
  prepared: ReturnType<typeof prepareMessage>,
  report: EventTarget,
): void {
  if (route.type === "local") {
    enqueue(route.port, ["message", /* data: */ prepared.data, /* ports: */ prepared.local]);
    return;
  }

  const token = prepared.shipped.length ? moveId() : null;
  const frame: MessageFrame = ["message", /* to: */ destination, /* data: */ prepared.data, /* ports: */ prepared.shipped, /* move: */ token];
  registerMove(router, null, route.link, frame);
  void writeFrame(route.link, frame).catch((error) => report.dispatchEvent(new WireMessageEvent("messageerror", { data: error })));
}

function postPort(port: WireMessagePort, value: unknown, transfer?: Transferable[] | StructuredSerializeOptions): void {
  const state = port[kState]();
  if (state.closed || ownerOf(state) !== port) throw invalidStateError("Port is not entangled");
  const route = state.router.routes.get(state.peer);
  if (!route) throw invalidStateError("Port is not entangled");
  sendPrepared(state.router, state.peer, route, prepareMessage(state.router, state.id, state.peer, route, value, transfer), port);
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
  const router = link.router;
  const [, to, data, ports, move] = frame;
  learnPeers(router, ports, link);

  if (to === null) {
    const states = ports.map((port) => importShipped(router, port, link));
    acknowledge(link, move);
    dispatchEndpoint(link.endpoint, data, states);
    return;
  }

  const route = router.routes.get(to);
  if (!route) {
    const states = ports.map((port) => importShipped(router, port, link));
    acknowledge(link, move);
    abandonPorts(states);
    return;
  }
  if (route.type === "local") {
    const states = ports.map((port) => importShipped(router, port, link));
    acknowledge(link, move);
    enqueue(route.port, ["message", /* data: */ data, /* ports: */ states]);
  } else {
    pointShipped(router, ports, route.link);
    registerMove(router, link, route.link, frame);
    void writeFrame(route.link, frame).catch(() => {});
  }
}

function receiveClose(link: Link, frame: CloseFrame): void {
  const [, to, from, clean] = frame;
  const table = link.router.routes;
  const route = table.get(to);
  table.delete(to);
  table.delete(from);
  if (route?.type === "local") enqueue(route.port, ["close", /* clean: */ clean]);
  else if (route?.type === "link") void writeFrame(route.link, frame).catch(() => {});
}

const isFrame = (value: unknown): value is Frame => Array.isArray(value)
  && ["message", "close", "moved"].includes(value[0]);

function receiveFrame(link: Link, value: unknown): void {
  if (!isFrame(value)) throw new Error("Malformed wire frame");
  if (value[0] === "message") receiveMessage(link, value);
  else if (value[0] === "close") receiveClose(link, value);
  else handleMoved(link, value[/* .move */ 1]);
}

async function readLink(link: Link): Promise<void> {
  let buffered: Uint8Array<ArrayBufferLike> = new Uint8Array();
  try {
    while (true) {
      const { done, value } = await link.reader.read();
      if (link.status !== "open") return;
      if (done) {
        if (buffered.byteLength) throw new Error("Truncated wire frame");
        disconnect(link, false, false);
        return;
      }
      buffered = append(buffered, value);
      let consumed = 0;
      while (buffered.byteLength - consumed >= 4) {
        const length = new DataView(buffered.buffer, buffered.byteOffset + consumed, 4).getUint32(0, true);
        const end = consumed + length + 4;
        if (buffered.byteLength < end) break;
        const body = buffered.subarray(consumed + 4, end);
        consumed = end;
        receiveFrame(link, deserializeFrame(body));
      }
      if (consumed) buffered = consumed === buffered.byteLength ? new Uint8Array() : buffered.slice(consumed);
    }
  } catch (error) {
    disconnect(link, false, false, error);
  }
}

function writeFrame(link: Link, frame: Frame): Promise<void> {
  if (link.status !== "open") return Promise.reject(new Error("Transport is closed"));
  const write = link.writes.then(async () => {
    await link.writer.ready;
    await link.writer.write(encodeFrame(frame));
  });
  link.writes = write.catch((error) => disconnect(link, false, false, error));
  return write;
}

const finishWriter = (link: Link) => link.writerDone ??= link.writes.catch(() => {}).then(() => link.writer.close()).catch(() => {});

function notifyRoute(route: Route | undefined, to: PortId, from: PortId, clean: boolean): void {
  if (route?.type === "local") enqueue(route.port, ["close", /* clean: */ clean]);
  else if (route?.type === "link") void writeFrame(route.link, ["close", /* to: */ to, /* from: */ from, /* clean: */ clean]).catch(() => {});
}

function disconnect(link: Link, clean: boolean, notifyRemote: boolean, error?: unknown): void {
  if (link.status !== "open") return;
  const table = link.router.routes;
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
    if (notifyRemote) void writeFrame(link, ["close", /* to: */ id, /* from: */ route.peer, /* clean: */ clean]).catch(() => {});
    notifyRoute(peerRoute, route.peer, id, clean);
  }

  for (const [token, move] of link.router.pendingMoves) {
    if (move.back === link || move.forward === link) link.router.pendingMoves.delete(token);
  }

  link.status = "closing";
  if (error !== undefined && !isAbortError(error)) link.endpoint.dispatchEvent(errorEvent(error));
  link.endpoint.dispatchEvent(closeEvent(clean));
  void finishWriter(link).finally(() => {
    link.status = "closed";
    void link.reader.cancel().catch(() => {});
  });
}

function closeAddress(router: Router, id: PortId, peer: PortId, clean: boolean): void {
  const table = router.routes;
  const route = table.get(peer);
  table.delete(id);
  table.delete(peer);
  notifyRoute(route, peer, id, clean);
}

function abandonPorts(ports: PortState[]): void {
  for (const state of ports) {
    const inbox = state.inbox.splice(0);
    closeAddress(state.router, state.id, state.peer, true);
    for (const envelope of inbox) if (envelope[0] === "message") abandonPorts(envelope[/* .ports */ 2]);
  }
}

function abandonInbox(state: PortState): void {
  for (const envelope of state.inbox.splice(0)) if (envelope[0] === "message") abandonPorts(envelope[/* .ports */ 2]);
}

function transcodeToNative(event: MessageEvent, post: (data: unknown, ports: MessagePort[]) => void): void {
  const wirePorts = event.ports as WireMessagePort[];
  const nativePorts = wirePorts.map((port) => port.toNative());
  post(replaceReferences(event.data, new Map(wirePorts.map((port, index) => [port, nativePorts[index]]))), nativePorts);
}

function transcodeFromNative(event: MessageEvent, router: Router, post: (data: unknown, ports: WireMessagePort[]) => void): void {
  const nativePorts = [...event.ports];
  const wirePorts = nativePorts.map((port) => WireMessagePort.fromNative(port, router));
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
  #nativeListeners?: AbortController;
  transferable = true;

  constructor(key: symbol, state?: PortState) {
    if (key !== kConstruct || !state) throw new TypeError("Illegal constructor");
    super(emptyBuffer);
    this.#state = state;
    this.#events = eventFacade(this as unknown as EventTarget, (listeners) => {
      if (this.#status === "active" && listeners.some((item) => item.type === "message")) state.router.retainedPorts.add(this);
      else state.router.retainedPorts.delete(this);
    });
    state.owner = new WeakRef(this);
    state.router.finalizer?.register(this, [state.id, state.peer], this);
  }

  [kState](): PortState { return this.#state; }

  [kDetach](): PortState {
    if (this.#status !== "active") throw dataCloneError("Cannot transfer detached port");
    this.#status = "detached";
    this.#events.clear();
    this.#state.router.retainedPorts.delete(this);
    this.#state.router.finalizer?.unregister(this);
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
      if (envelope[0] === "close") {
        this.#status = "closed";
        this.#events.dispatch(closeEvent(envelope[/* .clean */ 1]));
      } else {
        const ports = envelope[/* .ports */ 2].map(attach);
        try {
          this.#events.dispatch(new WireMessageEvent("message", { data: decodePayload(envelope[/* .data */ 1], ports), ports }));
        } catch (error) {
          this.#events.dispatch(new WireMessageEvent("messageerror", { data: error }));
        }
      }
      if (state.inbox.length) this[kSchedule]();
      else if (state.closed) state.router.retainedPorts.delete(this);
    });
  }

  postMessage(message: any, transfer?: Transferable[] | StructuredSerializeOptions): void { postPort(this, message, transfer); }

  start(): void {
    this.#started = true;
    this[kSchedule]();
  }

  close(): void {
    if (this.#status !== "active") return;
    this.#nativeListeners?.abort();
    this.#native?.close();
    this.#status = "closed";
    this.#state.closed = true;
    this.#state.router.finalizer?.unregister(this);
    closeAddress(this.#state.router, this.#state.id, this.#state.peer, true);
    if (this.#state.inbox.length === 0) this.#state.router.retainedPorts.delete(this);
  }

  [kDispose](): void {
    if (this.#status === "detached") return;
    const state = this.#state;
    this.close();
    abandonInbox(state);
    state.router.retainedPorts.delete(this);
  }

  static fromNative(port: MessagePort, router: Router = defaultRouter): WireMessagePort {
    const channel = new WireMessageChannel(router);
    const listeners = new AbortController();
    const closeBridge = (other: MessagePort) => () => { listeners.abort(); other.close(); };
    port.addEventListener("message", (event) => transcodeFromNative(event, router, (data, ports) => channel.port2.postMessage(data, ports)), { signal: listeners.signal });
    channel.port2.addEventListener("message", (event) => transcodeToNative(event, (data, ports) => port.postMessage(data, ports)), { signal: listeners.signal });
    port.addEventListener("close", closeBridge(channel.port2), { once: true, signal: listeners.signal });
    channel.port2.addEventListener("close", closeBridge(port), { once: true, signal: listeners.signal });
    port.start();
    channel.port2.start();
    return channel.port1;
  }

  toNative(): MessagePort {
    if (this.#native) return this.#native;
    this.transferable = false;
    const channel = new globalThis.MessageChannel();
    const listeners = this.#nativeListeners = new AbortController();
    listeners.signal.addEventListener("abort", () => {
      channel.port2.close();
      this.#nativeListeners = undefined;
    }, { once: true });
    this.addEventListener("message", (event) => transcodeToNative(event, (data, ports) => channel.port2.postMessage(data, ports)), { signal: listeners.signal });
    channel.port2.addEventListener("message", (event) => transcodeFromNative(event, this.#state.router, (data, ports) => this.postMessage(data, ports)), { signal: listeners.signal });
    this.addEventListener("close", () => listeners.abort(), { once: true, signal: listeners.signal });
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

  constructor(router: Router = defaultRouter) {
    const id1 = router.generateId();
    const id2 = router.generateId();
    this.port1 = attach(localState(router, id1, id2));
    this.port2 = attach(localState(router, id2, id1));
  }
}

export class WireEndpoint extends EventTarget {
  #link: Link;
  #onmessage: ((this: WireEndpoint, event: MessageEvent) => any) | null = null;
  #onmessageerror: ((this: WireEndpoint, event: MessageEvent) => any) | null = null;
  #onerror: ((this: WireEndpoint, event: ErrorEvent) => any) | null = null;

  constructor(stream: DuplexStream, _identifier?: unknown, router: Router = defaultRouter) {
    super();
    const link = {
      router,
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
    sendPrepared(link.router, null, route, prepareMessage(link.router, null, null, route, message, transfer), this);
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
