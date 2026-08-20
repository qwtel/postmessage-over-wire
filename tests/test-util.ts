import { _internals, WireEndpoint } from "../index";

export type TestRouter = ReturnType<typeof _internals.createRouter>;

export type DuplexStream = {
  readable: ReadableStream<Uint8Array>;
  writable: WritableStream<Uint8Array>;
};

export function createLinkedStreams(): [DuplexStream, DuplexStream] {
  const leftToRight = new TransformStream<Uint8Array, Uint8Array>();
  const rightToLeft = new TransformStream<Uint8Array, Uint8Array>();

  return [
    { readable: rightToLeft.readable, writable: leftToRight.writable },
    { readable: leftToRight.readable, writable: rightToLeft.writable },
  ];
}

export function createFragmentedLinkedStreams(fragmentSize = 1): [DuplexStream, DuplexStream] {
  const fragment = (writable: WritableStream<Uint8Array>) => {
    const writer = writable.getWriter();
    return new WritableStream<Uint8Array>({
      async write(chunk) {
        for (let offset = 0; offset < chunk.byteLength; offset += fragmentSize) {
          await writer.write(chunk.slice(offset, offset + fragmentSize));
        }
      },
      close() { return writer.close(); },
      abort(reason) { return writer.abort(reason); },
    });
  };
  const [left, right] = createLinkedStreams();
  return [
    { readable: left.readable, writable: fragment(left.writable) },
    { readable: right.readable, writable: fragment(right.writable) },
  ];
}

export function generateIds(prefix: string) {
  let next = 0;
  return () => `${prefix}-${++next}`;
}

export function createTestRouter(prefix: string): TestRouter {
  return _internals.createRouter({ generateId: generateIds(prefix) });
}

export const routeCount = _internals.routeCount;

export function nextEvent<T extends Event>(target: EventTarget, type: string): Promise<T> {
  return new Promise((resolve) => {
    target.addEventListener(type, resolve as EventListener, { once: true });
  });
}

export function nextMessage(target: EventTarget): Promise<MessageEvent> {
  return nextEvent<MessageEvent>(target, "message");
}

export function nextEvents<T extends Event>(target: EventTarget, type: string, count: number): Promise<T[]> {
  return new Promise((resolve) => {
    const events: T[] = [];
    const listener = (event: Event) => {
      events.push(event as T);
      if (events.length === count) {
        target.removeEventListener(type, listener);
        resolve(events);
      }
    };
    target.addEventListener(type, listener);
  });
}

export function nextMessages(target: EventTarget, count: number): Promise<MessageEvent[]> {
  return nextEvents<MessageEvent>(target, "message", count);
}

export function nextPortMessage(port: MessagePort): Promise<MessageEvent> {
  const message = nextMessage(port);
  port.start();
  return message;
}

export function nextPortMessages(port: MessagePort, count: number): Promise<MessageEvent[]> {
  const messages = nextMessages(port, count);
  port.start();
  return messages;
}

export function createEndpointPair(
  leftRouter = createTestRouter("left"),
  rightRouter = createTestRouter("right"),
): [WireEndpoint, WireEndpoint] {
  const [left, right] = createLinkedStreams();
  return [
    new WireEndpoint(left, "left", leftRouter),
    new WireEndpoint(right, "right", rightRouter),
  ];
}

export function closeAll(...targets: Array<{ close?: () => void; terminate?: () => void } | undefined>) {
  for (const target of targets) {
    try {
      target?.close?.();
    } catch {}
    try {
      target?.terminate?.();
    } catch {}
  }
}

export function settle(): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, 0));
}

export function timeout<T>(promise: Promise<T>, milliseconds = 25): Promise<T> {
  let timer: ReturnType<typeof setTimeout>|undefined;
  const expired = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error(`Timed out after ${milliseconds}ms`)), milliseconds);
  });
  return Promise.race([promise, expired]).finally(() => {
    if (timer !== undefined) clearTimeout(timer);
  });
}
