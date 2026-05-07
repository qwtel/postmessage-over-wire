import { createWireContext, WireContext, WireEndpoint, WireMessagePort } from "./index";

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

export function generateIds(prefix: string) {
  let next = 0;
  return () => `${prefix}-${++next}`;
}

export function createTestContext(prefix: string): WireContext {
  return createWireContext({ generateId: generateIds(prefix), finalizer: null });
}

export function nextEvent<T extends Event>(target: EventTarget, type: string): Promise<T> {
  return new Promise((resolve) => {
    target.addEventListener(type, resolve as EventListener, { once: true });
  });
}

export function nextMessage(target: EventTarget): Promise<MessageEvent> {
  return nextEvent<MessageEvent>(target, "message");
}

export function nextPortMessage(port: WireMessagePort): Promise<MessageEvent> {
  const message = nextMessage(port);
  port.start();
  return message;
}

export function createEndpointPair(
  leftContext = createTestContext("left"),
  rightContext = createTestContext("right"),
): [WireEndpoint, WireEndpoint] {
  const [left, right] = createLinkedStreams();
  return [
    new WireEndpoint(left, "left", leftContext),
    new WireEndpoint(right, "right", rightContext),
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
