import { WireEndpoint, WireMessagePort, WireMessageChannel } from './index'
import * as Caplink from '../comlink/src/caplink';

export * from './index'

// Integrates this library with Comlink by implementing 3 symbols that Comlink uses to adapt native objects to our custom version and vice-versa.
// The "correct" way to add these symbols would be to create subclasses,
// and then have "species" symbols that tells all internal methods to create these subclasses.
// However, that would be a massive minefield and boilerplate bonanza, while prototype hacking just takes a few lines.
// Performance be damned..
(WireEndpoint.prototype as WireEndpoint & Caplink.Endpoint)[Caplink.messageChannel] = WireMessageChannel;
(WireEndpoint.prototype as WireEndpoint & Caplink.Endpoint)[Caplink.adoptNative] = WireMessagePort.fromNative;
(WireEndpoint.prototype as WireEndpoint & Caplink.Endpoint)[Caplink.toNative] = function() { throw new Error("WireEndpoint has no native equivalent.") };

(WireMessagePort.prototype as WireMessagePort & Caplink.Endpoint)[Caplink.messageChannel] = WireMessageChannel;
(WireMessagePort.prototype as WireMessagePort & Caplink.Endpoint)[Caplink.adoptNative] = WireMessagePort.fromNative;
(WireMessagePort.prototype as WireMessagePort & Caplink.Endpoint)[Caplink.toNative] = function() { return this.toNative() };
