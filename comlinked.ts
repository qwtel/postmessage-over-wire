import { kContext, WireEndpoint, WireMessagePort, WireMessageChannel } from './index'
import * as Caplink from '@workers/caplink';

export * from './index'

// Add Caplink's channel factory and native-port adapters without coupling the wire core to Caplink.
(WireEndpoint.prototype as WireEndpoint & Caplink.Endpoint)[Caplink.messageChannel] = function() { return new WireMessageChannel(this[kContext]) };
(WireEndpoint.prototype as WireEndpoint & Caplink.Endpoint)[Caplink.adoptNative] = function(port) { return WireMessagePort.fromNative(port, this[kContext]) };
(WireEndpoint.prototype as WireEndpoint & Caplink.Endpoint)[Caplink.toNative] = function() { throw new Error("WireEndpoint has no native equivalent.") };

(WireMessagePort.prototype as WireMessagePort & Caplink.Endpoint)[Caplink.messageChannel] = function() { return new WireMessageChannel(this[kContext]) };
(WireMessagePort.prototype as WireMessagePort & Caplink.Endpoint)[Caplink.adoptNative] = function(port) { return WireMessagePort.fromNative(port, this[kContext]) };
(WireMessagePort.prototype as WireMessagePort & Caplink.Endpoint)[Caplink.toNative] = function() { return this.toNative() };
