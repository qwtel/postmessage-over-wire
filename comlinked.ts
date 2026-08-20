import { WireEndpoint, WireMessagePort, WireMessageChannel } from './index'
import * as Caplink from '@workers/caplink';

export * from './index'

// Add Caplink's MessageChannel constructor and native-port adapters without coupling the wire core to Caplink.
(WireEndpoint.prototype as WireEndpoint & Caplink.Endpoint)[Caplink.messageChannel] = WireMessageChannel;
(WireEndpoint.prototype as WireEndpoint & Caplink.Endpoint)[Caplink.adoptNative] = WireMessagePort.fromNative;
(WireEndpoint.prototype as WireEndpoint & Caplink.Endpoint)[Caplink.toNative] = function() { throw new Error("WireEndpoint has no native equivalent.") };

(WireMessagePort.prototype as WireMessagePort & Caplink.Endpoint)[Caplink.messageChannel] = WireMessageChannel;
(WireMessagePort.prototype as WireMessagePort & Caplink.Endpoint)[Caplink.adoptNative] = WireMessagePort.fromNative;
(WireMessagePort.prototype as WireMessagePort & Caplink.Endpoint)[Caplink.toNative] = function() { return this.toNative() };
