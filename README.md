# PostMessage Over Wire

An implementation of the Web Message Passing API over any readable/writable stream pair. 

**[WIP]** Works, but needs a revamp to correctly release resources and throw errors in the face of network failure. Not designed for adversarial contexts. No delivery or ordering guarantees. Only supports tree shaped networks.


