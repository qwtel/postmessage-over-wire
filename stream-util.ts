const EscapeCharCode = 92; // ASCII code for backslash '\'
const NewlineCharCode = 10; // ASCII code for newline '\n'

export class NewlineStream extends TransformStream<Uint8Array, Uint8Array> {
  constructor() {
    let buffer: Uint8Array;

    super({
      start() {
        buffer = new Uint8Array(0);
      },
      transform(chunk, controller) {
        const combined = new Uint8Array(buffer.length + chunk.length);
        combined.set(buffer);
        combined.set(chunk, buffer.length);

        let start = 0;
        let newlineIndex;
        while ((newlineIndex = combined.indexOf(NewlineCharCode, start)) !== -1) {
          // Check if the newline is escaped
          let escapeCount = 0;
          for (let j = newlineIndex - 1; j >= start && combined[j] === EscapeCharCode; j--) {
            escapeCount++;
          }
          if (escapeCount % 2 === 0) {
            const line = combined.subarray(start, newlineIndex);
            controller.enqueue(line);
            start = newlineIndex + 1;
          } else {
            // The newline was escaped, move past it
            start = newlineIndex + 1;
          }
        }

        buffer = combined.subarray(start);
      },
      flush(controller) {
        if (buffer.length > 0) {
          controller.enqueue(buffer);
        }
      }
    });
  }
}
