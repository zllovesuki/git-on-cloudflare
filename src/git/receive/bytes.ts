/** Shared byte-manipulation helpers for the streaming receive modules. */

export function appendBytes(
  left: Uint8Array<ArrayBufferLike>,
  right: Uint8Array<ArrayBufferLike>
): Uint8Array<ArrayBufferLike> {
  const merged = new Uint8Array(left.byteLength + right.byteLength);
  merged.set(left, 0);
  merged.set(right, left.byteLength);
  return merged;
}

export function cloneBytes(bytes: Uint8Array<ArrayBufferLike>): Uint8Array<ArrayBufferLike> {
  const cloned = new Uint8Array(bytes.byteLength);
  cloned.set(bytes);
  return cloned;
}
