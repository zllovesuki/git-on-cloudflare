import { asByteTransformStream, createBlobFromBytes } from "./webtypes.ts";

/**
 * Compression and decompression utilities using Web Streams API
 */

/**
 * Compress data using deflate algorithm
 * @param data - Uint8Array to compress
 * @returns Compressed Uint8Array
 */
export async function deflate(data: Uint8Array): Promise<Uint8Array> {
  const cs = new CompressionStream("deflate");
  const stream = createBlobFromBytes(data).stream().pipeThrough(cs);
  const buf = await new Response(stream).arrayBuffer();
  return new Uint8Array(buf);
}

/**
 * Decompress data using deflate algorithm
 * @param data - Compressed Uint8Array
 * @returns Decompressed Uint8Array
 */
export async function inflate(data: Uint8Array): Promise<Uint8Array> {
  const ds = new DecompressionStream("deflate");
  const stream = createBlobFromBytes(data).stream().pipeThrough(ds);
  const buf = await new Response(stream).arrayBuffer();
  return new Uint8Array(buf);
}

/**
 * Create a deflate compression transform stream
 * @returns TransformStream for compression
 */
export function createDeflateStream(): TransformStream<Uint8Array, Uint8Array> {
  return asByteTransformStream(new CompressionStream("deflate"));
}

/**
 * Create a deflate decompression transform stream
 * @returns TransformStream for decompression
 */
export function createInflateStream(): TransformStream<Uint8Array, Uint8Array> {
  return asByteTransformStream(new DecompressionStream("deflate"));
}
