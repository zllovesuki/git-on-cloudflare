export function asBodyInit(bytes: Uint8Array): BodyInit {
  return bytes as unknown as BodyInit;
}

export function asBlobPart(bytes: Uint8Array): BlobPart {
  return bytes as unknown as BlobPart;
}

export function asBufferSource(bytes: Uint8Array): BufferSource {
  return bytes as unknown as BufferSource;
}

export function createBlobFromBytes(bytes: Uint8Array): Blob {
  return new Blob([asBlobPart(bytes)]);
}

export function asByteTransformStream(
  stream: CompressionStream | DecompressionStream
): TransformStream<Uint8Array, Uint8Array> {
  return stream as unknown as TransformStream<Uint8Array, Uint8Array>;
}

export function createDigestStream(algorithm: string): WritableStream<Uint8Array> & {
  digest: Promise<ArrayBuffer>;
  getWriter(): WritableStreamDefaultWriter<Uint8Array>;
} {
  const DigestCtor = (crypto as Crypto & { DigestStream: typeof DigestStream }).DigestStream;
  return new DigestCtor(algorithm) as WritableStream<Uint8Array> & {
    digest: Promise<ArrayBuffer>;
    getWriter(): WritableStreamDefaultWriter<Uint8Array>;
  };
}
