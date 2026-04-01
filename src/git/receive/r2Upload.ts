import { bytesToHex, createDigestStream } from "@/common/index.ts";
import { packIndexKey } from "@/keys.ts";
import { SubrequestLimiter } from "../operations/limits.ts";
import { appendBytes, cloneBytes } from "./bytes.ts";

const MULTIPART_PART_BYTES = 8 * 1024 * 1024;
const PACK_HEADER_BYTES = 12;
const PACK_TRAILER_BYTES = 20;
const UPLOAD_PROGRESS_STEPS = 20;

export type StagedPackUpload = {
  packKey: string;
  packBytes: number;
  cleanup(): Promise<void>;
};

function formatProgressBytes(bytes: number): string {
  if (bytes >= 1024 * 1024) {
    return `${(bytes / 1024 / 1024).toFixed(1)} MiB`;
  }
  if (bytes >= 1024) {
    return `${(bytes / 1024).toFixed(1)} KiB`;
  }
  return `${bytes} B`;
}

function emitKnownLengthUploadProgress(args: {
  onProgress?: (message: string) => void;
  uploadedBytes: number;
  totalBytes: number;
  progressInterval: number;
  lastReportedStep: number;
}): number {
  if (!args.onProgress || args.totalBytes <= 0) return args.lastReportedStep;

  const percent = Math.floor((args.uploadedBytes / args.totalBytes) * 100);
  const nextStep = Math.min(
    UPLOAD_PROGRESS_STEPS,
    Math.floor(args.uploadedBytes / args.progressInterval)
  );
  if (nextStep <= args.lastReportedStep && args.uploadedBytes < args.totalBytes) {
    return args.lastReportedStep;
  }

  if (args.uploadedBytes >= args.totalBytes) {
    args.onProgress(
      `Uploading pack to object storage: 100% (${formatProgressBytes(args.totalBytes)}/${formatProgressBytes(args.totalBytes)}), done.\n`
    );
    return UPLOAD_PROGRESS_STEPS;
  }

  args.onProgress(
    `Uploading pack to object storage: ${percent}% (${formatProgressBytes(args.uploadedBytes)}/${formatProgressBytes(args.totalBytes)})\r`
  );
  return nextStep;
}

function emitStreamingUploadProgress(args: {
  onProgress?: (message: string) => void;
  uploadedBytes: number;
  reportEveryBytes: number;
  lastReportedBytes: number;
}): number {
  if (!args.onProgress) return args.lastReportedBytes;
  if (args.uploadedBytes < args.reportEveryBytes) return args.lastReportedBytes;
  if (args.uploadedBytes - args.lastReportedBytes < args.reportEveryBytes) {
    return args.lastReportedBytes;
  }
  args.onProgress(
    `Uploading pack to object storage: ${formatProgressBytes(args.uploadedBytes)} uploaded\r`
  );
  return args.uploadedBytes;
}

function parseContentLength(request: Request): number | undefined {
  const raw = request.headers.get("Content-Length");
  if (!raw) return undefined;
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed < 0) return undefined;
  return parsed;
}

function getRemainingBodyLength(request: Request, bytesConsumed: number): number | undefined {
  const contentLength = parseContentLength(request);
  if (contentLength === undefined || contentLength < bytesConsumed) return undefined;
  return contentLength - bytesConsumed;
}

function validatePackHeader(prefix: Uint8Array<ArrayBufferLike>): void {
  if (prefix.byteLength < PACK_HEADER_BYTES) return;
  if (prefix[0] !== 0x50 || prefix[1] !== 0x41 || prefix[2] !== 0x43 || prefix[3] !== 0x4b) {
    throw new Error("receive-pack body did not begin with a valid PACK header.");
  }
  const version = (prefix[4] << 24) | (prefix[5] << 16) | (prefix[6] << 8) | prefix[7];
  if (version !== 2) {
    throw new Error(`Unsupported pack version ${version}.`);
  }
}

async function updateTrailerWindow(
  digestWriter: WritableStreamDefaultWriter<Uint8Array>,
  previousTail: Uint8Array<ArrayBufferLike>,
  nextChunk: Uint8Array<ArrayBufferLike>
): Promise<Uint8Array<ArrayBufferLike>> {
  const combined = new Uint8Array(previousTail.byteLength + nextChunk.byteLength);
  combined.set(previousTail, 0);
  combined.set(nextChunk, previousTail.byteLength);

  if (combined.byteLength <= PACK_TRAILER_BYTES) {
    return combined;
  }

  const digestBytes = combined.subarray(0, combined.byteLength - PACK_TRAILER_BYTES);
  const nextTail = combined.subarray(combined.byteLength - PACK_TRAILER_BYTES);
  await digestWriter.write(digestBytes);
  return cloneBytes(nextTail);
}

async function stageKnownLengthPack(args: {
  env: Env;
  packKey: string;
  expectedLength: number;
  packStream: ReadableStream<Uint8Array>;
  limiter: SubrequestLimiter;
  countSubrequest(op: string, n?: number): void;
  onProgress?: (message: string) => void;
}): Promise<StagedPackUpload> {
  if (args.expectedLength <= 0) {
    throw new Error("Streaming receive expected a non-empty pack body.");
  }

  const fixedLengthStream = new FixedLengthStream(args.expectedLength);
  const uploadWriter = fixedLengthStream.writable.getWriter();
  const digestStream = createDigestStream("SHA-1");
  const digestWriter = digestStream.getWriter();
  const reader = args.packStream.getReader();

  args.countSubrequest("r2:put-pack");
  const putPromise = args.limiter.run("r2:put-pack", async () => {
    return await args.env.REPO_BUCKET.put(args.packKey, fixedLengthStream.readable);
  });
  let totalBytes = 0;
  let headerPrefix: Uint8Array<ArrayBufferLike> = new Uint8Array(0);
  let tail: Uint8Array<ArrayBufferLike> = new Uint8Array(0);
  const progressInterval = Math.max(1, Math.floor(args.expectedLength / UPLOAD_PROGRESS_STEPS));
  let lastReportedStep = 0;

  args.onProgress?.(
    `Uploading pack to object storage: 0% (0 B/${formatProgressBytes(args.expectedLength)})\r`
  );

  try {
    while (true) {
      const next = await reader.read();
      if (next.done) break;
      const chunk = cloneBytes(next.value);

      totalBytes += chunk.byteLength;
      if (headerPrefix.byteLength < PACK_HEADER_BYTES) {
        const headerNeeded = PACK_HEADER_BYTES - headerPrefix.byteLength;
        headerPrefix = appendBytes(headerPrefix, chunk.subarray(0, headerNeeded));
        validatePackHeader(headerPrefix);
      }

      tail = await updateTrailerWindow(digestWriter, tail, chunk);
      await uploadWriter.write(chunk);
      lastReportedStep = emitKnownLengthUploadProgress({
        onProgress: args.onProgress,
        uploadedBytes: totalBytes,
        totalBytes: args.expectedLength,
        progressInterval,
        lastReportedStep,
      });
    }

    if (totalBytes !== args.expectedLength) {
      throw new Error("Received pack length did not match Content-Length.");
    }
    if (totalBytes < PACK_HEADER_BYTES + PACK_TRAILER_BYTES) {
      throw new Error("Received pack body was too short.");
    }

    await digestWriter.close();
    const computedDigest = new Uint8Array(await digestStream.digest);
    if (bytesToHex(computedDigest) !== bytesToHex(tail)) {
      throw new Error("Received pack trailer SHA-1 did not match the streamed body.");
    }

    await uploadWriter.close();
    await putPromise;
    return {
      packKey: args.packKey,
      packBytes: totalBytes,
      async cleanup() {
        await args.env.REPO_BUCKET.delete(args.packKey);
        await args.env.REPO_BUCKET.delete(packIndexKey(args.packKey));
      },
    };
  } catch (error) {
    try {
      await uploadWriter.abort(error);
    } catch {}
    try {
      await reader.cancel(error);
    } catch {}
    try {
      await args.env.REPO_BUCKET.delete(args.packKey);
    } catch {}
    throw error;
  }
}

async function uploadMultipartPart(args: {
  upload: R2MultipartUpload;
  partNumber: number;
  bytes: Uint8Array;
  limiter: SubrequestLimiter;
  countSubrequest(op: string, n?: number): void;
}): Promise<R2UploadedPart> {
  args.countSubrequest("r2:upload-pack-part");
  return await args.limiter.run("r2:upload-pack-part", async () => {
    return await args.upload.uploadPart(args.partNumber, args.bytes);
  });
}

async function stageMultipartPack(args: {
  env: Env;
  packKey: string;
  packStream: ReadableStream<Uint8Array>;
  limiter: SubrequestLimiter;
  countSubrequest(op: string, n?: number): void;
  onProgress?: (message: string) => void;
}): Promise<StagedPackUpload> {
  args.countSubrequest("r2:create-pack-multipart");
  const upload = await args.limiter.run("r2:create-pack-multipart", async () => {
    return await args.env.REPO_BUCKET.createMultipartUpload(args.packKey);
  });
  const digestStream = createDigestStream("SHA-1");
  const digestWriter = digestStream.getWriter();
  const reader = args.packStream.getReader();

  const uploadedParts: R2UploadedPart[] = [];
  let buffered: Uint8Array<ArrayBufferLike> = new Uint8Array(0);
  let totalBytes = 0;
  let headerPrefix: Uint8Array<ArrayBufferLike> = new Uint8Array(0);
  let tail: Uint8Array<ArrayBufferLike> = new Uint8Array(0);
  let partNumber = 1;
  let lastReportedBytes = 0;

  args.onProgress?.("Uploading pack to object storage: streaming upload started\n");

  try {
    while (true) {
      const next = await reader.read();
      if (next.done) break;
      const chunk = cloneBytes(next.value);

      totalBytes += chunk.byteLength;
      if (headerPrefix.byteLength < PACK_HEADER_BYTES) {
        const headerNeeded = PACK_HEADER_BYTES - headerPrefix.byteLength;
        headerPrefix = appendBytes(headerPrefix, chunk.subarray(0, headerNeeded));
        validatePackHeader(headerPrefix);
      }

      tail = await updateTrailerWindow(digestWriter, tail, chunk);
      buffered = appendBytes(buffered, chunk);
      lastReportedBytes = emitStreamingUploadProgress({
        onProgress: args.onProgress,
        uploadedBytes: totalBytes,
        reportEveryBytes: MULTIPART_PART_BYTES,
        lastReportedBytes,
      });

      while (buffered.byteLength >= MULTIPART_PART_BYTES) {
        const partBytes = buffered.slice(0, MULTIPART_PART_BYTES);
        buffered = buffered.slice(MULTIPART_PART_BYTES);
        uploadedParts.push(
          await uploadMultipartPart({
            upload,
            partNumber,
            bytes: partBytes,
            limiter: args.limiter,
            countSubrequest: args.countSubrequest,
          })
        );
        partNumber++;
      }
    }

    if (totalBytes < PACK_HEADER_BYTES + PACK_TRAILER_BYTES) {
      throw new Error("Received pack body was too short.");
    }

    if (buffered.byteLength === 0 && uploadedParts.length === 0) {
      throw new Error("Streaming receive expected a non-empty pack body.");
    }

    uploadedParts.push(
      await uploadMultipartPart({
        upload,
        partNumber,
        bytes: buffered,
        limiter: args.limiter,
        countSubrequest: args.countSubrequest,
      })
    );

    await digestWriter.close();
    const computedDigest = new Uint8Array(await digestStream.digest);
    if (bytesToHex(computedDigest) !== bytesToHex(tail)) {
      throw new Error("Received pack trailer SHA-1 did not match the streamed body.");
    }

    args.countSubrequest("r2:complete-pack-multipart");
    await args.limiter.run("r2:complete-pack-multipart", async () => {
      await upload.complete(uploadedParts);
    });
    args.onProgress?.(
      `Uploading pack to object storage: done (${formatProgressBytes(totalBytes)})\n`
    );

    return {
      packKey: args.packKey,
      packBytes: totalBytes,
      async cleanup() {
        await args.env.REPO_BUCKET.delete(args.packKey);
        await args.env.REPO_BUCKET.delete(packIndexKey(args.packKey));
      },
    };
  } catch (error) {
    try {
      await reader.cancel(error);
    } catch {}
    try {
      await upload.abort();
    } catch {}
    try {
      await args.env.REPO_BUCKET.delete(args.packKey);
    } catch {}
    throw error;
  }
}

export async function stagePackToR2(args: {
  env: Env;
  request: Request;
  packStream: ReadableStream<Uint8Array>;
  packKey: string;
  bytesConsumed: number;
  limiter: SubrequestLimiter;
  countSubrequest(op: string, n?: number): void;
  onProgress?: (message: string) => void;
}): Promise<StagedPackUpload> {
  const remainingLength = getRemainingBodyLength(args.request, args.bytesConsumed);
  if (remainingLength !== undefined) {
    return await stageKnownLengthPack({
      env: args.env,
      packKey: args.packKey,
      expectedLength: remainingLength,
      packStream: args.packStream,
      limiter: args.limiter,
      countSubrequest: args.countSubrequest,
      onProgress: args.onProgress,
    });
  }

  return await stageMultipartPack({
    env: args.env,
    packKey: args.packKey,
    packStream: args.packStream,
    limiter: args.limiter,
    countSubrequest: args.countSubrequest,
    onProgress: args.onProgress,
  });
}

export async function deleteStagedPack(upload: StagedPackUpload | undefined): Promise<void> {
  if (!upload) return;
  await upload.cleanup();
}
