import { runInDurableObject } from "cloudflare:test";

import type { RepoDurableObject } from "@/do";

/**
 * Run work against a Durable Object instance and reacquire the stub if workerd
 * invalidated the previous instance during local test execution.
 */
export async function runDOWithRetry<T>(
  getStub: () => DurableObjectStub<RepoDurableObject>,
  fn: (instance: RepoDurableObject, state: DurableObjectState) => Promise<T> | T
): Promise<T> {
  const exec = async (stub: DurableObjectStub<RepoDurableObject>): Promise<T> => {
    return await runInDurableObject(stub, (instance, state) => fn(instance, state));
  };
  try {
    return await exec(getStub());
  } catch (error) {
    const message = String(error || "");
    if (message.includes("invalidating this Durable Object")) {
      return await exec(getStub());
    }
    throw error;
  }
}

/**
 * Call a stub method directly and retry once if the underlying instance was
 * invalidated between calls.
 */
export async function callStubWithRetry<T>(
  getStub: () => DurableObjectStub<RepoDurableObject>,
  fn: (stub: DurableObjectStub<RepoDurableObject>) => Promise<T>
): Promise<T> {
  try {
    return await fn(getStub());
  } catch (error) {
    const message = String(error || "");
    if (message.includes("invalidating this Durable Object")) {
      return await fn(getStub());
    }
    throw error;
  }
}
