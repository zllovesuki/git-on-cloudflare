import type { AuthStateSchema, AuthUsers, RateLimitEntry } from "./authState";

import { DurableObject } from "cloudflare:workers";
import { asBufferSource, createLogger } from "@/common";
import { asTypedStorage } from "../repo/repoState";
import { makeOwnerRateLimitKey, makeAdminRateLimitKey } from "./authState";

/**
 * Generates a cryptographically secure random salt
 * @returns 16-byte salt for password hashing
 */
function generateSalt(): Uint8Array {
  const bytes = new Uint8Array(16);
  crypto.getRandomValues(bytes);
  return bytes;
}

/**
 * Hash token with salt using native PBKDF2
 * @param token - Plain text token to hash
 * @param salt - Salt bytes for hashing
 * @param iterations - Number of PBKDF2 iterations (default: 100000, Cloudflare's limit)
 * @returns String in format "salt:iterations:hash" for storage
 */
const PBKDF2_ITERATIONS = 100000; // Cloudflare's current limit

async function hashTokenWithPBKDF2(
  token: string,
  salt: Uint8Array,
  iterations: number = PBKDF2_ITERATIONS
): Promise<string> {
  const encoder = new TextEncoder();
  const keyMaterial = await crypto.subtle.importKey("raw", encoder.encode(token), "PBKDF2", false, [
    "deriveBits",
  ]);

  const derivedBits = await crypto.subtle.deriveBits(
    {
      name: "PBKDF2",
      salt: asBufferSource(salt),
      iterations: iterations,
      hash: "SHA-256",
    },
    keyMaterial,
    256 // 32 bytes * 8 bits
  );

  const hashArray = new Uint8Array(derivedBits);
  const saltHex = Array.from(salt)
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
  const hashHex = Array.from(hashArray)
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
  return `${saltHex}:${iterations}:${hashHex}`; // Store salt:iterations:hash format
}

/**
 * Verifies a token against its stored hash
 * @param token - Plain text token to verify
 * @param storedHash - Stored hash in format "salt:iterations:hash"
 * @returns True if token matches the stored hash
 */
async function verifyToken(token: string, storedHash: string): Promise<boolean> {
  const parts = storedHash.split(":");
  if (parts.length !== 3) return false;

  // Format: salt:iterations:hash
  const [saltHex, iterStr, hashHex] = parts;
  const salt = new Uint8Array(saltHex.match(/.{2}/g)!.map((byte) => parseInt(byte, 16)));
  const iterations = parseInt(iterStr, 10);
  const computed = await hashTokenWithPBKDF2(token, salt, iterations);
  return computed === storedHash;
}

// Rate limiting configuration
const RATE_LIMIT = {
  maxAttempts: 5,
  windowMs: 60 * 1000, // 1 minute
  blockDurationMs: 5 * 60 * 1000, // 5 minutes
};

/**
 * Durable Object for managing repository authentication and rate limiting
 * Handles token verification, admin operations, and automatic cleanup
 */
export class AuthDurableObject extends DurableObject {
  declare env: Env;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
  }

  /**
   * Alarm handler that cleans up expired rate limit entries
   * Runs hourly to prevent unbounded storage growth
   */
  async alarm() {
    // Clean up old rate limit entries (older than 1 hour)
    const now = Date.now();
    const ONE_HOUR = 60 * 60 * 1000;
    const log = this.logger;
    try {
      const keys = await this.ctx.storage.list({ prefix: "ratelimit:" });
      let removed = 0;
      for (const [key, value] of keys) {
        const entry = value as RateLimitEntry;
        if (entry && entry.lastAttempt && now - entry.lastAttempt > ONE_HOUR) {
          try {
            await this.ctx.storage.delete(key);
            removed++;
          } catch (e) {
            log.warn("ratelimit:cleanup-failed", { key, error: String(e) });
          }
        }
      }
      if (removed > 0) log.info("ratelimit:cleanup", { removed });
    } catch (e) {
      log.error("alarm:error", { error: String(e) });
    }
  }

  private async getStore(): Promise<AuthUsers> {
    const store = asTypedStorage<AuthStateSchema>(this.ctx.storage);
    return (await store.get("users")) ?? {};
  }
  private async putStore(obj: AuthUsers) {
    const store = asTypedStorage<AuthStateSchema>(this.ctx.storage);
    await store.put("users", obj);
  }

  /**
   * RPC: Verify an owner's token with rate limiting (owner + client IP)
   * Mirrors the logic of POST /verify in the fetch() handler.
   */
  public async verify(
    owner: string,
    token: string,
    clientIp: string = "unknown"
  ): Promise<{ ok: boolean; blocked?: boolean; error?: string }> {
    const log = this.logger;
    owner = String(owner || "").trim();
    token = String(token || "").trim();
    if (!owner || !token) return { ok: false };

    // Owner/IP rate limit
    const rateLimitKey = makeOwnerRateLimitKey(owner, clientIp);
    const now = Date.now();
    const astore = asTypedStorage<AuthStateSchema>(this.ctx.storage);
    const rateLimit = (await astore.get(rateLimitKey)) as RateLimitEntry | undefined;

    // Ensure cleanup alarm is scheduled at least hourly
    const alarmTime = await this.ctx.storage.getAlarm();
    if (!alarmTime) {
      await this.ctx.storage.setAlarm(Date.now() + 60 * 60 * 1000);
    }

    if (rateLimit) {
      if (rateLimit.blockedUntil && now < rateLimit.blockedUntil) {
        log.warn("verify:blocked", { owner, clientIp });
        return { ok: false, blocked: true, error: "Too many attempts. Please try again later." };
      }
      if (now - rateLimit.lastAttempt < RATE_LIMIT.windowMs) {
        if (rateLimit.attempts >= RATE_LIMIT.maxAttempts) {
          rateLimit.blockedUntil = now + RATE_LIMIT.blockDurationMs;
          await astore.put(rateLimitKey, rateLimit);
          log.warn("verify:block", { owner, clientIp });
          return { ok: false, blocked: true, error: "Too many attempts. Please try again later." };
        }
      } else {
        rateLimit.attempts = 0;
      }
    }

    const users = await this.getStore();
    const list = users[owner] || [];
    if (list.length === 0) {
      await astore.put(rateLimitKey, {
        attempts: (rateLimit?.attempts || 0) + 1,
        lastAttempt: now,
        blockedUntil: rateLimit?.blockedUntil,
      });
      log.info("verify:unknown-owner", { owner, clientIp });
      return { ok: false };
    }

    for (const storedHash of list) {
      if (await verifyToken(token, storedHash)) {
        await astore.delete(rateLimitKey);
        log.info("verify:ok", { owner, clientIp });
        return { ok: true };
      }
    }

    await astore.put(rateLimitKey, {
      attempts: (rateLimit?.attempts || 0) + 1,
      lastAttempt: now,
      blockedUntil: rateLimit?.blockedUntil,
    });
    log.info("verify:fail", { owner, clientIp });
    return { ok: false };
  }

  /**
   * RPC: List users (admin data model exposure; callers must enforce admin)
   */
  public async getUsers(): Promise<{ owner: string; tokens: string[] }[]> {
    const users = await this.getStore();
    const data = Object.entries(users).map(([owner, hashes]) => ({ owner, tokens: hashes }));
    this.logger.debug("users:list", { count: data.length });
    return data;
  }

  /**
   * RPC: Add tokens for an owner (hash via PBKDF2). Returns updated count.
   */
  public async addTokens(
    owner: string,
    tokens: string[]
  ): Promise<{ ok: true; owner: string; count: number }> {
    owner = String(owner || "").trim();
    if (!owner || !Array.isArray(tokens) || tokens.length === 0) {
      throw new Error("owner and tokens required");
    }
    const users = await this.getStore();
    const cur = new Set<string>(users[owner] || []);
    for (const t of tokens) {
      const salt = generateSalt();
      const h = await hashTokenWithPBKDF2(String(t), salt);
      cur.add(h);
    }
    users[owner] = Array.from(cur);
    await this.putStore(users);
    this.logger.info("users:added", { owner, count: users[owner].length });
    return { ok: true as const, owner, count: users[owner].length };
  }

  /**
   * RPC: Delete all tokens for an owner
   */
  public async deleteOwner(owner: string): Promise<{ ok: true }> {
    owner = String(owner || "").trim();
    if (!owner) throw new Error("owner required");
    const users = await this.getStore();
    if (owner in users) {
      delete users[owner];
      await this.putStore(users);
      this.logger.info("users:owner-deleted", { owner });
    }
    return { ok: true as const };
  }

  /**
   * RPC: Admin authorization with rate limiting.
   * Returns { ok: true } if provided token matches env.AUTH_ADMIN_TOKEN.
   * Applies IP-based rate limiting on failures and clearing on success.
   */
  public async adminAuthorizeOrRateLimit(
    providedToken: string,
    clientIp: string = "unknown"
  ): Promise<{ ok: boolean; status: number; retryAfter?: number }> {
    const admin = this.env.AUTH_ADMIN_TOKEN || "";
    const astore = asTypedStorage<AuthStateSchema>(this.ctx.storage);
    const now = Date.now();
    const key = makeAdminRateLimitKey(clientIp);
    const cur = (await astore.get(key)) as RateLimitEntry | undefined;

    // Schedule cleanup if not already scheduled
    const alarmTime = await this.ctx.storage.getAlarm();
    if (!alarmTime) {
      await this.ctx.storage.setAlarm(Date.now() + 60 * 60 * 1000);
    }

    // Success path: verify token and clear rate limit entry
    if (admin.length > 0 && providedToken === admin) {
      await astore.delete(key);
      return { ok: true, status: 200 };
    }

    // Failure path: apply rate limiting
    if (cur) {
      if (cur.blockedUntil && now < cur.blockedUntil) {
        return {
          ok: false,
          status: 429,
          retryAfter: Math.ceil((cur.blockedUntil - now) / 1000),
        };
      }
      if (now - cur.lastAttempt < RATE_LIMIT.windowMs) {
        if (cur.attempts >= RATE_LIMIT.maxAttempts) {
          cur.blockedUntil = now + RATE_LIMIT.blockDurationMs;
          await astore.put(key, cur);
          return { ok: false, status: 429, retryAfter: RATE_LIMIT.blockDurationMs / 1000 };
        }
      } else {
        cur.attempts = 0;
      }
    }

    // Record failed attempt
    await astore.put(key, {
      attempts: (cur?.attempts || 0) + 1,
      lastAttempt: now,
      blockedUntil: cur?.blockedUntil,
    });
    return { ok: false, status: 401 };
  }

  /**
   * RPC: Delete a token by stored hash
   */
  public async deleteTokenByHash(owner: string, tokenHash: string): Promise<{ ok: true }> {
    owner = String(owner || "").trim();
    if (!owner || !tokenHash) throw new Error("owner and tokenHash required");
    const users = await this.getStore();
    users[owner] = (users[owner] || []).filter((x) => x !== tokenHash);
    if ((users[owner] || []).length === 0) delete users[owner];
    await this.putStore(users);
    this.logger.info("users:tokenhash-deleted", { owner });
    return { ok: true as const };
  }

  /**
   * RPC: Delete a token by verifying a provided plaintext token against stored hashes
   */
  public async deleteToken(owner: string, token: string): Promise<{ ok: true }> {
    owner = String(owner || "").trim();
    token = String(token || "").trim();
    if (!owner || !token) throw new Error("owner and token required");
    const users = await this.getStore();
    const remaining: string[] = [];
    for (const storedHash of users[owner] || []) {
      if (!(await verifyToken(token, storedHash))) remaining.push(storedHash);
    }
    if (remaining.length < (users[owner] || []).length) {
      users[owner] = remaining;
      if (users[owner].length === 0) delete users[owner];
      await this.putStore(users);
      this.logger.info("users:token-deleted", { owner });
    }
    return { ok: true as const };
  }

  private get logger() {
    return createLogger(this.env.LOG_LEVEL, {
      service: "AuthDO",
      doId: this.ctx.id.toString(),
    });
  }
}
