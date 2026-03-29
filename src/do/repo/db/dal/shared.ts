/**
 * Shared helpers for repo SQLite DAL modules.
 */

/**
 * Safe maximum bound parameters per SQLite query on the platform.
 * Keep some headroom below the documented 100 to account for potential
 * extra bound params Drizzle may include.
 */
export const SQLITE_PARAM_LIMIT = 100;
export const SAFE_ROWS_2COL = 45; // 2 columns per row -> 90 params (< 100)
export const SAFE_ROWS_3COL = 30; // 3 columns per row -> 90 params (< 100)
export const SAFE_ROWS_1COL = 80; // 1 column per row -> 80 params (< 100)

/**
 * Normalize a pack key to its basename (for example `pack-123.pack`).
 * SQLite stores basenames only to avoid duplicating the full repo prefix.
 */
export function normalizePackKey(key: string): string {
  if (!key) return key;
  const slash = key.lastIndexOf("/");
  return slash >= 0 ? key.slice(slash + 1) : key;
}
