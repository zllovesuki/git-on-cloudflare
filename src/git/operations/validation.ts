export type ReceiveCommand = {
  oldOid: string;
  newOid: string;
  ref: string;
};

export type ReceiveStatus = {
  ref: string;
  ok: boolean;
  msg?: string;
};

const ZERO_OID_RE = /^0{40}$/i;

export function isValidRefName(name: string): boolean {
  if (!name || name === "HEAD") return false;
  if (!name.startsWith("refs/")) return false;
  if (/[\x00-\x20~^:?*\[]/.test(name)) return false;
  if (name.includes("//") || name.startsWith("/") || name.endsWith("/")) return false;
  if (name.split("/").some((component) => component === "." || component === "..")) return false;
  if (name.endsWith(".") || name.endsWith(".lock")) return false;
  if (name.includes("@{")) return false;
  if (name.includes("\\")) return false;
  return true;
}

export function validateReceiveCommands(
  currentRefs: Array<{ name: string; oid: string }>,
  commands: ReceiveCommand[]
): ReceiveStatus[] {
  const refMap = new Map(currentRefs.map((ref) => [ref.name, ref.oid] as const));
  const statuses: ReceiveStatus[] = [];

  for (const command of commands) {
    const current = refMap.get(command.ref);
    const isZeroOld = ZERO_OID_RE.test(command.oldOid);
    const isZeroNew = ZERO_OID_RE.test(command.newOid);

    if (isZeroNew) {
      if (!current) {
        statuses.push({ ref: command.ref, ok: false, msg: "no such ref" });
        continue;
      }
      if (current.toLowerCase() !== command.oldOid.toLowerCase()) {
        statuses.push({ ref: command.ref, ok: false, msg: "stale old-oid" });
        continue;
      }
      statuses.push({ ref: command.ref, ok: true });
      continue;
    }

    if (!current) {
      if (!isZeroOld) {
        statuses.push({ ref: command.ref, ok: false, msg: "expected zero old-oid" });
        continue;
      }
      statuses.push({ ref: command.ref, ok: true });
      continue;
    }

    if (current.toLowerCase() !== command.oldOid.toLowerCase()) {
      statuses.push({ ref: command.ref, ok: false, msg: "stale old-oid" });
      continue;
    }

    statuses.push({ ref: command.ref, ok: true });
  }

  return statuses;
}

export function applyReceiveCommands(
  currentRefs: Array<{ name: string; oid: string }>,
  commands: ReceiveCommand[]
): Array<{ name: string; oid: string }> {
  const refMap = new Map(currentRefs.map((ref) => [ref.name, ref.oid] as const));
  for (const command of commands) {
    if (ZERO_OID_RE.test(command.newOid)) {
      refMap.delete(command.ref);
      continue;
    }
    refMap.set(command.ref, command.newOid);
  }
  return Array.from(refMap, ([name, oid]) => ({ name, oid }));
}
