import type { ReceiveCommand } from "@/git/operations/validation.ts";

export type ReceiveNegotiatedCapabilities = {
  reportStatus: boolean;
  sideBand64k: boolean;
  quiet: boolean;
  atomic: boolean;
  ofsDelta: boolean;
  agent?: string;
};

export type ParsedReceiveRequest = {
  commands: ReceiveCommand[];
  capabilities: ReceiveNegotiatedCapabilities;
};

export type ReceiveCommandList = ReceiveCommand[];

function parseCapabilities(firstLine: string): ReceiveNegotiatedCapabilities {
  const nulIndex = firstLine.indexOf("\0");
  const capabilityText = nulIndex >= 0 ? firstLine.slice(nulIndex + 1).trim() : "";
  const tokens = capabilityText.length > 0 ? capabilityText.split(/\s+/) : [];

  let agent: string | undefined;
  for (const token of tokens) {
    if (token.startsWith("agent=")) {
      agent = token;
      break;
    }
  }

  return {
    reportStatus: tokens.includes("report-status"),
    sideBand64k: tokens.includes("side-band-64k"),
    quiet: tokens.includes("quiet"),
    atomic: tokens.includes("atomic"),
    ofsDelta: tokens.includes("ofs-delta"),
    agent,
  };
}

export function parseReceiveRequest(lines: string[]): ParsedReceiveRequest {
  const commands: ReceiveCommand[] = [];
  const capabilities = parseCapabilities(lines[0] || "");

  for (let index = 0; index < lines.length; index++) {
    let line = lines[index] || "";
    if (index === 0) {
      const nulIndex = line.indexOf("\0");
      if (nulIndex >= 0) {
        line = line.slice(0, nulIndex);
      }
    }

    const parts = line.trim().split(/\s+/);
    if (parts.length < 3) continue;
    commands.push({
      oldOid: parts[0] || "",
      newOid: parts[1] || "",
      ref: parts.slice(2).join(" "),
    });
  }

  return { commands, capabilities };
}
