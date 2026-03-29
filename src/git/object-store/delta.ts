export function applyGitDelta(base: Uint8Array, delta: Uint8Array): Uint8Array {
  let pos = 0;

  const readVarint = () => {
    let out = 0;
    let shift = 0;
    while (true) {
      if (pos >= delta.length) throw new Error("delta:truncated-varint");
      const byte = delta[pos++];
      out |= (byte & 0x7f) << shift;
      if (!(byte & 0x80)) return out;
      shift += 7;
    }
  };

  const baseSize = readVarint();
  const resultSize = readVarint();
  if (baseSize !== base.length) throw new Error("delta:base-size-mismatch");

  const out = new Uint8Array(resultSize);
  let outPos = 0;

  while (pos < delta.length) {
    const opcode = delta[pos++];
    if (opcode & 0x80) {
      let copyOffset = 0;
      let copySize = 0;
      if (opcode & 0x01) copyOffset |= delta[pos++];
      if (opcode & 0x02) copyOffset |= delta[pos++] << 8;
      if (opcode & 0x04) copyOffset |= delta[pos++] << 16;
      if (opcode & 0x08) copyOffset |= delta[pos++] << 24;
      if (opcode & 0x10) copySize |= delta[pos++];
      if (opcode & 0x20) copySize |= delta[pos++] << 8;
      if (opcode & 0x40) copySize |= delta[pos++] << 16;
      if (copySize === 0) copySize = 0x10000;
      if (copyOffset < 0 || copyOffset + copySize > base.length) {
        throw new Error("delta:copy-out-of-bounds");
      }
      out.set(base.subarray(copyOffset, copyOffset + copySize), outPos);
      outPos += copySize;
      continue;
    }

    if (opcode === 0) throw new Error("delta:invalid-opcode");
    if (pos + opcode > delta.length) throw new Error("delta:insert-out-of-bounds");
    out.set(delta.subarray(pos, pos + opcode), outPos);
    outPos += opcode;
    pos += opcode;
  }

  if (outPos !== out.length) throw new Error("delta:result-size-mismatch");
  return out;
}
