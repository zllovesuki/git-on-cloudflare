// Utility formatting and content helpers

/**
 * Escapes HTML special characters to prevent XSS
 * @param s - String to escape
 * @returns HTML-safe string
 */
export function escapeHtml(s: string): string {
  return s.replace(
    /[&<>"]/g,
    (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" })[c] as string
  );
}

/**
 * Detects if content is binary by checking for non-text bytes
 * @param bytes - Content to check
 * @returns True if content appears to be binary
 * @note Checks first 8KB for null bytes or control characters
 */
export function detectBinary(bytes: Uint8Array): boolean {
  // Check first 8KB for null bytes or non-text characters
  const checkLength = Math.min(8192, bytes.length);
  for (let i = 0; i < checkLength; i++) {
    const byte = bytes[i];
    // Null byte or control characters (except tab, newline, carriage return)
    if (byte === 0 || (byte < 32 && byte !== 9 && byte !== 10 && byte !== 13)) {
      return true;
    }
  }
  return false;
}

/**
 * Formats byte size into human-readable string
 * @param bytes - Size in bytes
 * @returns Formatted string (e.g., "1.5 MB")
 */
export function formatSize(bytes: number): string {
  if (bytes < 1024) return bytes + " bytes";
  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + " KB";
  return (bytes / (1024 * 1024)).toFixed(1) + " MB";
}

/**
 * Converts byte array to text, handling various encodings
 * @param bytes - Raw bytes to decode
 * @returns Decoded text string
 * @note Handles UTF-8, UTF-16 LE/BE with BOM detection
 */
export function bytesToText(bytes: Uint8Array): string {
  if (!bytes || bytes.byteLength === 0) return "";
  // UTF-8 BOM
  if (bytes.length >= 3 && bytes[0] === 0xef && bytes[1] === 0xbb && bytes[2] === 0xbf) {
    return new TextDecoder("utf-8").decode(bytes.subarray(3));
  }
  // UTF-16 LE BOM
  if (bytes.length >= 2 && bytes[0] === 0xff && bytes[1] === 0xfe) {
    try {
      return new TextDecoder("utf-16le").decode(bytes.subarray(2));
    } catch {}
  }
  // UTF-16 BE BOM
  if (bytes.length >= 2 && bytes[0] === 0xfe && bytes[1] === 0xff) {
    try {
      return new TextDecoder("utf-16be").decode(bytes.subarray(2));
    } catch {}
  }
  // Default to UTF-8
  try {
    return new TextDecoder("utf-8", { fatal: true, ignoreBOM: false }).decode(bytes);
  } catch {
    return "(binary content)";
  }
}

export function formatWhen(epochSeconds: number, tz: string): string {
  try {
    const offsetMatch = tz.match(/^([+-])(\d{2})(\d{2})$/);
    const utcDate = new Date(epochSeconds * 1000);
    if (!offsetMatch) {
      const iso = utcDate.toISOString();
      const noMs = iso.replace(/\.\d{3}Z$/, "Z");
      return noMs.replace("T", " ").replace("Z", " UTC");
    }

    const sign = offsetMatch[1] === "-" ? -1 : 1;
    const hours = Number(offsetMatch[2]);
    const minutes = Number(offsetMatch[3]);
    const offsetMinutes = sign * (hours * 60 + minutes);
    const localDate = new Date(utcDate.getTime() + offsetMinutes * 60_000);
    const pad = (value: number) => String(value).padStart(2, "0");

    return (
      `${localDate.getUTCFullYear()}-${pad(localDate.getUTCMonth() + 1)}-` +
      `${pad(localDate.getUTCDate())} ${pad(localDate.getUTCHours())}:` +
      `${pad(localDate.getUTCMinutes())}:${pad(localDate.getUTCSeconds())} ${tz}`
    );
  } catch {
    return String(epochSeconds);
  }
}

export type FileIconName =
  | "code"
  | "database"
  | "diff"
  | "file"
  | "folder"
  | "image"
  | "spreadsheet"
  | "terminal"
  | "text";

/**
 * Determines the appropriate icon name for a file based on its extension
 * @param filename - The name of the file
 * @returns File icon name
 */
export function getFileIconName(filename: string): FileIconName {
  const defaultIcon: FileIconName = "file";

  // Get file extension (lowercase, without dot)
  const ext = filename.split(".").pop()?.toLowerCase() || "";

  // Map extensions to Bootstrap Icon classes
  // Using specific filetype-* icons where available, file-earmark-* for others
  const iconMap: Record<string, FileIconName> = {
    // JavaScript/TypeScript - specific filetype icons
    js: "code",
    mjs: "code",
    cjs: "code",
    jsx: "code",
    ts: "code",
    tsx: "code",

    // Web technologies - specific filetype icons
    html: "code",
    htm: "code",
    css: "code",
    scss: "code",
    sass: "code",
    less: "code",
    vue: "code",
    svelte: "code",

    // Programming languages - specific filetype icons where available
    py: "code",
    rb: "code",
    go: "code",
    java: "code",
    c: "code",
    h: "code",
    cc: "code",
    cpp: "code",
    cxx: "code",
    hpp: "code",
    cs: "code",
    php: "code",
    swift: "code",
    kt: "code",
    rs: "code",
    lua: "code",
    dart: "code",
    mm: "code",
    pl: "code",

    // Shell scripts - specific filetype icon
    sh: "terminal",
    bash: "terminal",
    zsh: "terminal",
    ps1: "terminal",
    psm1: "terminal",

    // Config/data files - specific filetype icons where available
    json: "code",
    jsonc: "code",
    xml: "code",
    yaml: "code",
    yml: "code",
    sql: "database",
    toml: "code",
    ini: "code",
    cfg: "code",
    conf: "code",
    diff: "diff",
    gradle: "code",
    proto: "code",

    // Text/Documentation files - specific filetype icons where available
    txt: "text",
    md: "text",
    markdown: "text",
    mdx: "text",
    rst: "text",
    tex: "text",
    log: "text",

    // Image files - specific filetype icons where available
    jpg: "image",
    jpeg: "image",
    png: "image",
    gif: "image",
    svg: "image",
    webp: "image",
    ico: "image",
    bmp: "image",
    tiff: "image",
    tif: "image",
    psd: "image",
    ai: "image",
    raw: "image",

    // Documents - specific filetype icons where available
    pdf: "text",
    doc: "text",
    docx: "text",
    xls: "spreadsheet",
    xlsx: "spreadsheet",
    csv: "spreadsheet",
    ppt: "text",
    pptx: "text",
    odt: "text",
    ods: "spreadsheet",
    odp: "text",

    // Archives - file-earmark-zip for all
    zip: "file",
    rar: "file",
    tar: "file",
    gz: "file",
    "7z": "file",
    bz2: "file",
    xz: "file",

    // Media files - specific filetype icons where available
    mp3: "file",
    mp4: "file",
    wav: "file",
    aac: "file",
    m4p: "file",
    m4a: "file",
    ogg: "file",
    flac: "file",

    avi: "file",
    mkv: "file",
    mov: "file",
    wmv: "file",
    webm: "file",
    flv: "file",

    // Font files - specific filetype icons where available
    ttf: "file",
    otf: "file",
    woff: "file",
    woff2: "file",
    eot: "file",

    // Binary/executable files
    exe: "file",
    dll: "file",
    so: "file",
    dylib: "file",
    wasm: "file",
    key: "file",
    heic: "image",
    bin: "file",
  };

  // Special cases for files without extensions or with special names
  const specialFiles: Record<string, FileIconName> = {
    readme: "text",
    license: "text",
    dockerfile: "code",
    makefile: "code",
    gemfile: "code",
    rakefile: "code",
    guardfile: "code",
    procfile: "code",
    gitignore: "code",
    gitattributes: "code",
    editorconfig: "code",
    "package-lock": "file",
    "yarn.lock": "file",
    "pnpm-lock": "file",
    tsconfig: "code",
    webpack: "code",
    babel: "code",
    eslint: "code",
    prettier: "code",
  };

  // Check special files first (case-insensitive)
  const filenameLower = filename.toLowerCase();
  for (const [pattern, icon] of Object.entries(specialFiles)) {
    if (filenameLower.includes(pattern)) {
      return icon;
    }
  }

  // Check extension mapping
  return iconMap[ext] || defaultIcon;
}
