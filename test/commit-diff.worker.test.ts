import { describe, it, expect } from "vitest";
import { env, SELF } from "cloudflare:test";
import type { RepoDurableObject } from "@/index";
import { encodeGitObjectAndDeflate, listCommitChangedFiles, readCommitFilePatch } from "@/git";
import { uniqueRepoId, runDOWithRetry } from "./util/test-helpers.ts";

type TreeSpec = {
  mode: string;
  name: string;
  oid: string;
};

async function putObject(
  getStub: () => DurableObjectStub<RepoDurableObject>,
  oid: string,
  zdata: Uint8Array
): Promise<void> {
  await runDOWithRetry(getStub, async (instance: RepoDurableObject) => {
    await instance.putLooseObject(oid, zdata);
  });
}

function hexToBytes(hex: string): Uint8Array {
  const out = new Uint8Array(hex.length / 2);
  for (let i = 0; i < out.length; i++) {
    out[i] = Number.parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  }
  return out;
}

async function createBlob(content: string): Promise<{ oid: string; zdata: Uint8Array }> {
  return await encodeGitObjectAndDeflate("blob", new TextEncoder().encode(content));
}

async function createTree(entries: TreeSpec[]): Promise<{ oid: string; zdata: Uint8Array }> {
  const encoder = new TextEncoder();
  const chunks: Uint8Array[] = [];
  let total = 0;
  for (const entry of [...entries].sort((a, b) => a.name.localeCompare(b.name))) {
    const head = encoder.encode(`${entry.mode} ${entry.name}\0`);
    const oidBytes = hexToBytes(entry.oid);
    const chunk = new Uint8Array(head.length + oidBytes.length);
    chunk.set(head, 0);
    chunk.set(oidBytes, head.length);
    chunks.push(chunk);
    total += chunk.length;
  }
  const payload = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    payload.set(chunk, offset);
    offset += chunk.length;
  }
  return await encodeGitObjectAndDeflate("tree", payload);
}

async function createCommit(args: {
  treeOid: string;
  parents?: string[];
  message: string;
}): Promise<{ oid: string; zdata: Uint8Array }> {
  const author = "You <you@example.com> 0 +0000";
  const parentLines = (args.parents || []).map((parent) => `parent ${parent}\n`).join("");
  const payload = new TextEncoder().encode(
    `tree ${args.treeOid}\n${parentLines}author ${author}\ncommitter ${author}\n\n${args.message}\n`
  );
  return await encodeGitObjectAndDeflate("commit", payload);
}

async function setMainRef(
  getStub: () => DurableObjectStub<RepoDurableObject>,
  oid: string
): Promise<void> {
  await runDOWithRetry(getStub, async (instance: RepoDurableObject) => {
    await instance.setRefs([{ name: "refs/heads/main", oid }]);
    await instance.setHead({ target: "refs/heads/main" });
  });
}

describe("commit diff v1", () => {
  it("lists added files for a root commit", async () => {
    const owner = "o";
    const repo = uniqueRepoId("r-diff-root");
    const repoId = `${owner}/${repo}`;
    const id = env.REPO_DO.idFromName(repoId);
    const getStub = () => env.REPO_DO.get(id) as DurableObjectStub<RepoDurableObject>;

    const readme = await createBlob("hello\n");
    await putObject(getStub, readme.oid, readme.zdata);

    const tree = await createTree([{ mode: "100644", name: "README.md", oid: readme.oid }]);
    await putObject(getStub, tree.oid, tree.zdata);

    const commit = await createCommit({ treeOid: tree.oid, message: "root" });
    await putObject(getStub, commit.oid, commit.zdata);
    await setMainRef(getStub, commit.oid);

    const diff = await listCommitChangedFiles(env as Env, repoId, commit.oid);

    expect(diff.compareMode).toBe("root");
    expect(diff.baseCommitOid).toBeUndefined();
    expect(diff.truncated).toBe(false);
    expect(diff.total).toBe(1);
    expect(diff.added).toBe(1);
    expect(diff.modified).toBe(0);
    expect(diff.deleted).toBe(0);
    expect(diff.entries).toEqual([
      {
        path: "README.md",
        changeType: "A",
        oldOid: undefined,
        newOid: readme.oid,
        oldMode: undefined,
        newMode: "100644",
      },
    ]);
  });

  it("detects modifications and file-to-directory transitions", async () => {
    const owner = "o";
    const repo = uniqueRepoId("r-diff-transition");
    const repoId = `${owner}/${repo}`;
    const id = env.REPO_DO.idFromName(repoId);
    const getStub = () => env.REPO_DO.get(id) as DurableObjectStub<RepoDurableObject>;

    const oldFile = await createBlob("before\n");
    const oldConfig = await createBlob("legacy\n");
    await putObject(getStub, oldFile.oid, oldFile.zdata);
    await putObject(getStub, oldConfig.oid, oldConfig.zdata);

    const baseTree = await createTree([
      { mode: "100644", name: "app.txt", oid: oldFile.oid },
      { mode: "100644", name: "config", oid: oldConfig.oid },
    ]);
    await putObject(getStub, baseTree.oid, baseTree.zdata);

    const baseCommit = await createCommit({ treeOid: baseTree.oid, message: "base" });
    await putObject(getStub, baseCommit.oid, baseCommit.zdata);

    const newFile = await createBlob("after\n");
    const nestedConfig = await createBlob("nested\n");
    await putObject(getStub, newFile.oid, newFile.zdata);
    await putObject(getStub, nestedConfig.oid, nestedConfig.zdata);

    const configDir = await createTree([
      { mode: "100644", name: "settings.json", oid: nestedConfig.oid },
    ]);
    await putObject(getStub, configDir.oid, configDir.zdata);

    const nextTree = await createTree([
      { mode: "100644", name: "app.txt", oid: newFile.oid },
      { mode: "40000", name: "config", oid: configDir.oid },
    ]);
    await putObject(getStub, nextTree.oid, nextTree.zdata);

    const nextCommit = await createCommit({
      treeOid: nextTree.oid,
      parents: [baseCommit.oid],
      message: "next",
    });
    await putObject(getStub, nextCommit.oid, nextCommit.zdata);
    await setMainRef(getStub, nextCommit.oid);

    const diff = await listCommitChangedFiles(env as Env, repoId, nextCommit.oid);

    expect(diff.compareMode).toBe("first-parent");
    expect(diff.baseCommitOid).toBe(baseCommit.oid);
    expect(diff.total).toBe(3);
    expect(diff.modified).toBe(1);
    expect(diff.added).toBe(1);
    expect(diff.deleted).toBe(1);
    expect(
      diff.entries.map((entry) => ({ path: entry.path, changeType: entry.changeType }))
    ).toEqual([
      { path: "app.txt", changeType: "M" },
      { path: "config", changeType: "D" },
      { path: "config/settings.json", changeType: "A" },
    ]);
  });

  it("truncates when maxFiles is exceeded", async () => {
    const owner = "o";
    const repo = uniqueRepoId("r-diff-truncate-files");
    const repoId = `${owner}/${repo}`;
    const id = env.REPO_DO.idFromName(repoId);
    const getStub = () => env.REPO_DO.get(id) as DurableObjectStub<RepoDurableObject>;

    const alpha = await createBlob("a\n");
    const beta = await createBlob("b\n");
    const gamma = await createBlob("c\n");
    await putObject(getStub, alpha.oid, alpha.zdata);
    await putObject(getStub, beta.oid, beta.zdata);
    await putObject(getStub, gamma.oid, gamma.zdata);

    const tree = await createTree([
      { mode: "100644", name: "alpha.txt", oid: alpha.oid },
      { mode: "100644", name: "beta.txt", oid: beta.oid },
      { mode: "100644", name: "gamma.txt", oid: gamma.oid },
    ]);
    await putObject(getStub, tree.oid, tree.zdata);

    const commit = await createCommit({ treeOid: tree.oid, message: "root" });
    await putObject(getStub, commit.oid, commit.zdata);

    const diff = await listCommitChangedFiles(env as Env, repoId, commit.oid, undefined, {
      maxFiles: 2,
    });

    expect(diff.truncated).toBe(true);
    expect(diff.truncateReason).toBe("max_files");
    expect(diff.total).toBe(2);
    expect(diff.entries).toHaveLength(2);
  });

  it("truncates when the time budget is exceeded", async () => {
    const owner = "o";
    const repo = uniqueRepoId("r-diff-truncate-time");
    const repoId = `${owner}/${repo}`;
    const id = env.REPO_DO.idFromName(repoId);
    const getStub = () => env.REPO_DO.get(id) as DurableObjectStub<RepoDurableObject>;

    const alpha = await createBlob("a\n");
    const beta = await createBlob("b\n");
    await putObject(getStub, alpha.oid, alpha.zdata);
    await putObject(getStub, beta.oid, beta.zdata);

    const tree = await createTree([
      { mode: "100644", name: "alpha.txt", oid: alpha.oid },
      { mode: "100644", name: "beta.txt", oid: beta.oid },
    ]);
    await putObject(getStub, tree.oid, tree.zdata);

    const commit = await createCommit({ treeOid: tree.oid, message: "root" });
    await putObject(getStub, commit.oid, commit.zdata);

    const realNow = Date.now;
    let tick = 0;
    Date.now = () => {
      tick += 5;
      return tick;
    };
    try {
      const diff = await listCommitChangedFiles(env as Env, repoId, commit.oid, undefined, {
        timeBudgetMs: 1,
      });
      expect(diff.truncated).toBe(true);
      expect(diff.truncateReason).toBe("time_budget");
    } finally {
      Date.now = realNow;
    }
  });

  it("generates a lazy patch for a modified text file", async () => {
    const owner = "o";
    const repo = uniqueRepoId("r-diff-patch");
    const repoId = `${owner}/${repo}`;
    const id = env.REPO_DO.idFromName(repoId);
    const getStub = () => env.REPO_DO.get(id) as DurableObjectStub<RepoDurableObject>;

    const before = await createBlob("before\nshared\n");
    const after = await createBlob("after\nshared\n");
    await putObject(getStub, before.oid, before.zdata);
    await putObject(getStub, after.oid, after.zdata);

    const baseTree = await createTree([{ mode: "100644", name: "note.txt", oid: before.oid }]);
    await putObject(getStub, baseTree.oid, baseTree.zdata);
    const baseCommit = await createCommit({ treeOid: baseTree.oid, message: "base" });
    await putObject(getStub, baseCommit.oid, baseCommit.zdata);

    const headTree = await createTree([{ mode: "100644", name: "note.txt", oid: after.oid }]);
    await putObject(getStub, headTree.oid, headTree.zdata);
    const headCommit = await createCommit({
      treeOid: headTree.oid,
      parents: [baseCommit.oid],
      message: "head",
    });
    await putObject(getStub, headCommit.oid, headCommit.zdata);

    const patch = await readCommitFilePatch(env as Env, repoId, headCommit.oid, "note.txt");

    expect(patch.changeType).toBe("M");
    expect(patch.skipped).toBeUndefined();
    expect(patch.patch).toContain("--- a/note.txt");
    expect(patch.patch).toContain("+++ b/note.txt");
    expect(patch.patch).toContain("-before");
    expect(patch.patch).toContain("+after");
  });

  it("skips binary patch previews", async () => {
    const owner = "o";
    const repo = uniqueRepoId("r-diff-binary");
    const repoId = `${owner}/${repo}`;
    const id = env.REPO_DO.idFromName(repoId);
    const getStub = () => env.REPO_DO.get(id) as DurableObjectStub<RepoDurableObject>;

    const before = await createBlob("\u0000old");
    const after = await createBlob("\u0000new");
    await putObject(getStub, before.oid, before.zdata);
    await putObject(getStub, after.oid, after.zdata);

    const baseTree = await createTree([{ mode: "100644", name: "data.bin", oid: before.oid }]);
    await putObject(getStub, baseTree.oid, baseTree.zdata);
    const baseCommit = await createCommit({ treeOid: baseTree.oid, message: "base" });
    await putObject(getStub, baseCommit.oid, baseCommit.zdata);

    const headTree = await createTree([{ mode: "100644", name: "data.bin", oid: after.oid }]);
    await putObject(getStub, headTree.oid, headTree.zdata);
    const headCommit = await createCommit({
      treeOid: headTree.oid,
      parents: [baseCommit.oid],
      message: "head",
    });
    await putObject(getStub, headCommit.oid, headCommit.zdata);

    const patch = await readCommitFilePatch(env as Env, repoId, headCommit.oid, "data.bin");

    expect(patch.binary).toBe(true);
    expect(patch.skipped).toBe(true);
    expect(patch.skipReason).toBe("binary");
    expect(patch.patch).toBeUndefined();
  });

  it("returns lazy patch JSON for a commit path", async () => {
    const owner = "o";
    const repo = uniqueRepoId("r-diff-route-patch");
    const repoId = `${owner}/${repo}`;
    const id = env.REPO_DO.idFromName(repoId);
    const getStub = () => env.REPO_DO.get(id) as DurableObjectStub<RepoDurableObject>;

    const readme = await createBlob("hello\n");
    await putObject(getStub, readme.oid, readme.zdata);

    const tree = await createTree([{ mode: "100644", name: "README.md", oid: readme.oid }]);
    await putObject(getStub, tree.oid, tree.zdata);
    const commit = await createCommit({ treeOid: tree.oid, message: "root" });
    await putObject(getStub, commit.oid, commit.zdata);
    await setMainRef(getStub, commit.oid);

    const res = await SELF.fetch(
      `https://example.com/${owner}/${repo}/commit/${commit.oid}/diff?path=${encodeURIComponent("README.md")}`
    );

    expect(res.status).toBe(200);
    expect(res.headers.get("Content-Type")).toContain("application/json");

    const payload = (await res.json()) as {
      path: string;
      changeType: string;
      patch?: string;
    };
    expect(payload.path).toBe("README.md");
    expect(payload.changeType).toBe("A");
    expect(payload.patch).toContain("--- /dev/null");
    expect(payload.patch).toContain("+++ b/README.md");
    expect(payload.patch).toContain("+hello");
  });

  it("commit page renders files changed and first-parent note for merge commits", async () => {
    const owner = "o";
    const repo = uniqueRepoId("r-diff-route");
    const repoId = `${owner}/${repo}`;
    const id = env.REPO_DO.idFromName(repoId);
    const getStub = () => env.REPO_DO.get(id) as DurableObjectStub<RepoDurableObject>;

    const alpha = await createBlob("alpha\n");
    const beta = await createBlob("beta\n");
    await putObject(getStub, alpha.oid, alpha.zdata);
    await putObject(getStub, beta.oid, beta.zdata);

    const rootTree = await createTree([]);
    await putObject(getStub, rootTree.oid, rootTree.zdata);

    const rootCommit = await createCommit({ treeOid: rootTree.oid, message: "root" });
    await putObject(getStub, rootCommit.oid, rootCommit.zdata);

    const firstParentTree = await createTree([
      { mode: "100644", name: "alpha.txt", oid: alpha.oid },
    ]);
    await putObject(getStub, firstParentTree.oid, firstParentTree.zdata);
    const firstParentCommit = await createCommit({
      treeOid: firstParentTree.oid,
      parents: [rootCommit.oid],
      message: "first",
    });
    await putObject(getStub, firstParentCommit.oid, firstParentCommit.zdata);

    const secondParentTree = await createTree([
      { mode: "100644", name: "beta.txt", oid: beta.oid },
    ]);
    await putObject(getStub, secondParentTree.oid, secondParentTree.zdata);
    const secondParentCommit = await createCommit({
      treeOid: secondParentTree.oid,
      parents: [rootCommit.oid],
      message: "second",
    });
    await putObject(getStub, secondParentCommit.oid, secondParentCommit.zdata);

    const mergeTree = await createTree([
      { mode: "100644", name: "alpha.txt", oid: alpha.oid },
      { mode: "100644", name: "beta.txt", oid: beta.oid },
    ]);
    await putObject(getStub, mergeTree.oid, mergeTree.zdata);
    const mergeCommit = await createCommit({
      treeOid: mergeTree.oid,
      parents: [firstParentCommit.oid, secondParentCommit.oid],
      message: "merge",
    });
    await putObject(getStub, mergeCommit.oid, mergeCommit.zdata);
    await setMainRef(getStub, mergeCommit.oid);

    const res = await SELF.fetch(`https://example.com/${owner}/${repo}/commit/${mergeCommit.oid}`);
    expect(res.status).toBe(200);
    expect(res.headers.get("Content-Type")).toContain("text/html");

    const html = await res.text();
    expect(html).toContain("Files changed");
    expect(html).toContain("Compared against first parent");
    expect(html).toContain("Show patch");
    expect(html).toContain("beta.txt");
    expect(html).toContain(
      `/${owner}/${repo}/blob?ref=${encodeURIComponent(mergeCommit.oid)}&amp;path=beta.txt`
    );
  });
});
