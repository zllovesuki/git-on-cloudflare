import {
  buildPackV2,
  encodeGitObjectAndDeflate,
  indexPackOnly,
  inflateAndParseHeader,
} from "@/git/index.ts";
import { r2PackKey } from "@/keys.ts";

import { asTypedStorage, objKey } from "../repoState.ts";
import type { RepoStateSchema } from "../repoState.ts";
import { getDb, insertPackOids } from "../db/index.ts";

export async function seedMinimalRepoState(args: {
  ctx: DurableObjectState;
  env: Env;
  prefix: string;
  withPack: boolean;
}): Promise<{ commitOid: string; treeOid: string }> {
  const store = asTypedStorage<RepoStateSchema>(args.ctx.storage);
  const db = getDb(args.ctx.storage);

  const treeContent = new Uint8Array(0);
  const { oid: treeOid, zdata: treeZ } = await encodeGitObjectAndDeflate("tree", treeContent);

  const author = `You <you@example.com> 0 +0000`;
  const message = "initial\n";
  const commitPayload =
    `tree ${treeOid}\n` + `author ${author}\n` + `committer ${author}\n` + `\n${message}`;
  const { oid: commitOid, zdata: commitZ } = await encodeGitObjectAndDeflate(
    "commit",
    new TextEncoder().encode(commitPayload)
  );

  if (args.withPack) {
    const treeParsed = await inflateAndParseHeader(treeZ);
    const commitParsed = await inflateAndParseHeader(commitZ);
    if (!treeParsed || !commitParsed) {
      throw new Error("Failed to parse minimal repo objects");
    }

    const packData = await buildPackV2([
      { type: treeParsed.type, payload: treeParsed.payload },
      { type: commitParsed.type, payload: commitParsed.payload },
    ]);

    const packKey = r2PackKey(args.prefix, `pack-test-${Date.now()}.pack`);
    await args.env.REPO_BUCKET.put(packKey, packData);
    const packOids = await indexPackOnly(packData, args.env, packKey, args.ctx, args.prefix);

    await store.put("lastPackKey", packKey);
    await store.put("lastPackOids", packOids);
    await store.put("packList", [packKey]);

    try {
      await insertPackOids(db, packKey, packOids);
    } catch {}

    await store.put(objKey(treeOid), treeZ);
    await store.put(objKey(commitOid), commitZ);
  } else {
    await store.put(objKey(treeOid), treeZ);
    await store.put(objKey(commitOid), commitZ);
  }

  await store.put("refs", [{ name: "refs/heads/main", oid: commitOid }]);
  await store.put("head", { target: "refs/heads/main" });

  return { treeOid, commitOid };
}
