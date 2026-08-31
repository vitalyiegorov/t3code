/**
 * GitHubAvatarResolver - the last step of project icon discovery: the avatar
 * of a github.com repository owner, for projects no local icon covers.
 * One fetch per owner ever; every failure resolves to null.
 */
import { parseGitHubRepositoryNameWithOwnerFromRemoteUrl } from "@t3tools/shared/git";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Encoding from "effect/Encoding";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import { HttpClient, HttpClientRequest } from "effect/unstable/http";

import * as ServerConfig from "../config.ts";
import * as RepositoryIdentityResolver from "./RepositoryIdentityResolver.ts";

const FETCH_TIMEOUT_MS = 5_000;
const MAX_AVATAR_BYTES = 1024 * 1024;
/** An owner with no avatar is remembered this long before one retry. */
const NEGATIVE_RESULT_TTL_MS = 7 * 24 * 60 * 60 * 1000;
/**
 * A transport failure mutes further fetch attempts for this long, in memory
 * only. An offline machine then pays the connect timeout once per owner, not
 * once per favicon request, and recovers the moment the process restarts or
 * the window lapses.
 */
const TRANSPORT_FAILURE_MUTE_MS = 5 * 60 * 1000;
// The asset route serves these files with their extension as the MIME type
// under nosniff, so only types the clients can render may enter the cache.
const AVATAR_EXTENSIONS_BY_CONTENT_TYPE: Record<string, string> = {
  "image/avif": ".avif",
  "image/gif": ".gif",
  "image/jpeg": ".jpg",
  "image/png": ".png",
  "image/webp": ".webp",
};
const CACHED_AVATAR_EXTENSIONS = Object.values(AVATAR_EXTENSIONS_BY_CONTENT_TYPE);

export class GitHubAvatarResolver extends Context.Service<
  GitHubAvatarResolver,
  {
    /** Absolute path of the cached avatar file, or null when there is none. Never fails. */
    readonly resolvePath: (cwd: string) => Effect.Effect<string | null>;
    /** True for files under this service's cache, so the asset route may serve them as project icons. */
    readonly isManagedPath: (filePath: string) => boolean;
  }
>()("t3/project/GitHubAvatarResolver") {}

export const make = Effect.gen(function* () {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const crypto = yield* Crypto.Crypto;
  const httpClient = yield* HttpClient.HttpClient;
  const config = yield* ServerConfig.ServerConfig;
  const repositoryIdentityResolver = yield* RepositoryIdentityResolver.RepositoryIdentityResolver;
  const semaphore = yield* Semaphore.make(1);
  const transportFailureAtMs = new Map<string, number>();

  const cacheDir = path.join(config.stateDir, "github-avatars");

  const isManagedPath = (filePath: string): boolean => {
    const relative = path.relative(path.resolve(cacheDir), path.resolve(filePath));
    return relative !== "" && !relative.startsWith("..") && !path.isAbsolute(relative);
  };

  const downloadAvatar = Effect.fn("GitHubAvatarResolver.downloadAvatar")(function* (
    owner: string,
  ) {
    // github.com/<owner>.png is the avatar the repository page itself displays;
    // it is not the rate-limited API. A 404 means the owner does not exist; any
    // other non-2xx is transient and must retry on a later request.
    const response = yield* httpClient.execute(
      HttpClientRequest.get(`https://github.com/${encodeURIComponent(owner)}.png`),
    );
    if (response.status === 404) return { _tag: "negative" } as const;
    if (response.status < 200 || response.status >= 300) return { _tag: "retry" } as const;
    // The body is read through the cap, so a response without a trustworthy
    // content-length can never buffer past MAX_AVATAR_BYTES.
    const chunks: Array<Uint8Array> = [];
    let total = 0;
    yield* Stream.runForEachWhile(response.stream, (chunk: Uint8Array) =>
      Effect.sync(() => {
        total += chunk.byteLength;
        if (total > MAX_AVATAR_BYTES) return false;
        chunks.push(chunk);
        return true;
      }),
    );
    if (total === 0 || total > MAX_AVATAR_BYTES) return { _tag: "negative" } as const;
    const contentType = (response.headers["content-type"] ?? "").split(";")[0]?.trim() ?? "";
    const extension = AVATAR_EXTENSIONS_BY_CONTENT_TYPE[contentType];
    // An unmapped type is authoritative absence: cached under a guessed
    // extension it would be served as the wrong MIME type and render broken.
    if (extension === undefined) return { _tag: "negative" } as const;
    const bytes = new Uint8Array(total);
    let offset = 0;
    for (const chunk of chunks) {
      bytes.set(chunk, offset);
      offset += chunk.byteLength;
    }
    return { _tag: "avatar", bytes, extension } as const;
  });

  const fetchAndCache = Effect.fn("GitHubAvatarResolver.fetchAndCache")(function* (
    cacheKey: string,
    owner: string,
  ) {
    const now = yield* Clock.currentTimeMillis;
    yield* fileSystem
      .makeDirectory(cacheDir, { recursive: true })
      .pipe(Effect.catchCause(() => Effect.void));
    // Transport failures, timeouts and transient responses return null without
    // a marker, so one blip never hides an icon for the negative TTL.
    const outcome = yield* downloadAvatar(owner).pipe(
      Effect.timeout(FETCH_TIMEOUT_MS),
      Effect.catchCause(() => Effect.succeed({ _tag: "retry" } as const)),
    );
    if (outcome._tag !== "avatar") {
      if (outcome._tag === "negative") {
        transportFailureAtMs.delete(cacheKey);
        // The miss file carries the marker epoch, so the negative TTL survives
        // restarts and is immune to clock jumps between write and read.
        yield* fileSystem
          .writeFileString(path.join(cacheDir, `${cacheKey}.miss`), String(now))
          .pipe(Effect.catchCause(() => Effect.void));
      } else {
        transportFailureAtMs.set(cacheKey, now);
      }
      return null;
    }
    transportFailureAtMs.delete(cacheKey);
    const targetPath = path.join(cacheDir, `${cacheKey}${outcome.extension}`);
    const temporaryPath = `${targetPath}.tmp`;
    const written = yield* fileSystem.writeFile(temporaryPath, outcome.bytes).pipe(Effect.option);
    const renamed =
      Option.isSome(written) &&
      Option.isSome(yield* fileSystem.rename(temporaryPath, targetPath).pipe(Effect.option));
    if (!renamed) return null;
    return targetPath;
  });

  const cachedAvatar = Effect.fn("GitHubAvatarResolver.cachedAvatar")(function* (cacheKey: string) {
    for (const extension of CACHED_AVATAR_EXTENSIONS) {
      const cachedPath = path.join(cacheDir, `${cacheKey}${extension}`);
      const info = yield* fileSystem.stat(cachedPath).pipe(Effect.option);
      if (Option.isSome(info) && info.value.type === "File") {
        return { _tag: "hit", path: cachedPath } as const;
      }
    }
    const now = yield* Clock.currentTimeMillis;
    // A recent transport failure answers as a fresh negative: null, no fetch.
    const mutedAtMs = transportFailureAtMs.get(cacheKey);
    if (mutedAtMs !== undefined && now - mutedAtMs < TRANSPORT_FAILURE_MUTE_MS) {
      return { _tag: "negative" } as const;
    }
    const miss = yield* fileSystem
      .readFileString(path.join(cacheDir, `${cacheKey}.miss`))
      .pipe(Effect.option);
    if (Option.isSome(miss)) {
      const markedAtMs = Number(miss.value);
      if (Number.isFinite(markedAtMs) && now - markedAtMs < NEGATIVE_RESULT_TTL_MS) {
        return { _tag: "negative" } as const;
      }
    }
    return { _tag: "miss" } as const;
  });

  const resolvePath = Effect.fn("GitHubAvatarResolver.resolvePath")(function* (cwd: string) {
    const identity = yield* repositoryIdentityResolver.resolve(cwd);
    const nameWithOwner =
      identity === null
        ? null
        : parseGitHubRepositoryNameWithOwnerFromRemoteUrl(identity.locator.remoteUrl);
    if (nameWithOwner === null) return null;
    const owner = nameWithOwner.split("/")[0];
    if (!owner) return null;

    // The avatar is an attribute of the owner, so every repository of that
    // owner shares one cache entry and one fetch. GitHub treats owner names
    // case-insensitively; remotes disagree on casing, the key must not.
    const digest = yield* crypto.digest("SHA-256", new TextEncoder().encode(owner.toLowerCase()));
    const cacheKey = Encoding.encodeHex(digest);

    // Hits, fresh negatives and muted failures answer without the permit, so a
    // reconnect burst of already-cached projects never queues behind a slow
    // first fetch (#7536).
    const fast = yield* cachedAvatar(cacheKey);
    if (fast._tag !== "miss") return fast._tag === "hit" ? fast.path : null;
    const decided = Effect.gen(function* () {
      const rechecked = yield* cachedAvatar(cacheKey);
      if (rechecked._tag !== "miss") return rechecked._tag === "hit" ? rechecked.path : null;
      return yield* fetchAndCache(cacheKey, owner);
    });
    return yield* semaphore.withPermits(1)(decided);
  });

  return GitHubAvatarResolver.of({
    resolvePath: (cwd) => resolvePath(cwd).pipe(Effect.catchCause(() => Effect.succeed(null))),
    isManagedPath,
  });
});

export const layer = Layer.effect(GitHubAvatarResolver, make);
