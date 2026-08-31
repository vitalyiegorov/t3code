import * as NodeServices from "@effect/platform-node/NodeServices";
import type { RepositoryIdentity } from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as TestClock from "effect/testing/TestClock";
import { FetchHttpClient } from "effect/unstable/http";

import * as ServerConfig from "../config.ts";
import * as GitHubAvatarResolver from "./GitHubAvatarResolver.ts";
import * as RepositoryIdentityResolver from "./RepositoryIdentityResolver.ts";

const configLayer = ServerConfig.ServerConfig.layerTest(process.cwd(), {
  prefix: "t3-github-avatar-test-",
});

const identity: RepositoryIdentity = {
  canonicalKey: "github.com/trinodb/trino",
  locator: {
    source: "git-remote",
    remoteName: "origin",
    remoteUrl: "https://github.com/trinodb/trino.git",
  },
};

const gitlabIdentity: RepositoryIdentity = {
  canonicalKey: "gitlab.com/owner/repo",
  locator: {
    source: "git-remote",
    remoteName: "origin",
    remoteUrl: "https://gitlab.com/owner/repo.git",
  },
};

const pngBytes = new Uint8Array([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]);

const makeLayer = (input: {
  readonly identity: RepositoryIdentity | null | ((cwd: string) => RepositoryIdentity | null);
  readonly count: { value: number };
  readonly response: () => Response | Promise<Response>;
}) =>
  GitHubAvatarResolver.layer.pipe(
    Layer.provide(
      Layer.succeed(
        RepositoryIdentityResolver.RepositoryIdentityResolver,
        RepositoryIdentityResolver.RepositoryIdentityResolver.of({
          resolve: (cwd) =>
            Effect.succeed(
              typeof input.identity === "function" ? input.identity(cwd) : input.identity,
            ),
        }),
      ),
    ),
    Layer.provide(
      FetchHttpClient.layer.pipe(
        Layer.provide(
          Layer.succeed(FetchHttpClient.Fetch, ((url: Parameters<typeof fetch>[0]) => {
            input.count.value += 1;
            const response = input.response();
            return response === undefined || response === null
              ? Promise.reject(new TypeError(`unrouted request: ${String(url)}`))
              : Promise.resolve(response);
          }) as typeof fetch),
        ),
      ),
    ),
    Layer.provideMerge(configLayer),
    Layer.provideMerge(NodeServices.layer),
  );

describe("GitHubAvatarResolver", () => {
  it.effect("fetches and caches the owner avatar once", () => {
    const count = { value: 0 };
    return Effect.gen(function* () {
      const resolver = yield* GitHubAvatarResolver.GitHubAvatarResolver;
      const fileSystem = yield* FileSystem.FileSystem;

      const first = yield* resolver.resolvePath("/worktrees/trino");
      const second = yield* resolver.resolvePath("/worktrees/trino");

      expect(first).not.toBeNull();
      if (first === null) return;
      expect(first).toBe(second);
      expect(first).toContain("github-avatars");
      expect(first).toMatch(/\.png$/);
      const bytes = yield* fileSystem.readFile(first);
      expect(Array.from(bytes)).toEqual(Array.from(pngBytes));
      expect(count.value).toBe(1);
      expect(resolver.isManagedPath(first)).toBe(true);
      expect(resolver.isManagedPath("/etc/passwd")).toBe(false);
    }).pipe(
      Effect.provide(
        makeLayer({
          identity,
          count,
          response: () => new Response(pngBytes, { headers: { "content-type": "image/png" } }),
        }),
      ),
    );
  });

  it.effect("shares one cache entry across worktrees of the same repository", () => {
    const count = { value: 0 };
    return Effect.gen(function* () {
      const resolver = yield* GitHubAvatarResolver.GitHubAvatarResolver;

      const [left, right] = yield* Effect.all(
        [resolver.resolvePath("/worktrees/one"), resolver.resolvePath("/worktrees/two")],
        { concurrency: "unbounded" },
      );

      expect(left).not.toBeNull();
      expect(left).toBe(right);
      expect(count.value).toBe(1);
    }).pipe(
      Effect.provide(
        makeLayer({
          identity,
          count,
          response: () => new Response(pngBytes, { headers: { "content-type": "image/png" } }),
        }),
      ),
    );
  });

  it.effect("shares one cache entry across repositories of the same owner", () => {
    const count = { value: 0 };
    const identities: Record<string, RepositoryIdentity> = {
      "/worktrees/trino": identity,
      "/worktrees/tpch": {
        canonicalKey: "github.com/trinodb/tpch",
        locator: {
          source: "git-remote",
          remoteName: "origin",
          // Different repository and different owner casing than /worktrees/trino.
          remoteUrl: "https://github.com/TrinoDB/tpch.git",
        },
      },
    };
    return Effect.gen(function* () {
      const resolver = yield* GitHubAvatarResolver.GitHubAvatarResolver;

      const first = yield* resolver.resolvePath("/worktrees/trino");
      const second = yield* resolver.resolvePath("/worktrees/tpch");

      expect(first).not.toBeNull();
      expect(first).toBe(second);
      expect(count.value).toBe(1);
    }).pipe(
      Effect.provide(
        makeLayer({
          identity: (cwd) => identities[cwd] ?? null,
          count,
          response: () => new Response(pngBytes, { headers: { "content-type": "image/png" } }),
        }),
      ),
    );
  });

  it.effect("skips non-github remotes without any request", () => {
    const count = { value: 0 };
    return Effect.gen(function* () {
      const resolver = yield* GitHubAvatarResolver.GitHubAvatarResolver;

      const resolved = yield* resolver.resolvePath("/worktrees/repo");

      expect(resolved).toBeNull();
      expect(count.value).toBe(0);
    }).pipe(
      Effect.provide(
        makeLayer({ identity: gitlabIdentity, count, response: () => new Response(null) }),
      ),
    );
  });

  it.effect("skips identities that are not repositories", () => {
    const count = { value: 0 };
    return Effect.gen(function* () {
      const resolver = yield* GitHubAvatarResolver.GitHubAvatarResolver;

      const resolved = yield* resolver.resolvePath("/worktrees/repo");

      expect(resolved).toBeNull();
      expect(count.value).toBe(0);
    }).pipe(
      Effect.provide(makeLayer({ identity: null, count, response: () => new Response(null) })),
    );
  });

  it.effect("remembers private repositories as negative", () => {
    const count = { value: 0 };
    return Effect.gen(function* () {
      const resolver = yield* GitHubAvatarResolver.GitHubAvatarResolver;

      expect(yield* resolver.resolvePath("/worktrees/trino")).toBeNull();
      expect(yield* resolver.resolvePath("/worktrees/trino")).toBeNull();
      expect(count.value).toBe(1);
    }).pipe(
      Effect.provide(
        makeLayer({ identity, count, response: () => new Response(null, { status: 404 }) }),
      ),
    );
  });

  it.effect("retries a negative result after its ttl", () => {
    const count = { value: 0 };
    return Effect.gen(function* () {
      const resolver = yield* GitHubAvatarResolver.GitHubAvatarResolver;

      expect(yield* resolver.resolvePath("/worktrees/trino")).toBeNull();
      yield* TestClock.adjust("8 days");
      expect(yield* resolver.resolvePath("/worktrees/trino")).toBeNull();
      expect(count.value).toBe(2);
    }).pipe(
      Effect.provide(
        makeLayer({ identity, count, response: () => new Response(null, { status: 404 }) }),
      ),
    );
  });

  it.effect("mutes refetching briefly after a transport failure, without a negative marker", () => {
    const count = { value: 0 };
    return Effect.gen(function* () {
      const resolver = yield* GitHubAvatarResolver.GitHubAvatarResolver;

      // The failure mutes further attempts, so an offline burst pays one
      // timeout per owner instead of one per favicon request.
      expect(yield* resolver.resolvePath("/worktrees/trino")).toBeNull();
      expect(yield* resolver.resolvePath("/worktrees/trino")).toBeNull();
      expect(count.value).toBe(1);

      yield* TestClock.adjust("6 minutes");
      expect(yield* resolver.resolvePath("/worktrees/trino")).toBeNull();
      expect(count.value).toBe(2);
    }).pipe(
      Effect.provide(
        makeLayer({
          identity,
          count,
          response: () => Promise.reject(new TypeError("offline")),
        }),
      ),
    );
  });

  it.effect("mutes rate-limited responses briefly, without a negative marker", () => {
    const count = { value: 0 };
    return Effect.gen(function* () {
      const resolver = yield* GitHubAvatarResolver.GitHubAvatarResolver;

      expect(yield* resolver.resolvePath("/worktrees/trino")).toBeNull();
      expect(yield* resolver.resolvePath("/worktrees/trino")).toBeNull();
      expect(count.value).toBe(1);

      yield* TestClock.adjust("6 minutes");
      expect(yield* resolver.resolvePath("/worktrees/trino")).toBeNull();
      expect(count.value).toBe(2);
    }).pipe(
      Effect.provide(
        makeLayer({ identity, count, response: () => new Response(null, { status: 429 }) }),
      ),
    );
  });

  it.effect("caches JPEG avatars with their own extension", () => {
    const count = { value: 0 };
    return Effect.gen(function* () {
      const resolver = yield* GitHubAvatarResolver.GitHubAvatarResolver;

      const first = yield* resolver.resolvePath("/worktrees/trino");
      const second = yield* resolver.resolvePath("/worktrees/trino");

      expect(first).not.toBeNull();
      if (first === null) return;
      expect(first).toBe(second);
      expect(first).toMatch(/\.jpg$/);
      expect(count.value).toBe(1);
    }).pipe(
      Effect.provide(
        makeLayer({
          identity,
          count,
          response: () => new Response(pngBytes, { headers: { "content-type": "image/jpeg" } }),
        }),
      ),
    );
  });

  it.effect("remembers unservable content types as negative", () => {
    const count = { value: 0 };
    return Effect.gen(function* () {
      const resolver = yield* GitHubAvatarResolver.GitHubAvatarResolver;

      expect(yield* resolver.resolvePath("/worktrees/trino")).toBeNull();
      expect(yield* resolver.resolvePath("/worktrees/trino")).toBeNull();
      expect(count.value).toBe(1);
    }).pipe(
      Effect.provide(
        makeLayer({
          identity,
          count,
          response: () =>
            new Response(pngBytes, { headers: { "content-type": "application/octet-stream" } }),
        }),
      ),
    );
  });

  it.effect("remembers oversized avatars as negative", () => {
    const count = { value: 0 };
    return Effect.gen(function* () {
      const resolver = yield* GitHubAvatarResolver.GitHubAvatarResolver;

      expect(yield* resolver.resolvePath("/worktrees/trino")).toBeNull();
      expect(yield* resolver.resolvePath("/worktrees/trino")).toBeNull();
      expect(count.value).toBe(1);
    }).pipe(
      Effect.provide(
        makeLayer({
          identity,
          count,
          response: () =>
            new Response(new Uint8Array(1024 * 1024 + 1), {
              headers: { "content-type": "image/png" },
            }),
        }),
      ),
    );
  });
});
