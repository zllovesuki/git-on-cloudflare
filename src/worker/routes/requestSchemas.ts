import { z } from "zod";

// Route handlers historically treated malformed JSON bodies as empty objects
// and then let field-specific validation choose the public error. These
// preprocessors preserve that behavior while moving the structural parsing to
// zod.
function objectOrEmpty(value: unknown): unknown {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return {};
  }
  return value;
}

const TrimmedStringSchema = z.preprocess(
  (value) => (typeof value === "string" ? value.trim() : ""),
  z.string()
);

const StringOrEmptySchema = z.preprocess(
  (value) => (typeof value === "string" ? value : ""),
  z.string()
);

const LowercaseSlugInputSchema = z.preprocess(
  (value) => (typeof value === "string" ? value.trim().toLowerCase() : ""),
  z.string()
);

const RepositoryVisibilitySchema = z.enum(["public", "private"]);

const NullableRepositoryVisibilitySchema = z.preprocess(
  (value) => (value === "public" || value === "private" ? value : null),
  RepositoryVisibilitySchema.nullable()
);

export const RepositoryCreateRequestSchema = z.preprocess(
  objectOrEmpty,
  z.object({
    namespaceSlug: LowercaseSlugInputSchema,
    slug: LowercaseSlugInputSchema,
    visibility: NullableRepositoryVisibilitySchema,
  })
);

export const RepositoryVisibilityRequestSchema = z.preprocess(
  objectOrEmpty,
  z.object({
    visibility: NullableRepositoryVisibilitySchema,
  })
);

export const NamespaceClaimRequestSchema = z.preprocess(
  objectOrEmpty,
  z.object({
    slug: LowercaseSlugInputSchema,
  })
);

const PatGrantLevelSchema = z.enum(["pull", "push"]);

const NullablePatGrantLevelSchema = z.preprocess(
  (value) => (value === "pull" || value === "push" ? value : null),
  PatGrantLevelSchema.nullable()
);

const PatNamespaceCreateRequestSchema = z.object({
  scope: z.literal("namespace"),
  name: TrimmedStringSchema,
  namespaceSlug: LowercaseSlugInputSchema,
  level: NullablePatGrantLevelSchema,
});

const PatRepoCreateRequestSchema = z.object({
  scope: z.literal("repo"),
  name: TrimmedStringSchema,
  namespaceSlug: LowercaseSlugInputSchema,
  repoSlug: LowercaseSlugInputSchema,
  level: NullablePatGrantLevelSchema,
});

export const PatCreateRequestSchema = z.preprocess(
  objectOrEmpty,
  z.discriminatedUnion("scope", [PatNamespaceCreateRequestSchema, PatRepoCreateRequestSchema])
);

export const AdminCompactionRequestSchema = z.preprocess(
  objectOrEmpty,
  z.object({
    dryRun: z.preprocess(
      (value) => (value === false ? false : undefined),
      z.literal(false).optional()
    ),
  })
);

export const AdminRefsPayloadSchema = z.array(
  z
    .object({
      name: z.string(),
      oid: z.string(),
    })
    .passthrough()
);

export const AdminHeadPayloadSchema = z
  .object({
    target: z.string(),
    oid: z.string().optional(),
    unborn: z.boolean().optional(),
  })
  .passthrough();

export const AdminPurgeRequestSchema = z.preprocess(
  objectOrEmpty,
  z.object({
    confirm: StringOrEmptySchema,
  })
);
