-- JWT signing keypair storage. This is platform-level, tenant-agnostic
-- data -- there is exactly one signing keypair for the whole service
-- instance, not one per tenant -- so, like the tenants table itself, NO
-- tenant_id column and NO Row-Level Security applies here; RLS exists to
-- separate tenants' data from each other, and a signing key isn't owned
-- by any single tenant.
--
-- Design choice (see services/tenant-identity/README.md for the fuller
-- writeup): generate an ECDSA P-256 keypair once on first startup if this
-- table is empty, persist the private key here, and load it on every
-- subsequent startup. This service already owns Postgres and nothing
-- else needs a new persistence mechanism invented just to hold one
-- keypair; a K8s Secret would work too, but would need to be created
-- out-of-band before first boot, whereas "generate on first empty read"
-- needs no operator action to get a working dev instance up. The public
-- key is logged prominently at startup for manual distribution via a
-- ConfigMap (see deploy/k8s/tenant-identity-public-key.example.yaml) --
-- this table is intentionally a single-row table (enforced by the
-- singleton_guard check below) since only one active keypair is supported
-- in this milestone (no key rotation).
CREATE TABLE tenant_identity_signing_key (
    -- Always 1: guarantees at most one row via the primary key, giving us
    -- a simple, explicit "singleton row" table rather than relying on a
    -- separate unique-index-on-constant trick.
    singleton_guard   SMALLINT PRIMARY KEY DEFAULT 1,
    -- PKCS#8 DER-encoded ECDSA P-256 private key, base64-text-encoded for
    -- simplicity of storage/inspection (BYTEA would work equally well;
    -- TEXT was chosen so the column is trivially eyeballed/copyable via
    -- psql if ever needed for a manual backup).
    private_key_pkcs8_b64 TEXT NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT singleton_guard_is_one CHECK (singleton_guard = 1)
);
