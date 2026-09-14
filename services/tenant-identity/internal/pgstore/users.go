package pgstore

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/thecraftynoob/ProjectPoutine/pkg/pgtenant"
)

// User mirrors one tenant-scoped identity record, including the bcrypt
// password hash -- callers in internal/grpcapi must never let
// PasswordHash escape to a proto response.
type User struct {
	UserID       uuid.UUID
	TenantID     uuid.UUID
	Username     string
	PasswordHash string
	Roles        []string
	CreatedAt    time.Time
}

// UserStore is the tenant-scoped user/agent identity repository. Every
// method funnels through pgtenant.WithTenant per the architecture doc's
// implementation rule (Section 1.1) -- no repository method here compiles
// without a tenant-scoped transaction.
type UserStore struct {
	pool *pgtenant.Pool
}

// NewUserStore constructs a UserStore.
func NewUserStore(pool *pgtenant.Pool) *UserStore {
	return &UserStore{pool: pool}
}

// CreateUser provisions a new user/agent identity record. A nil roles
// slice is normalized to an empty (non-NULL) slice before insert, since
// the roles column is NOT NULL -- callers (including internal/grpcapi and
// this package's own tests) may reasonably pass nil for "no roles".
// Returns ErrAlreadyExists if username is already taken within tenantID.
func (s *UserStore) CreateUser(ctx context.Context, tenantID uuid.UUID, username, passwordHash string, roles []string) (User, error) {
	if roles == nil {
		roles = []string{}
	}
	var out User
	err := s.pool.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		row := tx.QueryRow(ctx, `
			INSERT INTO tenant_identity_users (tenant_id, username, password_hash, roles)
			VALUES ($1, $2, $3, $4)
			RETURNING user_id, tenant_id, username, password_hash, roles, created_at
		`, tenantID, username, passwordHash, roles)
		return row.Scan(&out.UserID, &out.TenantID, &out.Username, &out.PasswordHash, &out.Roles, &out.CreatedAt)
	})
	if isUniqueViolation(err) {
		return User{}, ErrAlreadyExists
	}
	if err != nil {
		return User{}, err
	}
	return out, nil
}

// GetUser retrieves one user by ID within tenantID. Returns ErrNotFound
// if absent.
func (s *UserStore) GetUser(ctx context.Context, tenantID, userID uuid.UUID) (User, error) {
	var out User
	err := s.pool.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		row := tx.QueryRow(ctx, `
			SELECT user_id, tenant_id, username, password_hash, roles, created_at
			FROM tenant_identity_users
			WHERE user_id = $1
		`, userID)
		return row.Scan(&out.UserID, &out.TenantID, &out.Username, &out.PasswordHash, &out.Roles, &out.CreatedAt)
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return User{}, ErrNotFound
	}
	if err != nil {
		return User{}, err
	}
	return out, nil
}

// GetUserByUsername retrieves one user by username within tenantID --
// used by Login, which has no user_id yet, only tenant_id + username.
// Returns ErrNotFound if absent.
func (s *UserStore) GetUserByUsername(ctx context.Context, tenantID uuid.UUID, username string) (User, error) {
	var out User
	err := s.pool.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		row := tx.QueryRow(ctx, `
			SELECT user_id, tenant_id, username, password_hash, roles, created_at
			FROM tenant_identity_users
			WHERE username = $1
		`, username)
		return row.Scan(&out.UserID, &out.TenantID, &out.Username, &out.PasswordHash, &out.Roles, &out.CreatedAt)
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return User{}, ErrNotFound
	}
	if err != nil {
		return User{}, err
	}
	return out, nil
}

// ListUsers enumerates every user within tenantID.
func (s *UserStore) ListUsers(ctx context.Context, tenantID uuid.UUID) ([]User, error) {
	var out []User
	err := s.pool.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT user_id, tenant_id, username, password_hash, roles, created_at
			FROM tenant_identity_users
			ORDER BY created_at
		`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var u User
			if err := rows.Scan(&u.UserID, &u.TenantID, &u.Username, &u.PasswordHash, &u.Roles, &u.CreatedAt); err != nil {
				return err
			}
			out = append(out, u)
		}
		return rows.Err()
	})
	return out, err
}

// isUniqueViolation reports whether err is a Postgres unique_violation
// (SQLSTATE 23505), used to translate the tenant_identity_users
// (tenant_id, username) uniqueness constraint into ErrAlreadyExists.
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
