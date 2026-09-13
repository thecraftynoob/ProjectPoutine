package pgconfig

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// AttributeType enumerates spec Section 2.6's two permitted types.
type AttributeType string

const (
	AttributeNumeric AttributeType = "numeric"
	AttributeBoolean AttributeType = "boolean"
)

// ErrInvalidAttributeType is returned by RegisterAttribute for any type
// string other than "numeric"/"boolean".
var ErrInvalidAttributeType = errors.New("pgconfig: invalid attribute type")

// AttributeDefinition mirrors spec Section 2.6.
type AttributeDefinition struct {
	Name      string
	Type      AttributeType
	CreatedAt time.Time
}

// RegisterAttribute defines a new named, typed field. Returns
// ErrInvalidAttributeType on an unrecognized type, ErrAlreadyExists on a
// duplicate name (spec Section 3.6).
func (r *Registry) RegisterAttribute(ctx context.Context, tenantID uuid.UUID, name string, attrType AttributeType) (AttributeDefinition, error) {
	if attrType != AttributeNumeric && attrType != AttributeBoolean {
		return AttributeDefinition{}, ErrInvalidAttributeType
	}
	var out AttributeDefinition
	err := r.pool.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		row := tx.QueryRow(ctx, `
			INSERT INTO task_router_attributes (tenant_id, name, type)
			VALUES ($1, $2, $3)
			ON CONFLICT (tenant_id, name) DO NOTHING
			RETURNING name, type, created_at
		`, tenantID, name, string(attrType))
		var typ string
		if err := row.Scan(&out.Name, &typ, &out.CreatedAt); err != nil {
			return err
		}
		out.Type = AttributeType(typ)
		return nil
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return AttributeDefinition{}, ErrAlreadyExists
	}
	return out, err
}

// ListAttributes enumerates the registry.
func (r *Registry) ListAttributes(ctx context.Context, tenantID uuid.UUID) ([]AttributeDefinition, error) {
	var out []AttributeDefinition
	err := r.pool.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT name, type, created_at FROM task_router_attributes ORDER BY name`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var a AttributeDefinition
			var typ string
			if err := rows.Scan(&a.Name, &typ, &a.CreatedAt); err != nil {
				return err
			}
			a.Type = AttributeType(typ)
			out = append(out, a)
		}
		return rows.Err()
	})
	return out, err
}

// GetAttribute retrieves one attribute's definition. Returns ErrNotFound
// if absent.
func (r *Registry) GetAttribute(ctx context.Context, tenantID uuid.UUID, name string) (AttributeDefinition, error) {
	var out AttributeDefinition
	err := r.pool.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		row := tx.QueryRow(ctx, `SELECT name, type, created_at FROM task_router_attributes WHERE name = $1`, name)
		var typ string
		if err := row.Scan(&out.Name, &typ, &out.CreatedAt); err != nil {
			return err
		}
		out.Type = AttributeType(typ)
		return nil
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return AttributeDefinition{}, ErrNotFound
	}
	return out, err
}

// RemoveAttribute retires a named field (spec Section 3.6: existing
// values referencing it are unaffected).
func (r *Registry) RemoveAttribute(ctx context.Context, tenantID uuid.UUID, name string) error {
	return r.pool.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `DELETE FROM task_router_attributes WHERE name = $1`, name)
		return err
	})
}

// AttributeTypes returns a name->type lookup for every given attribute
// name that IS registered (missing names are simply absent from the
// result map, letting the caller distinguish "registered" from "not
// registered" per spec Section 4.4's shape-validation rule).
func (r *Registry) AttributeTypes(ctx context.Context, tenantID uuid.UUID, names []string) (map[string]AttributeType, error) {
	out := make(map[string]AttributeType)
	if len(names) == 0 {
		return out, nil
	}
	err := r.pool.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT name, type FROM task_router_attributes WHERE name = ANY($1::text[])
		`, names)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var name, typ string
			if err := rows.Scan(&name, &typ); err != nil {
				return err
			}
			out[name] = AttributeType(typ)
		}
		return rows.Err()
	})
	return out, err
}
