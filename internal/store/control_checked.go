package store

import (
	"context"
	"errors"
)

var errControlUnchanged = errors.New("control update unchanged")

// UpdateControlChecked preserves UpdateChecked's no-op/CAS contract without
// materializing v2 tenant hot/cold evidence. A false result discards all private
// mutations and does no durable Save. Legacy v1 retains its compatibility cost.
// The closure must be retry-safe and must not call an exported Store method.
func (s *Store) UpdateControlChecked(ctx context.Context, fn func(*Snapshot) (bool, error)) error {
	err := s.WithOperation(ctx, func(opCtx context.Context) error {
		return s.UpdateControl(opCtx, func(data *Snapshot) error {
			if err := opCtx.Err(); err != nil {
				return err
			}
			changed, err := fn(data)
			if err != nil {
				return err
			}
			if err := opCtx.Err(); err != nil {
				return err
			}
			if !changed {
				return errControlUnchanged
			}
			return nil
		})
	})
	if errors.Is(err, errControlUnchanged) {
		return nil
	}
	return err
}
