package internal

import "errors"

var (
	ErrNotFound             = errors.New("not found")
	ErrDeviceNotAssociated  = errors.New("device has no associated vehicle")
	ErrInvalidID            = errors.New("invalid id")
)
