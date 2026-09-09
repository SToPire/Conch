package sandbox

import "github.com/openeuler/Conch/internal/apperror"

// CleanupError preserves cleanup diagnostics independently of VM allocation
// ownership. ResourcesReleased confirms only that the VMM no longer owns CPU
// and guest RAM; a remaining mount, socket or other host cleanup can still fail.
// A false value must retain the runtime's capacity reservation for reconciliation.
type CleanupError struct {
	Err               error
	ResourcesReleased bool
}

func (e *CleanupError) Error() string {
	if e == nil || e.Err == nil {
		return "sandbox cleanup failed"
	}
	return e.Err.Error()
}

func (e *CleanupError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

var (
	ErrInvalidArgument        = apperror.Define(apperror.InvalidArgument, "sandbox.invalid_argument", "invalid sandbox argument")
	ErrInvalidEnvironment     = apperror.Define(apperror.InvalidArgument, "sandbox.invalid_environment", "invalid sandbox environment")
	ErrInitializationTooLarge = apperror.Define(apperror.InvalidArgument, "sandbox.initialization_too_large", "sandbox initialization message is too large")
	ErrNotFound               = apperror.Define(apperror.NotFound, "sandbox.not_found", "sandbox not found")
	ErrAlreadyExists          = apperror.Define(apperror.AlreadyExists, "sandbox.already_exists", "sandbox already exists")
	ErrFailedPrecondition     = apperror.Define(apperror.FailedPrecondition, "sandbox.failed_precondition", "sandbox operation failed its precondition")
	ErrResourceExhausted      = apperror.Define(apperror.ResourceExhausted, "sandbox.resource_exhausted", "sandbox resources are exhausted")
)
