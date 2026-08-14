package template

import (
	"errors"
	"fmt"
	"strings"

	"github.com/opencontainers/go-digest"
)

type Origin string

const (
	OriginImage      Origin = "image"
	OriginCheckpoint Origin = "checkpoint"
)

type BootMode string

const (
	BootModeCold   BootMode = "cold"
	BootModeResume BootMode = "resume"
)

var (
	ErrAlreadyExists = errors.New("template already exists")
	ErrInUse         = errors.New("template is in use")
)

// Entry is the non-persistent domain representation of a fully published and
// validated Template. An Entry has no lifecycle state: if it exists, it is
// ready to be consumed.
type Entry struct {
	ID               string
	Origin           Origin
	BootMode         BootMode
	ParentTemplateID string
	SourceSandboxID  string
	ImageName        string
	Labels           map[string]string
	CreatedAt        int64
}

// NormalizeEntry validates a complete Template and returns a defensive,
// canonical copy suitable for persistence.
func NormalizeEntry(entry Entry) (Entry, error) {
	rawID := strings.TrimSpace(entry.ID)
	parsed, err := digest.Parse(rawID)
	if err != nil {
		return Entry{}, fmt.Errorf("invalid template id %q: %w", rawID, err)
	}
	entry.ID = parsed.String()
	switch entry.Origin {
	case OriginImage, OriginCheckpoint:
	default:
		return Entry{}, fmt.Errorf("unknown template origin %q", entry.Origin)
	}
	switch entry.BootMode {
	case BootModeCold, BootModeResume:
	default:
		return Entry{}, fmt.Errorf("unknown template boot mode %q", entry.BootMode)
	}
	entry.ParentTemplateID = strings.TrimSpace(entry.ParentTemplateID)
	entry.SourceSandboxID = strings.TrimSpace(entry.SourceSandboxID)
	entry.ImageName = strings.TrimSpace(entry.ImageName)
	entry.Labels = copyMap(entry.Labels)
	return entry, nil
}
