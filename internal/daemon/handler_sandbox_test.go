package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/openeuler/Conch/internal/conchruntime"
	"github.com/openeuler/Conch/internal/daemon/state"
	conchimage "github.com/openeuler/Conch/internal/image"
	conchtemplate "github.com/openeuler/Conch/internal/template"
)

func TestHandleCreateSandboxReturnsGeneratedSandboxID(t *testing.T) {
	sandboxOps := &fakeSandboxOps{}
	runtimeService := conchruntime.New(sandboxOps, nil, nil, nil)
	server := &Daemon{router: http.NewServeMux(), runtimeService: runtimeService}
	server.routes()

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/api/v1/sandboxes", bytes.NewBufferString(`{}`))
	server.router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}

	var response createSandboxResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.SandboxID == "" || response.SandboxID != sandboxOps.createReq.SandboxID {
		t.Fatalf("sandbox identity = response:%q request:%q", response.SandboxID, sandboxOps.createReq.SandboxID)
	}
}

func TestHandleCreateSandboxReturnsConflictForExistingID(t *testing.T) {
	store, err := state.OpenBolt(t.TempDir() + "/state.db")
	if err != nil {
		t.Fatalf("OpenBolt() error = %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := store.UpsertSandbox(context.Background(), state.SandboxRecord{
		SandboxID:                     "sandbox-1",
		CheckpointHeadTemplateID:      "tmpl-existing",
		CheckpointHeadBootIndexDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}); err != nil {
		t.Fatalf("UpsertSandbox() seed error = %v", err)
	}

	runtimeService := conchruntime.New(&fakeSandboxOps{}, nil, nil, store)
	server := &Daemon{router: http.NewServeMux(), runtimeService: runtimeService}
	server.routes()
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/api/v1/sandboxes", bytes.NewBufferString(`{"sandbox_id":"sandbox-1","template_id":"tmpl-new"}`))
	server.router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d; body = %s", recorder.Code, http.StatusConflict, recorder.Body.String())
	}
}

func TestHandleCheckpointSandboxReturnsBootIndexDigest(t *testing.T) {
	const (
		sourceDigest     = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
		checkpointDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	)

	store, err := state.OpenBolt(t.TempDir() + "/state.db")
	if err != nil {
		t.Fatalf("OpenBolt() error = %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	ctx := context.Background()
	if err := store.CreateTemplate(ctx, conchtemplate.Entry{
		ID:              "tmpl-source",
		Origin:          conchtemplate.OriginImage,
		BootIndexDigest: sourceDigest,
		BootMode:        conchtemplate.BootModeCold,
	}); err != nil {
		t.Fatalf("CreateTemplate() error = %v", err)
	}
	if err := store.UpsertSandbox(ctx, state.SandboxRecord{
		SandboxID:                     "sandbox-1",
		CheckpointHeadTemplateID:      "tmpl-source",
		CheckpointHeadBootIndexDigest: sourceDigest,
	}); err != nil {
		t.Fatalf("UpsertSandbox() error = %v", err)
	}

	sandboxOps := &fakeSandboxOps{}
	imageOps := &fakeImageService{checkpointPublishResp: conchimage.PublishCheckpointBootImageResult{
		BootIndexDigest: checkpointDigest,
		ImageName:       "localhost/conch/template:checkpoint",
	}}
	runtimeService := conchruntime.New(sandboxOps, imageOps, imageOps, store)
	server := &Daemon{router: http.NewServeMux(), runtimeService: runtimeService}
	server.routes()

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/api/sandbox/checkpoint", bytes.NewBufferString(`{"sandbox_id":"sandbox-1"}`))
	server.router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}

	var response struct {
		Status          string `json:"status"`
		TemplateID      string `json:"template_id"`
		BootIndexDigest string `json:"boot_index_digest"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Status != "ok" || response.TemplateID == "" || response.BootIndexDigest != checkpointDigest {
		t.Fatalf("checkpoint response = %#v", response)
	}
	if sandboxOps.checkpointReq.SandboxID != "sandbox-1" {
		t.Fatalf("checkpoint request = %#v", sandboxOps.checkpointReq)
	}
	if imageOps.checkpointPublishReq.SourceBootIndexDigest != sourceDigest {
		t.Fatalf("publish request = %#v", imageOps.checkpointPublishReq)
	}
}
