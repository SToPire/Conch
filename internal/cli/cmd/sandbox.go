package cmd

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/openeuler/Conch/internal/image/client"
)

func printSandboxHelp(out io.Writer) {
	fmt.Fprintln(out, "Usage:")
	fmt.Fprintln(out, "  conch sandbox <command> [options]")
	fmt.Fprintln(out, "")
	fmt.Fprintln(out, "Commands:")
	fmt.Fprintln(out, "  create      Create a sandbox from a Template ID.")
	fmt.Fprintln(out, "  checkpoint  Checkpoint a sandbox into a resumable template.")
	fmt.Fprintln(out, "  suspend     Suspend a running sandbox.")
	fmt.Fprintln(out, "  resume      Resume a suspended sandbox.")
	fmt.Fprintln(out, "")
	fmt.Fprintln(out, "Run 'conch sandbox <command> --help' for command-specific usage.")
}

func RunSandbox(ctx context.Context, args []string) error {
	if len(args) == 0 || args[0] == "-h" || args[0] == "--help" {
		printSandboxHelp(os.Stdout)
		return nil
	}
	switch args[0] {
	case "create":
		return runSandboxCreate(ctx, args[1:])
	case "checkpoint":
		return runSandboxCheckpoint(ctx, args[1:])
	case "suspend":
		return runSandboxLifecycle(ctx, args[1:], "suspend")
	case "resume":
		return runSandboxLifecycle(ctx, args[1:], "resume")
	default:
		printSandboxHelp(os.Stderr)
		return fmt.Errorf("unknown sandbox command %q", args[0])
	}
}

func runSandboxCreate(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("sandbox create", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	templateID := fs.String("template-id", "", "template ID")
	sandboxID := fs.String("sandbox-id", "", "sandbox ID")
	configPath := fs.String("config", "", "config file path")
	ramMB := fs.Int64("ram-mb", 0, "memory size in MB")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("conch sandbox create: unexpected positional arguments: %v", fs.Args())
	}
	if *templateID == "" {
		return fmt.Errorf("conch sandbox create: --template-id is required")
	}
	id := *sandboxID
	if id == "" {
		id = fmt.Sprintf("sandbox-%d", time.Now().UnixNano())
	}
	conchClient, err := client.NewClientWithConfig("", *configPath)
	if err != nil {
		return fmt.Errorf("conch sandbox create: %w", err)
	}
	if err := conchClient.CreateSandbox(*templateID, id, *ramMB); err != nil {
		return fmt.Errorf("conch sandbox create: %w", err)
	}
	fmt.Fprintf(os.Stdout, "Sandbox: %s\n", id)
	return nil
}

func runSandboxCheckpoint(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("sandbox checkpoint", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	configPath := fs.String("config", "", "config file path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("conch sandbox checkpoint: exactly one sandbox ID is required")
	}
	conchClient, err := client.NewClientWithConfig("", *configPath)
	if err != nil {
		return fmt.Errorf("conch sandbox checkpoint: %w", err)
	}
	templateID, err := conchClient.CheckpointSandbox(ctx, fs.Arg(0))
	if err != nil {
		return fmt.Errorf("conch sandbox checkpoint: %w", err)
	}
	fmt.Fprintf(os.Stdout, "Template: %s\n", templateID)
	return nil
}

func runSandboxLifecycle(ctx context.Context, args []string, op string) error {
	fs := flag.NewFlagSet("sandbox "+op, flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	configPath := fs.String("config", "", "config file path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("conch sandbox %s: exactly one sandbox ID is required", op)
	}
	c, err := client.NewClientWithConfig("", *configPath)
	if err != nil {
		return fmt.Errorf("conch sandbox %s: %w", op, err)
	}
	id := fs.Arg(0)
	switch op {
	case "suspend":
		err = c.SuspendSandbox(ctx, id)
	case "resume":
		err = c.ResumeSandbox(ctx, id)
	}
	if err != nil {
		return fmt.Errorf("conch sandbox %s: %w", op, err)
	}
	fmt.Fprintf(os.Stdout, "%s sandbox: %s\n", op, id)
	return nil
}
