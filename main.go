package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/rambleraptor/aepbase/pkg/aepbase"
	"github.com/rambleraptor/aepbase/pkg/user"
)

func main() {
	// Subcommands are dispatched on the first argument. Anything else (no args
	// or a leading flag) runs the server, preserving the original CLI.
	if len(os.Args) > 1 && os.Args[1] == "create-superuser" {
		if err := createSuperuser(os.Args[2:]); err != nil {
			log.Fatal(err)
		}
		return
	}

	var opts aepbase.ServerOptions
	opts.RegisterFlags()
	flag.Parse()

	if err := aepbase.Run(opts); err != nil {
		log.Fatal(err)
	}
}

// createSuperuser implements the `create-superuser` subcommand. It writes a
// new superuser directly to the database so an operator or automated system
// can provision access when user authentication is enabled.
func createSuperuser(args []string) error {
	fs := flag.NewFlagSet("create-superuser", flag.ExitOnError)
	var opts aepbase.ServerOptions
	email := fs.String("email", "", "email address of the superuser (required)")
	password := fs.String("password", "", "password for the superuser (required)")
	displayName := fs.String("display-name", "", "display name for the superuser")
	fs.StringVar(&opts.DataDir, "data-dir", "aepbase_data", "directory for database files")
	fs.StringVar(&opts.DBFile, "db", "aepbase.db", "database file name")
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "Usage: aepbase create-superuser -email <email> -password <password> [flags]\n\n")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return err
	}

	if *email == "" || *password == "" {
		fs.Usage()
		return errors.New("both -email and -password are required")
	}

	if err := aepbase.CreateSuperuser(opts, *email, *displayName, *password); err != nil {
		if errors.Is(err, user.ErrEmailExists) {
			return fmt.Errorf("a superuser with email %q already exists", *email)
		}
		return err
	}

	fmt.Printf("Created superuser %s\n", *email)
	return nil
}
