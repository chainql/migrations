# migrations - A Better migration engine for [go-pg/pg](https://github.com/go-pg/pg)

## Basic Commands

- init
  - runs the specified intial migration as a batch on it's own.
- migrate
  - runs all available migrations that have not been run inside a batch
- rollback
  - reverts the last batch of migrations.
- create **name**
  - creates a migration file using the name provided.

## Usage

Make a `main.go` in a `migrations` folder

```golang
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/go-pg/pg/v10"
	"github.com/padm-io/migrations"
	"github.com/padm-io/pRPC/helpers/util"
	"github.com/padm-io/pRPC/services/postgres"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// Flags
var (
	// determines the command to execute
	cmd = flag.String("command", "migrate", "Specifies the command to execute. Supported commands are:\n"+
		"* init       - Runs the initial migration as a separate batch.\n"+
		"* migrate    - Executes all available migrations. (default)\n"+
		"* rollback   - Reverts the last batch of migrations.\n"+
		"* create     - Creates a new migration file.")

	// specifies the name of the migration
	name = flag.String("name", "", "Name of the migration (required for 'create' command).")

	// specifies the template filename
	templateName = flag.String("template", "", "Filename of the template (used with 'create' command, optional).")

	// contains additional parameters for the command
	extra = flag.String("extra", "", "Additional parameters for the command. "+
		"For 'migrate' command, use 'caller one-by-one' to run migrations one at a time.")
)

// Create Global Registry object. Registry holds a set of known migrations.
var registry = &migrations.Registry{}

func main() {
	util.InitGlobalZapLogger()
	flag.Usage = printUsageAndExit
	flag.Parse()

	// Create the migrator object with appropriate configurations
	migrator, err := buildDefaultMigrator()
	if err != nil {
		zap.L().Fatal("failed to create migrator object", zap.Error(err))
	}

	// Execute the command based on the user input
	err = nil
	switch *cmd {
	case "init":
		err = migrator.Init()
	case "migrate":
		if *extra == "one-by-one" {
			err = migrator.MigrateStepByStep()
		} else {
			err = migrator.MigrateBatch()
		}
	case "rollback":
		err = migrator.Rollback()
	case "create":
		if len(*name) == 0 {
			zap.L().Fatal("Please enter migration name.")
		}
		template := ""
		if len(*templateName) > 0 {
			template, err = readTemplateFileContent()
			if err != nil {
				zap.L().Fatal("Error reading template", zap.Error(err))
			}
		}
		if template != "" {
			err = migrator.CreateFromTemplate(*name, template)
		} else {
			err = migrator.Create(*name)
		}

	default:
		zap.L().Fatal("Unknown command", zap.String("command", *cmd))
	}
	// Handle any errors encountered during command execution
	if err != nil {
		zap.L().Fatal("Migration failed", zap.String("command", *cmd), zap.Error(err))
	}
}

// buildDefaultMigrator constructs and returns a New Migrator
func buildDefaultMigrator() (*migrations.Migrator, error) {
	return migrations.NewMigrator(
		func() *pg.DB { return postgres.GetOrCreatePGDBForContext() },
		migrations.WithMigrationTableName("public.padm_migrations"),
		migrations.WithInitialName("000000000000_init"),
		migrations.WithNameConvention(migrations.SnakeCase),
		migrations.WithLogger(zap.NewStdLog(zap.L())),
		migrations.WithPostgresFlavour(migrations.Postgres),
		migrations.WithMigrations(registry),
		migrations.WithVerbosity(0),
	)
}

// printUsageAndExit is used to set flag.Usage for program.
func printUsageAndExit() {
	const usageText = `Manage database migrations.

Usage:
  migrations [options]

Options:`

	fmt.Println(usageText)
	flag.PrintDefaults()
	os.Exit(2)
}

// readTemplateFileContent
func readTemplateFileContent() (string, error) {
	// Get the current working directory
	pwd, err := os.Getwd()
	if err != nil {
		return "", errors.Wrap(err, "readTemplateFileContent:failed to get current working directory.")
	}
	// Construct the path to the template file
	templatePath := filepath.Join(pwd, "templates", *templateName)
	// Read the template file
	buf, err := os.ReadFile(templatePath)
	if err != nil {
		return "", errors.Wrap(err, "readTemplateFileContent:failed to read template file")
	}
	return string(buf), nil
}

```

Compile it:

```bash
$> go build -i -o ./migrations/migrations ./migrations/*.go
```

Run it:

```bash
$> ./migrations/migrations migrate
```

## Notes on generated file names

```bash
$> ./migrations/migrations create new_index
```

Creates a file in the `./migrations` folder called `20240622230738_new_index.go` with the following contents:

```golang
package main

import (
	"github.com/go-pg/pg/v10"
	"github.com/padm-io/migrations"
)

func init() {
	register.Register(
		"20240622230738_new_index",
		up20240622230738NewIndex,
		down20240622230738NewIndex,
	)
}

func up20240622230738NewIndex(tx *pg.Tx, cont *migrations.Context) error {
	_, err := tx.Exec(``)
	return err
}

func down20240622230738NewIndex(tx *pg.Tx, cont *migrations.Context) error {
	_, err := tx.Exec(``)
	return err
}
```

Forward migration sql commands go in up and Rollback migrations sql commands go in down
