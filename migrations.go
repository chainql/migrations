package migrations

import (
	"bytes"
	"context"
	"html/template"
	"log"
	"os"
	"path"
	"sort"
	"time"

	"github.com/go-pg/pg/v10"
	"github.com/pkg/errors"
)

var (
	// ErrInvalidVerbosity indicates that verbosity has already been
	// specified when attempting to specify quiet, or that quiet has
	// already been specified when attempting to specify verbosity.
	ErrInvalidVerbosity = errors.New("verbosity already set in opposite direction")

	// ErrMigrationNotKnown indicates that a migration has been found
	// in the DB, but with no corresponding known migration.
	ErrMigrationNotKnown = errors.New("no migration by name")

	// ErrInitialMigrationNotKnown indicates that no migration was
	// found with the name of the initial migration.
	ErrInitialMigrationNotKnown = errors.New("initial migration not known")

	// ErrNoMigrationName indicates that an attempt was made to
	// create a migration, without specifying a name.
	ErrNoMigrationName = errors.New("no migration name specified")

	// ErrFileAlreadyExists indicates that an attempt was made to
	// create a migration, without specifying a name.
	ErrFileAlreadyExists = errors.New("migration file already exists")

	// ErrInvalidMigrationFuncRun indicates that a migration is being
	// run with a function with invalid function signature.
	ErrInvalidMigrationFuncRun = errors.New("invalid migration function run")
)

type migration struct {
	Name string
	Up   interface{}
	Down interface{}
}

const (
	// DefaultMigrationTableName is the table in which migrations will be
	// noted if not overridden in the Migrator.
	DefaultMigrationTableName = "public.hb_migrations"

	// DefaultInitialMigrationName is the name of the migration which will
	// be run by Init, if not overridden in the Migrator.
	DefaultInitialMigrationName = "000000000000_init"

	// DefaultMigrationNameConvention is the convention with which the names
	// for migration files and functions will be generated, if not overridden
	// in the Migrator.
	DefaultMigrationNameConvention = SnakeCase

	// DefaultMigrationTemplate is the template which will be used for Create,
	// when using Create without a template.
	//
	// Expects a file similar to the following to exist in the same package:
	// 	package main
	//
	// 	import (
	// 		migrations "github.com/getkalido/hb_migrations/v2"
	// 	)
	//
	// 	var (
	// 		registry	migrations.Registry
	// 	)
	//
	// 	func main() {
	// 		dbFactory := GetDB	// GetDB should return a *pg.DB.
	// 		migrator, err := migrations.NewMigrator(dbFactory, migrations.WithMigrations(&registry))
	// 		// Do things.
	// 	}
	DefaultMigrationTemplate = `package main

import (
	"github.com/go-pg/pg/v10"
	migrations "github.com/getkalido/hb_migrations/v2"
)

func init() {
	err := registry.Register(
		"{{.Filename}}",
		up{{.FuncName}},
		down{{.FuncName}},
	)
	if err != nil {
		panic(err)
	}
}

func up{{.FuncName}}(tx *pg.Tx, cont *migrations.Context) error {
	var err error
	_, err = tx.Exec(` + "`" + "`" + `)
	if err != nil {
		return err
	}
	return nil
}

func down{{.FuncName}}(tx *pg.Tx, cont *migrations.Context) error {
	var err error
	_, err = tx.Exec(` + "`" + "`" + `)
	if err != nil {
		return err
	}
	return nil
}
`
)

// DBFactory returns a DB instance which will house both the migration table
// (to track completed migrations) and the tables which will be affected by
// the migrations.
type DBFactory func() *pg.DB

// Migrator can create or manage migrations as indicated by
// options during construction.
//
// Should not be considered thread-safe.
type Migrator struct {
	dbFactory               func() *pg.DB
	ctx                     context.Context
	logger                  *log.Logger
	registry                Registry
	migrationTableName      string
	initialMigration        string
	migrationDir            string
	templateDir             string
	migrationNameConvention MigrationNameConvention
	explicitLock            bool
	verbosity               int
	context                 Context
}

// DefaultMigrator returns a migrator with the default options.
func DefaultMigrator() *Migrator {
	return &Migrator{
		migrationTableName:      "public.hb_migrations",
		initialMigration:        "000000000000_init",
		migrationNameConvention: SnakeCase,
		explicitLock:            true,
	}
}

// NewMigrator creates a Migrator with the specified options.
//
// DefaultMigrator is used to get a default migrator, then
// options are applied on top of the defaults.
func NewMigrator(dbFactory DBFactory, opts ...MigratorOpt) (*Migrator, error) {
	var err error
	migrator := DefaultMigrator()
	for _, opt := range opts {
		err = opt(migrator)
		if err != nil {
			return nil, err
		}
	}

	if migrator.logger == nil {
		migrator.logger = log.Default()
	}
	if migrator.ctx == nil {
		migrator.logWithMinVerbosity(1, "Using TODO context")
		migrator.ctx = context.TODO()
	}
	if migrator.migrationDir == "" {
		workingDir, err := os.Getwd()
		if err != nil {
			return nil, err
		}
		migrator.logWithMinVerbosity(1, "Setting migration directory: %s", workingDir)
		migrator.migrationDir = workingDir
	}
	migrator.dbFactory = dbFactory
	return migrator, nil
}

// MigratorOpt represents an option which can be applied to
// a migrator during creation. See With*() functions in this
// package.
type MigratorOpt func(*Migrator) error

// WithMigrationTableName sets the name of the table which will
// store completed migrations for a Migrator.
//
// Intended for use with NewMigrator.
func WithMigrationTableName(name string) MigratorOpt {
	return func(m *Migrator) error {
		m.migrationTableName = name
		return nil
	}
}

// WithInitialName sets the name of the initial migration which
// will be run by a Migrator when running the init command.
//
// Intended for use with NewMigrator.
func WithInitialName(name string) MigratorOpt {
	return func(m *Migrator) error {
		m.initialMigration = name
		return nil
	}
}

// WithNameConvention sets the name naming convention which will
// be used by a Migrator when generating new migrations.
//
// Intended for use with NewMigrator.
func WithNameConvention(convention MigrationNameConvention) MigratorOpt {
	return func(m *Migrator) error {
		m.migrationNameConvention = convention
		return nil
	}
}

// WithCapacity initialises a Migrator with enough capacity for
// a given number of migrations. Not necessary, but can limit
// allocations when building the list of migrations.
//
// Intended for use with NewMigrator.
func WithCapacity(capacity uint) MigratorOpt {
	return func(m *Migrator) error {
		m.registry.EnsureCapacity(int(capacity))
		return nil
	}
}

// WithMigrations loads migrations from an existing registry.
// Pre-emptively ensures that the Migrator has capacity for
// the migrations being copied.
//
// Intended for use with NewMigrator.
func WithMigrations(registry *Registry) MigratorOpt {
	return func(m *Migrator) error {
		m.registry.From(registry)
		return nil
	}
}

// WithoutExplicitLock initialises a Migrator which will
// try to explicitly lock the migrations table for each
// transaction. Currently the default behaviour.
//
// Intended for use with NewMigrator.
func WithExplicitLock() MigratorOpt {
	return func(m *Migrator) error {
		m.explicitLock = true
		return nil
	}
}

// WithoutExplicitLock initialises a Migrator which will not
// try to explicitly lock the migrations table for each
// transaction.
//
// Intended for use with NewMigrator.
func WithoutExplicitLock() MigratorOpt {
	return func(m *Migrator) error {
		m.explicitLock = false
		return nil
	}
}

// WithLogger initialises a Migrator with a logger to use
// when logging output. If no logger is specified, the
// standard logger from the log package is used.
//
// Intended for use with NewMigrator.
func WithLogger(logger *log.Logger) MigratorOpt {
	return func(m *Migrator) error {
		m.logger = logger
		return nil
	}
}

// WithVerbosity initialises a Migrator with verbosity level
// (default: 0). Non-zero values will increase the amount
// of logging.
//
// It is an error to set both verbosity and quiet to a
// non-zero value.
//
// Intended for use with NewMigrator.
func WithVerbosity(verbosity uint) MigratorOpt {
	return func(m *Migrator) error {
		if m.verbosity < 0 {
			return errors.Wrapf(
				ErrInvalidVerbosity,
				"current verbosity %d",
				m.verbosity,
			)
		}
		m.verbosity = int(verbosity)
		return nil
	}
}

// WithQuiet initialises a Migrator with quiet level
// (default: 0). Non-zero values will decrease the amount
// of logging.
//
// It is an error to set both verbosity and quiet to a
// non-zero value.
//
// Intended for use with NewMigrator.
func WithQuiet(quiet uint) MigratorOpt {
	return func(m *Migrator) error {
		if m.verbosity > 0 {
			return errors.Wrapf(
				ErrInvalidVerbosity,
				"current verbosity %d",
				m.verbosity,
			)
		}
		m.verbosity = int(quiet)
		return nil
	}
}

// WithContext initialises a Migrator with a context object.
// This is intended to allow the migrations to be easily
// stopped in a CLI tool.
//
// Intended for use with NewMigrator.
func WithContext(ctx context.Context) MigratorOpt {
	return func(m *Migrator) error {
		m.ctx = ctx
		return nil
	}
}

// WithTemplateDir initialises a Migrator with a given
// template directory. When searching for named templates,
// this directory will be used.
//
// Intended for use with NewMigrator.
func WithTemplateDir(path string) MigratorOpt {
	return func(m *Migrator) error {
		m.templateDir = path
		return nil
	}
}

// WithMigrationDir initialises a Migrator with a given
// migration directory. When generating new migrations,
// they will be created in this directory.
//
// Intended for use with NewMigrator.
func WithMigrationDir(path string) MigratorOpt {
	return func(m *Migrator) error {
		m.migrationDir = path
		return nil
	}
}

// WithPostgresFlavour initialises a Migrator with a given
// Postgres flavour. This is not directly used by Migrator
// and is merely a helper to allow migrations to act
// differently depending on which DB they are connected to.
//
// Intended for use with NewMigrator.
func WithPostgresFlavour(flavour PostgresFlavour) MigratorOpt {
	return func(m *Migrator) error {
		m.context.Flavour = flavour
		return nil
	}
}

// Register adds a migration to the list of known migrations.
//
// If a migration by the given name is already known, this will
// return ErrMigrationAlreadyExists.
//
// Valid function signatures for migration functions are:
//
//	func(*pg.Tx) error
//	func(*pg.Tx, *Context) error
func (m *Migrator) Register(
	name string,
	up interface{},
	down interface{},
) error {
	return m.registry.Register(name, up, down)
}

// logWithMinVerbosity will log the provided format string if
// a verbosity threshold is met.
//
// Quiet level is considered negative verbosity.
func (m *Migrator) logWithMinVerbosity(requiredVerbosity int, format string, v ...any) {
	currentVerbosity := m.verbosity
	if currentVerbosity >= requiredVerbosity {
		m.logger.Printf(format, v...)
	}
}

func (m *Migrator) ensureMigrationTable(db pg.DBI) error {
	_, err := db.Exec(
		`
			CREATE TABLE IF NOT EXISTS ? (
				id serial,
				name varchar,
				batch integer,
				migration_time timestamptz
			)
		`,
		pg.Ident(m.migrationTableName),
	)
	return err
}

func (m *Migrator) insertCompletedMigration(db pg.DBI, name string, batch int) error {
	_, err := db.Exec(
		"insert into ? (name, batch, migration_time) values (?, ?, now())",
		pg.Ident(m.migrationTableName),
		name,
		batch,
	)

	if err != nil {
		return err
	}

	return nil
}

func (m *Migrator) getCompletedMigrations(db pg.DBI) ([]string, error) {
	var results []string

	_, err := db.Query(&results, "select name from ?", pg.Ident(m.migrationTableName))
	if err != nil {
		return nil, err
	}

	return results, nil
}

func (m *Migrator) getMigrationsToRun(db pg.DBI) ([]string, error) {
	var completedMigrations []string

	completedMigrations, err := m.getCompletedMigrations(db)
	if err != nil {
		return nil, err
	}

	missingMigrations, _, migrationsToRun := difference(completedMigrations, m.registry.List())
	if len(missingMigrations) > 0 {
		return nil, errors.Wrapf(ErrMigrationNotKnown, "unknown migrations: %+v", missingMigrations)
	}
	if len(migrationsToRun) > 0 {
		sort.Strings(migrationsToRun)
	}

	return migrationsToRun, nil
}

// maybeLockTable will try to lock the table if explicit locking is
// enabled. If not, this does nothing.
func (m *Migrator) maybeLockTable(tx *pg.Tx) error {
	if !m.explicitLock {
		return nil
	}

	// https://www.postgresql.org/docs/current/explicit-locking.html
	// This mode protects a table against concurrent data changes, and is self-exclusive so that only one session can hold it at a time.
	// This means only one migration can run at a time, but pg_dump can still COPY from the table (since it acquires a ACCESS SHARE lock, or so I am told)
	_, err := tx.Exec(
		"LOCK ? in SHARE ROW EXCLUSIVE MODE",
		pg.Ident(m.migrationTableName),
	)
	return err
}

func (m *Migrator) getBatchNumber(db pg.DBI) (int, error) {
	var result int
	_, err := db.Query(
		pg.Scan(&result),
		"select max(batch) from ?",
		pg.Ident(m.migrationTableName),
	)
	if err != nil {
		return 0, err
	}

	return result, nil
}

// Init runs the initial migration against the configured DB. Attempting to
// run this without registering the initial migration is an error.
func (m *Migrator) Init() error {
	db := m.dbFactory()
	return db.RunInTransaction(
		m.ctx,
		func(tx *pg.Tx) (err error) {
			err = m.ensureMigrationTable(tx)
			if err != nil {
				return
			}

			err = m.maybeLockTable(tx)
			if err != nil {
				return
			}

			batch, err := m.getBatchNumber(tx)
			if err != nil {
				return err
			}

			batch++

			m.logWithMinVerbosity(0, "Batch %d run: %d migrations\n", batch, 1)
			migrationName := m.initialMigration
			migration, ok := m.registry.Get(migrationName)
			if !ok {
				err = errors.Wrap(ErrInitialMigrationNotKnown, "not found")
				return err
			}

			switch migrationFunc := migration.Up.(type) {
			case func(*pg.Tx) error:
				err = migrationFunc(tx)
			case func(*pg.Tx, *Context) error:
				err = migrationFunc(tx, &m.context)
			default:
				err = errors.Wrapf(
					ErrInvalidMigrationFuncRun,
					"invalid migration function %T",
					migrationFunc,
				)
			}
			if err != nil {
				err = errors.Wrapf(err, "%s failed to migrate", migrationName)
				return err
			}

			err = m.insertCompletedMigration(tx, migrationName, batch)
			if err != nil {
				return err
			}

			return nil
		},
	)
}

// MigrateStepByStep runs any migrations against the DB which have not been
// run yet. Each migration is run in its own transaction and marked as
// belonging to a separate batch.
func (m *Migrator) MigrateStepByStep() error {
	db := m.dbFactory()
	var migrationsToRun []string
	err := db.RunInTransaction(
		m.ctx,
		func(tx *pg.Tx) (err error) {
			err = m.ensureMigrationTable(tx)
			if err != nil {
				return
			}

			err = m.maybeLockTable(tx)
			if err != nil {
				return err
			}

			migrationsToRun, err = m.getMigrationsToRun(tx)
			return err
		},
	)

	if err != nil {
		return err
	}

	if len(migrationsToRun) == 0 {
		return nil
	}

	for _, migrationName := range migrationsToRun {
		err := db.RunInTransaction(
			m.ctx,
			func(tx *pg.Tx) (err error) {
				err = m.maybeLockTable(tx)
				if err != nil {
					return err
				}

				batch, err := m.getBatchNumber(tx)
				if err != nil {
					return err
				}

				batch++

				m.logWithMinVerbosity(0, "Batch %d run: 1 migration - %s\n", batch, migrationName)
				migration, exists := m.registry.Get(migrationName)
				if !exists {
					return errors.Wrapf(ErrMigrationNotKnown, "migration %s", migrationName)
				}

				switch migrationFunc := migration.Up.(type) {
				case func(*pg.Tx) error:
					err = migrationFunc(tx)
				case func(*pg.Tx, *Context) error:
					err = migrationFunc(tx, &m.context)
				default:
					err = errors.Wrapf(
						ErrInvalidMigrationFuncRun,
						"invalid migration function %T",
						migrationFunc,
					)
				}
				if err != nil {
					err = errors.Wrapf(err, "%s failed to migrate", migrationName)
					return err
				}

				err = m.insertCompletedMigration(tx, migrationName, batch)
				return err
			},
		)
		if err != nil {
			return err
		}
	}

	return nil
}

// MigrateStepByStep runs any migrations against the DB which have not been
// run yet. All migrations are run in a single migration and marked as
// belonging to the same batch.
func (m *Migrator) MigrateBatch() error {
	db := m.dbFactory()
	return db.RunInTransaction(
		m.ctx,
		func(tx *pg.Tx) (err error) {
			err = m.ensureMigrationTable(tx)
			if err != nil {
				return
			}

			err = m.maybeLockTable(tx)
			if err != nil {
				return err
			}

			migrationsToRun, err := m.getMigrationsToRun(tx)
			if err != nil {
				return err
			}

			if len(migrationsToRun) == 0 {
				return nil
			}

			batch, err := m.getBatchNumber(tx)
			if err != nil {
				return err
			}

			batch++

			m.logWithMinVerbosity(0, "Batch %d run: %d migrations\n", batch, len(migrationsToRun))
			for _, migrationName := range migrationsToRun {
				migration, exists := m.registry.Get(migrationName)
				if !exists {
					return errors.Wrapf(ErrMigrationNotKnown, "migration %s", migrationName)
				}

				switch migrationFunc := migration.Up.(type) {
				case func(*pg.Tx) error:
					err = migrationFunc(tx)
				case func(*pg.Tx, *Context) error:
					err = migrationFunc(tx, &m.context)
				default:
					err = errors.Wrapf(
						ErrInvalidMigrationFuncRun,
						"invalid migration function %T",
						migrationFunc,
					)
				}
				if err != nil {
					err = errors.Wrapf(err, "%s failed to migrate", migrationName)
					return err
				}

				err = m.insertCompletedMigration(tx, migrationName, batch)
				if err != nil {
					return err
				}
			}

			return err
		},
	)
}

func (m *Migrator) removeRolledbackMigration(db pg.DBI, name string) error {
	m.logWithMinVerbosity(0, "Rolled back %s\n", name)
	_, err := db.Exec("delete from ? where name = ?", pg.Ident(m.migrationTableName), name)
	if err != nil {
		return err
	}

	return nil
}

func (m *Migrator) getMigrationsInBatch(db pg.DBI, batch int) ([]string, error) {
	var results []string
	_, err := db.Query(
		&results,
		"select name from ? where batch = ? order by id desc",
		pg.Ident(m.migrationTableName),
		batch,
	)
	if err != nil {
		return nil, err
	}

	return results, nil
}

// Rollback rolls back all migrations in the most recent batch.
// If the most recent group of migrations was run with MigrateStepByStep,
// this will only roll back the most recent migration.
func (m *Migrator) Rollback() error {
	db := m.dbFactory()
	return db.RunInTransaction(
		m.ctx,
		func(tx *pg.Tx) (err error) {
			err = m.ensureMigrationTable(tx)
			if err != nil {
				return
			}

			err = m.maybeLockTable(tx)
			if err != nil {
				return err
			}

			completedMigrations, err := m.getCompletedMigrations(tx)
			if err != nil {
				return err
			}

			missingMigrations, _, _ := difference(completedMigrations, m.registry.List())
			if len(missingMigrations) > 0 {
				return errors.Wrapf(ErrMigrationNotKnown, "unknown migrations: %+v", missingMigrations)
			}

			batch, err := m.getBatchNumber(tx)
			if err != nil {
				return err
			}

			migrationsToRun, err := m.getMigrationsInBatch(tx, batch)
			if err != nil {
				return err
			}

			if len(migrationsToRun) == 0 {
				return nil
			}

			sort.Strings(migrationsToRun)
			m.logWithMinVerbosity(0, "Batch %d rollback: %d migrations\n", batch, len(migrationsToRun))
			for _, migrationName := range migrationsToRun {
				migration, exists := m.registry.Get(migrationName)
				if !exists {
					return errors.Wrapf(ErrMigrationNotKnown, "migration %s", migrationName)
				}

				switch migrationFunc := migration.Down.(type) {
				case func(*pg.Tx) error:
					err = migrationFunc(tx)
				case func(*pg.Tx, *Context) error:
					err = migrationFunc(tx, &m.context)
				default:
					err = errors.Wrapf(
						ErrInvalidMigrationFuncRun,
						"invalid migration function %T",
						migrationFunc,
					)
				}
				if err != nil {
					err = errors.Wrapf(err, "%s failed to rollback", migrationName)
					return err
				}

				err = m.removeRolledbackMigration(tx, migrationName)
				if err != nil {
					return err
				}
			}
			return nil
		},
	)
}

// Create renders the default migration template to the configured migration
// directory.
func (m *Migrator) Create(description string) error {
	caser, err := GetCaser(m.migrationNameConvention)
	if err != nil {
		return err
	}

	now := time.Now()
	filename := caser.ToFileCase(now, description)
	funcName := caser.ToFuncCase(now, description)
	filePath, err := m.createMigrationFile(
		filename,
		funcName,
		DefaultMigrationTemplate,
	)
	if err != nil {
		return err
	}

	m.logWithMinVerbosity(0, "Created migration %s", filePath)
	return nil
}

// CreateFromTemplate renders a migration template to the configured migration
// directory.
func (m *Migrator) CreateFromTemplate(description string, template string) error {
	caser, err := GetCaser(m.migrationNameConvention)
	if err != nil {
		return err
	}

	now := time.Now()
	filename := caser.ToFileCase(now, description)
	funcName := caser.ToFuncCase(now, description)
	filePath, err := m.createMigrationFile(
		filename,
		funcName,
		template,
	)
	if err != nil {
		return err
	}

	m.logWithMinVerbosity(0, "Created migration %s", filePath)
	return nil
}

func (m *Migrator) createMigrationFile(filename, funcName, templateString string) (string, error) {
	var err error
	filePath := path.Join(m.migrationDir, filename+".go")

	_, err = os.Stat(filePath)
	if !os.IsNotExist(err) {
		err := errors.Wrapf(
			ErrFileAlreadyExists,
			"file %s (%v)",
			filename,
			err,
		)
		return "", err
	}

	if len(templateString) == 0 {
		templateString = DefaultMigrationTemplate
	}

	data := map[string]interface{}{
		"Filename": filename,
		"FuncName": funcName,
	}

	t := template.Must(template.New("template").Parse(templateString))

	buf := &bytes.Buffer{}
	if err := t.Execute(buf, data); err != nil {
		return "", errors.Wrap(err, "failed to render template")
	}

	templateString = buf.String()

	err = os.WriteFile(filePath, []byte(templateString), 0644)
	if err != nil {
		return "", errors.Wrap(err, "could not write file")
	}
	return filePath, nil
}

// difference returns the sets of:
//
//	a - b
//	a union b
//	b - a
//
// Elements in the first two sets will be returned in the same order as
// their appearance in a. Elements in the last set will be returned in
// the same order as their appearance in b.
func difference(
	a []string,
	b []string,
) (
	aNotB []string,
	unionAB []string,
	bNotA []string,
) {
	aSet := make(map[string]struct{}, len(a))
	for _, name := range a {
		aSet[name] = struct{}{}
	}

	bSet := make(map[string]struct{}, len(b))
	for _, name := range b {
		bSet[name] = struct{}{}
	}

	aNotB = make([]string, 0)
	unionAB = make([]string, 0)
	bNotA = make([]string, 0)

	for _, name := range a {
		if _, ok := bSet[name]; ok {
			unionAB = append(unionAB, name)
		} else {
			aNotB = append(aNotB, name)
		}
	}
	for _, name := range b {
		if _, ok := aSet[name]; !ok {
			bNotA = append(bNotA, name)
		}
	}
	return aNotB, unionAB, bNotA
}
