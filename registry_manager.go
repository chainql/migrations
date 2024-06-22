package migrations

import (
	"sort"
	"sync"

	"github.com/go-pg/pg/v10"
	"github.com/pkg/errors"
)

var (
	// ErrMigrationAlreadyExists indicates that a migration is being
	// registered with a name which has already been used.
	ErrMigrationAlreadyExists = errors.New("migration already exists")

	// ErrNullMigrationFunc indicates that a migration is being
	// registered with a null function for the up or down migration.
	ErrNullMigrationFunc = errors.New("null migration functions not allowed")

	// ErrInvalidMigrationFuncRegistered indicates that a migration is being
	// registered with a function with invalid function signature.
	ErrInvalidMigrationFuncRegistered = errors.New("invalid migration function registered")
)

// PostgresFlavour indicates the type of Postgres-like API is being
// connected to.
type PostgresFlavour byte

const (
	// Postgres indicates that the DB is an original Postgres instance
	// or a DB which exactly matches the Postgres API.
	Postgres PostgresFlavour = iota

	// CockroachDB indicates that the DB is a CockroachDB instance.
	// Not all Postgres functionality is supported in CockroachDB
	// and CockroachDB has several extensions to Postgres syntax.
	CockroachDB
)

// Context contains some additional information which may be useful for
// migration functions.
type Context struct {
	// Flavour indicates which Postgres-like API can be expected.
	Flavour PostgresFlavour
}

// Registry holds a set of known migrations. Migrations can be registered
// individually with Register, or in bulk by using From to copy from
// another registry.
//
// Registered migrations may be retrieved all at once with List, or
// individually with Get.
//
// When it is necessary to register individual migrations in init functions,
// From makes it easy to copy these migrations to a registry in a Migrator.
type Registry struct {
	mtx            sync.RWMutex
	allMigrations  map[string]migration
	migrationNames []string
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
func (x *Registry) Register(name string, up interface{}, down interface{}) error {
	var err error
	x.mtx.Lock()
	defer x.mtx.Unlock()

	if x.allMigrations == nil {
		x.allMigrations = make(map[string]migration)
	}

	err = checkAllowedMigrationFunctions(up)
	if err != nil {
		return errors.Wrap(err, "invalid up migration")
	}

	err = checkAllowedMigrationFunctions(down)
	if err != nil {
		return errors.Wrap(err, "invalid down migration")
	}

	if _, exists := x.allMigrations[name]; exists {
		return errors.Wrapf(ErrMigrationAlreadyExists, "migrations %s", name)
	}
	x.migrationNames = append(x.migrationNames, name)
	x.allMigrations[name] = migration{
		Name: name,
		Up:   up,
		Down: down,
	}
	return nil
}

// Checks if supplied migrate function is allowed or not
func checkAllowedMigrationFunctions(fn interface{}) error {
	if fn == nil {
		return ErrNullMigrationFunc
	}

	switch fn.(type) {
	case func(*pg.Tx) error:
		return nil
	case func(*pg.Tx, *Context) error:
		return nil
	default:
		return errors.Wrapf(
			ErrInvalidMigrationFuncRegistered,
			"invalid function signature %T",
			fn,
		)
	}
}

// Get returns a migration with the given name and a bool
// to indicate whether it has been registered.
//
// If no migration has been registered with the given name,
// false will be returned.
func (x *Registry) Get(name string) (migration, bool) {
	x.mtx.RLock()
	defer x.mtx.RUnlock()
	if x.allMigrations == nil {
		return migration{}, false
	}

	m, exists := x.allMigrations[name]
	return m, exists
}

// From copies registered migrations from another registry. Migrations
// already in the registry are thrown away.
//
// This is a shallow copy. It is fine to add or remove items in other,
// as long as the items themselves are not modified after the copy.
func (x *Registry) From(other *Registry) {
	x.mtx.Lock()
	defer x.mtx.Unlock()
	if x.allMigrations == nil {
		x.allMigrations = make(map[string]migration)
	}

	// The other registry also needs to be locked for the duration
	// of the copy.
	other.mtx.Lock()
	defer other.mtx.Unlock()
	if len(other.allMigrations) == 0 {
		return
	}

	ensureCapacity(x, len(other.allMigrations))
	x.migrationNames = other.migrationNames[:]
	for name, migration := range other.allMigrations {
		x.allMigrations[name] = migration
	}

	sort.Strings(x.migrationNames)
}

// Sort sorts migrations in the registry by name, lexicographically.
func ensureCapacity(x *Registry, capacity int) {
	if cap(x.migrationNames) < capacity {
		tmp := make([]string, 0, capacity)
		tmp = append(tmp, x.migrationNames...)
		x.migrationNames = tmp
	}

	// There's no good way of getting the current capacity of a map,
	// so we'll only try to specify it if the registry is empty.
	if len(x.allMigrations) == 0 {
		x.allMigrations = make(map[string]migration)
	}
}

// List returns a slice of all registered migrations.
//
// This is a shallow copy. It is fine to add or remove items in the
// registry, as long as the items themselves are not modified after
// the copy.
func (x *Registry) List() []string {
	x.mtx.RLock()
	defer x.mtx.RUnlock()
	if x.allMigrations == nil {
		return []string{}
	}

	return x.migrationNames[:]
}

// Sort sorts migrations in the registry by name, lexicographically.
func (x *Registry) Sort() {
	x.mtx.Lock()
	defer x.mtx.Unlock()
	if x.allMigrations == nil {
		return
	}

	sort.Strings(x.migrationNames)
}

// EnsureCapacity increases the underlying storage of the registry,
// to reduce the chance of allocations when a known number of items
// is being added to the registry.
func (x *Registry) EnsureCapacity(capacity int) {
	x.mtx.Lock()
	defer x.mtx.Unlock()

	ensureCapacity(x, capacity)
}

// Count returns the number of migrations in the registry.
func (x *Registry) Count() int {
	x.mtx.RLock()
	defer x.mtx.RUnlock()

	return len(x.allMigrations)
}
