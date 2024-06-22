package migrations

/*
Run Runs the specified command with the options they require
Note:

	init - no options
	migrate - one option
		- "" for all migrations in a single batch
		- "one-by-one" for one migration in a batch mode
	rollback - no options
	create - two options
		- name - name of the migration (must be first)
		- template - string that contains the go code to use as a template, see migrationTemplate
*/
/*
func Run(db *pg.DB, cmd string, options ...string) error {
	switch cmd {
	case "init":
		return initialize(db)

	case "migrate":
		extra := ""
		if len(options) > 0 {
			extra = options[0]
		}
		return migrate(db, extra == "one-by-one")

	case "rollback":
		return rollback(db)

	case "create":
		name := ""
		template := ""
		if len(options) > 0 {
			name = options[0]
		}
		if len(options) > 1 {
			template = options[1]
		}
		if len(name) == 0 {
			return errors.New("Please enter migration name")
		}

		name = strings.Replace(name, " ", "_", -1)

		return create(name, template)
	}

	return errors.Errorf("unsupported command: %q", cmd)
}

func migrate(db *pg.DB, oneByOne bool) error {
	if oneByOne {
		return migrationOneByOne(db)
	}
	return migrationOneBatch(db)
}
*/
