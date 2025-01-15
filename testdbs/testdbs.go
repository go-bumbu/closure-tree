package testdbs

import (
	"context"
	"flag"
	"fmt"
	"github.com/glebarez/sqlite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	sqlitecgo "gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"log"
	"os"
	"path/filepath"
	"time"
)

type DbItem struct {
	Name  string
	Conn  *gorm.DB
	clean func()
}

var tmpDir = ""

var TargetDBS = []DbItem{}

func InitDBS() {
	mkTmpDir()
	gormLogger := logger.New(
		log.New(os.Stdout, "\r\n", log.LstdFlags),
		logger.Config{
			SlowThreshold:             time.Second,
			LogLevel:                  logger.Warn,
			IgnoreRecordNotFoundError: true,
			Colorful:                  false,
		},
	)

	// Initialize sqlite using the NO CGO implementation
	sqliteDbFile := newSqliteDbNoCgo(tmpDir, gormLogger)
	TargetDBS = append(TargetDBS, DbItem{
		Name: "sqlite_no_cgo",
		Conn: sqliteDbFile,
	})

	// stop here if running short tests
	flag.Parse()
	_, testAllEnv := os.LookupEnv("TESTDBS_ALL")
	if !testAllEnv && !testAll() {
		return
	}

	// Initialize sqlite using the CGO implementation
	sqliteDbFileCgo := newSqliteCgo(tmpDir, gormLogger)
	TargetDBS = append(TargetDBS, DbItem{
		Name: "sqlite_cgo",
		Conn: sqliteDbFileCgo,
	})

	// Initialize MySQL and add it to the map
	mysqlDb, clean := newMySQLDb(gormLogger)
	TargetDBS = append(TargetDBS, DbItem{
		Name:  "mysql",
		Conn:  mysqlDb,
		clean: clean,
	})

	// Initialize PostgresSQL and add it to the map
	postgresDb, clean := newPostgresDb(gormLogger)
	TargetDBS = append(TargetDBS, DbItem{
		Name:  "postgres",
		Conn:  postgresDb,
		clean: clean,
	})
}

var runAllDbs *bool

func init() {
	runAllDbs = flag.Bool("alldbs", false, "run the tests on all available DBs")
}
func testAll() bool {
	if runAllDbs == nil {
		panic("testing: testAll called before Init")
	}
	// Catch code that calls this from TestMain without first calling flag.Parse.
	if !flag.Parsed() {
		panic("testing: testAll called before Parse")
	}
	return *runAllDbs
}

func Clean() {
	closeDbs()
	cleanTmpDir()
}

func closeDbs() {
	for _, db := range TargetDBS {
		sqlDB, err := db.Conn.DB()
		if err != nil {
			panic(fmt.Sprintf("failed to get underlying DB: %v", err))
		}
		err = sqlDB.Close() // Ensure all connections are closed after the test
		if err != nil {
			panic(fmt.Sprintf("failed to close underlying DB: %v", err))
		}
	}

	for _, db := range TargetDBS {
		if db.clean != nil {
			db.clean()
		}
	}
}

func mkTmpDir() {
	dir, err := os.MkdirTemp("", "example")
	if err != nil {
		panic(fmt.Sprintf("error creating temporary directory: %v", err))
	}
	tmpDir = dir
}

func cleanTmpDir() {
	err := os.RemoveAll(tmpDir)
	if err != nil {
		panic(fmt.Sprintf("Error cleaning up temporary directory: %v", err))
	}
}

func newSqliteDbNoCgo(tmpDir string, logger logger.Interface) *gorm.DB {
	// NOTE: in memory database does not work well with concurrency, if not used with shared
	dbFile := filepath.Join(tmpDir, "test_no_cgo.sqlite")

	_, sqliteLocal := os.LookupEnv("SQLITE_LOCAL_DIR")
	if sqliteLocal {
		dbFile = "./test_no_cg.sqlite"
		if _, err := os.Stat(dbFile); err == nil {
			if err = os.Remove(dbFile); err != nil {
				panic(err)
			}
		}
	}

	db, err := gorm.Open(sqlite.Open(dbFile), &gorm.Config{
		Logger: logger,
	})
	if err != nil {
		panic(fmt.Sprintf("failed to open test database: %v", err))
	}
	return db
}

func newSqliteCgo(tmpDir string, logger logger.Interface) *gorm.DB {
	// NOTE: in memory database does not work well with concurrency, if not used with shared
	dbFile := filepath.Join(tmpDir, "testdb_cgo.sqlite")
	_, sqliteLocal := os.LookupEnv("SQLITE_LOCAL_DIR")
	if sqliteLocal {
		dbFile = "./testdb_cgo.sqlite"
		if _, err := os.Stat(dbFile); err == nil {
			if err = os.Remove(dbFile); err != nil {
				panic(err)
			}
		}
	}

	db, err := gorm.Open(sqlitecgo.Open(dbFile), &gorm.Config{
		Logger: logger,
	})
	if err != nil {
		panic(fmt.Sprintf("failed to open test database: %v", err))
	}
	return db
}

func newMySQLDb(logger logger.Interface) (*gorm.DB, func()) {
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "mysql:8.0",
		ExposedPorts: []string{"3306/tcp"},
		Env: map[string]string{
			"MYSQL_ROOT_PASSWORD": "password",
			"MYSQL_DATABASE":      "testdb",
			"MYSQL_USER":          "testuser",
			"MYSQL_PASSWORD":      "password",
		},
		WaitingFor: wait.ForListeningPort("3306/tcp").WithStartupTimeout(60 * time.Second),
	}

	mysqlContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		panic(fmt.Sprintf("failed to start MySQL container: %v", err))
	}

	host, err := mysqlContainer.Host(ctx)
	if err != nil {
		panic(fmt.Sprintf("failed to get MySQL container host: %v", err))
	}

	port, err := mysqlContainer.MappedPort(ctx, "3306")
	if err != nil {
		panic(fmt.Sprintf("failed to get MySQL container port: %v", err))
	}

	dsn := fmt.Sprintf("testuser:password@tcp(%s:%s)/testdb?charset=utf8mb4&parseTime=True&loc=Local", host, port.Port())
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: logger,
	})
	if err != nil {
		panic(fmt.Sprintf("failed to connect to MySQL test database: %v", err))
	}

	cleanup := func() {
		if err := mysqlContainer.Terminate(ctx); err != nil {
			panic(fmt.Sprintf("failed to terminate MySQL container: %v", err))
		}
	}

	return db, cleanup
}

func newPostgresDb(logger logger.Interface) (*gorm.DB, func()) {
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "postgres:13",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "testuser",
			"POSTGRES_PASSWORD": "password",
			"POSTGRES_DB":       "testdb",
		},
		WaitingFor: wait.ForListeningPort("5432/tcp").WithStartupTimeout(60 * time.Second),
	}

	postgresContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		panic(fmt.Sprintf("failed to start PostgreSQL container: %v", err))
	}

	host, err := postgresContainer.Host(ctx)
	if err != nil {
		panic(fmt.Sprintf("failed to get PostgreSQL container host: %v", err))
	}

	port, err := postgresContainer.MappedPort(ctx, "5432")
	if err != nil {
		panic(fmt.Sprintf("failed to get PostgreSQL container port: %v", err))
	}

	dsn := fmt.Sprintf("host=%s port=%s user=testuser dbname=testdb password=password sslmode=disable", host, port.Port())
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger,
	})
	if err != nil {
		panic(fmt.Sprintf("failed to connect to PostgreSQL test database: %v", err))
	}
	cleanup := func() {
		if err := postgresContainer.Terminate(ctx); err != nil {
			panic(fmt.Sprintf("failed to terminate MySQL container: %v", err))
		}
	}
	return db, cleanup
}
