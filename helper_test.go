package closuretree_test

import (
	"context"
	"fmt"
	"github.com/davecgh/go-spew/spew"
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
	"testing"
	"time"
)

func getTargetDBs(t *testing.T) map[string]*gorm.DB {

	gormLogger := logger.New(
		log.New(os.Stdout, "\r\n", log.LstdFlags),
		logger.Config{
			SlowThreshold:             time.Second,
			LogLevel:                  logger.Warn,
			IgnoreRecordNotFoundError: true,
			Colorful:                  false,
		},
	)
	databases := make(map[string]*gorm.DB)

	sqliteDbFile := newSqliteDbNoCGO(t, gormLogger)
	databases["sqlite_no_cgo"] = sqliteDbFile

	sqlitefilecgo := newSqliteCgo(t, gormLogger)
	databases["sqlite_with_cgo"] = sqlitefilecgo

	_, skipTestCont := os.LookupEnv("SKIP_TESTCONTAINERS")
	if testing.Short() || skipTestCont {
		return databases
	}

	// discard testcontainer messages
	testcontainers.Logger = testcontainers.TestLogger(t)

	// Initialize MySQL and add it to the map
	_, skipMysql := os.LookupEnv("SKIP_MYSQL")
	if !skipMysql {
		mysqlDb := newMySQLDb(t, gormLogger)
		databases["mysql"] = mysqlDb
	}

	// Initialize PostgresSQL and add it to the map
	_, skipPostgres := os.LookupEnv("SKIP_POSTGRES")
	if !skipPostgres {
		postgresDb := newPostgresDb(t, gormLogger)
		databases["postgres"] = postgresDb
	}

	return databases
}

var _ = spew.Dump //keep the dependency

func newSqliteDbNoCGO(t *testing.T, logger logger.Interface) *gorm.DB {
	// NOTE: in memory database does not work well with concurrency, if not used with shared
	tmpDir := t.TempDir()
	dbFile := filepath.Join(tmpDir, "test_no_cg.sqlite")

	_, sqliteLocal := os.LookupEnv("SQLITE_LOCAL_DIR")
	if sqliteLocal {
		dbFile = "./test_no_cg.sqlite"
		if _, err := os.Stat(dbFile); err == nil {
			if err = os.Remove(dbFile); err != nil {
				t.Fatal(err)
			}
		}
	}

	db, err := gorm.Open(sqlite.Open(dbFile), &gorm.Config{
		Logger: logger,
	})

	if err != nil {
		t.Fatalf("failed to open test database: %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("failed to get underlying DB: %v", err)
	}

	t.Cleanup(func() {
		sqlDB.Close() // Ensure all connections are closed after the test
	})
	return db
}

func newSqliteCgo(t *testing.T, logger logger.Interface) *gorm.DB {
	// NOTE: in memory database does not work well with concurrency, if not used with shared
	tmpDir := t.TempDir()
	dbFile := filepath.Join(tmpDir, "testdb_cgo.sqlite")
	_, sqliteLocal := os.LookupEnv("SQLITE_LOCAL_DIR")
	if sqliteLocal {
		dbFile = "./testdb_cgo.sqlite"
		if _, err := os.Stat(dbFile); err == nil {
			if err = os.Remove(dbFile); err != nil {
				t.Fatal(err)
			}
		}
	}

	db, err := gorm.Open(sqlitecgo.Open(dbFile), &gorm.Config{
		Logger: logger,
	})

	if err != nil {
		t.Fatalf("failed to open test database: %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("failed to get underlying DB: %v", err)
	}

	t.Cleanup(func() {
		sqlDB.Close() // Ensure all connections are closed after the test
	})
	return db
}

func newMySQLDb(t *testing.T, logger logger.Interface) *gorm.DB {
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
		t.Fatalf("failed to start MySQL container: %v", err)
	}

	host, err := mysqlContainer.Host(ctx)
	if err != nil {
		t.Fatalf("failed to get MySQL container host: %v", err)
	}

	port, err := mysqlContainer.MappedPort(ctx, "3306")
	if err != nil {
		t.Fatalf("failed to get MySQL container port: %v", err)
	}

	dsn := fmt.Sprintf("testuser:password@tcp(%s:%s)/testdb?charset=utf8mb4&parseTime=True&loc=Local", host, port.Port())
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: logger,
	})
	if err != nil {
		t.Fatalf("failed to connect to MySQL test database: %v", err)
	}

	t.Cleanup(func() {
		if err := mysqlContainer.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate MySQL container: %v", err)
		}
	})

	return db
}

func newPostgresDb(t *testing.T, logger logger.Interface) *gorm.DB {
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
		t.Fatalf("failed to start PostgreSQL container: %v", err)
	}

	host, err := postgresContainer.Host(ctx)
	if err != nil {
		t.Fatalf("failed to get PostgreSQL container host: %v", err)
	}

	port, err := postgresContainer.MappedPort(ctx, "5432")
	if err != nil {
		t.Fatalf("failed to get PostgreSQL container port: %v", err)
	}

	dsn := fmt.Sprintf("host=%s port=%s user=testuser dbname=testdb password=password sslmode=disable", host, port.Port())
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger,
	})
	if err != nil {
		t.Fatalf("failed to connect to PostgreSQL test database: %v", err)
	}

	t.Cleanup(func() {
		if err := postgresContainer.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate PostgreSQL container: %v", err)
		}
	})

	return db
}
