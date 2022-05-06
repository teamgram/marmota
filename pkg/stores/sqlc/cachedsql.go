package sqlc

import (
	"context"
	"database/sql"
	"time"

	"github.com/teamgram/marmota/pkg/stores/sqlx"

	"github.com/zeromicro/go-zero/core/stores/cache"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/core/syncx"
)

// see doc/sql-cache.md
const cacheSafeGapBetweenIndexAndPrimary = time.Second * 5

var (
	// ErrNotFound is an alias of sqlx.ErrNotFound.
	ErrNotFound = sqlx.ErrNotFound

	// can't use one SingleFlight per conn, because multiple conns may share the same cache key.
	singleFlights = syncx.NewSingleFlight()
	stats         = cache.NewStat("sqlc")
)

type (
	// ExecFn defines the sql exec method.
	ExecFn func(ctx context.Context, conn *sqlx.DB) (int64, int64, error)
	// IndexQueryFn defines the query method that based on unique indexes.
	IndexQueryFn func(ctx context.Context, conn *sqlx.DB, v interface{}) (interface{}, error)
	// PrimaryQueryFn defines the query method that based on primary keys.
	PrimaryQueryFn func(ctx context.Context, conn *sqlx.DB, v, primary interface{}) error
	// QueryFn defines the query method.
	QueryFn func(ctx context.Context, conn *sqlx.DB, v interface{}) error

	// A CachedConn is a DB connection with cache capability.
	CachedConn struct {
		db    *sqlx.DB
		cache cache.Cache
	}
)

// NewConn returns a CachedConn with a redis cluster cache.
func NewConn(db *sqlx.DB, c cache.CacheConf, opts ...cache.Option) CachedConn {
	cc := cache.New(c, singleFlights, stats, sql.ErrNoRows, opts...)
	return NewConnWithCache(db, cc)
}

// NewConnWithCache returns a CachedConn with a custom cache.
func NewConnWithCache(db *sqlx.DB, c cache.Cache) CachedConn {
	return CachedConn{
		db:    db,
		cache: c,
	}
}

// NewNodeConn returns a CachedConn with a redis node cache.
func NewNodeConn(db *sqlx.DB, rds *redis.Redis, opts ...cache.Option) CachedConn {
	c := cache.NewNode(rds, singleFlights, stats, sql.ErrNoRows, opts...)
	return NewConnWithCache(db, c)
}

// DelCache deletes cache with keys.
func (cc CachedConn) DelCache(ctx context.Context, keys ...string) error {
	return cc.cache.DelCtx(ctx, keys...)
}

// GetCache unmarshals cache with given key into v.
func (cc CachedConn) GetCache(ctx context.Context, key string, v interface{}) error {
	return cc.cache.GetCtx(ctx, key, v)
}

// Exec runs given exec on given keys, and returns execution result.
func (cc CachedConn) Exec(ctx context.Context, exec ExecFn, keys ...string) (lastInsertId, rowsAffected int64, err error) {
	lastInsertId, rowsAffected, err = exec(ctx, cc.db)
	if err != nil {
		return
	}

	err = cc.DelCache(ctx, keys...)
	return
}

// ExecNoCache runs exec with given sql statement, without affecting cache.
func (cc CachedConn) ExecNoCache(ctx context.Context, q string, args ...interface{}) (
	sql.Result, error) {
	return cc.db.Exec(ctx, q, args...)
}

// QueryRow unmarshals into v with given key and query func.
func (cc CachedConn) QueryRow(ctx context.Context, v interface{}, key string, query QueryFn) error {
	return cc.cache.TakeCtx(ctx, v, key, func(v interface{}) error {
		return query(ctx, cc.db, v)
	})
}

// QueryRowIndex unmarshals into v with given key.
func (cc CachedConn) QueryRowIndex(ctx context.Context, v interface{}, key string,
	keyer func(primary interface{}) string, indexQuery IndexQueryFn,
	primaryQuery PrimaryQueryFn) error {
	var primaryKey interface{}
	var found bool

	if err := cc.cache.TakeWithExpireCtx(ctx, &primaryKey, key,
		func(val interface{}, expire time.Duration) (err error) {
			primaryKey, err = indexQuery(ctx, cc.db, v)
			if err != nil {
				return
			}

			found = true
			return cc.cache.SetWithExpireCtx(ctx, keyer(primaryKey), v,
				expire+cacheSafeGapBetweenIndexAndPrimary)
		}); err != nil {
		return err
	}

	if found {
		return nil
	}

	return cc.cache.TakeCtx(ctx, v, keyer(primaryKey), func(v interface{}) error {
		return primaryQuery(ctx, cc.db, v, primaryKey)
	})
}

// QueryRowNoCache unmarshals into v with given statement.
func (cc CachedConn) QueryRowNoCache(ctx context.Context, v interface{}, q string,
	args ...interface{}) error {
	return cc.db.QueryRow(ctx, v, q, args...)
}

// QueryRowsNoCache unmarshals into v with given statement.
// It doesn't use cache, because it might cause consistency problem.
func (cc CachedConn) QueryRowsNoCache(ctx context.Context, v interface{}, q string,
	args ...interface{}) error {
	return cc.db.QueryRows(ctx, v, q, args...)
}

// SetCache sets v into cache with given key.
func (cc CachedConn) SetCache(ctx context.Context, key string, val interface{}) error {
	return cc.cache.SetCtx(ctx, key, val)
}

// Transact runs given fn in transaction mode.
func (cc CachedConn) Transact(ctx context.Context, fn func(c *sqlx.Tx) error) error {
	return cc.db.Transact(ctx, fn)
}
