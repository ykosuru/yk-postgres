# Query Cache Architecture for PostgreSQL

## Summary

This document describes the design and implementation of a shared-memory query result cache for PostgreSQL. The cache stores results of SELECT queries in shared memory, using query text and parameter values as cache keys. Cached entries are automatically invalidated when underlying tables are modified, and least-recently-used entries are evicted when memory is full.

## Requirements

1. Cache SELECT query results in shared memory
2. Use query text + parameter values (hashed together) as cache key
3. Automatic invalidation when underlying tables or data is modified
4. LRU cache eviction when cache is full
5. Configurable cache size via GUC parameters
6. Thread safety with appropriate locking
7. Enable/disable via query_cache GUC variable (boolean, default: false)

## Scope and Goals (Version 1)

### In Scope
- Cache only top-level simple SELECT statements
- Exclude queries with modifying CTEs or writes
- Exclude queries that read temp/unlogged tables
- Exclude queries with VOLATILE functions
- Conservative invalidation: invalidate on any change to any referenced relation
- Read-only playback: on cache hit, stream cached MinimalTuples to client
- Global (shared) cache, not per-session
- Thread safety via partitioned locks and condition variables
- LRU/CLOCK eviction per partition

### Out of Scope (Future Versions)
- Caching of prepared statements with generic plans
- Fine-grained invalidation (row-level, column-level)
- Persistent cache across server restarts
- Distributed cache across multiple PostgreSQL instances
- Caching of non-SELECT queries (INSERT RETURNING, etc.)

## Files to Create or Modify

### New Files

#### src/include/utils/querycache.h
Public interface for query cache functionality.

**Contents:**
- Function prototypes:
  - `Size QueryCacheShmemSize(void)` - Calculate shared memory requirements
  - `void QueryCacheShmemInit(void)` - Initialize shared memory structures
  - `void QueryCacheInitializeHooks(void)` - Install executor hooks
- Key types and constants (opaque to other modules)
- Statistics structure definitions

#### src/backend/utils/cache/querycache.c
Complete implementation of the query cache.

**Contents:**
- Shared memory control block management
- Partitioned hash table implementation
- DSA area management for result storage
- Cache key generation and hashing
- Cache lookup and storage functions
- LRU/CLOCK eviction implementation
- Invalidation callback registration and handling
- Executor hook handlers
- Result capture and playback DestReceiver implementations

### Modified Files

#### Build System

**src/backend/utils/cache/Makefile**
- Add `querycache.o` to OBJS list

**src/backend/utils/Makefile**
- Ensure cache/ subdirectory is compiled

#### GUC Definitions

**src/backend/utils/misc/guc_parameters.dat** (DONE)
- Added `query_cache` (bool, PGC_SUSET, QUERY_TUNING_OTHER)
- Added `query_cache_size` (int, PGC_POSTMASTER, QUERY_TUNING_OTHER, GUC_UNIT_MB)

**src/backend/utils/misc/guc_tables.c** (DONE)
- Added variables: `bool query_cache_enabled = false;`
- Added variables: `int query_cache_size_mb = 0;`

**src/include/utils/guc.h** (TODO)
- Add `extern PGDLLIMPORT bool query_cache_enabled;`
- Add `extern PGDLLIMPORT int query_cache_size_mb;`

**src/backend/utils/misc/guc_tables.inc.c** (TODO)
- Auto-generated from guc_parameters.dat using gen_guc_tables.pl

#### Shared Memory Integration

**src/backend/storage/ipc/ipci.c**
- In `CreateSharedMemoryAndSemaphores()`:
  - Call `QueryCacheShmemSize()` when calculating total shared memory size
  - Call `QueryCacheShmemInit()` after LWLocks are created

#### Executor Hook Installation

**src/backend/utils/init/postinit.c**
- In `InitPostgres()`:
  - Call `QueryCacheInitializeHooks()` once per backend after GUCs are loaded

#### Invalidation Callback Registration

**src/backend/utils/cache/querycache.c**
- Register relcache invalidation callback using `CacheRegisterRelcacheCallback()`
- Called from `QueryCacheShmemInit()` or on first backend attach

## Data Structures

### Shared Control Block

Located in main shared memory, created during postmaster startup.

```c
typedef struct QueryCacheControl
{
    /* DSA area for storing result payloads */
    dsa_handle dsa_handle;
    
    /* Memory management */
    Size capacity_bytes;              /* from query_cache_size_mb */
    pg_atomic_uint64 used_bytes;      /* current memory usage */
    
    /* Partitioning for reduced contention */
    int partitions;                   /* power of 2, e.g., 64 or 128 */
    LWLockPadded *part_lwlocks;       /* one LWLock per partition */
    
    /* Hash tables (using ShmemInitHash with HASH_PARTITION) */
    HTAB *entry_hash;                 /* key_hash -> QueryCacheEntry */
    HTAB *rel_to_entries;             /* relOid -> list of entry IDs */
    
    /* Statistics (optional) */
    pg_atomic_uint64 hits;
    pg_atomic_uint64 misses;
    pg_atomic_uint64 evictions;
    pg_atomic_uint64 invalidations;
} QueryCacheControl;
```

### Cache Key

The cache key is computed by hashing the following components:

```c
typedef struct QueryCacheKey
{
    /* Query identification */
    const char *query_text;           /* raw query text bytes */
    int query_len;                    /* length of query text */
    
    /* Parameter signature */
    int n_params;                     /* number of parameters */
    Oid *param_type_oids;             /* array of parameter type OIDs */
    
    /* Parameter values */
    Datum *param_values;              /* parameter values */
    bool *param_nulls;                /* null flags */
    
    /* Context */
    Oid database_oid;                 /* MyDatabaseId */
    Oid user_oid;                     /* GetUserId() */
    
    /* Future: search_path, collation, timezone fingerprints */
} QueryCacheKey;

/* Computed hash */
typedef uint64 QueryCacheKeyHash;
```

### Cache Entry

Stored in shared memory hash table, one per cached query result.

```c
typedef struct QueryCacheEntry
{
    /* Key and status */
    uint64 key_hash;                  /* hash of cache key */
    pg_atomic_uint32 status;          /* IN_PROGRESS / VALID / INVALID */
    pg_atomic_uint32 refcount;        /* prevent eviction while in use */
    
    /* Memory management */
    Size size_bytes;                  /* total size of this entry */
    dsa_pointer payload_ptr;          /* result payload in DSA */
    
    /* Dependencies for invalidation */
    dsa_pointer deps_ptr;             /* array of dependent rel OIDs */
    int ndeps;                        /* number of dependencies */
    
    /* Eviction metadata */
    uint32 partition_id;              /* which partition owns this entry */
    uint8 clock_usage;                /* usage count for CLOCK algorithm */
    dsa_pointer lru_prev;             /* LRU list pointers (if using LRU) */
    dsa_pointer lru_next;
    
    /* Synchronization */
    ConditionVariable cv;             /* waiters if IN_PROGRESS */
    
    /* Metadata */
    TimestampTz created_at;           /* creation timestamp */
} QueryCacheEntry;

/* Entry status values */
#define QCACHE_STATUS_IN_PROGRESS  0
#define QCACHE_STATUS_VALID        1
#define QCACHE_STATUS_INVALID      2
```

### Result Payload

Stored in DSA area, referenced by `payload_ptr` in cache entry.

```c
typedef struct QueryCachePayload
{
    /* Tuple descriptor information */
    int natts;                        /* number of attributes */
    
    /* Per-attribute metadata (array of natts) */
    struct {
        Oid atttypid;                 /* type OID */
        int32 atttypmod;              /* type modifier */
        Oid attcollation;             /* collation OID */
        int16 attlen;                 /* attribute length */
        bool attbyval;                /* pass by value? */
        char attalign;                /* alignment */
    } *attrs;
    
    /* Result storage */
    int ntuples;                      /* number of tuples */
    
    /* Option A: sharedtuplestore handle */
    dsa_pointer tuplestore_handle;
    
    /* Option B: inline storage */
    uint32 *tuple_offsets;            /* array of offsets to MinimalTuples */
    char *tuple_data;                 /* contiguous MinimalTuple payloads */
} QueryCachePayload;
```

## Key Algorithms

### Eligibility Check

Before attempting to cache a query, check eligibility:

```c
bool QueryCacheIsEligible(QueryDesc *queryDesc)
{
    /* Must be enabled */
    if (!query_cache_enabled || query_cache_size_mb == 0)
        return false;
    
    /* Must be top-level SELECT */
    if (queryDesc->operation != CMD_SELECT)
        return false;
    
    /* No modifying CTEs */
    if (HasModifyingCTE(queryDesc->plannedstmt))
        return false;
    
    /* No temp/unlogged tables */
    if (HasTempOrUnloggedTables(queryDesc->plannedstmt))
        return false;
    
    /* No VOLATILE functions */
    if (HasVolatileFunctions(queryDesc->plannedstmt))
        return false;
    
    /* Not SERIALIZABLE isolation */
    if (XactIsoLevel == XACT_SERIALIZABLE)
        return false;
    
    /* Result size limit check (if configured) */
    /* ... */
    
    return true;
}
```

### Cache Key Generation

```c
QueryCacheKeyHash ComputeCacheKey(QueryDesc *queryDesc, ParamListInfo params)
{
    uint64 hash = 0;
    
    /* Hash query text */
    hash = hash_combine64(hash, hash_bytes((uint8 *) queryDesc->sourceText,
                                           strlen(queryDesc->sourceText)));
    
    /* Hash parameter signature */
    if (params)
    {
        hash = hash_combine64(hash, params->numParams);
        for (int i = 0; i < params->numParams; i++)
        {
            hash = hash_combine64(hash, params->paramTypes[i]);
        }
        
        /* Hash parameter values */
        for (int i = 0; i < params->numParams; i++)
        {
            if (params->params[i].isnull)
            {
                hash = hash_combine64(hash, 0);
            }
            else
            {
                /* Hash Datum bytes based on type */
                hash = HashDatum(params->params[i].value,
                                params->paramTypes[i],
                                hash);
            }
        }
    }
    
    /* Hash context */
    hash = hash_combine64(hash, MyDatabaseId);
    hash = hash_combine64(hash, GetUserId());
    
    return hash;
}
```

### Cache Lookup (Hit Path)

```c
bool QueryCacheLookup(QueryCacheKeyHash key_hash, QueryDesc *queryDesc)
{
    uint32 partition_id = key_hash % control->partitions;
    LWLock *lock = &control->part_lwlocks[partition_id].lock;
    QueryCacheEntry *entry;
    bool found = false;
    
    /* Lock partition */
    LWLockAcquire(lock, LW_SHARED);
    
    /* Probe hash table */
    entry = hash_search(control->entry_hash, &key_hash, HASH_FIND, NULL);
    
    if (entry)
    {
        uint32 status = pg_atomic_read_u32(&entry->status);
        
        if (status == QCACHE_STATUS_VALID)
        {
            /* Hit! Bump usage count */
            if (entry->clock_usage < 255)
                entry->clock_usage++;
            
            /* Increment refcount to prevent eviction */
            pg_atomic_fetch_add_u32(&entry->refcount, 1);
            
            LWLockRelease(lock);
            
            /* Stream cached result to client */
            PlaybackCachedResult(entry, queryDesc);
            
            /* Decrement refcount */
            pg_atomic_fetch_sub_u32(&entry->refcount, 1);
            
            pg_atomic_fetch_add_u64(&control->hits, 1);
            found = true;
        }
        else if (status == QCACHE_STATUS_IN_PROGRESS)
        {
            /* Wait for fill to complete */
            LWLockRelease(lock);
            ConditionVariableSleep(&entry->cv, WAIT_EVENT_QUERY_CACHE_FILL);
            ConditionVariableCancelSleep();
            
            /* Retry lookup */
            return QueryCacheLookup(key_hash, queryDesc);
        }
    }
    
    if (!found)
    {
        LWLockRelease(lock);
        pg_atomic_fetch_add_u64(&control->misses, 1);
    }
    
    return found;
}
```

### Cache Storage (Miss Path)

```c
void QueryCacheStore(QueryCacheKeyHash key_hash, QueryDesc *queryDesc,
                     MinimalTuple *tuples, int ntuples)
{
    uint32 partition_id = key_hash % control->partitions;
    LWLock *lock = &control->part_lwlocks[partition_id].lock;
    QueryCacheEntry *entry;
    Size entry_size;
    
    /* Serialize result into DSA */
    dsa_pointer payload_ptr = SerializeResult(tuples, ntuples, queryDesc->tupDesc,
                                              &entry_size);
    
    /* Extract dependencies */
    Oid *dep_oids;
    int ndeps;
    ExtractDependencies(queryDesc->plannedstmt, &dep_oids, &ndeps);
    
    /* Lock partition */
    LWLockAcquire(lock, LW_EXCLUSIVE);
    
    /* Check if we need to evict */
    while (pg_atomic_read_u64(&control->used_bytes) + entry_size > 
           control->capacity_bytes)
    {
        if (!EvictOneEntry(partition_id))
            break;  /* No more entries to evict */
    }
    
    /* Create or update entry */
    entry = hash_search(control->entry_hash, &key_hash, HASH_ENTER, NULL);
    
    entry->key_hash = key_hash;
    pg_atomic_write_u32(&entry->status, QCACHE_STATUS_VALID);
    pg_atomic_write_u32(&entry->refcount, 0);
    entry->size_bytes = entry_size;
    entry->payload_ptr = payload_ptr;
    entry->deps_ptr = StoreDependencies(dep_oids, ndeps);
    entry->ndeps = ndeps;
    entry->partition_id = partition_id;
    entry->clock_usage = 1;
    entry->created_at = GetCurrentTimestamp();
    
    /* Update memory accounting */
    pg_atomic_fetch_add_u64(&control->used_bytes, entry_size);
    
    /* Add to rel->entries mapping for invalidation */
    for (int i = 0; i < ndeps; i++)
    {
        AddEntryToDependencyMap(dep_oids[i], entry);
    }
    
    /* Signal any waiters */
    ConditionVariableBroadcast(&entry->cv);
    
    LWLockRelease(lock);
}
```

### Eviction (CLOCK Algorithm)

```c
bool EvictOneEntry(uint32 partition_id)
{
    /* Walk CLOCK hand for this partition */
    QueryCacheEntry *entry;
    bool evicted = false;
    
    /* Iterate through entries in partition */
    HASH_SEQ_STATUS status;
    hash_seq_init(&status, control->entry_hash);
    
    while ((entry = hash_seq_search(&status)) != NULL)
    {
        if (entry->partition_id != partition_id)
            continue;
        
        /* Skip if in use */
        if (pg_atomic_read_u32(&entry->refcount) > 0)
            continue;
        
        /* Skip if in progress */
        if (pg_atomic_read_u32(&entry->status) == QCACHE_STATUS_IN_PROGRESS)
            continue;
        
        /* Decrement usage count */
        if (entry->clock_usage > 0)
        {
            entry->clock_usage--;
            continue;
        }
        
        /* Evict this entry */
        EvictEntry(entry);
        evicted = true;
        break;
    }
    
    hash_seq_term(&status);
    return evicted;
}

void EvictEntry(QueryCacheEntry *entry)
{
    /* Free DSA memory */
    dsa_free(control->dsa_handle, entry->payload_ptr);
    dsa_free(control->dsa_handle, entry->deps_ptr);
    
    /* Update memory accounting */
    pg_atomic_fetch_sub_u64(&control->used_bytes, entry->size_bytes);
    
    /* Remove from dependency mappings */
    RemoveEntryFromDependencyMaps(entry);
    
    /* Remove from hash table */
    hash_search(control->entry_hash, &entry->key_hash, HASH_REMOVE, NULL);
    
    /* Update stats */
    pg_atomic_fetch_add_u64(&control->evictions, 1);
}
```

### Invalidation

```c
void QueryCacheInvalidateCallback(Datum arg, Oid relid)
{
    /* Find all entries that depend on this relation */
    List *entries = GetEntriesDependingOnRel(relid);
    ListCell *lc;
    
    foreach(lc, entries)
    {
        QueryCacheEntry *entry = (QueryCacheEntry *) lfirst(lc);
        uint32 partition_id = entry->partition_id;
        LWLock *lock = &control->part_lwlocks[partition_id].lock;
        
        /* Lock partition */
        LWLockAcquire(lock, LW_EXCLUSIVE);
        
        /* Mark as invalid and evict */
        pg_atomic_write_u32(&entry->status, QCACHE_STATUS_INVALID);
        EvictEntry(entry);
        
        LWLockRelease(lock);
    }
    
    pg_atomic_fetch_add_u64(&control->invalidations, 1);
}
```

## Integration Points

### Shared Memory Initialization

In `src/backend/storage/ipc/ipci.c`, function `CreateSharedMemoryAndSemaphores()`:

```c
/* Add to size calculation */
size = add_size(size, QueryCacheShmemSize());

/* Add to initialization */
QueryCacheShmemInit();
```

### Executor Hook Installation

In `src/backend/utils/init/postinit.c`, function `InitPostgres()`:

```c
/* After GUCs are loaded */
if (query_cache_enabled && query_cache_size_mb > 0)
{
    QueryCacheInitializeHooks();
}
```

### Executor Hooks Implementation

```c
static ExecutorStart_hook_type prev_ExecutorStart_hook = NULL;
static ExecutorRun_hook_type prev_ExecutorRun_hook = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd_hook = NULL;

void QueryCacheInitializeHooks(void)
{
    prev_ExecutorStart_hook = ExecutorStart_hook;
    ExecutorStart_hook = QueryCache_ExecutorStart;
    
    prev_ExecutorRun_hook = ExecutorRun_hook;
    ExecutorRun_hook = QueryCache_ExecutorRun;
    
    prev_ExecutorEnd_hook = ExecutorEnd_hook;
    ExecutorEnd_hook = QueryCache_ExecutorEnd;
}
```

## Locking Strategy

### Partition Locks
- One LWLock per partition (e.g., 64 or 128 partitions)
- Held only during metadata updates (hash table operations, LRU/CLOCK updates)
- Never held during query execution or large DSA allocations

### Entry-Level Synchronization
- `status` and `refcount` fields use atomics (`pg_atomic_uint32`)
- `ConditionVariable` for waiters when entry is IN_PROGRESS
- Refcount prevents eviction of in-use entries

### Memory Accounting
- `used_bytes` uses atomic operations (`pg_atomic_uint64`)
- Updated atomically during allocation and deallocation

## Memory Management

### DSA Area
- Created during postmaster startup with size = `query_cache_size_mb`
- Handle stored in control block
- Backends attach on first use via `dsa_attach()`

### Shared Hash Tables
- Entry hash: `ShmemInitHash()` with `HASH_PARTITION` flag
- Rel-to-entries map: `ShmemInitHash()` for dependency tracking

### Result Storage
- **Option A (Preferred)**: Use `sharedtuplestore` for multi-backend reads
- **Option B (Fallback)**: Custom DSA container with MinimalTuple array

## Implementation Phases

### Phase 1: Foundation (This PR)
- ✅ GUC parameters (`query_cache`, `query_cache_size`)
- ✅ Architecture document (this file)
- TODO: Header file (`querycache.h`) with stub functions
- TODO: Implementation file (`querycache.c`) with stubs
- TODO: Build system updates (Makefiles)
- TODO: Extern declarations in `guc.h`
- TODO: Generate `guc_tables.inc.c`

**Deliverable**: Compiles successfully, no functional behavior yet

### Phase 2: Shared Memory Infrastructure
- Implement `QueryCacheShmemSize()` and `QueryCacheShmemInit()`
- Create control block, DSA area, partitioned locks
- Create shared hash tables
- Wire into `ipci.c`
- Implement backend attach logic

**Deliverable**: Shared memory structures initialized, can be inspected

### Phase 3: Cache Lookup and Miss Handling
- Implement eligibility checks
- Implement cache key generation and hashing
- Implement cache lookup (always returns miss initially)
- Install executor hooks
- Add statistics counters

**Deliverable**: Hooks installed, eligibility filtering works, always misses

### Phase 4: Result Capture and Storage
- Implement tee DestReceiver to capture MinimalTuples
- Implement result serialization into DSA
- Implement dependency extraction
- Implement cache storage on miss
- Publish entries as VALID

**Deliverable**: Queries are cached, but no invalidation yet

### Phase 5: Cache Playback
- Implement cache playback DestReceiver
- Stream cached results to client on hit
- Handle concurrent access (IN_PROGRESS state, condition variables)

**Deliverable**: Cache hits work, results are served from cache

### Phase 6: Invalidation
- Extract relation dependencies from PlannedStmt
- Maintain rel-to-entries mapping
- Register relcache invalidation callback
- Implement invalidation handler

**Deliverable**: Cached entries are invalidated on table changes

### Phase 7: Eviction
- Implement CLOCK algorithm per partition
- Implement memory accounting
- Trigger eviction on memory pressure
- Respect refcount and IN_PROGRESS status

**Deliverable**: Cache evicts LRU entries when full

### Phase 8: Hardening and Observability
- Add entry size limits
- Improve eligibility checks (STABLE functions, RLS, etc.)
- Add `pg_query_cache_stats()` function
- Add regression tests
- Performance testing and tuning

**Deliverable**: Production-ready query cache

## Testing Strategy

### Unit Tests
- Cache key generation with various parameter types
- Hash collision handling
- Eviction algorithm correctness
- Invalidation callback handling

### Integration Tests
- End-to-end cache hit/miss scenarios
- Concurrent access (multiple backends)
- Invalidation on INSERT/UPDATE/DELETE
- Memory limits and eviction
- GUC parameter changes

### Performance Tests
- Overhead when cache is disabled
- Overhead on cache miss
- Speedup on cache hit
- Scalability with multiple backends
- Memory usage patterns

## Security Considerations

### User Isolation
- Include `current_user` OID in cache key
- Consider role memberships for RLS
- Skip caching when RLS policies might apply (v1)

### Permission Checks
- Cached results bypass permission checks
- Must ensure cache key includes sufficient user context
- Consider re-checking permissions on cache hit (future)

### Resource Limits
- Enforce `query_cache_size` limit strictly
- Prevent single query from monopolizing cache
- Consider per-user or per-database quotas (future)

## Performance Considerations

### Hot Path Optimization
- Minimize work when cache is disabled (single GUC check)
- Use atomics for refcount/status to avoid lock contention
- Partition hash table to reduce lock contention
- Keep critical sections small

### Memory Efficiency
- Use MinimalTuple format for compactness
- Store only necessary TupleDesc metadata
- Consider compression for large results (future)

### Cache Effectiveness
- Monitor hit rate via statistics
- Tune partition count based on workload
- Consider adaptive eviction policies (future)

## Future Enhancements

### Version 2+
- Fine-grained invalidation (track which rows/columns changed)
- Support for prepared statements with generic plans
- Persistent cache across server restarts
- Compression of cached results
- Adaptive eviction policies (frequency-based, size-based)
- Per-database or per-user cache quotas
- Cache warming on startup
- Distributed cache for multi-node setups

### Observability
- Detailed statistics (hit rate, eviction rate, memory usage per partition)
- `pg_query_cache_entries` view to inspect cache contents
- Logging of cache events (configurable)
- Integration with `pg_stat_statements`

## References

- PostgreSQL Shared Memory: `src/backend/storage/ipc/ipci.c`
- DSA (Dynamic Shared Areas): `src/include/utils/dsa.h`
- Shared Tuple Store: `src/include/utils/sharedtuplestore.h`
- Executor Hooks: `src/include/executor/executor.h`
- Relcache Invalidation: `src/backend/utils/cache/relcache.c`
- GUC System: `src/backend/utils/misc/guc.c`

## Authors

- Architecture: Devin AI
- Implementation: TBD
- Review: TBD

## Revision History

- 2025-11-22: Initial architecture document created
