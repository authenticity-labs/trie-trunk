#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

typedef struct TTCollection TTCollection;
typedef struct TTCollectionSnapshot TTCollectionSnapshot;
typedef struct { uint32_t minor; uint8_t generation[16]; } TTBinding;
typedef struct { uint8_t object[16]; uint32_t revision, hour, record; } TTPosting;
typedef struct { int32_t os_error; char message[256]; } TTCollectionError;

TTCollection* tt_collection_open(int32_t, TTCollectionError*);
void tt_collection_close(TTCollection*);
TTCollectionSnapshot* tt_collection_pin(TTCollection*);
void tt_collection_release(TTCollectionSnapshot*);
void tt_collection_sources(TTCollection*, uintptr_t, void (*)(uintptr_t,uint32_t,uint8_t*,uint8_t,uint64_t));
void tt_collection_append(TTCollection*, TTBinding, uint8_t, uint64_t, uint64_t, bool, TTPosting*,size_t,uintptr_t,bool (*)(uintptr_t,bool),TTCollectionError*);
typedef struct { uint8_t id[16]; } TTLookupKey;
typedef struct { uint64_t address; uint32_t revision; TTCollectionError *error; } TTLookup;
TTLookup tt_collection_get(TTCollection*, TTBinding, TTLookupKey, uint32_t, bool);
void tt_collection_error_free(TTCollectionError*);
typedef struct TTHistoryCursor TTHistoryCursor;
typedef struct { uint32_t revision, hour, record; } TTHistoryRow;
typedef struct { size_t len; TTHistoryCursor *cursor; TTCollectionError *error; } TTBatchResult;
typedef struct { TTHistoryRow rows[768]; } TTHistoryBatch;
TTBatchResult tt_history_page(TTCollection*,TTCollectionSnapshot*,TTBinding,TTLookupKey,TTHistoryCursor*,TTHistoryBatch*);
void tt_history_close(TTHistoryCursor*);
void tt_collection_each(TTCollectionSnapshot*,TTBinding,uintptr_t,bool (*)(uintptr_t,uint8_t*,uint32_t,uint64_t),TTCollectionError*);

extern void goCollectionSource(uintptr_t,uint32_t,uint8_t*,uint8_t,uint64_t);
extern bool goCollectionVisit(uintptr_t,uint8_t*,uint32_t,uint64_t);
extern bool goCollectionHook(uintptr_t,bool);
void tt_collection_pages(TTCollection*, TTBinding, size_t*);
