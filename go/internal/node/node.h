#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
typedef struct { const uint8_t *data; size_t len; } NodeSlice;
typedef struct { uint64_t generation, length, summary_length, index_length; size_t live, summaries; bool published; } NodeSnapshot;
void *trunk_node_open(NodeSlice, NodeSlice, size_t, uint64_t);
void trunk_node_free(void *);
NodeSlice trunk_node_error(const void *);
uint16_t trunk_node_status(const void *);
void *trunk_node_ingest(const void *, uint16_t, NodeSlice, uint64_t);
void *trunk_node_commit(const void *, uint64_t);
void *trunk_node_logs(const void *, NodeSlice, NodeSlice);
void *trunk_node_log(const void *, NodeSlice);
void *trunk_node_prepare(const void *, uint16_t, uint64_t);
void *trunk_node_register(const void *, NodeSlice, NodeSlice);
void *trunk_node_registration(const void *, NodeSlice, NodeSlice);
void *trunk_node_publish(const void *, NodeSlice, NodeSlice);
void *trunk_node_registry(const void *);
void *trunk_node_trie(const void *);
void *trunk_node_memory(const void *);
void *trunk_node_postgres(const void *, NodeSlice, NodeSlice);
void *trunk_node_serve(const void *, NodeSlice);
void *trunk_node_snapshot(const void *, uint16_t, NodeSlice, bool, uint64_t, bool, uint64_t);
NodeSnapshot trunk_node_snapshot_info(const void *);
NodeSlice trunk_node_bytes(const void *, uint32_t, size_t);
uint64_t trunk_node_number(const void *, size_t);
int32_t trunk_node_file(const void *, uint32_t);
void *trunk_node_command(NodeSlice,int32_t,int32_t,int32_t,bool);
