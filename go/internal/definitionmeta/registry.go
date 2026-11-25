package definitionmeta

const RegistryFormat uint32 = 6

// Registry binds metadata packs and stored branches to their Definitions.
type Registry[B ~uint8] struct {
	ResourceTypes       map[string]string       `pil:",omitempty"`
	RetiredRows         map[string]map[B]uint64 `pil:",omitempty"`
	Format              uint32
	Cache, Tenant       string
	Definitions         map[string][32]byte
	Generations         map[string]map[B][16]byte
	GenerationSequences map[string]map[B]uint64 `pil:",omitempty"`
	Preferred           map[string][32]byte     `pil:",omitempty"`
	// Exact Definition digest -> PAC ranges in definition.meta and synonym.map.
	// Optional presentation/translation resources do not change event storage.
	DefinitionResources map[string]Resources `pil:",omitempty"`
}
