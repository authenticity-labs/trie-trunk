import {renderSpecificationText} from './specification-text.mjs';
// Definition documentation snapshot; update alongside the authoring language.
export const definitionSpecification = `
DOC creates, edits, transcodes, and samples data into binary \`.rx\`
Definitions. Each \`.rx\` is a PAC whose members contain a Definition header
and an ordered field array. RX and FX use Definitions to prepare prescriptions
for encoding and reading Pills.

DOC imports supported CDDL constructs and provides an editable JSON view.
Unsupported constructs are reported explicitly.

### Definition Records

Each PAC member contains \`header\` and \`fields\`. \`fields\` is ordered; changing its
order changes the structural hash. The embedded meta-layout is selected by
\`pil-version\` (currently zero). No separate meta-definition file is required.

| Header Field | Type |
| --- | --- |
| \`pil-version\` | uint(2) |
| \`rx-name\` | tstr |
| \`rx-major\`, \`rx-minor\` | uint(4) |
| \`rx-hash\` | bytes(32), structural SHA-256 |
| \`namespace\` | tstr |

Each member hash includes its ordered
structural fields and referenced structural hashes, excluding hash slots and
annotations. The PAC's named \`rx-hash\` application header hashes the ordered
member hashes. It identifies structure, not annotation integrity.

| Field Property | Representation | Meaning |
| --- | --- | --- |
| \`name\` | tstr | Field name |
| \`type\` | JSON \`[first, second]\`; separate uint(1) slots on wire | Categories or PAC pointer |
| \`size\` | uint(2), explicit zero means variable | Scalar bytes, container count, or enum index bytes |
| \`annotations\` | array of tstr | Opaque application annotations |
| \`values\` | Existing PIL object/array encoding | Enum literals, inline field records, choices, or member pointer ordinal |
| \`element-size\` | uint(2), present on wire for arrays | Homogeneous array element width, zero means variable |
| \`key-size\`, \`value-size\` | uint(2), present on wire for maps | Map member widths, zero means variable |

### Paired Types and Sizes

The Definition's fixed-value section places each applicable type beside its
size. Type slots are one byte; size slots are two bytes, little-endian. These are
fields of the embedded Definition meta-layout, encoded by the existing PIL codec.

| Shape | Definition Fixed Values In Order |
| --- | --- |
| Scalar | \`[type][size]\` |
| Array | \`[ARRAY][count][element-type][element-size]\` |
| Map | \`[key-type][key-size][value-type][value-size][count]\` |
| Enum | \`[ENUM][index-size][literal-type]\` |
| Inline Record | \`[RECORD][size]\`, child fields in values |
| Choice | \`[CHOICE][size]\`, alternatives in values |
| Object Pointer | \`[OBJECT][size][ordinal]\` |

Scalars carry no second type or unused member-size payload. Applicable widths
are explicitly zero when variable; irrelevant slots use PIL's existing absent
state and contribute no value bytes. Maps retain a separate count: it is not a
key or value width. The JSON editing view keeps the logical type pair and named
sizing properties; its scalar second-byte zero is not a stored placeholder.
Omitted author-input sizes normalize to zero for their applicable wire slots.
\`doc read\` renders the primary size explicitly, including zero.

An enum's fixed width is its stored index width. Dictionary literals use their
own type encoding and require no additional literal-size slot: an enum with a
one-byte index can reference strings of different lengths. A pointer ordinal is
an address, not a type; it has no extra size. Inline records use their child
Definitions. \`any\` identifies types in its values. Parameterized choices remain
deferred; this change does not introduce new descriptors for alternatives.

Empty annotations/values are omitted. Null is not an alternative spelling of
omitted sizing. \`values-type\` is not stored: the logical type pair determines
interpretation. Scalars have no values payload. Enum literal count is
\`len(values)\`, not \`size\`. 

| Categories | Code |
| --- | --- |
| None (reserved, not a field type) | 0 |
| uint, int, float, bytes, tstr, bool, null | 1–7 respectively |
| Unassigned | 8–121 |
| any, choice, record, enum, array, object pointer | 122–127 respectively |

The first byte's high bit (\`0x80\`) marks a Header field. Its low seven bits are
the category. The second category and pointer ordinal are never masked: an object ordinal
may use all eight bits. Category codes do not encode sizes.

| Form | Type Pair | Payload |
| --- | --- | --- |
| Scalar | \`[S, 0]\` | None |
| Array | \`[126, E]\` | None for scalar elements |
| Map | \`[K, V]\` | None for scalar members |
| Enum | \`[125, L]\` | Ordered literals of category L |
| Inline Record | \`[124, 0]\` | Ordered nested field records |
| Choice | \`[123, 0]\` | Two-byte alternatives |
| Object Pointer | \`[127, ordinal]\` | None |

Arrays/maps of object references put the target ordinal in \`values\` and use 127
as their member category. PAC pointers address members 0–255 within the same
file. Name-only, external, cyclic, and out-of-range references are rejected.
CDDL named groups and record references expand inline, preserving their field
annotations. Maps require explicit scalar key categories; ARRAY/OBJECT keys are
not accepted.

CDDL \`uint\` becomes size zero; \`uint .size 4\` becomes size four. \`float16/32/64\`
normalize to float with size 2/4/8. Homogeneous arrays and typed maps preserve
member widths independently of container count. Homogeneous literal choices
become enums; plain type alternatives become choices. Optional markers add no
required-field state: all fields are optional. Bounded integer ranges select a
fitting width, without promising runtime validation. Controls other than the
supported \`.size\` mapping, including \`.feature\`, are rejected rather than
silently reinterpreted as application annotations.

\`\`\`sh
doc write example.cddl example.rx
doc read example.rx > example.json
doc write example.json edited.rx
doc edit edited.rx
doc discover objects.jsonl --base example.rx --output candidate.rx
\`\`\`

JSON input is either an array of \`{ "header": ..., "fields": [...] }\` members or
an ordered field array with \`--name\`. Sampling streams observations into a
candidate, preserves base ordinals, and reports conflicts for review; it does not
infer permanent fixed widths or dictionaries from observed values. Output files
must be new paths. DOC presents the native Definition as editable JSON.

### Unsupported Definition Constructs

The following constructs are not currently supported:

- Prescribed nested array/map types with parameters at every level.
- Parameterized choice alternatives, such as \`bytes(10) / bytes(16)\`.
- Positional tuple constraints. \`array:any\` already describes mixed element types;
  converting a CDDL tuple to it would lose the tuple's positional constraints.
- Undefined as a value distinct from null and an absent field.
- General tagged-value semantics and other CBOR simple values.
- Exact decimal fractions and big floats, and full large-integer coverage.
- Preservation of CDDL constraints without requiring their runtime enforcement.
- Recursive definitions.

Unsupported constructs are rejected during ingestion.

The Definition type byte uses a seven-bit category with the high
bit designating a Header field, using the same categories for headers and ordinary
fields. This structural role participates in the Definition hash; opaque application
annotations do not. 

Header fields must form a contiguous prefix of each ordered field list. A Header
field after an ordinary field is invalid; authoring and reading must reject it,
not silently reorder fields. The prefix may be empty or include every field.
Headers and ordinary fields share one ordinal sequence and the normal field-state
mask; the boundary adds no padding, mask, or stored header count. Absent and null
retain their existing meanings. This is ordinal ordering, not a new contiguous
physical header section: fixed and variable values retain the existing PIL layout.
Reordering fields changes structural identity and requires matching encoded records.

DOC trims surrounding whitespace from field names and annotation strings before
validation, hashing, and serialization. Reject field names that become empty and
name collisions introduced by trimming; reject empty annotation entries. This is
authoring normalization, not a codec behavior. Preserve internal whitespace,
case, literal dictionary values, and quoted annotation arguments.

After surrounding whitespace is trimmed, application annotation strings preserve
their spelling, including prefixes such as \`@\`, \`#\`, and \`!\`, arguments, and
ordering. Do not strip or normalize prefixes: applications may assign different
meanings to otherwise identical names. DOC updates the visible JSON on editor focus loss, F2/F3 transitions, or Save.
Idle validation does not rewrite text while typing.
Annotations remain opaque to the record codec and excluded from structural hashes.
Optional annotation handlers belong to an explicitly invoked application utility;
a custom handler replaces the supplied handler unless the application delegates.
The default convention \`@default(n)\` denotes an ordinal, never a literal default
value; only the application handler interprets it.

The default CDDL comment reader recognizes \`@\`, \`#\`, and \`!\` prefixes, splitting
commas only outside quoted arguments and nested brackets. The authoring API
\`cddl_import::read_with_annotation_handler\` lets an application replace that
comment convention entirely. This is explicit Definition authoring, never an
implicit callback from record encoding, decoding, or field access. JSON
annotations may contain other application syntax unchanged.

Known parser limitation: the existing complete-CDDL-tree test for \`b64'…'\`
literals fails in the vendored CDDL reader. These literals are not advertised as
supported native ingestion. The Definition test suites cover supported mappings,
PAC references, annotation hashing, trimming, and invalid-input rejection.
`;
export const nativeDefinitionExample = [
  {
    "header": {
      "pil-version": 0,
      "rx-name": "example",
      "rx-major": 0,
      "rx-minor": 0,
      "namespace": ""
    },
    "fields": [
      {
        "name": "id",
        "type": [
          1,
          0
        ],
        "size": 2
      },
      {
        "name": "count",
        "type": [
          1,
          0
        ],
        "size": 4
      },
      {
        "name": "state",
        "type": [
          125,
          5
        ],
        "size": 1,
        "values": [
          "pending",
          "authorized",
          "canceled"
        ]
      },
      {
        "name": "note",
        "type": [
          5,
          0
        ],
        "size": 0
      }
    ]
  }
];
export const nativeDefinitionCDDL = "example = {\n  ? id: uint .size 2,\n  ? count: uint .size 4,\n  ? state: \"pending\" / \"authorized\" / \"canceled\",\n  ? note: tstr\n}\n";

export function specificationText(runtimeReference, libraryReference) {
 // The established record API has not yet been switched to the new authoring format.
 // Keep that contract explicit instead of presenting two incompatible RX schemas as one.
 const runtime = runtimeReference.slice(runtimeReference.indexOf('### 1. Definition'))
   .split('DOC authors Definitions interactively')[0]
   .replace('### 1. Definition', '### 1. Established Runtime Definition')
   .replace('The wire Definition is PIL; the codec reads its layout automatically.',
     'The established runtime Definition is PIL. This section describes the input still used by the record APIs, not the new PAC-based DOC output.')
   .replace('### 10. Schema Mapping', '### 10. Established Runtime Schema Mapping')
   .replace('The current compiler translates CDDL', 'The established record-layout compiler translates CDDL')
   .replace('### 11. Command-Line Reference', '### 11. Existing RX Record Commands')
   .replaceAll('](schemas/definition.cddl)', '](https://trietrunk.com/definition.cddl)');
 return renderSpecificationText(
   '## Scope\n\nDOC creates and transcodes binary .rx Definitions, including candidates sampled from data. RX encodes and reads Pills through prescriptions; FX provides fixed-length execution. PIL describes typed values using shared Definitions. PAC groups Pills or child packs with member addressing, optional shared values, and compression. Sections 1–12 describe the current record codec and library APIs. Appendix A describes the PAC-based Definition language authored by DOC. The current record APIs do not yet consume that authoring format.\n\n'+
   runtime+'\n'+libraryReference+'\n## Appendix A. Definition Language and DOC Authoring\n\n'+definitionSpecification
 );
}
