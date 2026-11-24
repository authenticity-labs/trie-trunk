import {highlight,highlightShell} from './highlight.mjs';
import {nativeDefinitionExample,nativeDefinitionCDDL} from './definition-reference.mjs';
import {pilBenchPanels} from './pil-bench.mjs';

export const layoutDefinition={pil:0,definition:'example/records',version:0,fields:[{name:'id',type:'U16'},{name:'count',type:'U32'},{name:'state',type:'STRING',values:['pending','authorized','canceled']},{name:'note',type:'STRING'}]};
export const layoutRecord={id:42,count:1500,state:'authorized',note:'OK'};
export const layoutHex='AB 2A 00 DC 05 00 00 01 02 00 4F 4B';
const escape=value=>String(value).replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;');
const table=(heads,rows)=>`<div class="pil-table-wrap" tabindex="0"><table class="pil-table"><thead><tr>${heads.map(h=>`<th scope="col">${h}</th>`).join('')}</tr></thead><tbody>${rows.map(row=>`<tr>${row.map(cell=>`<td>${cell}</td>`).join('')}</tr>`).join('')}</tbody></table></div>`;
const copyButton=(id,label)=>`<button class="code-copy" type="button" data-copy-code="${id}" aria-label="${label}"><svg class="copy-icon" viewBox="0 0 24 24" aria-hidden="true"><rect x="9" y="9" width="12" height="12" rx="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></svg><svg class="copied-icon" viewBox="0 0 24 24" aria-hidden="true"><path d="m5 12 4 4L19 6"/></svg><span class="copy-feedback" data-copy-feedback role="status" aria-live="polite"></span></button>`;
const codeBox=(id,title,code)=>`<div class="query-example"><div class="example-title"><span>${title}</span></div><div class="pil-copy-code">${copyButton(id,`Copy ${title}`)}<pre><code id="${id}">${highlightShell(code)}</code></pre></div></div>`;
export const definitionAgentPrompt = "Create a PIL Definition for [describe the object].\nExample data: [paste values or link a sample].\nReference: https://trietrunk.com/pill-packer.txt\n\nUse DOC to create the .rx and its editable JSON view.\nExplain the chosen types and any assumptions about the sample.\nIdentify any requested shape the current tools cannot represent.\nVerify that writing and reading the Definition preserves its fields and values.";
export const libraryExamples = [
  [
    "javascript",
    "JavaScript",
    "import { doc } from \"pill-packer\";\n\nconst rx = await doc.write(\"/pil-example.rx\");\nconst value = {\n  id: 42,\n  count: 1500,\n  state: \"authorized\",\n  note: \"OK\"\n};\n\nconst pill = rx.fill(value);\nconst restored = pill.crush();\nconsole.log(restored);\n\nrx.dispose();"
  ],
  [
    "rust",
    "Rust",
    "use pil_codec::api::{Compression, Doc, Fill, PackOptions};\n\nfn main() -> Result<(), Box<dyn std::error::Error>> {\n    let pill_bytes = std::fs::read(\"pil-example.pil\")?;\n    let rx = Doc::write(\"pil-example.rx\")?;\n    let pill = rx.fill(Fill::Encoded(&pill_bytes))?;\n    let mut pack = rx.pack(&[], PackOptions::default())?;\n\n    pack.push(pill)?;\n    let cpack = pack.seal(Compression::default())?;\n\n    let pack = cpack.unseal()?;\n    let pill = pack.get(0)?;\n    println!(\"{}\", std::str::from_utf8(&pill.json()?)?);\n    Ok(())\n}"
  ],
  [
    "go",
    "Go",
    "package main\n\nimport (\n    \"fmt\"\n    \"os\"\n    pillpacker \"github.com/authenticity-labs/pill-packer/go/api\"\n)\n\nfunc roundTrip() ([]byte, error) {\n    pillBytes, err := os.ReadFile(\"pil-example.pil\")\n    if err != nil { return nil, err }\n    rx, err := (pillpacker.Doc{}).Write(\"pil-example.rx\")\n    if err != nil { return nil, err }\n    defer rx.Close()\n    pill, err := rx.Fill(pillBytes)\n    if err != nil { return nil, err }\n    pack, err := rx.Pack(nil, pillpacker.PackOptions{})\n    if err != nil { return nil, err }\n\n    if err = pack.Push(pill); err != nil { return nil, err }\n    cpack, err := pack.Seal(pillpacker.Compression{Algorithm: \"zstd\", Level: 1})\n    if err != nil { return nil, err }\n\n    pack, err = cpack.Unseal()\n    if err != nil { return nil, err }\n    pill, err = pack.Get(0)\n    if err != nil { return nil, err }\n    return pill.JSON()\n}\n\nfunc main() {\n    json, err := roundTrip()\n    if err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }\n    fmt.Println(string(json))\n}"
  ]
];

export const prescriptionOperations = [
  ["Prescriptions", "rx.pack(objects)", "Encode an array of JavaScript objects into a Pill Pack."],
  ["Pill Packs", "pack.crush(fields)", "Read a pack as an array of objects, optionally selecting fields."],
  [
    "Docs",
    "doc.write(input[, format])",
    "Prepare a prescription from a filename or bytes. File extensions select the format; raw .rx / PIL bytes need no format argument."
  ],
  [
    "Docs",
    "doc.read(definitionBytes, recordBytes)",
    "Quick read; prepares a prescription and binds one encoded Pill."
  ],
  [
    "Prescriptions",
    "rx.fill(object) / rx.fill(bytes) / rx.fill(fieldBuffers)",
    "Encode a JavaScript object, bind encoded PIL bytes, or assemble ordered field buffers."
  ],
  [
    "Prescriptions",
    "rx.pack(headerBytes, options)",
    "Create a mutable pack. Header bytes encode named application headers."
  ],
  [
    "Prescriptions",
    "rx.open(bytes) / rx.unpack(bytes)",
    "Open a bound pack, or read all decoded members as pill views. Unseal compressed packs first."
  ],
  [
    "Pill Packs",
    "pack.push(pill) / pack.push(bytes)",
    "Append in place; keep the packing options."
  ],
  [
    "Pill Packs",
    "pack.pop()",
    "Remove and return the last pill; an empty pack returns no pill."
  ],
  [
    "Pill Packs",
    "pack.get(ordinal) / pack.offset(byteOffset)",
    "Read without removing. Ordinals start at zero; offsets are exact member-relative byte positions."
  ],
  [
    "Pill Packs",
    "pack.seal(compression)",
    "Return a sealed wrapper. The original pack and its views remain valid."
  ],
  [
    "Sealed Packs",
    "cpack.unseal()",
    "Restore a mutable pack and its representation."
  ],
  [
    "Pill Packs",
    "pack.unpack()",
    "Return logical pill views, including inherited common fields."
  ],
  [
    "Pills",
    "pill.field(name) / pill.field(ordinal)",
    "Read encoded field bytes, or an explicit absent/null state."
  ],
  [
    "Pills",
    "pill.crush() / pill.crush(fields)",
    "Materialize all present fields, or selected names / Definition ordinals. Explicitly selected absent fields are undefined; null stays null."
  ],
  [
    "Pills",
    "pill.json()",
    "Render JSON text only when requested."
  ]
];

export const libraryNotes = [
  "crush([\"state\", \"note\"]) and crush([2, 3]) select fields by name or zero-based Definition ordinal. Ordinals do not shift when earlier fields are omitted; they are not byte offsets. Reuse the selection array across Pills to reuse resolved names. The bulk path locates fields together and decodes only selected values, including common PAC fields. Dictionaries expand to text; byte values remain borrowed. Without a selection, absent fields are omitted.",
  "Raw bytes default to .rx / PIL: doc.write(bytes). The same call accepts a filename: doc.write(\"example.rx\"), Rust Doc::write(\"example.rx\"), or Go (Doc{}).Write(\"example.rx\"). Extensions select .rx/.pil, .json, or .cbor. Node, Rust, and Go read local files; browsers fetch relative paths. JavaScript also accepts JSON text and browser File objects. Explicit byte formats remain supported.",
  "Pill Packer treats named application headers as opaque bytes. Member addressing and compression do not interpret their contents.",
  "Rust and Go use idiomatic capitalization and explicit variants where overloading is unavailable. Rust uses Fill::Encoded, Fill::Fields, and Fill::Empty; raw-byte appends use push_bytes. Go Fill accepts encoded bytes or ordered field buffers.",
  "fill(bytes) binds one already-encoded Pill without copying its host buffer. Validation still runs; JavaScript uploads bytes into reusable WASM scratch memory. Field reads borrow encoded bytes rather than constructing decoded objects. Keep borrowed buffers immutable while views use them.",
  "Empty field bytes, absent, and null are distinct. JavaScript uses absent and nullValue, Rust uses FieldBytes, and Go supports explicit codec.Value states. Unknown states are rejected.",
  "push and pop mutate the pack, retain Pill references, and defer serialization until bytes or seal is requested. Fixed width, indexed access, named headers, and common-factoring options survive these mutations and the in-memory seal/unseal round trip. Reopening serialized bytes recovers the encoded representation; a factoring policy that produced no common fields cannot be inferred from the wire.",
  "Compression produces another buffer; seal does not overwrite the original pack. onlyIfSmaller explicitly permits an uncompressed result. Rust unseal consumes the wrapper and can move its uncompressed buffer. Release JavaScript prescriptions with dispose() and Go prescriptions with Close() after their views are finished; Rust uses ownership.",
  "Prepare a prescription once for repeated work. doc.read is a convenience that prepares it again. Bulk fixed-width and indexed constructors avoid creating a pill for every append. For direct application views, JavaScript provides view(ordinal, sink), popView(sink), and ordinalAtOffset(offset). For repeated reads, Go provides GetInto, OffsetInto, and FieldWith; Rust provides fill_with and field_with with reusable scratch storage. Dynamic common-field discovery can still require whole-pack checks."
];

export const librarySpecification = `### 12. Libraries and Prescriptions

DOC creates, edits, transcodes, and samples data into binary .rx Definitions.
RX encodes and reads Pills through a reusable prescription. FX does the same
for fixed-length layouts, reserving slots for optional fields.

Language wrappers expose prescriptions, Pills, and Pill Packs to applications.
JavaScript and TypeScript use the WASM codecs; Rust and Go also provide native
bindings. CLI wrappers make Definition authoring and Pill conversion available
from the terminal.

The examples below use the current RX prescription API. See Build and Setup
for supported Definition inputs and language bindings.

| Owner | Operation | Meaning |
| --- | --- | --- |
${prescriptionOperations.map(row=>`| ${row.join(' | ')} |`).join('\n')}

${libraryNotes.join('\n\n')}

#### Bulk Addressing and Headers

JavaScript accepts rx.pack(headers, recordBytes, recordWidth) and rx.pack(headers, addressBytes, offsets, recordBytes). Rust uses pack_fixed / pack_indexed; Go uses PackFixed / PackIndexed. The stored directory automatically grows through two-, three-, and four-byte offset regions. Variable packs include a directory by default; fixed packs use selector 00 with their Pill width. External variable addressing is explicit: JavaScript index: false, Rust address_bytes: 0, or Go External: true. Nonzero addressBytes / AddressBytes / address_bytes enables indexing without imposing a stored width. For supplied offsets, the explicit width describes the input buffer. Those offsets are little-endian, begin at zero, and describe every Pill; mutation regenerates the compact directory. Ordinals and offsets refer to the current pack. Keep published packs immutable while application indexes refer to them.

encodeHeaders (Rust encode_headers; Go EncodeHeaders) encodes a PIL map of text names to byte values. The library generates count, lengths, and flags in a 16-byte base header. The two-bit addressing selector determines whether a Pill-width field or a directory follows. Other optional sections are present only when flagged. The caller supplies the Prescription for PIL-member packs. Bit 5 selects complete PAC members, requiring no outer Definition. NestedPack preserves compressed and uncompressed child bytes and supports header-plus-child streaming. The PAC stores neither a Definition digest nor a mandatory checksum. Applications can carry integrity or binding metadata in named headers. Gzip retains its payload CRC; structural bounds checks are not a checksum.

${libraryExamples.map(([id,label,code])=>`#### ${label}\n\n\`\`\`${id}\n${code}\n\`\`\``).join('\n\n')}
`;
const notebook=(id,label,panels)=>`<div class="pil-notebook" data-pil-tabs data-pil-deep-links>
<div class="pil-definition-tabs" role="tablist" aria-label="${label}" aria-orientation="vertical">${panels.map(([key,title],i)=>`<button type="button" role="tab" id="${id}-tab-${key}" aria-controls="${id}-${key}" aria-selected="${i===0}" tabindex="${i===0?0:-1}">${title}</button>`).join('')}</div>
<div class="pil-notebook-pages">${panels.map(([key,title,content],i)=>`<div id="${id}-${key}" role="tabpanel" aria-labelledby="${id}-tab-${key}" tabindex="0"${i?' hidden':''}>${content}</div>`).join('')}</div></div>`;
export function pilPage(arrow){
const libraryPanels=libraryExamples.map(([id,label,code])=>[id,label,`<div class="pil-example-heading"><h3>${label}${id==='javascript'?' <small class="pil-codec-note">*Codecs in WASM</small>':''}</h3></div><div class="pil-library-code pil-copy-code">${copyButton(`library-code-${id}`,`Copy ${label} Example`)}<pre><code id="library-code-${id}">${highlight(code)}</code></pre></div><details class="pil-detail"><summary>Run This Example</summary><p>${id==='javascript'?'Install <code>pill-packer</code> in your browser application and serve the Definition at <code>/pil-example.rx</code>.':'Save these two files in the working directory used by the example.'}</p><div class="actions"><a class="text-link" href="/pil-example.rx" download>Example Definition (.rx)</a>${id==='javascript'?'':'<a class="text-link" href="/pil-example.pil" download>Example Pill (.pil)</a>'}</div><p>Filename inputs use the current source build. These samples use the runtime Definition format; the new DOC-authored format is not yet integrated with RX.</p><a class="text-link" href="https://github.com/authenticity-labs/pill-packer#build--test">Build and Setup</a></details>`]);
libraryPanels.push(['documentation','Apis',table(
 ['Owner','Operation','Result'],
 prescriptionOperations.map(([owner,operation,result])=>[owner,`<code>${escape(operation)}</code>`,result])
)]);
const definitionPanels=[
['cli','Command',`
<h3 id="reader">Read, Write, &amp; Fill Prescriptions with RX</h3><p class="seed-lead">Read Pills as JSONL, write Definitions from CDDL, and fill Pills through the codec library.</p>
<div class="query-example"><div class="example-title">Write a Prescription, Then Inspect It</div><pre><code>${highlightShell("rx write purchase.cddl purchase.rx --root purchase\nrx read purchase.rx --pretty")}</code></pre></div><p class="pil-copy">Start with the <a href="/purchase.cddl">example CDDL source</a>. The first command compiles it into <code>purchase.rx</code>; the second renders that binary Definition as readable JSON. <code>.rx</code> is the compiled schema used by the codec.</p>
<details class="pil-detail"><summary>Try the Worked Example</summary><p>These sample files belong to the 12-byte Pill shown in the Pills section. Download the JSON schema and the data you want to read into one folder. Compile your own prescription, then inspect the Pills:</p><div class="actions"><a class="text-link" href="/pil-example.json" download>Example Schema (JSON) ${arrow}</a><a class="text-link" href="/pil-example.pil" download>Example Pill (PIL) ${arrow}</a><a class="text-link" href="/pil-example.pac" download>Example Pack (PAC) ${arrow}</a><a class="text-link" href="/pil-example.compressed.pac" download>Example Compressed Pack ${arrow}</a></div><pre><code>${highlightShell("rx write pil-example.json pil-example.rx\nrx read pil-example.rx pil-example.pil\nrx read pil-example.rx pil-example.pac\nrx read pil-example.rx pil-example.compressed.pac --pretty")}</code></pre></details>

<details class="pil-detail"><summary>Base64 and Hex Output</summary><p>Output the exact PIL or PAC bytes as text, preserving pack headers and compression. No Definition is required. Use one flag at a time; neither combines with <code>--pretty</code>.</p>${codeBox("rx-text-output","PIL and PAC as Text","rx read record.pil --b64 > record.pil.b64\nrx read record.pil --hex > record.pil.hex\nrx read records.pac --b64 > records.pac.b64\nrx read records.pac --hex > records.pac.hex")}<p>Base64 uses standard padding. Hex uses lowercase digits. Both stream without line wrapping and end with one newline. Filename suffixes do not select the encoding.</p></details>
<div class="rx-install"><h3>Install DOC &amp; RX</h3><p class="pil-copy">macOS · Linux · Windows via WSL.</p>${codeBox("rx-install-command","Terminal","curl -fsSL https://trietrunk.com/install-rx.sh | sh")}<p class="pil-copy">Installs into <code>~/.cargo/bin</code> by default. <a href="/install-rx.sh">View the Installer</a></p></div>
<p class="pil-copy">Omit the data path to inspect a Definition. <code>--pretty</code> indents JSON output. Record filling uses the prescription’s <code>rx.fill</code> API; the CLI provides <code>read</code> and <code>write</code>.</p><p class="pil-copy">These RX examples use the established runtime layout. Native Definition PAC authoring is covered in Editor; its runtime integration is pending.</p><a class="text-link" href="/pill-packer.txt">Complete Specification & Commands ${arrow}</a>
`],
['editor','Editor',`
<h3 id="doc-editor">Edit a Definition with DOC</h3><p class="pil-copy">Edit the native Definition as JSON. CDDL imports and JSON samples can provide a starting point; review unsupported constructs and sample-derived suggestions before saving.</p>
${codeBox("doc-inputs","Open an Input","doc\ndoc --json samples.json\ndoc --cddl example.cddl\ndoc --rx example.rx")}
${table(["Key or Event","Action"],[["F2","Switch editing panes"],["F3","Switch input mode"],["F5","Validate the Definition"],["Ctrl+S","Save as RX"],["Editor Focus Loss","Trim field names and annotation strings visibly"]])}
<p>Trimming removes surrounding whitespace only. Prefixes such as <code>@</code>, <code>#</code>, and <code>!</code>, quoted arguments, internal spaces, case, and literal values are preserved. Empty names, duplicate names after trimming, and empty annotation entries are rejected. Idle validation does not rewrite text while typing.</p>
<h3>Write and Read Definitions</h3>
${codeBox("doc-definition-commands","Write and Read a Definition","doc write example.cddl example.rx --root example\ndoc read example.rx > example.json\ndoc write example.json edited.rx\ndoc edit edited.rx")}
<p class="pil-copy">Download the <a href="/example.cddl">example CDDL</a> or <a href="/example-definition.json">editable Definition JSON</a>. JSON is the native language’s view, not CDDL syntax. Output paths must be new files. <code>doc read</code> renders every member, including its computed structural hash.</p>
${codeBox("doc-discovery","Propose a Definition from Samples","doc discover samples.jsonl --base example.rx --output candidate.rx")}
<p class="pil-copy">Sampling preserves base field order and reports observed conflicts for review. It does not infer permanent widths or enum literals from a sample.</p>

<a class="text-link" href="https://github.com/authenticity-labs/pill-packer#doc-definition-editor">DOC Editor Reference ${arrow}</a>

`],
['schema','Schema',`
<h3>Describe the Object Once</h3><p>A Definition gives each field a name and a type. Pills carry the values without repeating those names.</p>
${table(['Field','Type','Example Value'],[['id','Unsigned Integer','42'],['count','Unsigned Integer','1500'],['state','Enum','authorized'],['note','Text','OK']])}
<p>The enum stores a small index for <code>pending</code>, <code>authorized</code>, or <code>canceled</code>. An absent field and a field whose value is null remain distinct.</p>
<details class="pil-detail"><summary>CDDL Input</summary><pre><code>${escape(nativeDefinitionCDDL)}</code></pre><a class="text-link" href="/example.cddl" download>Download CDDL</a></details>
<details class="pil-detail"><summary>Editable Definition JSON</summary><pre><code>${escape(JSON.stringify(nativeDefinitionExample,null,2))}</code></pre><a class="text-link" href="/example-definition.json" download>Download Definition JSON</a></details>
<h3>Application Annotations</h3><p>Annotations carry application instructions alongside a field. PIL preserves them; your application decides what they mean. They do not change how a Pill is encoded.</p>
<details class="pil-detail"><summary>Definition Format Reference</summary><p>The linked specification covers category codes, widths, arrays, maps, enums, Header fields, PAC references, structural hashes, and CDDL support.</p><a class="text-link" href="/pill-packer.txt">Read the Definition Specification ${arrow}</a></details>
`],
['agents','Agents',`
<h3>Teach Coding Agent Definition Creation</h3><p class="pil-copy">Replace the placeholders, then copy the instructions to your coding agent.</p>
<div class="pil-agent-card"><details class="pil-agent-details"><summary>View Agent Instructions</summary><div class="pil-copy-code">${copyButton('definition-agent-prompt','Copy Agent Instructions')}<pre><code id="definition-agent-prompt">${escape(definitionAgentPrompt)}</code></pre></div></details></div>
`]
];
const recordPanels=[
['example','Example',`<h3>One Pill, 12 Bytes</h3><pre class="pil-record"><code>${escape(JSON.stringify(layoutRecord))}</code></pre><p>Field names live in the Definition. The Pill stores values and a small mask. The repeated string <code>authorized</code> becomes dictionary entry <code>1</code>.</p>
<div class="pil-byte-example" aria-label="Twelve-byte Pill with byte positions">${[['0','AB','Field States','10 · 10 · 10 · 11'],['1–2','2A 00','id','42 · U16'],['3–6','DC 05 00 00','count','1500 · U32'],['7','01','state','Dictionary entry 1'],['8–9','02 00','Offset','2 bytes into variable data'],['10–11','4F 4B','note','OK · UTF-8']].map(([pos,hex,label,detail])=>`<div><span class="pil-position">Byte ${pos}</span><code>${hex}</code><strong>${label}</strong><span>${detail}</span></div>`).join('')}</div>
<details class="pil-detail"><summary>Read the Bytes</summary><div class="pil-explain-grid"><div><h3>Read the Mask</h3><p><code>AB</code> is binary <code>10 10 10 11</code>. Read from the high bits: <code>id</code>, <code>count</code>, and <code>state</code> use fixed-width values; <code>note</code> uses an offset. Four fields fit in one mask byte.</p></div><div><h3>Read the Values</h3><p><code>2A 00</code> is 42 and <code>DC 05 00 00</code> is 1500 in little-endian order. The dictionary turns <code>01</code> into <code>authorized</code>. No copy of that string is stored in the Pill.</p></div><div><h3>Find the Variable Data</h3><p>The variable section starts at byte 10. Its first value starts at relative offset 0; the end offset is 2. Bytes 10–11 are <code>4F 4B</code>, or <code>OK</code>. Each later value starts at the previous value’s end.</p></div></div>
</details>`],
['layout','Layout',`<h3>Pill Layout</h3><div class="pil-lane-labels" aria-hidden="true"><span>Field</span><span>Fixed</span><span>Variable</span></div>
<div class="byte-lane">${[['Field','States','2 bits per field'],['Field','Lengths','Optional integer widths'],['Fixed','Values','Scalars and dictionary IDs'],['Variable','Offsets','One per variable value'],['Variable','Values','Text, bytes, objects, arrays']].map(([group,name,detail],i)=>`<div role="group" aria-label="${group} ${name}"><small>0${i+1}</small><strong><span class="pil-lane-mobile-group">${group}</span>${name}</strong><span>${detail}</span></div>`).join('')}</div>

<details class="pil-detail"><summary>Masks and Value Storage</summary>${table(['State Bits','Meaning','Value Storage'],[['<code>00</code>','Absent','No value bytes'],['<code>01</code>','Null','No value bytes'],['<code>10</code>','Fixed-Width Value','Value or dictionary ID in the fixed section'],['<code>11</code>','Offset','End offset plus data in the variable section']])}
<p class="pil-copy">Absent and null are distinct. Neither reserves a value slot. The state mask uses <code>ceil(field count / 4)</code> bytes; unused low bits are zero. There is no standalone Pill-length prefix: masks, widths, and the final end offset determine its boundary.</p>

<h3>How Types Occupy the Layout</h3>
${table(['Representation','Section','Encoding'],[['Integers, floats, UUIDs, timestamps','Fixed','Declared width; eligible integers may use a separate width mask'],['Dictionary-backed strings','Fixed or Variable','Dictionary ID, or literal UTF-8 bytes'],['Text and bytes','Variable','Contiguous data addressed by cumulative end offsets'],['Fixed-capacity text','Fixed','Explicit byte length + UTF-8 content + zero padding to capacity'],['Arrays','Variable','Count followed by elements; variable elements have end offsets'],['Nested Pills','Variable','Schema-bound child PIL frames; the schema supplies child layouts']])}
</details><details class="pil-detail" id="cddl"><summary>Definition and Runtime Mapping</summary><p>DOC authors the native Definition language described below. Its two-byte type categories and sizing properties describe shapes; they do not introduce new Pill encodings. Supported CDDL constructs map into that language. Unsupported constructs fail explicitly.</p><p>The existing Pill APIs still consume their established compiled layouts. Their CDDL subset and runtime validation rules are separate from native DOC authoring. Application annotations in the new Definition language are opaque and do not select codec behavior.</p></details>`],
['arrays','Arrays',`<h3 id="arrays">Array Layout</h3><p>Fixed-width elements sit next to each other. Variable elements use a compact directory so an element can be located without walking the preceding values.</p><details class="pil-detail"><summary>Array Layout and Limits</summary><p>The Definition determines element width. Fixed-width elements are contiguous; variable elements use compact one-, two-, and three-byte end offsets. Two flag bits share the count header, which occupies two bytes for up to 16,383 elements and three bytes for larger arrays.</p><p>Transition ordinals locate each offset directly. Every element retains its end offset, including the last. Reads borrow the original bytes; the parent field bounds the array.</p><pre class="pil-code"><code>03 80 | 05 0a 13 | JasonSmithAlexander
Count: 3 · Flags: 10 · Ends: 5, 10, 19</code></pre><p>This string array occupies 24 bytes. Variable arrays support up to 4,194,303 elements and 16,777,215 payload bytes. Fixed-width arrays need no directory.</p></details>`]
];
const packPanels=[
['overview','Packing',`
<h3 id="packs">Shared Values</h3><p>A PAC contains ordered Pills. Values shared by every member can live in one common frame; logical Pill reads combine that frame with the member’s own values.</p>
<p>Use <code>push()</code> and <code>pop()</code> to edit a pack, <code>get(ordinal)</code> to read a member, and <code>seal()</code> to compress it. Serialization is deferred until bytes or a sealed pack are requested.</p>
<details class="pil-detail"><summary>PAC Header Layout</summary><div class="pil-pack-flow"><div>Fixed Header <span>16 bytes · Total length, flags, header length, member count</span></div><div>Optional Header Data <span>Extensions, addressing controls, named headers, common frame, directory</span></div><div>Members <span>Pills or Complete PACs</span></div></div>
${table(['Byte Position','Bytes','Header Value'],[['0–3','4','Total decoded PAC length, including header'],['4–7','4','Flags'],['8–11','4','Header length / first member position'],['12–15','4','Member count']])}
<p class="pil-copy">All four values are little-endian U32. Two flag bits select the addressing layout for each PAC. PIL members use the application’s Prescription. Nested PAC members retain their own bindings.</p>
</details>
`],
['addressing','Addressing',`<h3>Member Addressing</h3><p>Fixed-width packs use ordinal × Pill width. Variable packs use a directory that grows with the member positions. Neither requires a scan of preceding Pills.</p><details class="pil-detail"><summary>Addressing Modes</summary>
${table(['Bits 4:3','Pill Width','Addressing'],[['<code>00</code>','Nonzero U32','Fixed-width arithmetic'],['<code>00</code>','Zero U32','Caller-supplied addressing'],['<code>10</code>','Omitted','Two-byte directory'],['<code>01</code>','Omitted','Directory extending to three bytes'],['<code>11</code>','Omitted','Directory extending to four bytes']])}
<p class="pil-copy">Only <code>00</code> carries the four-byte Pill-width field. Nonzero selects fixed-width Pills; zero leaves addressing to the caller. The decoder reads each PAC’s layout independently, so fixed and variable packs can share a stream.</p>
<details class="pil-detail"><summary>Directory Layout</summary><p>Offsets are absolute, little-endian positions relative to the member body. The directory starts with two-byte entries and grows only as needed. Normal variable packs include it by default; fixed-width packs need no directory. Push and pop recalculate the regions while retaining indexed access and the other packing options.</p>
${table(['Member Start Offset','Entry Width'],[['0–65,535','2 bytes'],['65,536–16,777,215','3 bytes'],['16,777,216 and above','4 bytes']])}
<p>Selector <code>10</code> needs no transition prefix. Selector <code>01</code> prefixes one U32 identifying the first three-byte Pill ordinal. Selector <code>11</code> prefixes two U32 values identifying the first three-byte and four-byte ordinals. Equal boundaries allow a direct two-to-four-byte transition. The flags identify the layout; no prefix space is reserved in smaller packs.</p><p>To find an entry, select its ordinal’s region and multiply by that region’s width. No cumulative offset summation or member scan is required. Supplied external offsets retain their explicit input width; the pack’s stored directory is generated automatically.</p></details>
</details>`],
['compression','Compression',`
<h3>Compression</h3><p>Zstd level 1 is the default. Gzip and Brotli are also available. Compression applies to the complete decoded PAC; unseal it before accessing individual members.</p>
<p>A sealed wrapper leaves the original mutable pack and its views valid. A PAC can also carry complete child packs, preserving each child’s compression.</p>
<details class="pil-detail"><summary>Compression Wrapper and Integrity</summary><p>The 16-byte wrapper records stored length, codec, decoded length, and member count. The base PAC has no mandatory checksum or digest. Gzip includes a payload CRC; applications may supply integrity metadata in named headers. Bounds and framing checks are not payload checksums.</p></details>
`],
['nested','Nested PACs',`<h3>Pill Packs Containing Pill Packs</h3><p>Bundle complete packs without unpacking their Pills. Children retain their own headers and can use different compression algorithms.</p><details class="pil-detail"><summary>Nested Members and Streaming</summary><p class="pil-copy">Bit 5 marks members as complete PACs. One container can hold compressed and uncompressed packs without an outer Definition. Each child retains its original bytes and headers. The outer pack checks member boundaries without decompressing or decoding the children.</p><p class="pil-copy">Read and remove children as borrowed bytes. Stream the outer header followed by the original child buffers, or request one contiguous container. Common-field factoring belongs to PIL-member packs. Applications determine whether each pack is complete or temporary.</p><p class="pil-copy">The PAC has no mandatory digest or checksum. Gzip includes a payload CRC; applications may add integrity or Definition-binding metadata through named headers. Full reads validate structure and values; trusted reads still check bounds and framing. Structural validation is not a payload checksum.</p></details>`]
];
const pillGroups=[['record','Pills',recordPanels],['pack','Pill Packs',packPanels]];
const pillsNotebook=`<div class="pil-notebook" data-pil-tabs data-pil-deep-links>
<div class="pil-definition-tabs" role="tablist" aria-label="Pills and Pill Packs" aria-orientation="vertical">${pillGroups.map(([group,title,panels])=>`<div role="group" aria-labelledby="${group}-tab-title"><h2 class="pil-tab-group-title" id="${group}-tab-title">${title}</h2>${panels.map(([key,label],i)=>`<button type="button" role="tab" id="${group}-tab-${key}" aria-controls="${group}-${key}" aria-selected="${group==='record'&&i===0}" tabindex="${group==='record'&&i===0?0:-1}">${label}</button>`).join('')}</div>`).join('')}</div>
<div class="pil-notebook-pages">${pillGroups.map(([group,title,panels])=>panels.map(([key,label,content],i)=>`<div id="${group}-${key}" role="tabpanel" aria-labelledby="${group}-tab-${key}" tabindex="0"${group==='record'&&i===0?'':' hidden'}>${content}</div>`).join('')).join('')}</div></div>`;
return `
<section class="wrap pil-hero"><div class="pil-hero-copy"><p class="project-line">PIL / Positional Interchange Layout</p><h1>Positional Interchange Layout</h1><p class="intro">Typed binary data. Shared Definitions. Compact packaging.<br>For application payloads, messaging, and storage.</p><div class="actions"><a class="button" href="/pill-packer.txt">Read the Specification ${arrow}</a><a class="text-link" href="https://github.com/authenticity-labs/pill-packer">Pill Packer on GitHub ${arrow}</a></div></div><img class="pil-hero-art" src="/assets/pil-capsule-graphic.png" width="1536" height="1024" alt="" decoding="async"></section>
<section class="wrap seed-section layout-section" id="layout" aria-label="Pills and Pill Packs">${pillsNotebook}</section>
<section class="wrap seed-section pil-libraries" id="libraries"><h2>Code</h2><p class="seed-lead">Prepare a prescription, bind a Pill, then pack and read it.</p>${notebook('library','Pill Packer',libraryPanels)}</section>
<section class="wrap seed-section" id="definition"><h2>Definitions</h2><p class="seed-lead">Ordered fields define the object. CDDL is an input language; JSON is the editable view of the Definition.</p>${notebook('definition','Definitions',definitionPanels)}</section>
<section class="wrap seed-section" id="bench"><h2>Bench</h2>${notebook('pill-bench','Pill Packer Bench',pilBenchPanels(table))}</section>
`;}
