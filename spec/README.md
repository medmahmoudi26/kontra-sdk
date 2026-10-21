# spec/

The language-neutral part of the contract seam (③). A seller in any language, a buyer's
generated client and the catalog all read these files; `kontra.contract` loads them (from
here in a checkout, from `kontra/_spec` in the wheel) and never keeps a second copy.

| File | What it fixes |
|---|---|
| `x-kontra.v2.json` | JSON Schema 2020-12 of the `x-kontra` block: the offering facts Kontra adds at the root of a contract document (PRD §11.1). |
| `x-kontra.v1.json` | The same block before 0.2.0, which also sealed the seller's `public_key`. Read-only: a v1 document is read back for the jobs hired under it, never published, quoted or hired (decisions I22). |
| `kontra-types.v1.json` | The canonical bodies of the reserved definitions `KontraArtifact` and `KontraQuote` (decisions I2). |
| `offering-streams.v1.json` | `offering.yaml`'s `streams:`: the workflow-stream topics a buyer may read; catalog data, never hashed (decisions I31). Not part of the contract document: it travels beside it (`PublishRequest.streams`) and is fixed per published version. |

## The contract document

```yaml
nexusrpc: "1.0.0"
$schema: https://json-schema.org/draft/2020-12/schema
services:
  AcmeEnrich:                 # nexgen service key, built from the offering name
    fqn: acme.enrich          # the Nexus service name on the wire (no "/")
    description: ...
    operations:
      quote:   {fqn: quote,   input: {$ref: "#/$defs/EnrichInput"}, output: {$ref: "#/$defs/KontraQuote"}}
      execute: {fqn: execute, input: {$ref: "#/$defs/EnrichInput"}, output: {$ref: "#/$defs/EnrichOutput"}}
$defs: {EnrichInput: ..., EnrichOutput: ..., KontraArtifact: ..., KontraQuote: ...}
x-kontra: {name: acme/enrich, version: "1.2.0", ..., contract_hash: "sha256:..."}
```

Operations carry an explicit `fqn` because nexgen would otherwise put `Quote` and
`Execute` (the PascalCase of the key) on the wire, which the handler does not serve.

nexgen 0.2.5 refuses `x-kontra` as an unknown root keyword, so it is given the *nexgen
view*: the same document without `x-kontra` (`kontra.contract.nexgen_view`), taken only
after the hash has been verified.

## Rules (what the catalog enforces)

* Exactly the top-level members above; one service whose key and `fqn` are built from
  `x-kontra.name`; exactly the operations `quote` and `execute`, with the same input, and
  `quote` returning `KontraQuote`. Operation types are objects.
* Every `$ref` is `#/$defs/<Name>` and resolves; no `$id`, `$anchor`, `$dynamicRef` or
  nested `$defs`. Nothing is ever fetched.
* Every object sets `additionalProperties`: `false` for a model, a schema for a typed map;
  never absent, never `true` or `{}`.
* `Kontra*` definitions equal the bodies here after RFC 8785 canonicalisation; a seller's
  type may not start with `Kontra`. Other definitions are objects, `oneOf` unions or `$ref`
  aliases (nexgen names nothing else); enums and constrained scalars are written inline.
* nexgen's strict subset: no `anyOf`, `not`, `if`/`then`/`else`, tuples,
  `patternProperties`, `unevaluated*`, OpenAPI keywords; `type` is one string; `null`
  only as a bare `oneOf` branch; scalar defaults only, never `default: null`; the formats
  nexgen asserts (`uri`, `uri-reference`, `hostname`, `email`, `ipv4`, `ipv6`, `uuid`,
  `date`, `time`, `date-time`, `duration`); a `default`, `const` or `enum` value satisfies
  the constraints beside it, and a required member has no `default`; a `description` or
  `title` is non-empty text without control characters other than newline and tab (a
  `title` is one line).
* Strings nexgen writes into generated source are inert in every target. nexgen 0.2.5 puts
  a property name verbatim inside a Go struct tag (a backtick ends it, and what follows is
  Go code that runs in `init()`) and enum/const strings verbatim inside TypeScript template
  literals (`${...}` runs when a value is checked). So a property name is
  `[A-Za-z_][A-Za-z0-9_-]{0,99}`; an enum or const string is 1-128 of
  `A-Z a-z 0-9 _ . : / @ + = ! ? , ; # ~ ^ | ( ) [ ] -`; an `x-go-name` (and the other
  language overrides) is an identifier.
* `pattern`s pass nexgen's own gate (no inline flags, named groups, lookaround, word
  boundaries, `\A`/`\z`/`\Z`, POSIX or nested classes, set operators, lone `{ } ]`,
  non-portable escapes; its rule D7 against ambiguous unbounded loops), Go's repeat limit
  (each count, and the product of nested counts, at most 1000: nexgen emits
  `regexp.MustCompile`, which would panic a Go client at import), and one rule of ours:
  every repetition is deterministic (one character never continues a loop two ways), so no
  pattern backtracks exponentially in Python, JavaScript or Java. `^(x[a-z]+)*$` passes
  nexgen and takes Python seconds on 50 characters; it is refused here.
* No definition reaches itself through `$ref`, `oneOf` or `allOf` alone (a validator would
  recurse forever); recursion through a property or an item is fine.
* At most 32 levels deep, 256 KiB canonical, 200 definitions.
* `x-kontra` validates against `x-kontra.v2.json`, with durations and sizes in canonical
  form (largest exact duration unit, e.g. `1d` not `24h`; smallest size number, decimal on
  a tie, e.g. `5GB`, `1GiB`), and `contract_hash` equals
  `sha256(RFC 8785 JSON of the document without x-kontra.contract_hash)`.

## KontraQuote

`status` is `ok`, `rejected` or `busy`. Members that do not apply are absent, never
`null`. JSON Schema cannot say "amount when ok" without `if`/`then`, which nexgen refuses,
so the pairing is a rule of the wire model (`kontra.contract.KontraQuote`):

| status | requires | forbids |
|---|---|---|
| `ok` | `amount_minor`, `currency`, `max_duration_seconds` | `reason`, `eta_seconds` |
| `rejected` | `reason` | price members, `eta_seconds` |
| `busy` | `eta_seconds` (`reason` optional) | price members |

`amount_minor` is at most 2^53-1, the largest integer every target reads exactly. The wire
model also requires an `ok` quote's `currency` to be one the ledger holds (`USD`, `USDC`):
the gate locks the quoted amount, and the quote comes from the seller's process.

## Versioning

A file's name carries its major version. A change that would make an accepted document
invalid, or change a reserved body, is a new file (`*.v2.json`) and a new document
dialect; the `v1` files are never edited in place, because every published hash depends
on them.
