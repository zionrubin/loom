# example

One line on what this pipeline is for and what it produces.

## How it works

```
items → classify (mock-fast ↗ mock-deep) → severe-only → digest (mock-deep)
```

Describe each stage in a sentence: what the model is asked for, why the
binding is what it is, and what the reduce aggregates. Note anything a reader
would otherwise have to reverse-engineer from the prompts — a privacy scrub, a
field that must exist upstream, a fan-in chosen for a context-window reason.

## Run

```sh
go test ./examples/example              # offline, mock models, no key
go run  ./examples/example -explain     # what it would cost, no calls issued
go run  ./examples/example              # run it offline

LOOM_STATE=./state go run ./examples/example   # twice: the second is free
```

Flags: `-explain` (price only), `-budget` (hard USD ceiling), `-workers`.
