# Third-party software notices

`THIRD-PARTY-LICENSES.html` lists the production dependencies included in the
Linux x86_64 binary and their license texts. It is generated from the locked
dependency graph with cargo-about 0.9.1:

```sh
cargo about generate --locked --all-features --fail \
  --output-file third-party/THIRD-PARTY-LICENSES.html third-party/about.hbs
```

The template renders every cargo-about `licenses` record, not only one SPDX
overview text per license family, so dependency-specific copyright notices are
preserved. CI compares the rendered record count with cargo-about's JSON output
and requires an exact file match. Update it whenever `Cargo.lock`,
`about.toml`, or `third-party/about.hbs` changes.
