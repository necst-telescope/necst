# Obsolete example paths to remove

The current OMU 1.85 m examples have been consolidated into:

```text
docs/examples/OMU1P85M_4board_12co_13co_c18o_current/
```

Remove the old duplicated directory if it exists in a local checkout:

```bash
rm -rf docs/examples/OMU1P85M_single115_xx
```

The analysis TOML files from the old directory have been renamed and moved into the current 4-board directory.
