# Known Limitations

- The original ERD image artifacts render, but some long column names are
  clipped in the exported images. The `.erd` source files are preserved in
  `docs/diagrams/` for future regeneration.
- The staging SQL load script uses placeholder dataset paths. Update the
  `LOAD DATA LOCAL INFILE` paths before running it on a new machine.
- The notebook-exported ETL scripts preserve the submitted business logic and
  still require a live MySQL-compatible database plus Java/Spark runtime for
  full execution.
- Live database tests are opt-in and skipped by default to keep CI safe.
