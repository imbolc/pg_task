# pg_task

[![License](https://img.shields.io/crates/l/pg_task.svg)](https://choosealicense.com/licenses/mit/)
[![Crates.io](https://img.shields.io/crates/v/pg_task.svg)](https://crates.io/crates/pg_task)
[![Docs.rs](https://docs.rs/pg_task/badge.svg)][docs]

FSM-based resumable Postgres tasks.

The full crate documentation, tutorial, and API examples live on
[docs.rs/pg_task][docs]

## Contributing

Create and migrate a dev db:

```sh
echo 'DATABASE_URL=postgres:///pg_task' >.env
sqlx db create
sqlx mig run
```

Please run [.pre-commit.sh] before sending a PR.

## License

This project is licensed under the
[MIT license](https://github.com/imbolc/pg_task/blob/main/LICENSE).

[.pre-commit.sh]: https://github.com/imbolc/pg_task/blob/main/.pre-commit.sh
[docs]: https://docs.rs/pg_task
