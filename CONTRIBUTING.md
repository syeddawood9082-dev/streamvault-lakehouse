# Contributing

Thanks for looking at the code. This project stays small on purpose, so the bar for a change is that it makes the pipeline clearer or more correct.

## Getting set up

Clone the repo and start the local stack with `docker compose up -d`. The README walks through seeding data and registering the connectors. Run `python -m pytest` before you change anything so you know the suite starts green.

## Making a change

Open an issue first for anything bigger than a small fix. It keeps the discussion in one place and saves wasted work when an idea does not fit the shape of the project.

Keep pull requests focused on one thing. A connector change and a transform change land better as two pull requests than one. Write the commit message as a plain sentence about what changed.

New transform logic needs a test. Bug fixes need a test that fails without the fix. If a change touches the Hudi write path, say in the pull request how you verified the table still reads correctly in Trino.

## Style

Python code follows the layout already in the ingestion folder. Functions stay small and take plain dictionaries in and out where they can, which keeps them testable without Spark. SQL files use lowercase keywords and one clause per line.

## Reporting a problem

Open an issue with the connector config or query that failed, the error text, and what you expected to happen. Trim any real data out of examples and swap in synthetic rows.
