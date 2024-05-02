# DRH Orchestration Engine (DRH-OE) Strategy

The objective of DRH-OE is to get ingestable content from CSV, Excel, and
similar formats into an SQL-queryable analyst-friendly format as quickly as
possible. Once content is SQL-queryable and analyst-friendly, it can be
anonymized, enriched, cleansed, validated, transformed, and pushed to the Doltlab repository.

To quickly facilitate getting ingestable content into an SQL-queryable
analyst-friendly format, DRH-OE employs the following architecture strategy:

- SQL-native encourages performing work inside a DuckDB database as early as
  possible in the ingestion process but all orchestrated resources are currently pushed to the Diabetes Research Hub (DRH) application which is a self hosted [Doltlab](https://www.doltlab.com/).
  Doltlab can be replaced with other databases such as PostgreSQL etc, in future, if required.
- A TypeScript type-safe Runtime (Deno) is used to drive the DuckDB SQL and uses
  OS-specific execution of DuckDB Shell (CLI) for parallelization and
  scalability.
- Automatic upgrades of code using `semver` and GitHub tags.

## Getting Started

The following instructions are for setting up the code on a
[developer sandbox](<https://en.wikipedia.org/wiki/Sandbox_(software_development)>)
("[dev sandbox](<https://en.wikipedia.org/wiki/Sandbox_(software_development)>)"
or just
"[sandbox](<https://en.wikipedia.org/wiki/Sandbox_(software_development)>)").

For dev sandboxes you should be able to use workstations or laptops that have:

- Modern i5 or i7 class CPUs (circa 2021 or later)
- 32GB RAM
- 25GB of disk space
- Windows 11 if possible (Windows 10 is a possibility, too)
- Windows Subsystem for Linux (WSL)

_Instructions for deploying to test servers or production servers will be
provided later._

### Quick start (Windows):

If you have a relatively modern Windows 10/11 system with `winget` and `scoop`
you can use "Windows Terminal (Administrator)" to install Git, Deno, DuckDB,
SQLite and VS Code IDE:

```psh
$ winget install Git.Git deno SQLite.SQLite DuckDB.cli Microsoft.VisualStudioCode
$ scoop install sqlpage
```

**IMPORTANT**: `winget` installations will update your PATH so exit your
terminal, close VS Code, etc. and restart your Windows Terminal (Administrator)
session and VS Code _before you try out the code_.

If you want a nice, easier to read, CLI prompt install and setup
[Oh My Posh](https://ohmyposh.dev/docs/installation/windows).

You can also evaluate the code in a
[Windows Sandbox](./support/docs/windows-sandbox-setup.md) environment

### Quick start (Linux or MacOS):

For Linux or MacOS use [pkgx](https://pkgx.sh/) and
[eget](https://github.com/zyedidia/eget/releases) to install dependencies. For
guidance see
[Strategy Coach Workspaces Host](https://github.com/strategy-coach/workspaces-host)
and then:

```bash
$ pkgx install sqlite.org duckdb.org
$ eget lovasoa/SQLpage --to=$HOME/.local/bin/sqlpage
```

### Dependendies Elaboration

See [support/docs/dependencies.md](support/docs/dependencies.md) if you need
further details about what the code depends on at runtime.

## Try out the code

Once you've installed Git and Deno you can run the code directly from GitHub
(the latest version or any specific pinned version) without cloning the GitHub
repo or clone the repo and run the code locally.

### windows

The instructions below assume `c:\workspaces` as your workspaces root but you
should change that to `D:\` or `/home/user/workspaces` or whatever your
workspaces root happens to be (based on your operating system).

```bash
$ md c:\workspaces                        # create the destination if required
$ cd c:\workspaces                        # or wherever your sources are stored
$ deno run -A https://raw.githubusercontent.com/diabetes-research/workspaces/main/ws-bootstrap-typical.ts

# after repo cloning command (above) is complete:
$ cd github.com/diabetes-research/ingestion-center
$ deno task                               # list available tasks in `deno.jsonc`
$ deno task doctor                        # see if dependencies are installed properly
```

### linux/macos

The instructions below assume `$HOME/workspaces' (//home/<user>/workspaces) as your workspaces root but you can clone the code and run locally if you prefer.

```bash
$ cd $HOME/workspaces                     # move to the workspaces folder or or wherever your sources are stored
# after repo cloning command is complete:
$ cd github.com/diabetes-research/ingestion-center
$ deno task                               # list available tasks in `deno.jsonc`
$ deno task doctor                        # see if dependencies are installed properly
```

If `deno task doctor` reports dependencies are installed properly:

```bash
$ deno task ingestion-center-drh-test-e2e
```

Use VS Code to open the `github.com/diabetes-research/ingestion-center` folder
and open
`support\assurance\ingestion-center-elt\drh\results-test-e2e\resource.sqlite.db`
(the `SQLite3 Editor` extension, if you accepted VS Code's recommendation, will
open it).

If the above works, and you installed SQLPage, you can start a webserver, too:

```bash
$ deno task ingestion-center-drh-test-serve
```

## Build (Development) Dependencies

During build (development) in a sandbox you will need all the runtime
dependencies mentioned above plus do the following:

- Download [Visual Studio Code](https://code.visualstudio.com/download) IDE and
  use it for editing or viewing of CSV and other assets. VS Code is available
  for all major OS platforms.
  - Install the recommended extensions (see `.vscode/extensions.json`). VS Code
    will usually suggest those automatically.

## Architecture and Approach

This code allows multiple operating models, but these two are the most likely
use cases:

- Clinicians and engineers are now using deidentified continuous glucose monitor (CGM) real-world data to develop physiology models of glycemia for 1. constructing better algorithms for closed loop systems and 2. understanding performance of these systems at different levels of glycemia and during different periods of rate change. Eventually, clinicians will want to use CGM data collected by way of population health to predict, prevent, and treat complications of diabetes. Also NIH is now requiring researchers whom they fund to store collected data into a repository and no such product exists for CGM data

### Serverless Execution

All of the components of this repo should be able to run in a Serverless
environment like AWS Lamba. See:

- [serverless-duckdb](https://github.com/tobilg/serverless-duckdb) and
- [SQLpage serverless](https://github.com/lovasoa/SQLpage?tab=readme-ov-file#serverless).

### Architecture

![Architecture](support/docs/drh-product-architecture.drawio.svg)

### Detailed Architecture

![Detailed Architecture ](support/docs/drh-detailed-architecture.drawio.svg)
