@echo off
setlocal

rem ===========================================================================
rem  BUILD.CMD - build the M2_P2P_DB executable for deployment.
rem
rem  Builds the binary and copies it out as target\M2_P2P_DB-<git-sha>.exe, so
rem  every artifact is traceable to the exact revision it was built from. That
rem  matters when one binary is rolled out across a fleet: the filename itself
rem  says which revision a machine is running.
rem
rem  Usage:  BUILD.CMD            release build (default)
rem          BUILD.CMD debug      debug build
rem
rem  The name carries the 8-char SHA of HEAD, plus "-dirty" when tracked files
rem  have uncommitted changes, so a modified working tree can never be shipped
rem  under a clean revision's name.
rem ===========================================================================

rem Always operate from the repository root, wherever this was invoked from.
cd /d "%~dp0"

set "CRATE=M2_P2P_DB"

rem Profile: release by default; `BUILD.CMD debug` selects the dev profile.
set "PROFILE=release"
set "CARGO_ARGS=--release"
if /i "%~1"=="debug" (
    set "PROFILE=debug"
    set "CARGO_ARGS="
)

where cargo >nul 2>&1
if errorlevel 1 (
    echo [ERROR] cargo was not found on PATH.
    echo         Install Rust from https://rustup.rs and reopen the terminal.
    exit /b 1
)

rem --- Version tag: short SHA of HEAD, plus "-dirty" if tracked files changed.
rem (Untracked files are ignored: they do not affect the compiled binary.)
rem Note: use `--short` with no argument - the 8-char length is its default, and
rem `--short=8` is parsed by git as an extra revision argument and fails.
set "SHA="
for /f "delims=" %%i in ('git rev-parse --short HEAD 2^>nul') do set "SHA=%%i"
set "DIRTY="
rem Check dirtiness in its own `if errorlevel` block, before any `set` runs:
rem `set` resets errorlevel, so testing it after an assignment always passes.
if defined SHA (
    git diff-index --quiet HEAD -- >nul 2>&1
    if errorlevel 1 (
        set "DIRTY=-dirty"
    )
)
if not defined SHA (
    set "SHA=unknown"
    echo [WARN] Not a git checkout - naming the build without a revision.
)

echo === Building %CRATE% (%PROFILE%, %SHA%%DIRTY%) ===
rem --locked keeps the build reproducible against the committed Cargo.lock,
rem which matters when the same binary is rolled out to many machines.
cargo build %CARGO_ARGS% --locked -p %CRATE%
if errorlevel 1 (
    echo.
    echo [ERROR] Build failed. See the cargo output above.
    exit /b 1
)

set "BUILT=target\%PROFILE%\%CRATE%.exe"
if not exist "%BUILT%" (
    echo.
    echo [ERROR] Build reported success but the binary is missing:
    echo         %BUILT%
    exit /b 1
)

set "OUT=target\%CRATE%-%SHA%%DIRTY%.exe"
copy /y "%BUILT%" "%OUT%" >nul
if errorlevel 1 (
    echo.
    echo [ERROR] Could not copy the binary to %OUT%
    exit /b 1
)

echo.
echo === Done ===
echo   executable: %CD%\%OUT%
for %%F in ("%OUT%") do echo   size:       %%~zF bytes
endlocal
exit /b 0
