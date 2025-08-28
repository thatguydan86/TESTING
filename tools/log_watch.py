"""
Log watch utility for the Zoopla CI pipeline.

This script attempts to fetch recent logs from two sources:
  - GitHub Actions logs for the current workflow run (if available)
  - Railway logs for the "zoopla" service (requires `railway` CLI and token)

It then scans the logs for common blocking errors (proxy errors, network
failures, Python exceptions, and missing summary lines) and writes
structured reports to docs/ERRORS.md and logs/.

Exit codes:
  0 → no blocking errors found and summary line present
  1 → blocking errors detected or summary line missing
"""
import os
import re
import sys
import subprocess
import datetime


def fetch_railway_logs(service: str = "zoopla", limit: int = 300) -> str:
    """Fetch latest logs from Railway for the specified service."""
    try:
        cmd = ["railway", "logs", "--service", service, "--limit", str(limit)]
        env = os.environ.copy()
        if "RAILWAY_TOKEN" in env:
            env["RAILWAY_TOKEN"] = env["RAILWAY_TOKEN"]
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env, timeout=60)
        return result.stdout + "\n" + result.stderr
    except Exception:
        return ""


def fetch_github_actions_logs() -> str:
    """Attempt to fetch GitHub Actions logs using the gh CLI."""
    run_id = os.getenv("GITHUB_RUN_ID")
    repo = os.getenv("GITHUB_REPOSITORY")
    if not run_id or not repo:
        return ""
    try:
        tmp_dir = "/tmp/gh_logs"
        subprocess.run([
            "gh", "run", "download", run_id,
            "--repo", repo, "--log", "--dir", tmp_dir
        ], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=60)
        text = ""
        for root, _dirs, files in os.walk(tmp_dir):
            for fname in files:
                path = os.path.join(root, fname)
                try:
                    with open(path, "r", encoding="utf-8", errors="ignore") as f:
                        text += f.read() + "\n"
                except Exception:
                    continue
        return text
    except Exception:
        return ""


def scan_logs(log_text: str) -> list[str]:
    """Scan logs for known blocking errors."""
    errors: list[str] = []
    if not log_text:
        errors.append("No logs captured.")
        return errors
    patterns = [
        r"ERR_INVALID_ARGUMENT",
        r"net::ERR_",
        r"\b403\b",
        r"\b429\b",
        r"\b5\d{2}\b",
        r"Traceback",
        r"KeyError",
        r"TypeError",
        r"ValueError",
        r"pytest failed",
        r"PlaywrightError",
        r"TimeoutError",
    ]
    compiled = [re.compile(p) for p in patterns]
    for line in log_text.splitlines():
        for pat in compiled:
            if pat.search(line):
                errors.append(line.strip())
                break
    if "ZP_RUN_COMPLETE" not in log_text:
        errors.append("Missing summary line ZP_RUN_COMPLETE")
    # deduplicate while preserving order
    seen: set[str] = set()
    deduped: list[str] = []
    for err in errors:
        if err not in seen:
            deduped.append(err)
            seen.add(err)
    return deduped


def write_reports(errors: list[str], ci_logs: str, railway_logs: str) -> None:
    """Write errors and log snapshots to docs/ERRORS.md and logs directory."""
    ts = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    os.makedirs("docs", exist_ok=True)
    os.makedirs("logs/ci", exist_ok=True)
    os.makedirs("logs/railway", exist_ok=True)
    with open("docs/ERRORS.md", "w", encoding="utf-8") as f:
        f.write("# Detected Errors\n\n")
        if errors:
            for err in errors:
                f.write(f"- {err}\n")
        else:
            f.write("None.\n")
    with open(f"logs/ci/{ts}.txt", "w", encoding="utf-8") as f:
        f.write(ci_logs)
    with open(f"logs/railway/{ts}.txt", "w", encoding="utf-8") as f:
        f.write(railway_logs)


def main() -> int:
    ci = fetch_github_actions_logs()
    rw = fetch_railway_logs()
    combined = ci + "\n" + rw
    errors = scan_logs(combined)
    write_reports(errors, ci, rw)
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
