#!/usr/bin/env python3
"""
Discord username sniper — POST /applications (Discord API v10)
Termux · Python 3.11+ · async · multi-token · proxy rotation · state resume

Search tags for adapting to API changes:
  [ENDPOINT]    app + bot creation URL paths
  [ERR_CODE]    400 taken-detection — TAKEN_SUBS + _is_taken()
  [BOT_CREATE]  bot token retrieval — _create_bot()
  [HEADERS]     full header set — build_headers()
  [BUILD_NUM]   client_build_number — update from Discord's /api/v9/experiments
                or by grepping for buildNumber in app.*.js on discord.com
"""
from __future__ import annotations

import asyncio
import argparse
import base64
import json
import logging
import signal
import sys
import time
from dataclasses import dataclass, field
import random
from itertools import product
from pathlib import Path
from typing import Any, Optional

import aiohttp
from aiohttp_socks import ProxyConnector
from colorama import Fore, Style, init as colorama_init
from tqdm import tqdm

colorama_init(autoreset=True)

# ── file / version ─────────────────────────────────────────────────────────────
VERSION    = "1.0.0"
STATE_FILE = "state.json"
SNIPE_FILE = "sniped.txt"
LOG_FILE   = "log.txt"

# [ENDPOINT] — update these strings if Discord renames the routes
PATH_APP = "/applications"
PATH_BOT = "/applications/{app_id}/bot"

# [ERR_CODE] — lowercase substrings that confirm a name-taken 400
# Check log.txt for raw 400 bodies and add any new patterns here.
TAKEN_SUBS: frozenset[str] = frozenset({
    "username_taken",
    "that username is taken",
    "username already taken",
    "already been registered",
})

# ── logging ────────────────────────────────────────────────────────────────────
_fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
_fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)-8s %(message)s"))
_fh.setLevel(logging.DEBUG)

_ch = logging.StreamHandler(sys.stdout)
_ch.setFormatter(logging.Formatter("%(message)s"))
_ch.setLevel(logging.INFO)

logging.basicConfig(level=logging.DEBUG, handlers=[_fh, _ch])
log = logging.getLogger("sniper")

G  = lambda s: f"{Fore.GREEN}{s}{Style.RESET_ALL}"
Y  = lambda s: f"{Fore.YELLOW}{s}{Style.RESET_ALL}"
R  = lambda s: f"{Fore.RED}{s}{Style.RESET_ALL}"
C  = lambda s: f"{Fore.CYAN}{s}{Style.RESET_ALL}"
DM = lambda s: f"{Style.DIM}{s}{Style.RESET_ALL}"


# ── Config ─────────────────────────────────────────────────────────────────────
@dataclass
class Config:
    base_url:         str           = "https://discord.com/api/v10"
    alphabet:         str           = "abcdefghijklmnopqrstuvwxyz0123456789"
    length:           int           = 4
    concurrency:      int           = 1
    rate_limit_delay: float         = 5.0
    team_id:          Optional[str] = None

    webhook_url: str = ""   # Discord webhook URL — empty = disabled

    # [HEADERS]
    user_agent: str = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/126.0.0.0 Safari/537.36"
    )

    # [HEADERS] [BUILD_NUM]
    super_properties_raw: dict[str, Any] = field(default_factory=lambda: {
        "os":                          "Windows",
        "browser":                     "Chrome",
        "device":                      "",
        "system_locale":               "en-US",
        "browser_user_agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/126.0.0.0 Safari/537.36"
        ),
        "browser_version":             "126.0.0.0",
        "os_version":                  "10",
        "referrer":                    "https://discord.com/developers/applications",
        "referring_domain":            "discord.com",
        "referrer_current":            "",
        "referring_domain_current":    "",
        "release_channel":             "stable",
        "client_build_number":         260813,
        "client_event_source":         None,
    })

    @property
    def x_super_properties(self) -> str:
        raw = json.dumps(self.super_properties_raw, separators=(",", ":"))
        return base64.b64encode(raw.encode()).decode()

    @classmethod
    def from_file(cls, path: str) -> "Config":
        p = Path(path)
        if not p.exists():
            return cls()
        data: dict[str, Any] = json.loads(p.read_text(encoding="utf-8"))
        cfg = cls()
        for k, v in data.items():
            if k == "super_properties_raw" and isinstance(v, dict):
                cfg.super_properties_raw.update(v)
            elif hasattr(cfg, k):
                setattr(cfg, k, v)
        return cfg


# ── Token pool ─────────────────────────────────────────────────────────────────
@dataclass
class _Token:
    value:         str
    last_used:     float = 0.0
    backoff_until: float = 0.0

    @property
    def is_backed_off(self) -> bool:
        return time.monotonic() < self.backoff_until

    def back_off(self, secs: float) -> None:
        self.backoff_until = time.monotonic() + secs
        log.warning(Y(f"  tok …{self.value[-8:]} backed off {secs:.0f}s"))

    async def wait(self, delay: float) -> None:
        elapsed = time.monotonic() - self.last_used
        if elapsed < delay:
            await asyncio.sleep(delay - elapsed)
        self.last_used = time.monotonic()


class TokenPool:
    def __init__(self, values: list[str], delay: float) -> None:
        self._tokens = [_Token(v) for v in values]
        self._delay  = delay
        self._idx    = 0
        self._lock   = asyncio.Lock()

    async def acquire(self) -> _Token:
        while True:
            async with self._lock:
                n = len(self._tokens)
                for _ in range(n):
                    t = self._tokens[self._idx % n]
                    self._idx = (self._idx + 1) % n
                    if not t.is_backed_off:
                        await t.wait(self._delay)
                        return t
            await asyncio.sleep(0.1)


# ── Proxy pool ─────────────────────────────────────────────────────────────────
class ProxyPool:
    def __init__(self, proxies: list[str]) -> None:
        self._proxies = proxies
        self._idx     = 0
        self._lock    = asyncio.Lock()

    @property
    def empty(self) -> bool:
        return not self._proxies

    async def next(self) -> Optional[str]:
        if not self._proxies:
            return None
        async with self._lock:
            p = self._proxies[self._idx]
            self._idx = (self._idx + 1) % len(self._proxies)
            return p


# ── Username stream ────────────────────────────────────────────────────────────
def _build_shuffled_index(total: int, seed: int) -> list[int]:
    """
    Builds a shuffled list of indices 0..total-1 using a fixed seed.
    Same seed → same order every run → state.json resume works correctly.
    Seed is saved in state.json alongside last_idx.
    36^4 = 1,679,616 ints × 4 bytes ≈ 6.4 MB RAM — fine on Termux.
    """
    idx = list(range(total))
    random.seed(seed)
    random.shuffle(idx)
    return idx


def _index_to_username(i: int, alphabet: str, length: int) -> str:
    """Converts a positional index back to its username string."""
    chars = []
    base  = len(alphabet)
    for _ in range(length):
        chars.append(alphabet[i % base])
        i //= base
    return "".join(reversed(chars))


async def username_stream(
    alphabet:  str,
    length:    int,
    start:     int = 0,
    seed:      int = 0,
):
    """
    Async generator — yields (position_in_shuffled_order, username_string).
    Order is random but deterministic (fixed seed). Resume via `start`.
    """
    total    = len(alphabet) ** length
    shuffled = _build_shuffled_index(total, seed)
    for pos in range(start, total):
        raw_idx  = shuffled[pos]
        username = _index_to_username(raw_idx, alphabet, length)
        yield pos, username
        if pos % 2000 == 0:
            await asyncio.sleep(0)


# ── State ──────────────────────────────────────────────────────────────────────
class State:
    def __init__(self, path: str = STATE_FILE) -> None:
        self._path    = path
        self.last_idx = 0
        self.seed     = random.randint(0, 2**32 - 1)  # new random seed by default
        self._load()

    def _load(self) -> None:
        try:
            data = json.loads(Path(self._path).read_text())
            self.last_idx = int(data.get("last_idx", 0))
            self.seed     = int(data.get("seed", self.seed))
            if self.last_idx:
                log.info(C(
                    f"Resuming from position {self.last_idx:,}  "
                    f"(seed={self.seed})"
                ))
        except (FileNotFoundError, json.JSONDecodeError, ValueError, KeyError):
            log.info(C(f"New session — shuffle seed={self.seed}"))

    def save(self, idx: int) -> None:
        self.last_idx = idx
        try:
            Path(self._path).write_text(
                json.dumps({"last_idx": idx, "seed": self.seed})
            )
        except OSError as exc:
            log.error(R(f"State save failed: {exc}"))


# ── HTTP ───────────────────────────────────────────────────────────────────────
def build_headers(token: str, cfg: Config) -> dict[str, str]:
    # [HEADERS]
    return {
        "Authorization":      token,
        "Content-Type":       "application/json",
        "User-Agent":         cfg.user_agent,
        "X-Super-Properties": cfg.x_super_properties,
        "X-Discord-Locale":   "en-US",
        "X-Discord-Timezone": "Europe/Paris",
        "Origin":             "https://discord.com",
        "Referer":            "https://discord.com/developers/applications",
        "Accept":             "*/*",
        "Accept-Language":    "en-US,en;q=0.9",
        "Sec-Fetch-Dest":     "empty",
        "Sec-Fetch-Mode":     "cors",
        "Sec-Fetch-Site":     "same-origin",
        "X-Debug-Options":    "bugReporterEnabled",
    }


def _make_session(proxy: Optional[str]) -> aiohttp.ClientSession:
    if proxy and proxy.lower().startswith(("socks4://", "socks5://")):
        connector = ProxyConnector.from_url(proxy, ssl=True, limit=16)
    else:
        connector = aiohttp.TCPConnector(ssl=True, limit=16)
    return aiohttp.ClientSession(connector=connector)


# ── 400 classification ─────────────────────────────────────────────────────────
def _is_taken(username: str, data: dict) -> bool:
    # [ERR_CODE]
    for err in data.get("errors", {}).get("name", {}).get("_errors", []):
        combined = (
            str(err.get("code", "")) + " " + str(err.get("message", ""))
        ).lower()
        if any(sub in combined for sub in TAKEN_SUBS):
            log.debug(DM(f"  taken [{err.get('code')}] {username!r}"))
            return True
    raw = json.dumps(data).lower()
    if any(sub in raw for sub in TAKEN_SUBS):
        log.debug(DM(f"  taken (raw fallback) {username!r}"))
        return True
    return False


# ── Bot creation ───────────────────────────────────────────────────────────────
async def _create_bot(
    session:  aiohttp.ClientSession,
    app_id:   str,
    tok:      _Token,
    cfg:      Config,
    req_kw:   dict[str, Any],
) -> str:
    # [BOT_CREATE] [ENDPOINT]
    url = cfg.base_url + PATH_BOT.format(app_id=app_id)
    try:
        async with session.post(
            url, json={},
            headers=build_headers(tok.value, cfg),
            timeout=aiohttp.ClientTimeout(total=20),
            **req_kw,
        ) as resp:
            if resp.status in (200, 201):
                body = await resp.json(content_type=None)
                token = body.get("token", "UNKNOWN")
                log.info(G(f"  ✓ bot token fetched  app={app_id}"))
                return token
            text = await resp.text()
            log.warning(Y(f"  bot create {resp.status} for app={app_id}: {text[:200]}"))
            return "UNKNOWN"
    except Exception as exc:
        log.warning(Y(f"  bot create exception app={app_id}: {exc}"))
        return "UNKNOWN"


def _save_snipe(username: str, app_id: str, bot_token: str) -> None:
    line = f"USERNAME={username!r}  APP_ID={app_id}  BOT_TOKEN={bot_token}\n"
    try:
        with open(SNIPE_FILE, "a", encoding="utf-8") as f:
            f.write(line)
    except OSError as exc:
        log.error(R(f"  Cannot write {SNIPE_FILE}: {exc}"))
        print(G(f"\n  ★ SNIPE (not saved): {line.strip()}\n"))
        return
    log.info(G(f"  ✓ saved → {SNIPE_FILE}"))

    # Fire webhook if configured
    webhook_url = _WEBHOOK_URL.strip()
    if webhook_url:
        asyncio.create_task(_notify_webhook(webhook_url, username, app_id, bot_token))


# Webhook URL — set via config.json ("webhook_url") or --webhook CLI flag.
# Left empty to disable.
_WEBHOOK_URL: str = ""


async def _notify_webhook(
    url:       str,
    username:  str,
    app_id:    str,
    bot_token: str,
) -> None:
    """
    POST a Discord embed to the webhook URL on every successful snipe.
    Non-blocking — called as a background task from _save_snipe.
    Retries up to 3 times on 429 / network error.
    """
    payload = {
        "username": "Sniper",
        "avatar_url": "https://cdn.discordapp.com/embed/avatars/0.png",
        "embeds": [
            {
                "title": "★ Username sniped",
                "color": 0x57F287,   # Discord green
                "fields": [
                    {"name": "Username",   "value": f"`{username}`",  "inline": True},
                    {"name": "App ID",     "value": f"`{app_id}`",    "inline": True},
                    {"name": "Bot Token",  "value": f"||`{bot_token}`||", "inline": False},
                ],
                "footer": {"text": "discord-sniper"},
            }
        ],
    }

    for attempt_n in range(3):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url, json=payload,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    if resp.status == 429:
                        d     = await resp.json(content_type=None)
                        after = float(d.get("retry_after", 2.0))
                        await asyncio.sleep(after)
                        continue
                    if resp.status in (200, 204):
                        log.info(G("  ✓ webhook sent"))
                        return
                    text = await resp.text()
                    log.warning(Y(f"  webhook {resp.status}: {text[:120]}"))
                    return
        except Exception as exc:
            wait = 2 ** attempt_n
            log.warning(Y(f"  webhook error: {exc} — retry {wait}s"))
            await asyncio.sleep(wait)

    log.error(R("  webhook failed after 3 attempts"))


# ── Core attempt ───────────────────────────────────────────────────────────────
async def attempt(
    username: str,
    tok:      _Token,
    proxy:    Optional[str],
    cfg:      Config,
    dry_run:  bool,
) -> str:
    if dry_run:
        await asyncio.sleep(0.005)
        return "taken"

    url  = cfg.base_url + PATH_APP  # [ENDPOINT]
    body = {"name": username, "team_id": cfg.team_id}
    hdrs = build_headers(tok.value, cfg)

    req_kw: dict[str, Any] = {}
    if proxy and not proxy.lower().startswith(("socks4://", "socks5://")):
        req_kw["proxy"] = proxy

    async with _make_session(proxy) as session:
        for n in range(6):
            try:
                async with session.post(
                    url, json=body, headers=hdrs,
                    timeout=aiohttp.ClientTimeout(total=20),
                    **req_kw,
                ) as resp:

                    if resp.status == 429:
                        try:
                            d     = await resp.json(content_type=None)
                            after = float(d.get("retry_after", 5.0))
                        except Exception:
                            after = float(resp.headers.get("Retry-After", 5.0))
                        tok.back_off(after + 0.5)
                        log.warning(Y(
                            f"  429 on {username!r} — "
                            f"tok …{tok.value[-8:]} sleeps {after:.1f}s"
                        ))
                        await asyncio.sleep(after + 0.5)
                        continue

                    if resp.status >= 500:
                        wait = min(2 ** n, 60)
                        log.warning(Y(f"  {resp.status} on {username!r} — backoff {wait}s"))
                        await asyncio.sleep(wait)
                        continue

                    if resp.status == 400:
                        try:
                            d = await resp.json(content_type=None)
                        except Exception:
                            return "error"
                        if _is_taken(username, d):
                            return "taken"
                        log.warning(Y(
                            f"  unknown 400 on {username!r}: "
                            f"{json.dumps(d)[:300]}"
                        ))
                        return "error"

                    if resp.status in (401, 403):
                        log.error(R(
                            f"  auth {resp.status} on tok …{tok.value[-8:]} "
                            f"username={username!r}"
                        ))
                        tok.back_off(3600.0)
                        return "retry"

                    if resp.status in (200, 201):
                        try:
                            d = await resp.json(content_type=None)
                        except Exception:
                            d = {}
                        app_id = d.get("id", "?")
                        log.info(G(f"\n  ★ APP CREATED — name={username!r}  id={app_id}\n"))
                        bot_token = await _create_bot(session, app_id, tok, cfg, req_kw)
                        _save_snipe(username, app_id, bot_token)
                        return "sniped"

                    log.warning(Y(f"  unexpected {resp.status} on {username!r}"))
                    return "error"

            except aiohttp.ClientProxyConnectionError as exc:
                log.warning(Y(f"  proxy error: {exc}"))
                return "retry"

            except (aiohttp.ClientError, aiohttp.ServerConnectionError) as exc:
                wait = min(2 ** n, 30)
                log.warning(Y(f"  network: {exc} — retry {wait}s"))
                await asyncio.sleep(wait)

            except asyncio.TimeoutError:
                wait = min(2 ** n, 30)
                log.warning(Y(f"  timeout on {username!r} — retry {wait}s"))
                await asyncio.sleep(wait)

    log.error(R(f"  gave up after 6 attempts: {username!r}"))
    return "error"


# ── Worker ─────────────────────────────────────────────────────────────────────
async def _worker(
    wid:       int,
    queue:     asyncio.Queue,
    tok_pool:  TokenPool,
    prx_pool:  ProxyPool,
    cfg:       Config,
    state:     State,
    dry_run:   bool,
    pbar:      tqdm,
    stop:      asyncio.Event,
    prod_done: asyncio.Event,
) -> None:
    while not stop.is_set():
        try:
            item = await asyncio.wait_for(queue.get(), timeout=0.5)
        except asyncio.TimeoutError:
            if prod_done.is_set() and queue.empty():
                break
            continue

        idx, username, retries = item
        tok   = await tok_pool.acquire()
        proxy = await prx_pool.next()

        pbar.set_description(username, refresh=False)
        result = await attempt(username, tok, proxy, cfg, dry_run)

        state.save(idx)
        pbar.update(1)
        queue.task_done()

        match result:
            case "sniped":
                log.info(G(f"[W{wid}] ★ SNIPED {username!r}"))
            case "taken":
                log.debug(DM(f"[W{wid}] taken {username!r}"))
            case "retry":
                if retries < 3:
                    log.warning(Y(f"[W{wid}] retry {retries+1}/3 — {username!r}"))
                    await queue.put((idx, username, retries + 1))
                else:
                    log.error(R(f"[W{wid}] max retries — skip {username!r}"))
            case "error":
                log.warning(Y(f"[W{wid}] error — skip {username!r}"))


# ── Main ───────────────────────────────────────────────────────────────────────
async def run(args: argparse.Namespace) -> None:
    cfg = Config.from_file(args.config)

    if args.threads  is not None: cfg.concurrency  = args.threads
    if args.alphabet is not None: cfg.alphabet     = args.alphabet
    if args.length   is not None: cfg.length       = args.length
    if args.webhook  is not None: cfg.webhook_url  = args.webhook

    # Expose to _save_snipe (sync context) via module-level var
    global _WEBHOOK_URL
    _WEBHOOK_URL = cfg.webhook_url
    if _WEBHOOK_URL:
        log.info(C(f"Webhook: {_WEBHOOK_URL[:60]}{'…' if len(_WEBHOOK_URL) > 60 else ''}"))

    tp = Path(args.token_file)
    if not tp.exists():
        log.error(R(f"Token file not found: {args.token_file}"))
        sys.exit(1)
    tokens = [
        ln.strip()
        for ln in tp.read_text(encoding="utf-8").splitlines()
        if ln.strip() and not ln.strip().startswith("#")
    ]
    if not tokens:
        log.error(R("tokens.txt is empty or has only comments."))
        sys.exit(1)
    log.info(C(f"Tokens loaded: {len(tokens)}"))

    proxies: list[str] = []
    if args.proxy_file:
        pp = Path(args.proxy_file)
        if pp.exists():
            proxies = [
                ln.strip()
                for ln in pp.read_text(encoding="utf-8").splitlines()
                if ln.strip() and not ln.strip().startswith("#")
            ]
            log.info(C(f"Proxies loaded: {len(proxies)}"))
        else:
            log.warning(Y(f"Proxy file not found: {args.proxy_file} — running direct"))

    tok_pool = TokenPool(tokens, cfg.rate_limit_delay)
    prx_pool = ProxyPool(proxies)
    state    = State()

    start_idx = args.start_from if args.start_from is not None else state.last_idx
    total     = len(cfg.alphabet) ** cfg.length
    remaining = max(0, total - start_idx)

    log.info(C(
        f"alphabet={len(cfg.alphabet)} chars  length={cfg.length}  "
        f"total={total:,}  start={start_idx:,}  remaining={remaining:,}  "
        f"workers={cfg.concurrency}  delay={cfg.rate_limit_delay}s/tok  "
        f"dry={args.dry_run}"
    ))

    queue: asyncio.Queue[tuple[int, str, int]] = asyncio.Queue(
        maxsize=cfg.concurrency * 8
    )
    stop      = asyncio.Event()
    prod_done = asyncio.Event()

    def _on_sigint() -> None:
        log.warning(Y("\n⚡ SIGINT — saving state, stopping…"))
        state.save(state.last_idx)
        stop.set()

    loop = asyncio.get_running_loop()
    try:
        loop.add_signal_handler(signal.SIGINT, _on_sigint)
    except (NotImplementedError, AttributeError):
        pass

    async def producer() -> None:
        async for pos, name in username_stream(
            cfg.alphabet, cfg.length, start_idx, state.seed
        ):
            if stop.is_set():
                break
            await queue.put((pos, name, 0))
        prod_done.set()

    with tqdm(
        total=remaining, unit="u",
        bar_format=(
            "{l_bar}{bar}| {n_fmt}/{total_fmt} "
            "[{elapsed}<{remaining}, {rate_fmt}]"
        ),
        colour="cyan", dynamic_ncols=True,
    ) as pbar:
        workers = [
            asyncio.create_task(
                _worker(
                    i, queue, tok_pool, prx_pool, cfg,
                    state, args.dry_run, pbar, stop, prod_done,
                )
            )
            for i in range(cfg.concurrency)
        ]
        prod = asyncio.create_task(producer())

        try:
            await asyncio.gather(prod, *workers, return_exceptions=True)
        except asyncio.CancelledError:
            pass

    state.save(state.last_idx)
    log.info(C(f"Done. Results → {SNIPE_FILE}  |  Log → {LOG_FILE}"))


# ── CLI ─────────────────────────────────────────────────────────────────────────
def _parse() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="sniper.py",
        description="Discord username sniper — POST /applications (API v10)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "examples:\n"
            "  python sniper.py\n"
            "  python sniper.py --threads 2 --start-from 840000\n"
            "  python sniper.py --proxy-file proxies.txt --dry-run\n"
            "  python sniper.py --alphabet abcdefghijklmnopqrstuvwxyz --length 4\n"
        ),
    )
    p.add_argument("--config",      default="config.json", metavar="FILE")
    p.add_argument("--token-file",  default="tokens.txt",  metavar="FILE")
    p.add_argument("--proxy-file",  default=None,          metavar="FILE")
    p.add_argument("--threads",     type=int, default=None, metavar="N")
    p.add_argument("--alphabet",    default=None,           metavar="CHARS")
    p.add_argument("--length",      type=int, default=None, metavar="N")
    p.add_argument("--start-from",  type=int, default=None, metavar="IDX")
    p.add_argument("--dry-run",     action="store_true")
    p.add_argument("--webhook",     default=None, metavar="URL",
                   help="Discord webhook URL pour notifier les snipes (overrides config)")
    p.add_argument("--version",     action="version", version=f"sniper {VERSION}")
    return p.parse_args()


if __name__ == "__main__":
    asyncio.run(run(_parse()))
