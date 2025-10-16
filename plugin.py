import os
import json
import re
import time
import threading
from datetime import datetime
from typing import Dict, Any, List, Tuple, Callable, Set, Optional
import requests


# Global scheduler state
_bg_thread = None
_stop_event = threading.Event()

# Global debug logger (thread-safe)
_debug_logger = None
_debug_lock = threading.Lock()


# Constants
DEFAULT_TIMEOUT = 20
API_TIMEOUT = 30
SAFE_NAME_RE = re.compile(r'[\\/:*?"<>|\t\r\n\']+')
DEFAULT_MAX_NAME_LENGTH = 180
TMDB_RATE_LIMIT_DELAY = 0.26
DRY_RUN_LIMIT = 1000


class DualLogger:
    """Logger wrapper that writes to both Django logger and debug file"""
    def __init__(self, django_logger, debug_logger=None):
        self.django_logger = django_logger
        self.debug_logger = debug_logger
    
    def info(self, msg, *args):
        self.django_logger.info(msg, *args)
        if self.debug_logger:
            self.debug_logger.log(f"INFO: {msg % args if args else msg}")
    
    def warning(self, msg, *args):
        self.django_logger.warning(msg, *args)
        if self.debug_logger:
            self.debug_logger.log(f"WARNING: {msg % args if args else msg}")
    
    def error(self, msg, *args):
        self.django_logger.error(msg, *args)
        if self.debug_logger:
            self.debug_logger.log(f"ERROR: {msg % args if args else msg}")
    
    def exception(self, msg, *args):
        self.django_logger.exception(msg, *args)
        if self.debug_logger:
            self.debug_logger.log(f"EXCEPTION: {msg % args if args else msg}")


def safe_name(s: str, maxlen: int = DEFAULT_MAX_NAME_LENGTH) -> str:
    """Sanitize a string for use as a filename."""
    s = (s or "").strip()
    s = SAFE_NAME_RE.sub(" ", s)
    s = re.sub(r"\s+", " ", s)
    s = s[:maxlen].rstrip(" .")
    s = s.lstrip(".")
    return s.strip() or "unknown"


def ensure_dir(path: str) -> None:
    """Create directory if it doesn't exist."""
    os.makedirs(path, exist_ok=True)


def read_text(path: str) -> str:
    """Read text file, return empty string on error."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception:
        return ""


def write_text_if_changed(path: str, content: str, dry_run: bool) -> Tuple[bool, str]:
    """Write text to file only if content differs."""
    existing = read_text(path)
    if existing == content:
        return (False, "unchanged")
    if dry_run:
        return (False, "dry_run")
    tmp = f"{path}.tmp-{int(time.time() * 1000)}"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(content)
    os.replace(tmp, path)
    return (True, "written")


def load_manifest(root: str) -> Dict[str, Any]:
    """Load manifest file or return default."""
    manifest_path = os.path.join(root, ".vod_strm_manifest.json")
    try:
        with open(manifest_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"files": {}, "tmdb_cache": {}, "version": 1}


def save_manifest(root: str, manifest: Dict[str, Any]) -> None:
    """Save manifest file atomically."""
    manifest_path = os.path.join(root, ".vod_strm_manifest.json")
    tmp = f"{manifest_path}.tmp-{int(time.time() * 1000)}"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, sort_keys=True)
    os.replace(tmp, manifest_path)


def normalize_host(host: str) -> str:
    """Normalize host by removing scheme and trailing slashes."""
    host = (host or "").strip()
    host = re.sub(r"^\s*https?://", "", host, flags=re.I)
    return host.strip().strip("/")


class Plugin:
    """VOD .STRM Writer Plugin for Dispatcharr proxy."""

    name = "VOD .STRM Writer (Proxy)"
    version = "0.3.0"
    description = "Writes .strm and .nfo files for Movies & Series using Dispatcharr proxy."

    fields = [
        {"id": "dispatcharr_host", "label": "Dispatcharr Host (host:port)", "type": "string", "default": "tv.local:9191", "help_text": "NO scheme. Example: tv.local:9191"},
        {"id": "api_username", "label": "API Username", "type": "string", "default": "tv"},
        {"id": "api_password", "label": "API Password", "type": "string", "default": "tv"},
        {"id": "tmdb_api_key", "label": "TMDB API Key (optional)", "type": "string", "default": "", "help_text": "Get free API key from themoviedb.org. If empty, uses original names."},
        {"id": "write_nfo_files", "label": "Write NFO Files", "type": "boolean", "default": False, "help_text": "Write .nfo metadata files for Jellyfin/Emby/Plex. Requires TMDB API key."},
        {"id": "movies_root", "label": "Movies Root", "type": "string", "default": "/VODs/movies"},
        {"id": "series_root", "label": "Series Root", "type": "string", "default": "/VODs/series"},
        {"id": "probe_urls", "label": "Probe URLs (HEAD)", "type": "boolean", "default": False},
        {"id": "cleanup_removed", "label": "Remove Stale .strm", "type": "boolean", "default": False},
        {"id": "dry_run", "label": "Dry Run", "type": "boolean", "default": False, "help_text": "Simulates the run without writing or deleting files. Limits processing to 1000 items. Note: Scheduled runs always run for real."},
        {"id": "verbose", "label": "Verbose Per-Item Logs", "type": "boolean", "default": True},
        {"id": "debug_log", "label": "Debug Logging", "type": "boolean", "default": False, "help_text": "Write detailed debug logs to /data/vod_strm_debug.log (file is overwritten each run)"},
        {"id": "scheduled_times_movies", "label": "Movie Schedule (24-hour format)", "type": "string", "default": "", "placeholder": "0300,1500", "help_text": "Comma-separated times to run movies daily (HHMM format). Example: 0300,1500. Leave blank to disable. Uses system timezone."},
        {"id": "scheduled_times_series", "label": "Series Schedule (24-hour format)", "type": "string", "default": "", "placeholder": "0400,1600", "help_text": "Comma-separated times to run series daily (HHMM format). Example: 0400,1600. Leave blank to disable. Uses system timezone."},
    ]

    actions = [
        {"id": "write_movies", "label": "Write Movie .STRM Files"},
        {"id": "write_series", "label": "Write Series .STRM Files"},
    ]

    def __init__(self):
        self.api_base = "http://127.0.0.1:9191"
        self.last_tmdb_request = 0
        self.settings_file = "/data/vod_strm_settings.json"
        self.debug_log_file = "/data/vod_strm_debug.log"
        self.debug_logger = None
        self._load_settings()

    def _setup_debug_logger(self, enabled):
        """Setup or teardown debug file logger"""
        global _debug_logger
        
        if not enabled:
            with _debug_lock:
                _debug_logger = None
            self.debug_logger = None
            return

        with open(self.debug_log_file, 'w') as f:
            f.write(f"=== VOD STRM Debug Log - {datetime.now().isoformat()} ===\n\n")

        class DebugLogger:
            def __init__(self, filepath):
                self.filepath = filepath

            def log(self, msg):
                try:
                    with _debug_lock:
                        with open(self.filepath, 'a') as f:
                            f.write(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}\n")
                except Exception:
                    pass

        with _debug_lock:
            _debug_logger = DebugLogger(self.debug_log_file)
        self.debug_logger = _debug_logger

    def _load_settings(self):
        """Load saved settings from disk and start scheduler"""
        try:
            if os.path.exists(self.settings_file):
                with open(self.settings_file, 'r') as f:
                    self.saved_settings = json.load(f)
                    self._start_background_scheduler(self.saved_settings)
            else:
                self.saved_settings = {}
        except Exception:
            self.saved_settings = {}

    def _save_settings(self, settings):
        """Save settings to disk"""
        try:
            with open(self.settings_file, 'w') as f:
                json.dump(settings, f, indent=2)
            self.saved_settings = settings
        except Exception:
            pass

    def _parse_scheduled_times(self, scheduled_times_str):
        """Parse scheduled times string into list of datetime.time objects"""
        if not scheduled_times_str or not scheduled_times_str.strip():
            return []
        times = []
        for time_str in scheduled_times_str.split(','):
            time_str = time_str.strip()
            if len(time_str) == 4 and time_str.isdigit():
                hour = int(time_str[:2])
                minute = int(time_str[2:])
                if 0 <= hour <= 23 and 0 <= minute <= 59:
                    times.append(datetime.strptime(time_str, "%H%M").time())
        return times

    def _start_background_scheduler(self, settings):
        """Start background thread for scheduled runs"""
        global _bg_thread

        movies_times_str = settings.get("scheduled_times_movies", "").strip()
        series_times_str = settings.get("scheduled_times_series", "").strip()

        if not movies_times_str and not series_times_str:
            return

        movies_times = self._parse_scheduled_times(movies_times_str)
        series_times = self._parse_scheduled_times(series_times_str)

        if not movies_times and not series_times:
            return

        if _bg_thread and _bg_thread.is_alive():
            _stop_event.set()
            _bg_thread.join(timeout=5)
            _stop_event.clear()

        def scheduler_loop():
            import logging
            from django.utils import timezone as django_tz
            logger = logging.getLogger("vod_strm_scheduler")
            last_run_date = {"movies": None, "series": None}

            while not _stop_event.is_set():
                try:
                    now = django_tz.now()
                    current_date = now.date()

                    for scheduled_time in movies_times:
                        scheduled_dt = django_tz.make_aware(datetime.combine(current_date, scheduled_time))
                        time_diff = (scheduled_dt - now).total_seconds()

                        if -30 <= time_diff <= 30 and last_run_date["movies"] != current_date:
                            logger.info(f"Scheduled movie scan triggered at {now.strftime('%Y-%m-%d %H:%M %Z')}")
                            if _debug_logger:
                                _debug_logger.log(f"=== SCHEDULED MOVIE SCAN TRIGGERED at {now.strftime('%Y-%m-%d %H:%M %Z')} ===")
                            try:
                                scheduled_settings = settings.copy()
                                scheduled_settings["dry_run"] = False

                                token_container = {"token": self._login(
                                    settings.get("api_username", "tv"),
                                    settings.get("api_password", "tv")
                                )}
                                def relogin():
                                    token_container["token"] = self._login(
                                        settings.get("api_username", "tv"),
                                        settings.get("api_password", "tv")
                                    )
                                    return token_container["token"]

                                result = self._write_movies(scheduled_settings, logger, token_container["token"], relogin)
                                logger.info(f"Scheduled movie scan completed: {result.get('message', 'Done')}")
                            except Exception as e:
                                logger.error(f"Error in scheduled movie scan: {e}")
                            last_run_date["movies"] = current_date
                            break

                    for scheduled_time in series_times:
                        scheduled_dt = django_tz.make_aware(datetime.combine(current_date, scheduled_time))
                        time_diff = (scheduled_dt - now).total_seconds()

                        if -30 <= time_diff <= 30 and last_run_date["series"] != current_date:
                            logger.info(f"Scheduled series scan triggered at {now.strftime('%Y-%m-%d %H:%M %Z')}")
                            if _debug_logger:
                                _debug_logger.log(f"=== SCHEDULED SERIES SCAN TRIGGERED at {now.strftime('%Y-%m-%d %H:%M %Z')} ===")
                            try:
                                scheduled_settings = settings.copy()
                                scheduled_settings["dry_run"] = False

                                token_container = {"token": self._login(
                                    settings.get("api_username", "tv"),
                                    settings.get("api_password", "tv")
                                )}
                                def relogin():
                                    token_container["token"] = self._login(
                                        settings.get("api_username", "tv"),
                                        settings.get("api_password", "tv")
                                    )
                                    return token_container["token"]

                                result = self._write_series(scheduled_settings, logger, token_container["token"], relogin)
                                logger.info(f"Scheduled series scan completed: {result.get('message', 'Done')}")
                            except Exception as e:
                                logger.error(f"Error in scheduled series scan: {e}")
                            last_run_date["series"] = current_date
                            break

                    _stop_event.wait(30)
                except Exception as e:
                    logger.error(f"Error in scheduler loop: {e}")
                    _stop_event.wait(60)

        _bg_thread = threading.Thread(target=scheduler_loop, name="vod-strm-scheduler", daemon=True)
        _bg_thread.start()

    def _stop_background_scheduler(self):
        """Stop background scheduler thread"""
        global _bg_thread
        if _bg_thread and _bg_thread.is_alive():
            _stop_event.set()
            _bg_thread.join(timeout=5)
            _stop_event.clear()

    def _escape_xml(self, text: str) -> str:
        """Escape special XML characters."""
        if not text:
            return ""
        text = str(text)
        text = text.replace("&", "&amp;")
        text = text.replace("<", "&lt;")
        text = text.replace(">", "&gt;")
        text = text.replace('"', "&quot;")
        text = text.replace("'", "&apos;")
        return text

    def _tmdb_lookup_movie(self, tmdb_id: int, api_key: str, logger: Any) -> Optional[Dict[str, Any]]:
        """Lookup movie details from TMDB."""
        try:
            elapsed = time.time() - self.last_tmdb_request
            if elapsed < TMDB_RATE_LIMIT_DELAY:
                time.sleep(TMDB_RATE_LIMIT_DELAY - elapsed)
            url = f"https://api.themoviedb.org/3/movie/{tmdb_id}?api_key={api_key}"
            response = requests.get(url, timeout=DEFAULT_TIMEOUT)
            self.last_tmdb_request = time.time()
            if response.status_code == 200:
                return response.json()
            else:
                logger.warning("TMDB movie lookup failed for ID %s: %s", tmdb_id, response.status_code)
                return None
        except Exception as e:
            logger.warning("TMDB movie lookup error for ID %s: %s", tmdb_id, e)
            return None

    def _tmdb_lookup_series(self, tmdb_id: int, api_key: str, logger: Any) -> Optional[Dict[str, Any]]:
        """Lookup series details from TMDB."""
        try:
            elapsed = time.time() - self.last_tmdb_request
            if elapsed < TMDB_RATE_LIMIT_DELAY:
                time.sleep(TMDB_RATE_LIMIT_DELAY - elapsed)
            url = f"https://api.themoviedb.org/3/tv/{tmdb_id}?api_key={api_key}"
            response = requests.get(url, timeout=DEFAULT_TIMEOUT)
            self.last_tmdb_request = time.time()
            if response.status_code == 200:
                return response.json()
            else:
                logger.warning("TMDB series lookup failed for ID %s: %s", tmdb_id, response.status_code)
                return None
        except Exception as e:
            logger.warning("TMDB series lookup error for ID %s: %s", tmdb_id, e)
            return None

    def _write_movie_nfo(self, filepath: str, tmdb_data: Dict[str, Any], dry_run: bool) -> bool:
        """Write movie NFO file."""
        if dry_run:
            return False
        try:
            nfo_path = filepath.rsplit('.', 1)[0] + '.nfo'
            title = tmdb_data.get("title", "")
            original_title = tmdb_data.get("original_title", "")
            plot = tmdb_data.get("overview", "")
            rating = tmdb_data.get("vote_average", 0)
            year = tmdb_data.get("release_date", "")[:4] if tmdb_data.get("release_date") else ""
            runtime = tmdb_data.get("runtime", 0)
            tmdb_id = tmdb_data.get("id", "")
            poster = f"https://image.tmdb.org/t/p/original{tmdb_data.get('poster_path')}" if tmdb_data.get("poster_path") else ""
            fanart = f"https://image.tmdb.org/t/p/original{tmdb_data.get('backdrop_path')}" if tmdb_data.get("backdrop_path") else ""
            genres = [g.get("name", "") for g in tmdb_data.get("genres", [])]

            nfo_content = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n<movie>\n'
            nfo_content += f'  <title>{self._escape_xml(title)}</title>\n'
            if original_title and original_title != title:
                nfo_content += f'  <originaltitle>{self._escape_xml(original_title)}</originaltitle>\n'
            nfo_content += f'  <plot>{self._escape_xml(plot)}</plot>\n'
            nfo_content += f'  <rating>{rating}</rating>\n'
            nfo_content += f'  <year>{year}</year>\n'
            if runtime:
                nfo_content += f'  <runtime>{runtime}</runtime>\n'
            nfo_content += f'  <tmdbid>{tmdb_id}</tmdbid>\n'
            if poster:
                nfo_content += f'  <thumb>{poster}</thumb>\n'
            if fanart:
                nfo_content += f'  <fanart>{fanart}</fanart>\n'
            for genre in genres:
                nfo_content += f'  <genre>{self._escape_xml(genre)}</genre>\n'
            nfo_content += '</movie>\n'

            tmp = f"{nfo_path}.tmp-{int(time.time() * 1000)}"
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(nfo_content)
            os.replace(tmp, nfo_path)
            return True
        except Exception:
            return False

    def _write_tvshow_nfo(self, series_dir: str, tmdb_data: Dict[str, Any], dry_run: bool) -> bool:
        """Write tvshow.nfo file."""
        if dry_run:
            return False
        try:
            nfo_path = os.path.join(series_dir, "tvshow.nfo")
            title = tmdb_data.get("name", "")
            original_title = tmdb_data.get("original_name", "")
            plot = tmdb_data.get("overview", "")
            rating = tmdb_data.get("vote_average", 0)
            year = tmdb_data.get("first_air_date", "")[:4] if tmdb_data.get("first_air_date") else ""
            tmdb_id = tmdb_data.get("id", "")
            poster = f"https://image.tmdb.org/t/p/original{tmdb_data.get('poster_path')}" if tmdb_data.get("poster_path") else ""
            fanart = f"https://image.tmdb.org/t/p/original{tmdb_data.get('backdrop_path')}" if tmdb_data.get("backdrop_path") else ""
            genres = [g.get("name", "") for g in tmdb_data.get("genres", [])]

            nfo_content = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n<tvshow>\n'
            nfo_content += f'  <title>{self._escape_xml(title)}</title>\n'
            if original_title and original_title != title:
                nfo_content += f'  <originaltitle>{self._escape_xml(original_title)}</originaltitle>\n'
            nfo_content += f'  <plot>{self._escape_xml(plot)}</plot>\n'
            nfo_content += f'  <rating>{rating}</rating>\n'
            nfo_content += f'  <year>{year}</year>\n'
            nfo_content += f'  <tmdbid>{tmdb_id}</tmdbid>\n'
            if poster:
                nfo_content += f'  <thumb>{poster}</thumb>\n'
            if fanart:
                nfo_content += f'  <fanart>{fanart}</fanart>\n'
            for genre in genres:
                nfo_content += f'  <genre>{self._escape_xml(genre)}</genre>\n'
            nfo_content += '</tvshow>\n'

            tmp = f"{nfo_path}.tmp-{int(time.time() * 1000)}"
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(nfo_content)
            os.replace(tmp, nfo_path)
            return True
        except Exception:
            return False

    def _write_episode_nfo(self, filepath: str, episode: Dict[str, Any], season_num: int, tmdb_api_key: str, series_tmdb_id: int, logger: Any, dry_run: bool) -> bool:
        """Write episode NFO file."""
        if dry_run or not tmdb_api_key:
            return False
        try:
            elapsed = time.time() - self.last_tmdb_request
            if elapsed < TMDB_RATE_LIMIT_DELAY:
                time.sleep(TMDB_RATE_LIMIT_DELAY - elapsed)
            ep_num = episode.get("episode_number", 0)
            url = f"https://api.themoviedb.org/3/tv/{series_tmdb_id}/season/{season_num}/episode/{ep_num}?api_key={tmdb_api_key}"
            response = requests.get(url, timeout=DEFAULT_TIMEOUT)
            self.last_tmdb_request = time.time()
            if response.status_code != 200:
                return False
            tmdb_ep_data = response.json()
            nfo_path = filepath.rsplit('.', 1)[0] + '.nfo'
            title = tmdb_ep_data.get("name", "")
            plot = tmdb_ep_data.get("overview", "")
            rating = tmdb_ep_data.get("vote_average", 0)
            aired = tmdb_ep_data.get("air_date", "")
            still = f"https://image.tmdb.org/t/p/original{tmdb_ep_data.get('still_path')}" if tmdb_ep_data.get("still_path") else ""

            nfo_content = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n<episodedetails>\n'
            nfo_content += f'  <title>{self._escape_xml(title)}</title>\n'
            nfo_content += f'  <season>{season_num}</season>\n'
            nfo_content += f'  <episode>{ep_num}</episode>\n'
            nfo_content += f'  <plot>{self._escape_xml(plot)}</plot>\n'
            nfo_content += f'  <rating>{rating}</rating>\n'
            if aired:
                nfo_content += f'  <aired>{aired}</aired>\n'
            if still:
                nfo_content += f'  <thumb>{still}</thumb>\n'
            nfo_content += '</episodedetails>\n'

            tmp = f"{nfo_path}.tmp-{int(time.time() * 1000)}"
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(nfo_content)
            os.replace(tmp, nfo_path)
            return True
        except Exception:
            return False

    def _get_movie_name_and_year(self, movie: Dict[str, Any], tmdb_api_key: str, tmdb_cache: Dict[str, Any], logger: Any, verbose: bool) -> Tuple[str, Optional[int], Optional[Dict[str, Any]]]:
        """Get movie name and year using TMDB if available."""
        movie_id = movie.get("id")
        tmdb_id = movie.get("tmdb_id")
        original_name = movie.get("name")
        original_year = movie.get("year")
        cache_key = f"movie_{tmdb_id}"
        if tmdb_id and cache_key in tmdb_cache:
            cached = tmdb_cache[cache_key]
            if verbose:
                logger.info("[MOVIE %s] Using cached TMDB data", movie_id)
            return cached.get("title", original_name), cached.get("year", original_year), cached.get("full_data")
        if tmdb_api_key and tmdb_id:
            if verbose:
                logger.info("[MOVIE %s] Looking up TMDB ID %s", movie_id, tmdb_id)
            tmdb_data = self._tmdb_lookup_movie(tmdb_id, tmdb_api_key, logger)
            if tmdb_data:
                title = tmdb_data.get("title", original_name)
                release_date = tmdb_data.get("release_date", "")
                year = int(release_date[:4]) if release_date and len(release_date) >= 4 else original_year
                tmdb_cache[cache_key] = {"title": title, "year": year, "full_data": tmdb_data}
                if verbose:
                    logger.info("[MOVIE %s] TMDB lookup success: '%s' (%s)", movie_id, title, year)
                return title, year, tmdb_data
        if verbose:
            if not tmdb_api_key:
                logger.info("[MOVIE %s] Using original name (no TMDB API key)", movie_id)
            elif not tmdb_id:
                logger.info("[MOVIE %s] Using original name (no TMDB ID): '%s'", movie_id, original_name)
            else:
                logger.info("[MOVIE %s] Using original name (TMDB lookup failed): '%s'", movie_id, original_name)
        return original_name, original_year, None

    def _get_series_name_and_year(self, series: Dict[str, Any], tmdb_api_key: str, tmdb_cache: Dict[str, Any], logger: Any, verbose: bool) -> Tuple[str, Optional[int], Optional[Dict[str, Any]]]:
        """Get series name and year using TMDB if available."""
        series_id = series.get("id")
        tmdb_id = series.get("tmdb_id")
        original_name = series.get("name")
        original_year = series.get("year")
        cache_key = f"series_{tmdb_id}"
        if tmdb_id and cache_key in tmdb_cache:
            cached = tmdb_cache[cache_key]
            if verbose:
                logger.info("[SERIES %s] Using cached TMDB data", series_id)
            return cached.get("name", original_name), cached.get("year", original_year), cached.get("full_data")
        if tmdb_api_key and tmdb_id:
            if verbose:
                logger.info("[SERIES %s] Looking up TMDB ID %s", series_id, tmdb_id)
            tmdb_data = self._tmdb_lookup_series(tmdb_id, tmdb_api_key, logger)
            if tmdb_data:
                name = tmdb_data.get("name", original_name)
                first_air_date = tmdb_data.get("first_air_date", "")
                year = int(first_air_date[:4]) if first_air_date and len(first_air_date) >= 4 else original_year
                tmdb_cache[cache_key] = {"name": name, "year": year, "full_data": tmdb_data}
                if verbose:
                    logger.info("[SERIES %s] TMDB lookup success: '%s' (%s)", series_id, name, year)
                return name, year, tmdb_data
        if verbose:
            if not tmdb_api_key:
                logger.info("[SERIES %s] Using original name (no TMDB API key)", series_id)
            elif not tmdb_id:
                logger.info("[SERIES %s] Using original name (no TMDB ID): '%s'", series_id, original_name)
            else:
                logger.info("[SERIES %s] Using original name (TMDB lookup failed): '%s'", series_id, original_name)
        return original_name, original_year, None

    def _login(self, username: str, password: str) -> str:
        """Authenticate and return access token."""
        url = f"{self.api_base}/api/accounts/token/"
        response = requests.post(url, json={"username": username, "password": password}, timeout=DEFAULT_TIMEOUT)
        response.raise_for_status()
        return response.json().get("access", "")

    def _get_headers(self, token: str) -> Dict[str, str]:
        """Build authorization headers."""
        return {"Authorization": f"Bearer {token}"} if token else {}

    def _api_request(self, url_or_path: str, token: str, relogin_callback: Callable) -> Dict[str, Any]:
        """Make authenticated API request with auto-relogin on 401."""
        url = url_or_path if url_or_path.startswith("http") else f"{self.api_base.rstrip('/')}{url_or_path}"
        response = requests.get(url, headers=self._get_headers(token), timeout=API_TIMEOUT)
        if response.status_code == 401 and relogin_callback:
            token = relogin_callback()
            response = requests.get(url, headers=self._get_headers(token), timeout=API_TIMEOUT)
        response.raise_for_status()
        return response.json() if response.content else {}

    def _paginate(self, first_path: str, token: str, relogin_callback: Callable, logger: Any, dry_run: bool = False) -> List[Dict[str, Any]]:
        """Follow pagination until exhausted or dry_run limit is hit."""
        results = []
        url_or_path = first_path
        page_num = 1
        while url_or_path:
            if dry_run and len(results) >= DRY_RUN_LIMIT:
                logger.info("Dry run limit of %d reached. Stopping pagination.", DRY_RUN_LIMIT)
                break
            page = self._api_request(url_or_path, token, relogin_callback)
            page_results = page.get("results", [])
            results.extend(page_results)
            url_or_path = page.get("next")
            if url_or_path:
                logger.info("Fetched page %d (%d items, %d total)", page_num, len(page_results), len(results))
            else:
                logger.info("Fetched page %d (%d items, %d total) - Complete!", page_num, len(page_results), len(results))
            page_num += 1
        if dry_run and len(results) > DRY_RUN_LIMIT:
            return results[:DRY_RUN_LIMIT]
        return results

    def _probe_url(self, url: str) -> bool:
        """Check if URL is accessible via HEAD request."""
        try:
            response = requests.head(url, timeout=DEFAULT_TIMEOUT, allow_redirects=True)
            return 200 <= response.status_code < 400
        except Exception:
            return False

    def _build_movie_url(self, host: str, uuid: str) -> str:
        """Build proxy URL for movie."""
        return f"http://{normalize_host(host)}/proxy/vod/movie/{uuid}"

    def _build_episode_url(self, host: str, uuid: str) -> str:
        """Build proxy URL for episode."""
        return f"http://{normalize_host(host)}/proxy/vod/episode/{uuid}"

    def _cleanup_stale_files(self, root: str, live_paths: Set[str], logger: Any, dry_run: bool) -> Tuple[int, List[str]]:
        """Remove .strm files and empty directories not in live_paths set."""
        removed_count = 0
        notes = []
        for dirpath, _, files in os.walk(root):
            for filename in files:
                if not filename.lower().endswith(".strm"):
                    continue
                filepath = os.path.normpath(os.path.join(dirpath, filename))
                if filepath not in live_paths:
                    if dry_run:
                        notes.append(f"dry_remove: {filepath}")
                        continue
                    try:
                        os.remove(filepath)
                        removed_count += 1
                        notes.append(f"removed: {filepath}")
                    except Exception as e:
                        notes.append(f"remove_failed: {filepath} ({e})")
        if removed_count > 0 and not dry_run:
            for dirpath, dirnames, filenames in os.walk(root, topdown=False):
                if not dirnames and not filenames:
                    try:
                        os.rmdir(dirpath)
                        notes.append(f"removed empty dir: {dirpath}")
                    except OSError as e:
                        notes.append(f"failed to remove empty dir: {dirpath} ({e})")
        return removed_count, notes

    def _write_movies(self, settings: Dict[str, Any], logger: Any, token: str, relogin_callback: Callable) -> Dict[str, Any]:
        """Write .strm files for movies."""
        root = settings.get("movies_root", "/VODs/movies")
        page_size = 100
        dry_run = bool(settings.get("dry_run", False))
        verbose = bool(settings.get("verbose", True))
        probe = bool(settings.get("probe_urls", False))
        cleanup = bool(settings.get("cleanup_removed", False))
        write_nfo = bool(settings.get("write_nfo_files", False))
        dispatch_host = normalize_host(settings.get("dispatcharr_host", "tv.local:9191"))
        tmdb_api_key = settings.get("tmdb_api_key", "").strip()
        debug_log = bool(settings.get("debug_log", False))

        self._setup_debug_logger(debug_log)
        
        # Wrap logger to write to both django logs and debug file
        logger = DualLogger(logger, self.debug_logger)
        
        logger.info("=== STARTING MOVIE PROCESSING ===")
        logger.info("Root: %s, Dry Run: %s, TMDB: %s", root, dry_run, 'enabled' if tmdb_api_key else 'disabled')

        logger.info("Starting movie .strm generation (root: %s, dry_run: %s)", root, dry_run)
        if tmdb_api_key and not dry_run:
            logger.info("TMDB lookups enabled")
            if write_nfo:
                logger.info("NFO file generation enabled")
        elif tmdb_api_key and dry_run:
            logger.info("TMDB lookups disabled for dry run")

        ensure_dir(root)
        endpoint = f"/api/vod/movies/?page_size={page_size}"
        logger.info("Fetching movies from API...")
        logger.info("Fetching from API endpoint: %s", endpoint)

        rows = self._paginate(endpoint, token, relogin_callback, logger, dry_run=dry_run)
        logger.info("Fetched %d movies from API", len(rows))
        logger.info("Total movies fetched: %d", len(rows))

        manifest = load_manifest(root)
        manifest_files = manifest.get("files", {})
        tmdb_cache = manifest.get("tmdb_cache", {})
        live_paths = set()
        created = updated = skipped = errors = 0
        reasons: Dict[str, int] = {}
        logger.info("Processing %d movies...", len(rows))

        for idx, movie in enumerate(rows):
            movie_id = movie.get("id")
            uuid = movie.get("uuid")
            if idx > 0 and idx % 50 == 0:
                logger.info("Progress: %d/%d movies processed", idx, len(rows))
            if not uuid:
                skipped += 1
                reasons["missing_uuid"] = reasons.get("missing_uuid", 0) + 1
                if verbose:
                    logger.info("[MOVIE %s] skip: missing_uuid", movie_id)
                continue

            tmdb_data = None
            if tmdb_api_key and not dry_run:
                name, year, tmdb_data = self._get_movie_name_and_year(movie, tmdb_api_key, tmdb_cache, logger, verbose)
            else:
                name = movie.get("name")
                year = movie.get("year")

            movie_folder_name = f"{safe_name(name or f'movie-{movie_id}')}{f' ({year})' if year else ''}"
            movie_folder = os.path.join(root, movie_folder_name)
            ensure_dir(movie_folder)
            filename = f"{movie_folder_name}.strm"
            filepath = os.path.join(movie_folder, filename)
            url = self._build_movie_url(dispatch_host, uuid)

            if probe and not self._probe_url(url):
                skipped += 1
                reasons["probe_failed"] = reasons.get("probe_failed", 0) + 1
                if verbose:
                    logger.info("[MOVIE %s] skip: probe_failed %s", movie_id, url)
                continue

            changed, reason = write_text_if_changed(filepath, url + "\n", dry_run)
            normalized_path = os.path.normpath(filepath)
            live_paths.add(normalized_path)

            if changed and reason == "written":
                if normalized_path in manifest_files:
                    updated += 1
                else:
                    created += 1
                manifest_files[normalized_path] = {"uuid": uuid, "type": "movie", "id": movie_id}
                if write_nfo and tmdb_data:
                    self._write_movie_nfo(filepath, tmdb_data, dry_run)
                if verbose:
                    logger.info("[MOVIE %s] wrote %s", movie_id, filepath)
            else:
                skipped += 1
                reasons[reason] = reasons.get(reason, 0) + 1
                if verbose and reason != 'unchanged':
                    logger.info("[MOVIE %s] skip %s (%s)", movie_id, filepath, reason)

        manifest["files"] = manifest_files
        manifest["tmdb_cache"] = tmdb_cache
        if not dry_run:
            save_manifest(root, manifest)
        logger.info("Movie processing complete: created=%d, updated=%d, skipped=%d, errors=%d", created, updated, skipped, errors)

        removed = 0
        if cleanup:
            logger.info("Running cleanup to remove stale files...")
            removed, notes = self._cleanup_stale_files(root, live_paths, logger, dry_run)
            logger.info("Cleanup complete. Removed %d stale file(s).", removed)
            if verbose and notes:
                for note in notes[:20]:
                    logger.info("[MOVIE CLEANUP] %s", note)
                if len(notes) > 20:
                    logger.info("[MOVIE CLEANUP] ...and %d more notes.", len(notes) - 20)

        return {"status": "ok", "scope": "movies", "created": created, "updated": updated, "skipped": skipped, "errors": errors, "removed": removed, "dry_run": dry_run, "count_input": len(rows), "skip_reasons": reasons}

    def _write_series(self, settings: Dict[str, Any], logger: Any, token: str, relogin_callback: Callable) -> Dict[str, Any]:
        """Write .strm files for series episodes."""
        root = settings.get("series_root", "/VODs/series")
        page_size = 100
        dry_run = bool(settings.get("dry_run", False))
        verbose = bool(settings.get("verbose", True))
        probe = bool(settings.get("probe_urls", False))
        cleanup = bool(settings.get("cleanup_removed", False))
        write_nfo = bool(settings.get("write_nfo_files", False))
        dispatch_host = normalize_host(settings.get("dispatcharr_host", "tv.local:9191"))
        tmdb_api_key = settings.get("tmdb_api_key", "").strip()
        debug_log = bool(settings.get("debug_log", False))

        self._setup_debug_logger(debug_log)
        
        # Wrap logger to write to both django logs and debug file
        logger = DualLogger(logger, self.debug_logger)
        
        logger.info("=== STARTING SERIES PROCESSING ===")
        logger.info("Root: %s, Dry Run: %s, TMDB: %s", root, dry_run, 'enabled' if tmdb_api_key else 'disabled')

        logger.info("Starting series .strm generation (root: %s, dry_run: %s)", root, dry_run)
        if tmdb_api_key and not dry_run:
            logger.info("TMDB lookups enabled")
            if write_nfo:
                logger.info("NFO file generation enabled")
        elif tmdb_api_key and dry_run:
            logger.info("TMDB lookups disabled for dry run")

        ensure_dir(root)
        endpoint = f"/api/vod/series/?page_size={page_size}"
        logger.info("Fetching series from API...")
        logger.info("Fetching from API endpoint: %s", endpoint)

        series_rows = self._paginate(endpoint, token, relogin_callback, logger, dry_run=dry_run)
        logger.info("Fetched %d series from API", len(series_rows))
        logger.info("Total series fetched: %d", len(series_rows))

        manifest = load_manifest(root)
        manifest_files = manifest.get("files", {})
        tmdb_cache = manifest.get("tmdb_cache", {})
        live_paths = set()
        created = updated = skipped = errors = 0
        reasons: Dict[str, int] = {}
        logger.info("Processing %d series...", len(series_rows))

        for idx, series in enumerate(series_rows):
            series_id = series.get("id")
            series_tmdb_id = series.get("tmdb_id")
            if idx > 0 and idx % 20 == 0:
                logger.info("Progress: %d/%d series processed", idx, len(series_rows))

            tmdb_data = None
            if tmdb_api_key and not dry_run:
                series_name, series_year, tmdb_data = self._get_series_name_and_year(series, tmdb_api_key, tmdb_cache, logger, verbose)
            else:
                series_name = series.get("name")
                series_year = series.get("year")

            series_dir_name = f"{safe_name(series_name or f'series-{series_id}')}{f' ({series_year})' if series_year else ''}"
            series_dir = os.path.join(root, series_dir_name)

            try:
                episodes_data = self._api_request(f"/api/vod/series/{series_id}/episodes/", token, relogin_callback)
            except Exception as e:
                errors += 1
                if verbose:
                    logger.exception("[SERIES %s] episodes fetch error: %s", series_id, e)
                continue

            if not episodes_data:
                skipped += 1
                reasons["no_episodes"] = reasons.get("no_episodes", 0) + 1
                if verbose:
                    logger.info("[SERIES %s] skip: no episodes found (%s)", series_id, series_name)
                continue

            ensure_dir(series_dir)
            if write_nfo and tmdb_data:
                self._write_tvshow_nfo(series_dir, tmdb_data, dry_run)

            seasons = {}
            for episode in episodes_data:
                season_num = episode.get("season_number", 0)
                if season_num not in seasons:
                    seasons[season_num] = []
                seasons[season_num].append(episode)

            for season_num in sorted(seasons.keys()):
                season_dir = os.path.join(series_dir, f"Season {season_num:02d}")
                ensure_dir(season_dir)
                sorted_episodes = sorted(seasons[season_num], key=lambda e: e.get("episode_number", 0))

                for episode in sorted_episodes:
                    ep_uuid = episode.get("uuid")
                    ep_num = episode.get("episode_number", 0)
                    ep_title = episode.get("name") or f"E{ep_num:02d}"
                    if not ep_uuid:
                        skipped += 1
                        reasons["missing_uuid"] = reasons.get("missing_uuid", 0) + 1
                        if verbose:
                            logger.info("[SERIES %s] skip S%02dE%02d: missing_uuid (%s)", series_id, season_num, ep_num, ep_title)
                        continue

                    url = self._build_episode_url(dispatch_host, ep_uuid)
                    if probe and not self._probe_url(url):
                        skipped += 1
                        reasons["probe_failed"] = reasons.get("probe_failed", 0) + 1
                        if verbose:
                            logger.info("[SERIES %s] skip S%02dE%02d: probe_failed %s", series_id, season_num, ep_num, url)
                        continue

                    filename = f"{series_dir_name} - S{season_num:02d}E{ep_num:02d} - {safe_name(ep_title)}.strm"
                    filepath = os.path.join(season_dir, filename)
                    changed, reason = write_text_if_changed(filepath, url + "\n", dry_run)
                    normalized_path = os.path.normpath(filepath)
                    live_paths.add(normalized_path)

                    if changed and reason == "written":
                        if normalized_path in manifest_files:
                            updated += 1
                        else:
                            created += 1
                        manifest_files[normalized_path] = {"uuid": ep_uuid, "type": "episode", "series_id": series_id, "season": season_num, "episode": ep_num}
                        if write_nfo and series_tmdb_id:
                            self._write_episode_nfo(filepath, episode, season_num, tmdb_api_key, series_tmdb_id, logger, dry_run)
                        if verbose:
                            logger.info("[SERIES %s] wrote %s", series_id, filepath)
                    else:
                        skipped += 1
                        reasons[reason] = reasons.get(reason, 0) + 1
                        if verbose and reason != 'unchanged':
                            logger.info("[SERIES %s] skip %s (%s)", series_id, filepath, reason)

        manifest["files"] = manifest_files
        manifest["tmdb_cache"] = tmdb_cache
        if not dry_run:
            save_manifest(root, manifest)
        logger.info("Series processing complete: created=%d, updated=%d, skipped=%d, errors=%d", created, updated, skipped, errors)

        removed = 0
        if cleanup:
            logger.info("Running cleanup to remove stale files...")
            removed, notes = self._cleanup_stale_files(root, live_paths, logger, dry_run)
            logger.info("Cleanup complete. Removed %d stale file(s).", removed)
            if verbose and notes:
                for note in notes[:20]:
                    logger.info("[SERIES CLEANUP] %s", note)
                if len(notes) > 20:
                    logger.info("[SERIES CLEANUP] ...and %d more notes.", len(notes) - 20)

        return {"status": "ok", "scope": "series", "created": created, "updated": updated, "skipped": skipped, "errors": errors, "removed": removed, "dry_run": dry_run, "count_input": len(series_rows), "skip_reasons": reasons}

    def run(self, action: str, params: dict, context: dict) -> Dict[str, Any]:
        """Main entry point for plugin actions."""
        logger = context.get("logger")
        settings = context.get("settings", {})

        logger.info("VOD STRM Plugin v%s - Action: %s", self.version, action)

        # Auto-save settings and restart scheduler whenever any action runs
        self._save_settings(settings)
        self._start_background_scheduler(settings)

        api_user = settings.get("api_username", "tv")
        api_pass = settings.get("api_password", "tv")

        try:
            logger.info("Authenticating with API...")
            token_container = {"token": self._login(api_user, api_pass)}
            logger.info("Authentication successful")

            def relogin():
                logger.warning("API token expired or invalid, attempting to re-authenticate...")
                token_container["token"] = self._login(api_user, api_pass)
                logger.info("Re-authentication successful")
                return token_container["token"]

            if action == "write_movies":
                return self._write_movies(settings, logger, token_container["token"], relogin)
            if action == "write_series":
                return self._write_series(settings, logger, token_container["token"], relogin)
            return {"status": "error", "message": f"Unknown action: {action}"}

        except requests.exceptions.HTTPError as e:
            logger.exception("HTTP Error during plugin execution: %s", e.response.text)
            return {"status": "error", "message": f"API Request Failed: {e}"}
        except Exception as e:
            logger.exception("An unexpected error occurred during plugin execution.")
            return {"status": "error", "message": f"An unexpected error occurred: {e}"}


# Plugin exports
plugin = Plugin()
fields = Plugin.fields
actions = Plugin.actions