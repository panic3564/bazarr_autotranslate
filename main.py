import os
import sys
import time
import httpx
import signal
import asyncio
import logging
import threading
from typing import List, Optional
from dotenv import load_dotenv
from unique_queue import UniqueQueue
from class_types import Serie, Movie, SubtitleTranslate
from logging.handlers import TimedRotatingFileHandler


def get_env_or_default(env, default):
    val = os.getenv(env)
    return val if val is not None else default


def get_attr_or_key(obj, name):
    if hasattr(obj, name):
        return getattr(obj, name)
    elif isinstance(obj, dict) and name in obj:
        return obj[name]
    else:
        raise AttributeError(f"Missing attribute or key '{name}'")


def parse_bool(value, env_name, default):
    """
    Parse a boolean environment variable.

    bool("false") evaluates to True, so plain bool() must never be used on
    environment variables: any non-empty string is truthy, which silently
    made SERIES_SCAN=false and MOVIES_SCAN=false impossible to set.
    """
    if value is None:
        return default
    normalized = str(value).strip().lower()
    if normalized in ("true", "1", "yes", "on"):
        return True
    if normalized in ("false", "0", "no", "off"):
        return False
    print(f"Invalid boolean value for {env_name}: {value!r}, expected true or false")
    sys.exit(1)


def parse_int(env_name, default):
    """Parse an integer environment variable with a clear error message."""
    val = os.getenv(env_name)
    if val is None:
        return default
    try:
        return int(val)
    except ValueError:
        print(f"Invalid integer value for {env_name}: {val!r}")
        sys.exit(1)


def task_key(is_serie: bool, video_id: int, to_language: str) -> str:
    """Unique key for one video-to-language translation request."""
    return f" {'s' if is_serie else 'm'} {video_id}_{to_language}"


# Get configuration and setup things
load_dotenv()

# Bazarr returns languages as lowercase code2, so normalize the input
# to make "EN" match "en" instead of silently never matching anything.
base_languages = [lang.strip().lower() for lang in os.getenv("BASE_LANGUAGES", "").split(",") if lang.strip()]
to_languages = [lang.strip().lower() for lang in os.getenv("TO_LANGUAGES", "").split(",") if lang.strip()]

translation_request_timeout = parse_int("TRANSLATION_REQUEST_TIMEOUT", 15 * 60)
num_workers = parse_int("NUM_WORKERS", 1)
interval_between_scans = parse_int("INTERVAL_BETWEEN_SCANS", 5 * 60)
# Minimum age (in seconds) a source subtitle file must have before it is
# queued for translation. Default: 24 hours. Gives Bazarr time to find and
# download a proper subtitle from providers first.
min_subtitle_age = parse_int("MIN_SUBTITLE_AGE_SECONDS", 24 * 60 * 60)
# Safety limits. A value of 0 disables the limit.
# MAX_RETRIES: how often a single failing translation may be retried before
#   it is skipped for the lifetime of the process.
# MAX_TRANSLATIONS_PER_SCAN: maximum number of translation requests queued
#   per scan, to protect a paid translation service from large bursts.
max_retries = parse_int("MAX_RETRIES", 3)
max_translations_per_scan = parse_int("MAX_TRANSLATIONS_PER_SCAN", 0)

log_level = get_env_or_default("LOG_LEVEL", "INFO")
log_directory = get_env_or_default("LOG_DIRECTORY", "logs/")
series_scan = parse_bool(os.getenv("SERIES_SCAN"), "SERIES_SCAN", True)
movies_scan = parse_bool(os.getenv("MOVIES_SCAN"), "MOVIES_SCAN", True)

key_fn = lambda x: task_key(get_attr_or_key(x, "is_serie"), get_attr_or_key(x, "video_id"), get_attr_or_key(x, "to_language"))

task_queue = UniqueQueue(key_fn=key_fn)
shutdown_event = asyncio.Event()
# Failed translation attempts per task key, used to enforce MAX_RETRIES
failure_counts: dict[str, int] = {}
failure_lock = threading.Lock()
logger = logging.getLogger("bazarr_lingarr")


def is_subtitle_old_enough(path: str) -> bool:
    """
    Return True only if the subtitle file at `path` is at least
    `min_subtitle_age` seconds old, based on its modification time.
    The modification time corresponds to the moment Bazarr wrote the
    file to disk, so this effectively delays translation until Bazarr
    has had `min_subtitle_age` seconds to satisfy the language through
    its providers.

    If the file cannot be inspected (not mounted, permission issue),
    the subtitle is skipped and a warning is logged, so that a path
    mapping problem never silently disables the age gate.
    """
    try:
        age = time.time() - os.path.getmtime(path)
    except OSError:
        logger.warning(f"Skipping translation, unable to determine age of subtitle file: {path}. "
                       f"Verify that the media path is mounted in this container at the same path Bazarr reports.")
        return False
    if age < min_subtitle_age:
        logger.debug(f"Skipping translation, subtitle is only {int(age // 3600)} hours old "
                     f"(minimum age: {int(min_subtitle_age // 3600)} hours): {path}")
        return False
    return True


async def get_episodes_metadata(
    client: httpx.AsyncClient,
    base_url: str,
    api_key: str,
    series_ids: Optional[List[int]] = None,
    episode_ids: Optional[List[int]] = None,
) -> Optional[List[Serie]]:
    """
    Get metadata for episodes/series

    Args:
        client (httpx.AsyncClient): Shared HTTP client, reused for all requests
        base_url (str): Base URL of Bazarr API
        api_key (str): API key for authentication
        series_ids (list[int], optional): List of series IDs to get metadata for
        episode_ids (list[int], optional): List of episode IDs to get metadata for
    """
    logger.debug(f"Getting metadata for series: {series_ids}, episodes: {episode_ids}")
    endpoint = f"{base_url}/api/episodes"
    headers = {"X-API-KEY": api_key}

    params = {}
    if series_ids:
        params["seriesid[]"] = series_ids
    if episode_ids:
        params["episodeid[]"] = episode_ids

    try:
        response = await client.get(endpoint, headers=headers, params=params)
        response.raise_for_status()
        json = response.json()["data"]

        logger.debug(f"received: {json}")
        return [Serie.from_dict(obj) for obj in json]
    except Exception:
        logger.exception(f"Error while getting episode metadata for series: {series_ids}, episodes: {episode_ids}")
        return None


async def get_wanted_episodes(
    client: httpx.AsyncClient,
    base_url: str,
    api_key: str,
    start: int = 0,
    length: int = -1,
) -> Optional[List[Serie]]:
    """
    Get wanted subtitles for episodes

    Args:
        client (httpx.AsyncClient): Shared HTTP client, reused for all requests
        base_url (str): Base URL of Bazarr API
        api_key (str): API key for authentication
        start (int): Paging start integer (default: 0)
        length (int): Paging length (default: -1)
    """
    logger.debug("Getting wanted episodes")
    endpoint = f"{base_url}/api/episodes/wanted"
    headers = {"X-API-KEY": api_key}
    params = {"start": start, "length": length}

    try:
        response = await client.get(endpoint, headers=headers, params=params)
        response.raise_for_status()
        json = response.json()["data"]

        logger.debug(f"received: {json}")
        return [Serie.from_dict(obj) for obj in json]
    except Exception:
        logger.exception("Error while getting wanted episodes")
        return None


async def get_movies_metadata(
    client: httpx.AsyncClient,
    base_url: str,
    api_key: str,
    movie_ids: Optional[List[int]] = None,
) -> Optional[List[Movie]]:
    """
    Get metadata for movies

    Args:
        client (httpx.AsyncClient): Shared HTTP client, reused for all requests
        base_url (str): Base URL of Bazarr API
        api_key (str): API key for authentication
        movie_ids (list[int], optional): List of movie IDs to get metadata for
    """
    logger.debug(f"Getting metadata for movies: {movie_ids}")
    endpoint = f"{base_url}/api/movies"
    headers = {"X-API-KEY": api_key}
    params = {}

    if movie_ids:
        params["radarrid[]"] = movie_ids

    try:
        response = await client.get(endpoint, headers=headers, params=params)
        response.raise_for_status()
        json = response.json()["data"]

        logger.debug(f"received: {json}")
        return [Movie.from_dict(obj) for obj in json]
    except Exception:
        logger.exception(f"Error while getting movie metadata for movies: {movie_ids}")
        return None


async def get_wanted_movies(
    client: httpx.AsyncClient,
    base_url: str,
    api_key: str,
    start: int = 0,
    length: int = -1,
) -> Optional[List[Movie]]:
    """
    Get wanted subtitles for movies

    Args:
        client (httpx.AsyncClient): Shared HTTP client, reused for all requests
        base_url (str): Base URL of Bazarr API
        api_key (str): API key for authentication
        start (int): Paging start integer (default: 0)
        length (int): Paging length (default: -1)
    """
    logger.debug("Getting wanted movies")
    endpoint = f"{base_url}/api/movies/wanted"
    headers = {"X-API-KEY": api_key}
    params = {"start": start, "length": length}

    try:
        response = await client.get(endpoint, headers=headers, params=params)
        response.raise_for_status()
        json = response.json()["data"]

        logger.debug(f"received: {json}")
        return [Movie.from_dict(obj) for obj in json]
    except Exception:
        logger.exception("Error while getting wanted movies")
        return None


async def find_base_language_subtitles_from_missing_subtitles(
    client: httpx.AsyncClient,
    base_url: str,
    api_key: str,
    videos: List[Serie] | List[Movie],
) -> Optional[List[SubtitleTranslate]]:
    # Making a video id to languages map, useful later on.
    # A video can be missing several target languages at once, so every
    # missing language is collected instead of keeping only the last one.
    video_id_languages_map: dict[int, List[str]] = {}
    for video in videos:
        # Get video id from the correct property depending on the video instance
        video_id = video.sonarr_episode_id if isinstance(video, Serie) else video.radarr_id
        for missing_sub in video.missing_subtitles:
            # Check if the missing subtitle is in the list of languages to translate to
            if missing_sub.code2 in to_languages:
                # Check if that subtitle is already in the translation list
                if task_queue.check({"is_serie": isinstance(video, Serie), "video_id": video_id, "to_language": missing_sub.code2}):
                    logger.debug(f"Skipping subtitle, already in translation queue, {missing_sub.to_dict()}")
                    continue

                video_id_languages_map.setdefault(video_id, []).append(missing_sub.code2)

    if len(video_id_languages_map) == 0:
        logger.info("No missing subtitles found that are in the list of languages to be translated")
        return None

    if isinstance(videos[0], Serie):
        metadata = await get_episodes_metadata(
            client, base_url, api_key, episode_ids=list(video_id_languages_map.keys())
        )
    else:
        metadata = await get_movies_metadata(client, base_url, api_key, movie_ids=list(video_id_languages_map.keys()))

    if metadata is None:
        logger.info("No metadata returned, couldn't find already existing subtitles")
        return None

    video_id_to_video_map: dict[int, Serie | Movie] = {}
    for video in metadata:
        video_id = video.sonarr_episode_id if isinstance(video, Serie) else video.radarr_id
        video_id_to_video_map[video_id] = video

    # Check the metadata for already existing subtitles
    # Match the existing subtitles from the base language list to the missing ones for translation
    subtitles_to_translate: List[SubtitleTranslate] = []
    for video_id, languages in video_id_languages_map.items():
        # Get the video associated
        video = video_id_to_video_map.get(video_id)
        if video is None:
            logger.warning(f"Skipping video: {video_id}, no metadata was returned for it")
            continue

        # Check if there are subtitles
        if video.subtitles is None:
            logger.debug(f"Skipping video: {video_id}, no currently existing subtitles found")
            continue

        for language in languages:
            found = False
            for sub in video.subtitles:
                if language == sub.code2:
                    # Skip metadata for a subtitle if it is already in the language to translate to
                    # I don't think this should happen but better safe than sorry
                    continue

                # If the subtitle is in the base language list, associate it with the language to translate to.
                # Only external subtitle files (with a path) that satisfy the minimum age are considered
                if sub.code2 in base_languages and sub.path and is_subtitle_old_enough(sub.path):
                    subtitles_to_translate.append(SubtitleTranslate(sub, language, video_id, isinstance(video, Serie)))
                    found = True
                    break

            if not found:
                logger.debug(f"No matching existing subtitle found for: {language} for video: {video_id}")

    if len(subtitles_to_translate) == 0:
        logger.info("No already existing subtitles matched with requested translation subs")
        return None

    if max_translations_per_scan > 0 and len(subtitles_to_translate) > max_translations_per_scan:
        logger.warning(
            f"Found {len(subtitles_to_translate)} matching subtitles, "
            f"capping this scan at MAX_TRANSLATIONS_PER_SCAN={max_translations_per_scan}"
        )
        subtitles_to_translate = subtitles_to_translate[:max_translations_per_scan]

    logger.debug(f"Matching subtitles: {[w.to_dict() for w in subtitles_to_translate]}")
    logger.info(f"Found {len(subtitles_to_translate)} matching subtitles to translate")
    return subtitles_to_translate


def queue_subtitles_for_translation(subtitles: List[SubtitleTranslate]) -> int:
    queued = 0
    for sub in subtitles:
        key = task_key(sub.is_serie, sub.video_id, sub.to_language)
        # Skip items that have exceeded the retry limit, so a permanently
        # failing subtitle cannot hammer a (paid) translation service forever
        if max_retries > 0 and failure_counts.get(key, 0) >= max_retries:
            logger.warning(
                f"Skipping translation of {sub.base_subtitle.path} to {sub.to_language}: "
                f"it already failed {failure_counts[key]} times (MAX_RETRIES={max_retries}). "
                f"Restart the process to retry it again."
            )
            continue
        task_queue.put(sub)
        queued += 1
        logger.info(f"Queued: {sub.base_subtitle.path} to be translated to: {sub.to_language}")
    return queued


def translation_worker(worker_id, base_url, api_key):
    endpoint = f"{base_url}/api/subtitles"
    headers = {"X-API-KEY": api_key}
    with httpx.Client(timeout=translation_request_timeout) as client:
        while True:
            sub: SubtitleTranslate | None = task_queue.get()
            if sub is None:
                # Shutdown sentinel put by main() during a graceful shutdown
                logger.info(f"[Worker: {worker_id}] Shutting down")
                return
            key = task_key(sub.is_serie, sub.video_id, sub.to_language)
            logger.info(f"[Worker: {worker_id}] Translating: {sub.base_subtitle.path} to: {sub.to_language}")
            try:
                params = {
                    "action": "translate",
                    "language": sub.to_language,
                    "path": sub.base_subtitle.path,
                    "type": "episode" if sub.is_serie else "movie",
                    "id": sub.video_id,
                    "forced": sub.base_subtitle.forced,
                    "hi": sub.base_subtitle.hi,
                    "original_format": True,
                }

                response = client.patch(endpoint, headers=headers, params=params)
                response.raise_for_status()

                logger.info(f"[Worker: {worker_id}] Translation finished")
                # A success clears the failure counter so a future re-translation
                # is not blocked by an old failure
                with failure_lock:
                    failure_counts.pop(key, None)
            except Exception:
                logger.exception(f"[Worker: {worker_id}] Error while translating: {sub.base_subtitle.path} to: {sub.to_language}")
                with failure_lock:
                    failure_counts[key] = failure_counts.get(key, 0) + 1
            finally:
                task_queue.done(sub)


async def scan_and_process_series(client, base_url, api_key):
    logger.info("Scanning for episodes")
    series = await get_wanted_episodes(client, base_url, api_key)
    if series is None or len(series) == 0:
        logger.info("Found no missing subtitles for episodes")
        return

    logger.info(f"Found {len(series)} missing subtitles for episodes")
    subtitles_to_translate = await find_base_language_subtitles_from_missing_subtitles(client, base_url, api_key, series)
    if subtitles_to_translate is None:
        return

    queue_subtitles_for_translation(subtitles_to_translate)


async def scan_and_process_movies(client, base_url, api_key):
    logger.info("Scanning for movies")
    movies = await get_wanted_movies(client, base_url, api_key)
    if movies is None or len(movies) == 0:
        logger.info("Found no missing subtitles for movies")
        return

    logger.info(f"Found {len(movies)} missing subtitles for movies")
    subtitles_to_translate = await find_base_language_subtitles_from_missing_subtitles(client, base_url, api_key, movies)
    if subtitles_to_translate is None:
        return

    queue_subtitles_for_translation(subtitles_to_translate)


async def main(base_url, api_key):
    worker_threads = []
    for i in range(num_workers):
        t = threading.Thread(target=translation_worker, args=(i, base_url, api_key), daemon=True)
        t.start()
        worker_threads.append(t)

    # One shared client for all API calls instead of a new client per request
    async with httpx.AsyncClient(timeout=30) as client:
        while not shutdown_event.is_set():
            try:
                if series_scan:
                    await scan_and_process_series(client, base_url, api_key)
                if movies_scan:
                    await scan_and_process_movies(client, base_url, api_key)
            except Exception:
                logger.exception("Uncaught exception during scan")

            try:
                # Interruptible sleep: a shutdown signal is honored immediately
                # instead of only after the full scan interval
                await asyncio.wait_for(shutdown_event.wait(), timeout=interval_between_scans)
                break
            except asyncio.TimeoutError:
                pass

    # Wake all idle workers so they exit cleanly instead of being killed
    # mid-translation when the process exits
    for _ in range(num_workers):
        task_queue.put(None)
    for t in worker_threads:
        t.join(timeout=30)
    logger.info("Shutdown complete")


def handle_shutdown():
    logger.info("Received exit signal, shutting down after the current scan")
    shutdown_event.set()


if __name__ == "__main__":
    # Do verification on arguments
    base_url = os.getenv("BAZARR_BASE_URL")
    api_key = os.getenv("BAZARR_API_KEY")

    if base_url is None:
        print("BAZARR_BASE_URL is missing")
        sys.exit(1)

    if api_key is None:
        print("BAZARR_API_KEY is missing")
        sys.exit(1)

    if len(base_languages) == 0:
        print("Missing BASE_LANGUAGES")
        sys.exit(1)

    wrong_languages = [lang for lang in base_languages if len(lang) != 2]
    if len(wrong_languages) > 0:
        print(f"Wrong languages given in BASE_LANGUAGES, wrong ones: {wrong_languages}, expected to be 2 characters long (code2)")
        sys.exit(1)

    if len(to_languages) == 0:
        print("Missing TO_LANGUAGES")
        sys.exit(1)

    wrong_languages = [lang for lang in to_languages if len(lang) != 2]
    if len(wrong_languages) > 0:
        print(f"Wrong languages given in TO_LANGUAGES, wrong ones: {wrong_languages}, expected to be 2 characters long (code2)")
        sys.exit(1)

    if not series_scan and not movies_scan:
        print("Both series and movies scan are disabled, nothing will be done")
        sys.exit(1)

    if num_workers < 1:
        print("NUM_WORKERS must be at least 1")
        sys.exit(1)

    # Setup logger
    logger.propagate = False
    trailing_slash = "/" if not log_directory.endswith("/") else ""
    os.makedirs(log_directory, exist_ok=True)
    handler = TimedRotatingFileHandler(
        f"{log_directory}{trailing_slash}bazarr_lingarr_autotranslate.log", when="midnight", interval=1, backupCount=4
    )
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    # Also log to stdout so `docker logs` shows the activity
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    match log_level.lower():
        case "debug":
            logger.setLevel(logging.DEBUG)
            logger.debug("Configuration: --------------------")
            logger.debug(f"bazarr_base_url: {base_url}")
            logger.debug(f"base_languages: {base_languages}")
            logger.debug(f"to_languages: {to_languages}")
            logger.debug(f"translation_request_timeout: {translation_request_timeout}")
            logger.debug(f"num_workers: {num_workers}")
            logger.debug(f"interval_between_scans: {interval_between_scans}")
            logger.debug(f"min_subtitle_age: {min_subtitle_age}")
            logger.debug(f"max_retries: {max_retries}")
            logger.debug(f"max_translations_per_scan: {max_translations_per_scan}")
            logger.debug(f"log_level: {log_level}")
            logger.debug(f"log_directory: {log_directory}")
            logger.debug(f"series_scan: {series_scan}")
            logger.debug(f"movies_scan: {movies_scan}")
            logger.debug("End Configuration: ----------------")
        case "info":
            logger.setLevel(logging.INFO)
        case "warning" | "warn":
            logger.setLevel(logging.WARNING)
        case "error":
            logger.setLevel(logging.ERROR)
        case _:
            print(f"Unknown LOG_LEVEL: {log_level!r}, falling back to INFO")
            logger.setLevel(logging.INFO)

    # Start running things
    loop = asyncio.new_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handle_shutdown)

    loop.run_until_complete(main(base_url, api_key))
