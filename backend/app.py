#!/usr/bin/env python3
"""
TRMNL Trakt.tv Backend  (FastAPI + asyncio)
- /query   — aggregated Trakt.tv data with resolved image URLs per item
- /health  — health check
"""

import asyncio
import hashlib
import json
import logging
import math
import os
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional

import httpx
import redis.asyncio as aioredis
from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware


# ============================================================================
# CONFIGURATION
# ============================================================================

LOG_LEVEL = logging.DEBUG if os.getenv('DEBUG', 'false').lower() == 'true' else logging.INFO
logging.basicConfig(
    level=LOG_LEVEL,
    format='%(asctime)s [%(levelname)s] %(name)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)

ENABLE_IP_WHITELIST = os.getenv('ENABLE_IP_WHITELIST', 'true').lower() == 'true'
TRMNL_IPS_API       = 'https://trmnl.com/api/ips'
LOCALHOST_IPS       = {'127.0.0.1', '::1', 'localhost'}

TMDB_API_KEY       = os.getenv('TMDB_API_KEY', '')
FANART_API_KEY     = os.getenv('FANART_API_KEY', '')
REDIS_URL          = os.getenv('REDIS_URL', 'redis://redis:6379/0')
ALLOWED_CLIENT_IDS = set(filter(None, (s.strip() for s in os.getenv('ALLOWED_CLIENT_IDS', '').split(','))))

CACHE_TTL           = int(os.getenv('CACHE_TTL_SECONDS',          '604800'))  # 7 days
CACHE_TTL_NOT_FOUND = int(os.getenv('CACHE_TTL_NOT_FOUND_SECONDS', '86400'))  # 1 day
QUERY_CACHE_TTL     = int(os.getenv('QUERY_CACHE_TTL_SECONDS',     '300'))    # 5 minutes
TMDB_IMAGE_SIZE     = os.getenv('TMDB_IMAGE_SIZE', 'w185')

TMDB_API_BASE   = 'https://api.themoviedb.org/3'
TMDB_IMAGE_BASE = 'https://image.tmdb.org/t/p'
FANART_API_BASE = 'https://webservice.fanart.tv/v3'
TRAKT_API_BASE  = 'https://api.trakt.tv'

NOT_FOUND_SENTINEL = b'__NOT_FOUND__'

# Global clients — initialised in lifespan
http:         Optional[httpx.AsyncClient] = None
redis_client: Optional[aioredis.Redis]    = None
TRMNL_IPS:    set                         = set(LOCALHOST_IPS)


# ============================================================================
# LIFESPAN (startup / shutdown)
# ============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    global http, redis_client

    http = httpx.AsyncClient(timeout=15.0)

    for attempt in range(5):
        try:
            rc = aioredis.Redis.from_url(REDIS_URL, decode_responses=False)
            await rc.ping()
            redis_client = rc
            logger.info("Connected to Redis")
            break
        except Exception as e:
            logger.warning(f"Redis attempt {attempt + 1}/5 failed: {e}")
            await asyncio.sleep(2)
    else:
        logger.error("Could not connect to Redis after 5 attempts")

    if ENABLE_IP_WHITELIST:
        await _refresh_trmnl_ips()
        asyncio.create_task(_ip_refresh_loop())
    else:
        logger.warning("IP whitelist DISABLED — all IPs allowed")

    if not TMDB_API_KEY:
        logger.warning("TMDB_API_KEY not set — image URLs will not resolve")

    logger.info("Application ready")
    yield

    await http.aclose()
    if redis_client:
        await redis_client.aclose()


app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["GET"])


# ============================================================================
# IP WHITELIST
# ============================================================================

async def _refresh_trmnl_ips():
    global TRMNL_IPS
    try:
        resp = await http.get(TRMNL_IPS_API)
        resp.raise_for_status()
        data = resp.json().get('data', {})
        TRMNL_IPS = set(data.get('ipv4', []) + data.get('ipv6', [])) | LOCALHOST_IPS
        logger.info(f"Loaded {len(TRMNL_IPS)} TRMNL IPs")
    except Exception as e:
        logger.error(f"Failed to fetch TRMNL IPs: {e}")


async def _ip_refresh_loop():
    while True:
        now = datetime.now()
        await asyncio.sleep((60 - now.minute) * 60 - now.second)
        logger.info("Refreshing TRMNL IPs…")
        await _refresh_trmnl_ips()


def _client_ip(request: Request) -> str:
    for header in ('CF-Connecting-IP', 'X-Forwarded-For', 'X-Real-IP'):
        val = request.headers.get(header)
        if val:
            return val.split(',')[0].strip()
    return request.client.host


async def require_whitelisted_ip(request: Request):
    if ENABLE_IP_WHITELIST and _client_ip(request) not in TRMNL_IPS:
        logger.warning(f"Blocked IP: {_client_ip(request)}")
        raise HTTPException(status_code=403, detail="Access denied")


# ============================================================================
# IMAGE URL RESOLUTION
# ============================================================================

def _tmdb_auth() -> tuple[dict, dict]:
    if TMDB_API_KEY.startswith('eyJ'):
        return {'Authorization': f'Bearer {TMDB_API_KEY}'}, {}
    return {}, {'api_key': TMDB_API_KEY}


async def _tmdb_poster_url(tmdb_type: str, tmdb_id: str) -> Optional[str]:
    if not TMDB_API_KEY:
        return None
    headers, params = _tmdb_auth()
    try:
        resp = await http.get(f"{TMDB_API_BASE}/{tmdb_type}/{tmdb_id}", headers=headers, params=params)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        path = resp.json().get('poster_path')
        return f"{TMDB_IMAGE_BASE}/{TMDB_IMAGE_SIZE}{path}" if path else None
    except Exception as e:
        logger.error(f"TMDB error {tmdb_type}/{tmdb_id}: {e}")
        return None


async def _tvdb_id(tmdb_id: str) -> Optional[int]:
    """Resolve TVDB ID from TMDB ID (cached)."""
    cache_key = f"tvdb_id:{tmdb_id}"
    if redis_client:
        cached = await redis_client.get(cache_key)
        if cached == NOT_FOUND_SENTINEL:
            return None
        if cached:
            return int(cached)
    headers, params = _tmdb_auth()
    try:
        resp = await http.get(f"{TMDB_API_BASE}/tv/{tmdb_id}/external_ids", headers=headers, params=params)
        resp.raise_for_status()
        tvdb = resp.json().get('tvdb_id')
        if redis_client:
            val = str(tvdb).encode() if tvdb else NOT_FOUND_SENTINEL
            ttl = CACHE_TTL if tvdb else CACHE_TTL_NOT_FOUND
            await redis_client.set(cache_key, val, ex=ttl)
        return tvdb
    except Exception as e:
        logger.error(f"TMDB external_ids tv/{tmdb_id}: {e}")
        return None


async def _fanart_poster_url(tmdb_type: str, tmdb_id: str) -> Optional[str]:
    """Return a direct Fanart.tv poster URL (no image bytes fetched)."""
    if not FANART_API_KEY:
        return None
    try:
        if tmdb_type == 'tv':
            tvdb = await _tvdb_id(tmdb_id)
            if not tvdb:
                return None
            url = f"{FANART_API_BASE}/tv/{tvdb}"
        else:
            url = f"{FANART_API_BASE}/movies/{tmdb_id}"

        resp = await http.get(url, params={'api_key': FANART_API_KEY})
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        key     = 'tvposter' if tmdb_type == 'tv' else 'movieposter'
        posters = resp.json().get(key, [])
        return posters[0].get('url') if posters else None
    except Exception as e:
        logger.error(f"Fanart error {tmdb_type}/{tmdb_id}: {e}")
        return None


async def resolve_image_url(media_type: str, tmdb_id: Optional[int]) -> Optional[str]:
    """Return a poster URL for an item, using Redis cache → TMDB → Fanart fallback."""
    if not tmdb_id:
        return None
    tmdb_type = 'tv' if media_type == 'show' else 'movie'
    cache_key = f"img_url:{media_type}:{tmdb_id}"

    if redis_client:
        cached = await redis_client.get(cache_key)
        if cached == NOT_FOUND_SENTINEL:
            return None
        if cached:
            return cached.decode()

    url = await _tmdb_poster_url(tmdb_type, str(tmdb_id))
    if not url:
        url = await _fanart_poster_url(tmdb_type, str(tmdb_id))

    if redis_client:
        val = url.encode() if url else NOT_FOUND_SENTINEL
        ttl = CACHE_TTL if url else CACHE_TTL_NOT_FOUND
        await redis_client.set(cache_key, val, ex=ttl)

    return url


async def enrich_images(items: list) -> list:
    """Resolve and attach image_url to every item in parallel."""
    async def add_url(item):
        item['image_url'] = await resolve_image_url(item.get('media_type'), item.get('tmdb_id'))
        return item
    return list(await asyncio.gather(*[add_url(item) for item in items]))


# ============================================================================
# TRAKT API
# ============================================================================

CATEGORY_TITLES = {
    'watching':          'Now',
    'continue_watching': 'Paused',
    'recently_watched':  'Recent',
    'upcoming':          'Upcoming',
    'recommended':       'Recommended',
    'watchlist':         'Watchlist',
    'collection':        'Collection',
    'stats':             'Stats',
    'trending':          'Trending',
}
VALID_CATEGORIES      = set(CATEGORY_TITLES)
SELF_ONLY_CATEGORIES  = {'continue_watching', 'recently_watched', 'upcoming', 'recommended'}


async def trakt_get(path: str, token: str, client_id: str, params: dict = None) -> tuple[int, any]:
    try:
        resp = await http.get(
            f"{TRAKT_API_BASE}{path}",
            headers={
                'Authorization':     f'Bearer {token}',
                'trakt-api-key':     client_id,
                'trakt-api-version': '2',
                'Content-Type':      'application/json',
            },
            params=params or {},
        )
        if resp.status_code == 204: return 204, None
        if resp.status_code == 200: return 200, resp.json()
        logger.warning(f"Trakt {path} → {resp.status_code}")
        return resp.status_code, None
    except Exception as e:
        logger.error(f"Trakt error {path}: {e}")
        return 0, None


# ---- data helpers ----

def _ep(i):    return i.get('episode') or {}
def _show(i):  return i.get('show')    or {}
def _movie(i): return i.get('movie')   or {}
def _ids(o):   return o.get('ids')     or {}


def _group_episodes(items: list, ep_mapper) -> list:
    show_map, order, seen = {}, [], set()
    for item in items:
        show     = _show(item)
        show_key = show.get('title', 'Unknown')
        ep       = _ep(item)
        ep_key   = f"{show_key}-S{ep.get('season')}E{ep.get('number')}"
        if ep_key in seen:
            continue
        seen.add(ep_key)
        if show_key not in show_map:
            show_map[show_key] = {
                'type': 'show_group', 'show': show.get('title'),
                'tmdb_id': _ids(show).get('tmdb'), 'trakt_slug': _ids(show).get('slug'),
                'media_type': 'show', 'episodes': [],
            }
            order.append(show_key)
        show_map[show_key]['episodes'].append(ep_mapper(item))
    return [show_map[k] for k in order]


def _dedupe_movies(items: list, mapper) -> list:
    seen, order = {}, []
    for item in items:
        m   = _movie(item)
        key = f"{m.get('title')}-{m.get('year')}"
        if key not in seen:
            seen[key] = mapper(item)
            order.append(key)
    return [seen[k] for k in order]


def _restructure_seasons(sg: dict, seasons_progress: dict):
    """Replace flat 'episodes' with a 'seasons' array, each entry carrying its own progress."""
    eps_by_season: dict[str, list] = {}
    for ep in sg.pop('episodes', []):
        sn = str(ep.get('season', 0))
        eps_by_season.setdefault(sn, []).append(ep)
    sg['seasons'] = [
        {
            'number':   int(sn) if sn.isdigit() else 0,
            'progress': seasons_progress.get(sn),
            'episodes': eps,
        }
        for sn, eps in eps_by_season.items()
    ]


async def enrich_progress_all(cat_items: list[list], token: str, client_id: str):
    """
    Fetch /shows/:slug/progress/watched once per unique slug across all categories.
    Restructures each show_group's flat 'episodes' list into a 'seasons' array with
    per-season progress attached. Also attaches overall show_progress to the group.
    """
    slug_map: dict[str, list] = {}
    all_show_groups: list = []
    for items in cat_items:
        for item in items:
            if item.get('type') == 'show_group':
                all_show_groups.append(item)
                slug = item.get('trakt_slug')
                if slug:
                    slug_map.setdefault(slug, []).append(item)

    if not all_show_groups:
        return

    async def fetch_and_apply(slug: str, groups: list):
        status, data = await trakt_get(f"/shows/{slug}/progress/watched", token, client_id)
        show_progress    = None
        seasons_progress = {}
        if status == 200 and data:
            show_progress    = {'aired': data.get('aired', 0), 'completed': data.get('completed', 0)}
            seasons_progress = {
                str(s.get('number')): {'aired': s.get('aired', 0), 'completed': s.get('completed', 0)}
                for s in data.get('seasons', [])
            }
            logger.debug(f"progress {slug}: {show_progress['completed']}/{show_progress['aired']} seasons={list(seasons_progress)}")
        else:
            logger.debug(f"progress {slug}: no data (status={status})")
        for sg in groups:
            sg['show_progress']  = show_progress
            sg['total_seasons']  = len(seasons_progress)
            _restructure_seasons(sg, seasons_progress)

    await asyncio.gather(*[fetch_and_apply(slug, groups) for slug, groups in slug_map.items()])

    # Restructure any show_groups that had no slug (no progress fetch)
    slugged = {id(sg) for groups in slug_map.values() for sg in groups}
    for sg in all_show_groups:
        if id(sg) not in slugged:
            sg['show_progress'] = None
            sg['total_seasons'] = 0
            _restructure_seasons(sg, {})


# ---- stat / trending helpers ----

def _build_stat_items(stats_data: dict) -> list:
    """Build display items from a Trakt /users/{user}/stats response."""
    items   = []
    movies  = stats_data.get('movies')   or {}
    shows   = stats_data.get('shows')    or {}
    eps     = stats_data.get('episodes') or {}
    net     = stats_data.get('network')  or {}
    ratings = stats_data.get('ratings')  or {}

    if movies:
        parts = []
        if movies.get('watched'):   parts.append(f"{movies['watched']} watched")
        if movies.get('plays'):     parts.append(f"{movies['plays']} plays")
        h = (movies.get('minutes') or 0) // 60
        if h: parts.append(f"{h}h")
        if movies.get('collected'): parts.append(f"{movies['collected']} collected")
        if parts:
            items.append({'type': 'stat', 'label': 'Movies', 'line': ' · '.join(parts)})

    if eps:
        parts = []
        if eps.get('watched'):   parts.append(f"{eps['watched']} watched")
        if eps.get('plays'):     parts.append(f"{eps['plays']} plays")
        h = (eps.get('minutes') or 0) // 60
        if h: parts.append(f"{h}h")
        if eps.get('collected'): parts.append(f"{eps['collected']} collected")
        if parts:
            items.append({'type': 'stat', 'label': 'Episodes', 'line': ' · '.join(parts)})

    if shows:
        parts = []
        if shows.get('watched'):   parts.append(f"{shows['watched']} watched")
        if shows.get('collected'): parts.append(f"{shows['collected']} collected")
        if shows.get('ratings'):   parts.append(f"{shows['ratings']} rated")
        if parts:
            items.append({'type': 'stat', 'label': 'Shows', 'line': ' · '.join(parts)})

    if net:
        parts = []
        if net.get('friends'):   parts.append(f"{net['friends']} friends")
        if net.get('followers'): parts.append(f"{net['followers']} followers")
        if net.get('following'): parts.append(f"{net['following']} following")
        if parts:
            items.append({'type': 'stat', 'label': 'Network', 'line': ' · '.join(parts)})

    total = ratings.get('total', 0)
    if total:
        items.append({'type': 'stat', 'label': 'Ratings', 'line': f"{total} rated"})

    return items


async def _fetch_trending(token, client_id) -> list:
    (_, shows), (_, movs) = await asyncio.gather(
        trakt_get('/shows/trending', token, client_id, {'limit': 10, 'extended': 'full'}),
        trakt_get('/movies/trending', token, client_id, {'limit': 10, 'extended': 'full'}),
    )
    def _rating(obj): return round(obj['rating'] * 10) / 10 if obj.get('rating') else None

    trend_shows, seen = [], set()
    for item in (shows or []):
        s = _show(item)
        if s.get('title') not in seen:
            seen.add(s.get('title'))
            trend_shows.append({
                'type': 'show', 'title': s.get('title'), 'year': s.get('year'),
                'genres': s.get('genres') or [], 'overview': s.get('overview'),
                'rating': _rating(s), 'network': s.get('network'),
                'watchers': item.get('watchers'),
                'tmdb_id': _ids(s).get('tmdb'), 'media_type': 'show',
            })

    trend_movs, seen = [], set()
    for item in (movs or []):
        m   = _movie(item)
        key = f"{m.get('title')}-{m.get('year')}"
        if key not in seen:
            seen.add(key)
            trend_movs.append({
                'type': 'movie', 'title': m.get('title'), 'year': m.get('year'),
                'genres': m.get('genres') or [], 'overview': m.get('overview'),
                'rating': _rating(m), 'watchers': item.get('watchers'),
                'tmdb_id': _ids(m).get('tmdb'), 'media_type': 'movie',
            })

    result = []
    for i in range(max(len(trend_shows), len(trend_movs))):
        if i < len(trend_movs):  result.append(trend_movs[i])
        if i < len(trend_shows): result.append(trend_shows[i])
    return result


# ---- per-category fetchers ----

async def _fetch_watching(token, client_id, username: str = '') -> list:
    target = username or 'me'
    status, data = await trakt_get(f'/users/{target}/watching', token, client_id)
    if status != 200 or not data:
        return []
    t = data.get('type')
    if t == 'episode':
        show, ep = _show(data), _ep(data)
        show_groups = [{'type': 'show_group', 'show': show.get('title'),
                        'tmdb_id': _ids(show).get('tmdb'), 'trakt_slug': _ids(show).get('slug'),
                        'media_type': 'show',
                        'episodes': [{'season': ep.get('season'), 'episode': ep.get('number'),
                                      'title': ep.get('title'), 'overview': ep.get('overview')}]}]
        return show_groups
    if t == 'movie':
        m = _movie(data)
        return [{'type': 'movie', 'title': m.get('title'), 'year': m.get('year'),
                 'overview': m.get('overview'), 'tmdb_id': _ids(m).get('tmdb'), 'media_type': 'movie'}]
    return []


async def _fetch_continue_watching(token, client_id) -> list:
    (_, eps), (_, movs) = await asyncio.gather(
        trakt_get('/sync/playback/episodes', token, client_id, {'extended': 'full'}),
        trakt_get('/sync/playback/movies',   token, client_id, {'extended': 'full'}),
    )
    show_groups = _group_episodes(eps or [], lambda i: {
        'season': _ep(i).get('season'), 'episode': _ep(i).get('number'),
        'title': _ep(i).get('title'), 'overview': _ep(i).get('overview'),
        'progress': round(i.get('progress', 0)),
    })

    movies = _dedupe_movies(movs or [], lambda i: {
        'type': 'movie', 'title': _movie(i).get('title'), 'year': _movie(i).get('year'),
        'overview': _movie(i).get('overview'), 'progress': round(i.get('progress', 0)),
        'tmdb_id': _ids(_movie(i)).get('tmdb'), 'media_type': 'movie',
    })
    return show_groups + movies


async def _fetch_recently_watched(token, client_id) -> list:
    (_, eps), (_, movs) = await asyncio.gather(
        trakt_get('/sync/history/episodes', token, client_id, {'page': 1, 'limit': 10, 'extended': 'full'}),
        trakt_get('/sync/history/movies',   token, client_id, {'page': 1, 'limit': 10, 'extended': 'full'}),
    )
    show_groups = _group_episodes(eps or [], lambda i: {
        'season': _ep(i).get('season'), 'episode': _ep(i).get('number'),
        'title': _ep(i).get('title'), 'overview': _ep(i).get('overview'),
        'watched_at': i.get('watched_at'),
    })

    movies = _dedupe_movies(movs or [], lambda i: {
        'type': 'movie', 'title': _movie(i).get('title'), 'year': _movie(i).get('year'),
        'overview': _movie(i).get('overview'), 'watched_at': i.get('watched_at'),
        'tmdb_id': _ids(_movie(i)).get('tmdb'), 'media_type': 'movie',
    })
    return show_groups + movies


async def _fetch_upcoming(token, client_id, today: str) -> list:
    (_, shows), (_, movs) = await asyncio.gather(
        trakt_get(f'/calendars/my/shows/{today}/7',   token, client_id, {'extended': 'full'}),
        trakt_get(f'/calendars/my/movies/{today}/30', token, client_id, {'extended': 'full'}),
    )
    show_groups = _group_episodes(shows or [], lambda i: {
        'season': _ep(i).get('season'), 'episode': _ep(i).get('number'),
        'title': _ep(i).get('title'), 'overview': _ep(i).get('overview'),
        'airs_at': i.get('first_aired'),
    })
    movies = _dedupe_movies(movs or [], lambda i: {
        'type': 'movie', 'title': _movie(i).get('title'), 'year': _movie(i).get('year'),
        'overview': _movie(i).get('overview'), 'released': i.get('released'),
        'tmdb_id': _ids(_movie(i)).get('tmdb'), 'media_type': 'movie',
    })
    return show_groups + movies


async def _fetch_recommended(token, client_id) -> list:
    (_, shows), (_, movs) = await asyncio.gather(
        trakt_get('/recommendations/shows',  token, client_id, {'limit': 10, 'extended': 'full'}),
        trakt_get('/recommendations/movies', token, client_id, {'limit': 10, 'extended': 'full'}),
    )
    def _rating(item): return round(item['rating'] * 10) / 10 if item.get('rating') else None

    rec_shows, seen = [], set()
    for item in (shows or []):
        if item.get('title') not in seen:
            seen.add(item['title'])
            rec_shows.append({'type': 'show', 'title': item.get('title'), 'year': item.get('year'),
                              'genres': item.get('genres') or [], 'overview': item.get('overview'),
                              'rating': _rating(item), 'network': item.get('network'),
                              'tmdb_id': _ids(item).get('tmdb'), 'media_type': 'show'})
    rec_movs, seen = [], set()
    for item in (movs or []):
        key = f"{item.get('title')}-{item.get('year')}"
        if key not in seen:
            seen.add(key)
            rec_movs.append({'type': 'movie', 'title': item.get('title'), 'year': item.get('year'),
                             'genres': item.get('genres') or [], 'overview': item.get('overview'),
                             'rating': _rating(item), 'tmdb_id': _ids(item).get('tmdb'), 'media_type': 'movie'})

    result = []
    for i in range(max(len(rec_shows), len(rec_movs))):
        if i < len(rec_movs):  result.append(rec_movs[i])
        if i < len(rec_shows): result.append(rec_shows[i])
    return result


async def _fetch_list(cat: str, token: str, client_id: str, username: str = '') -> list:
    target = username or 'me'
    (_, raw_shows), (_, raw_movs) = await asyncio.gather(
        trakt_get(f'/users/{target}/{cat}/shows',  token, client_id, {'extended': 'full'}),
        trakt_get(f'/users/{target}/{cat}/movies', token, client_id, {'extended': 'full'}),
    )
    def _rating(obj): return round(obj['rating'] * 10) / 10 if obj.get('rating') else None

    shows, seen = [], set()
    for item in (raw_shows or []):
        s = _show(item)
        collected_at = item.get('last_collected_at', None)
        updated_at = item.get('last_updated_at', None)
        if s.get('title') not in seen:
            seen.add(s['title'])
            show_ = {'type': 'show', 'title': s.get('title'), 'year': s.get('year'),
                     'genres': s.get('genres') or [], 'overview': s.get('overview'),
                     'rating': _rating(s), 'network': s.get('network'),
                     'tmdb_id': _ids(s).get('tmdb'), 'media_type': 'show'
                     }
            if collected_at:
                show_['collected_at'] = collected_at
            if updated_at:
                show_['updated_at'] = updated_at

            shows.append(show_)
    movs, seen = [], set()
    for item in (raw_movs or []):
        m   = _movie(item)
        collected_at = item.get('collected_at', None)
        updated_at = item.get('updated_at', None)

        key = f"{m.get('title')}-{m.get('year')}"
        if key not in seen:
            seen.add(key)
            movie_ = {'type': 'movie', 'title': m.get('title'), 'year': m.get('year'),
                      'genres': m.get('genres') or [], 'overview': m.get('overview'),
                      'rating': _rating(m), 'tmdb_id': _ids(m).get('tmdb'), 'media_type': 'movie'

                      }
            if collected_at:
                movie_['collected_at'] = collected_at
            if updated_at:
                movie_['updated_at'] = updated_at
            movs.append(movie_)
    return shows + movs


async def fetch_category(cat: str, token: str, client_id: str, today: str, username: str = '') -> list:
    if cat == 'watching':          return await _fetch_watching(token, client_id, username)
    if cat == 'continue_watching': return await _fetch_continue_watching(token, client_id)
    if cat == 'recently_watched':  return await _fetch_recently_watched(token, client_id)
    if cat == 'upcoming':          return await _fetch_upcoming(token, client_id, today)
    if cat == 'recommended':       return await _fetch_recommended(token, client_id)
    if cat in ('watchlist', 'collection'): return await _fetch_list(cat, token, client_id, username)
    if cat == 'trending':          return await _fetch_trending(token, client_id)
    return []


# ============================================================================
# ROUTES
# ============================================================================

@app.get('/health')
async def health():
    redis_ok = False
    if redis_client:
        try:
            await redis_client.ping()
            redis_ok = True
        except Exception:
            pass
    return {
        'status': 'healthy',
        'timestamp': datetime.utcnow().isoformat(),
        'redis': 'connected' if redis_ok else 'disconnected',
        'tmdb_configured': bool(TMDB_API_KEY),
        'fanart_configured': bool(FANART_API_KEY),
        'ip_whitelist_enabled': ENABLE_IP_WHITELIST,
    }


@app.get('/query')
async def trakt_tv_data(
    request: Request,
    categories: str = Query(default=''),
    username:   str = Query(default=''),
    _=Depends(require_whitelisted_ip),
):
    """
    Aggregate Trakt.tv data for the TRMNL plugin.

    Headers:  Authorization: Bearer {token}   trakt-api-key: {client_id}
    Params:   categories=watching,continue_watching,…
              username=someone   (optional; self-only categories are skipped)
    """
    token     = request.headers.get('Authorization', '').removeprefix('Bearer ').strip()
    client_id = request.headers.get('trakt-api-key', '').strip()
    if not token or not client_id:
        raise HTTPException(status_code=401, detail='Missing Authorization or trakt-api-key header')
    if ALLOWED_CLIENT_IDS and client_id not in ALLOWED_CLIENT_IDS:
        logger.warning(f"Blocked client_id: {client_id[:8]}…")
        raise HTTPException(status_code=403, detail='Access denied')

    cats = [c for c in (c.strip() for c in categories.split(',')) if c in VALID_CATEGORIES]
    if username:
        cats = [c for c in cats if c not in SELF_ONLY_CATEGORIES]
    if not cats:
        cats = ['watching', 'watchlist', 'collection'] if username else \
               ['continue_watching', 'recently_watched', 'upcoming', 'recommended']

    cache_key = 'query_v1:' + hashlib.md5(
        f'{token}:{client_id}:{",".join(cats)}:{username}'.encode()
    ).hexdigest()
    if redis_client:
        cached = await redis_client.get(cache_key)
        if cached:
            logger.debug("query cache hit")
            return json.loads(cached)

    today      = datetime.utcnow().strftime('%Y-%m-%d')
    stats_path = f'/users/{username}/stats' if username else '/users/me/stats'

    # stats is built from already-fetched stats_data — don't pass it to fetch_category
    non_stats_cats = [c for c in cats if c != 'stats']

    results = await asyncio.gather(
        trakt_get('/users/me',             token, client_id),
        trakt_get(stats_path,              token, client_id),
        trakt_get('/sync/ratings/movies',  token, client_id),
        trakt_get('/sync/ratings/shows',   token, client_id),
        *[fetch_category(cat, token, client_id, today, username) for cat in non_stats_cats],
    )
    (_, user_data), (_, stats_data), (_, rated_movies), (_, rated_shows), *cat_items = results

    user_data  = user_data  or {}
    stats_data = stats_data or {}
    ep_stats   = stats_data.get('episodes') or {}
    mov_stats  = stats_data.get('movies')   or {}
    show_stats = stats_data.get('shows')    or {}

    # Build favorites sets from ratings ≥ 8
    fav_movie_ids = {_ids(_movie(i)).get('tmdb') for i in (rated_movies or []) if i.get('rating', 0) >= 8} - {None}
    fav_show_ids  = {_ids(_show(i)).get('tmdb')  for i in (rated_shows  or []) if i.get('rating', 0) >= 8} - {None}

    await enrich_progress_all(cat_items, token, client_id)

    # Mark favorites
    for items in cat_items:
        for item in items:
            tid = item.get('tmdb_id')
            if item.get('type') == 'movie' and tid in fav_movie_ids:
                item['favorited'] = True
            elif item.get('type') in ('show', 'show_group') and tid in fav_show_ids:
                item['favorited'] = True

    all_items = [item for items in cat_items for item in items]
    enriched  = await enrich_images(all_items)

    idx = 0
    enriched_cats = []
    for items in cat_items:
        n = len(items)
        enriched_cats.append(enriched[idx:idx + n])
        idx += n

    categories_out = []
    has_content    = False
    non_stats_idx  = 0
    for cat in cats:
        if cat == 'stats':
            items = _build_stat_items(stats_data)
        else:
            items = enriched_cats[non_stats_idx]
            non_stats_idx += 1
        if items:
            has_content = True
        categories_out.append({'key': cat, 'title': CATEGORY_TITLES[cat], 'items': items})

    summary = {c['key']: len(c['items']) for c in categories_out}
    logger.info(f"query — user={user_data.get('username')!r} target={username!r} cats={summary}")

    result = {
        'data': {
            'user':  {'username': user_data.get('username')},
            'stats': {
                'hours_watched':      math.floor(ep_stats.get('minutes', 0) / 60),
                'episodes_collected': ep_stats.get('collected', 0),
                'movies_collected':   mov_stats.get('collected', 0),
                'shows_collected':    show_stats.get('collected', 0),
            },
            'categories':  categories_out,
            'has_content': has_content,
        }
    }

    if redis_client:
        await redis_client.set(cache_key, json.dumps(result), ex=QUERY_CACHE_TTL)

    return result
