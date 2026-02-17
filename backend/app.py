#!/usr/bin/env python3
"""
TRMNL Trakt.tv Image Proxy
Serves poster images for shows and movies via TMDB (primary) and Fanart.tv (fallback).
Images are cached in Redis to minimize external API calls.
"""

import logging
import os
import threading
import time
from datetime import datetime
from functools import wraps
from typing import Optional

import httpx
import redis
from flask import Flask, Response, jsonify, request
from flask_cors import CORS


# ============================================================================
# CONFIGURATION
# ============================================================================

LOG_LEVEL = logging.DEBUG if os.getenv('DEBUG', 'false').lower() == 'true' else logging.INFO
logging.basicConfig(
    level=LOG_LEVEL,
    format='%(asctime)s [%(levelname)s] %(name)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# IP whitelist
ENABLE_IP_WHITELIST = os.getenv('ENABLE_IP_WHITELIST', 'true').lower() == 'true'
IP_REFRESH_HOURS = int(os.getenv('IP_REFRESH_HOURS', '24'))
TRMNL_IPS_API = 'https://trmnl.com/api/ips'
LOCALHOST_IPS = ['127.0.0.1', '::1', 'localhost']

# API keys
TMDB_API_KEY = os.getenv('TMDB_API_KEY', '')
FANART_API_KEY = os.getenv('FANART_API_KEY', '')

# Redis / cache
REDIS_URL = os.getenv('REDIS_URL', 'redis://redis:6379/0')
CACHE_TTL = int(os.getenv('CACHE_TTL_SECONDS', '604800'))  # 7 days
CACHE_TTL_NOT_FOUND = int(os.getenv('CACHE_TTL_NOT_FOUND_SECONDS', '86400'))  # 1 day

# TMDB
TMDB_IMAGE_SIZE = os.getenv('TMDB_IMAGE_SIZE', 'w185')
TMDB_API_BASE = 'https://api.themoviedb.org/3'
TMDB_IMAGE_BASE = 'https://image.tmdb.org/t/p'
FANART_API_BASE = 'https://webservice.fanart.tv/v3'

# Sentinel value stored in Redis when no image is found
NOT_FOUND_SENTINEL = b'__NOT_FOUND__'

# Global state
TRMNL_IPS = set(LOCALHOST_IPS)
TRMNL_IPS_LOCK = threading.Lock()
last_ip_refresh: Optional[datetime] = None
redis_client: Optional[redis.Redis] = None


# ============================================================================
# FLASK APP
# ============================================================================

app = Flask(__name__)
CORS(app)


# ============================================================================
# REDIS
# ============================================================================

def connect_redis():
    """Connect to Redis with retries on startup."""
    global redis_client
    for attempt in range(5):
        try:
            client = redis.Redis.from_url(REDIS_URL, decode_responses=False)
            client.ping()
            redis_client = client
            logger.info("Connected to Redis")
            return
        except Exception as e:
            logger.warning(f"Redis connection attempt {attempt + 1}/5 failed: {e}")
            time.sleep(2)
    logger.error("Could not connect to Redis after 5 attempts")


# ============================================================================
# IP WHITELIST
# ============================================================================

def fetch_trmnl_ips_sync():
    """Fetch current TRMNL server IPs from their API (synchronous)."""
    try:
        with httpx.Client(timeout=10.0) as client:
            response = client.get(TRMNL_IPS_API)
            response.raise_for_status()
            data = response.json()
            ipv4_list = data.get('data', {}).get('ipv4', [])
            ipv6_list = data.get('data', {}).get('ipv6', [])
            ips = set(ipv4_list + ipv6_list + LOCALHOST_IPS)
            logger.info(f"Loaded {len(ips)} TRMNL IPs ({len(ipv4_list)} IPv4, {len(ipv6_list)} IPv6)")
            return ips
    except Exception as e:
        logger.error(f"Failed to fetch TRMNL IPs: {e}")
        return set(LOCALHOST_IPS)


def update_trmnl_ips():
    """Update the global TRMNL IP set."""
    global TRMNL_IPS, last_ip_refresh
    ips = fetch_trmnl_ips_sync()
    with TRMNL_IPS_LOCK:
        TRMNL_IPS = ips
        last_ip_refresh = datetime.now()


def ip_refresh_worker():
    """Background worker that refreshes TRMNL IPs on the hour."""
    while True:
        try:
            now = datetime.now()
            seconds_to_next_hour = (60 - now.minute) * 60 - now.second
            time.sleep(seconds_to_next_hour)
            logger.info("Refreshing TRMNL IPs...")
            update_trmnl_ips()
            time.sleep(IP_REFRESH_HOURS * 3600 - 1)
        except Exception as e:
            logger.error(f"IP refresh worker error: {e}")
            time.sleep(3600)


def start_ip_refresh_worker():
    """Start background thread for IP refresh."""
    if not ENABLE_IP_WHITELIST:
        return
    t = threading.Thread(target=ip_refresh_worker, daemon=True, name='IP-Refresh-Worker')
    t.start()
    logger.info(f"IP refresh worker started (every {IP_REFRESH_HOURS}h)")


def get_client_ip():
    """Get the real client IP address, accounting for proxies."""
    if request.headers.get('CF-Connecting-IP'):
        return request.headers.get('CF-Connecting-IP').strip()
    if request.headers.get('X-Forwarded-For'):
        return request.headers.get('X-Forwarded-For').split(',')[0].strip()
    if request.headers.get('X-Real-IP'):
        return request.headers.get('X-Real-IP').strip()
    return request.remote_addr


def require_whitelisted_ip(f):
    """Decorator to enforce IP whitelisting on routes."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not ENABLE_IP_WHITELIST:
            return f(*args, **kwargs)
        client_ip = get_client_ip()
        with TRMNL_IPS_LOCK:
            allowed = TRMNL_IPS.copy()
        if client_ip not in allowed:
            logger.warning(f"Blocked unauthorized IP: {client_ip}")
            return jsonify({
                'error': 'Access denied',
                'message': 'Your IP address is not authorized to access this service'
            }), 403
        return f(*args, **kwargs)
    return decorated_function


# ============================================================================
# IMAGE LOOKUP
# ============================================================================

def fetch_tmdb_poster(media_type: str, tmdb_id: str) -> Optional[bytes]:
    """
    Fetch a poster image from TMDB.
    media_type: 'tv' or 'movie'
    Returns raw image bytes or None.
    """
    if not TMDB_API_KEY:
        logger.warning("TMDB_API_KEY not configured")
        return None

    try:
        with httpx.Client(timeout=10.0) as client:
            # Get metadata to find poster_path
            url = f"{TMDB_API_BASE}/{media_type}/{tmdb_id}"
            resp = client.get(url, params={'api_key': TMDB_API_KEY})
            if resp.status_code == 404:
                logger.info(f"TMDB: {media_type}/{tmdb_id} not found")
                return None
            resp.raise_for_status()
            data = resp.json()

            poster_path = data.get('poster_path')
            if not poster_path:
                logger.info(f"TMDB: {media_type}/{tmdb_id} has no poster")
                return None

            # Fetch the actual image
            image_url = f"{TMDB_IMAGE_BASE}/{TMDB_IMAGE_SIZE}{poster_path}"
            img_resp = client.get(image_url)
            img_resp.raise_for_status()
            logger.info(f"TMDB: fetched poster for {media_type}/{tmdb_id} ({len(img_resp.content)} bytes)")
            return img_resp.content

    except Exception as e:
        logger.error(f"TMDB error for {media_type}/{tmdb_id}: {e}")
        return None


def get_tvdb_id_from_tmdb(tmdb_id: str) -> Optional[int]:
    """Get TVDB ID for a show via TMDB external_ids endpoint. Result is cached in Redis."""
    cache_key = f"tvdb_id:{tmdb_id}"

    if redis_client:
        cached = redis_client.get(cache_key)
        if cached is not None:
            if cached == NOT_FOUND_SENTINEL:
                return None
            return int(cached)

    if not TMDB_API_KEY:
        return None

    try:
        with httpx.Client(timeout=10.0) as client:
            resp = client.get(
                f"{TMDB_API_BASE}/tv/{tmdb_id}/external_ids",
                params={'api_key': TMDB_API_KEY}
            )
            resp.raise_for_status()
            tvdb_id = resp.json().get('tvdb_id')

            if redis_client:
                if tvdb_id:
                    redis_client.set(cache_key, str(tvdb_id), ex=CACHE_TTL)
                else:
                    redis_client.set(cache_key, NOT_FOUND_SENTINEL, ex=CACHE_TTL_NOT_FOUND)

            return tvdb_id
    except Exception as e:
        logger.error(f"TMDB external_ids error for tv/{tmdb_id}: {e}")
        return None


def fetch_fanart_poster(media_type: str, tmdb_id: str) -> Optional[bytes]:
    """
    Fetch a poster image from Fanart.tv as fallback.
    For movies: uses TMDB ID directly.
    For shows: looks up TVDB ID first.
    Returns raw image bytes or None.
    """
    if not FANART_API_KEY:
        return None

    try:
        with httpx.Client(timeout=10.0) as client:
            if media_type == 'tv':
                tvdb_id = get_tvdb_id_from_tmdb(tmdb_id)
                if not tvdb_id:
                    logger.info(f"Fanart: no TVDB ID for show {tmdb_id}")
                    return None
                fanart_url = f"{FANART_API_BASE}/tv/{tvdb_id}"
            else:
                fanart_url = f"{FANART_API_BASE}/movies/{tmdb_id}"

            resp = client.get(fanart_url, params={'api_key': FANART_API_KEY})
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            data = resp.json()

            # Try tvposter for shows, movieposter for movies
            poster_key = 'tvposter' if media_type == 'tv' else 'movieposter'
            posters = data.get(poster_key, [])
            if not posters:
                return None

            # Fetch the first poster image
            poster_url = posters[0].get('url')
            if not poster_url:
                return None

            img_resp = client.get(poster_url)
            img_resp.raise_for_status()
            logger.info(f"Fanart: fetched poster for {media_type}/{tmdb_id} ({len(img_resp.content)} bytes)")
            return img_resp.content

    except Exception as e:
        logger.error(f"Fanart error for {media_type}/{tmdb_id}: {e}")
        return None


def get_image(media_type: str, tmdb_id: str) -> tuple[Optional[bytes], str]:
    """
    Get image bytes for a media item, using cache -> TMDB -> Fanart.tv.
    Returns (image_bytes, source) where source is 'cache', 'tmdb', 'fanart', or 'none'.
    """
    # media_type from URL is 'show' or 'movie'; TMDB uses 'tv' or 'movie'
    tmdb_type = 'tv' if media_type == 'show' else 'movie'
    cache_key = f"img:{media_type}:{tmdb_id}"

    # Check Redis cache
    if redis_client:
        try:
            cached = redis_client.get(cache_key)
            if cached is not None:
                if cached == NOT_FOUND_SENTINEL:
                    return None, 'none'
                return cached, 'cache'
        except Exception as e:
            logger.error(f"Redis read error: {e}")

    # Try TMDB
    image_bytes = fetch_tmdb_poster(tmdb_type, tmdb_id)
    if image_bytes:
        if redis_client:
            try:
                redis_client.set(cache_key, image_bytes, ex=CACHE_TTL)
            except Exception as e:
                logger.error(f"Redis write error: {e}")
        return image_bytes, 'tmdb'

    # Try Fanart.tv fallback
    image_bytes = fetch_fanart_poster(tmdb_type, tmdb_id)
    if image_bytes:
        if redis_client:
            try:
                redis_client.set(cache_key, image_bytes, ex=CACHE_TTL)
            except Exception as e:
                logger.error(f"Redis write error: {e}")
        return image_bytes, 'fanart'

    # Nothing found — cache the miss
    if redis_client:
        try:
            redis_client.set(cache_key, NOT_FOUND_SENTINEL, ex=CACHE_TTL_NOT_FOUND)
        except Exception as e:
            logger.error(f"Redis write error: {e}")

    return None, 'none'


# ============================================================================
# API ROUTES
# ============================================================================

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint."""
    redis_ok = False
    if redis_client:
        try:
            redis_client.ping()
            redis_ok = True
        except Exception:
            pass

    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.utcnow().isoformat(),
        'redis': 'connected' if redis_ok else 'disconnected',
        'tmdb_configured': bool(TMDB_API_KEY),
        'fanart_configured': bool(FANART_API_KEY),
        'ip_whitelist_enabled': ENABLE_IP_WHITELIST,
        'last_ip_refresh': last_ip_refresh.isoformat() if last_ip_refresh else None,
    })


@app.route('/image/<media_type>/<tmdb_id>', methods=['GET'])
@require_whitelisted_ip
def serve_image(media_type, tmdb_id):
    """
    Serve a poster image for a show or movie.
    media_type: 'show' or 'movie'
    tmdb_id: TMDB ID (numeric string)
    """
    if media_type not in ('show', 'movie'):
        return jsonify({'error': 'Invalid media type, use "show" or "movie"'}), 400

    if not tmdb_id.isdigit():
        return jsonify({'error': 'Invalid TMDB ID'}), 400

    image_bytes, source = get_image(media_type, tmdb_id)

    if image_bytes is None:
        return Response('Not Found', status=404, headers={
            'X-Image-Source': source,
            'Cache-Control': 'public, max-age=3600',
        })

    return Response(
        image_bytes,
        status=200,
        content_type='image/jpeg',
        headers={
            'X-Image-Source': source,
            'Cache-Control': 'public, max-age=86400',
        },
    )


# ============================================================================
# STARTUP
# ============================================================================

def initialize():
    """Initialize the application."""
    logger.info("Starting TRMNL Trakt.tv Image Proxy")

    connect_redis()

    if ENABLE_IP_WHITELIST:
        logger.info("IP whitelist enabled")
        update_trmnl_ips()
        start_ip_refresh_worker()
    else:
        logger.warning("IP whitelist DISABLED — all IPs allowed")

    if not TMDB_API_KEY:
        logger.warning("TMDB_API_KEY not set — image lookups will fail")

    logger.info("Application initialized successfully")


initialize()


if __name__ == '__main__':
    port = int(os.getenv('PORT', '5000'))
    app.run(host='0.0.0.0', port=port, debug=False)
