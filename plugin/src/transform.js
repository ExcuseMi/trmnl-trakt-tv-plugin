/**
 * Slim the backend payload before TRMNL stores it (must be < 100 KB).
 *
 * Main savings:
 *  - Remove backend-only fields: tmdb_id, media_type, trakt_slug
 *  - Truncate overview text (300+ chars → 200 max)
 *  - Limit each show_group to 1 season with 3 episodes (template uses limit:1 / limit:3)
 *  - Limit genres to 2 (template uses limit:2)
 *  - Drop null / undefined values
 */
function transform(input) {
  var data = input.data;
  if (!data || !data.categories) return { data: data || {} };

  var STRIP = { tmdb_id: 1, media_type: 1, trakt_slug: 1 };
  var OV_MAX = 200;

  function ov(text) {
    if (!text) return undefined;
    return text.length > OV_MAX ? text.slice(0, OV_MAX) : text;
  }

  function slimEp(ep) {
    return compact({
      episode:    ep.episode,
      title:      ep.title,
      overview:   ov(ep.overview),
      watched_at: ep.watched_at,
      airs_at:    ep.airs_at,
      progress:   ep.progress,
    });
  }

  function slimSeason(s) {
    return compact({
      number:   s.number,
      progress: s.progress,
      episodes: (s.episodes || []).slice(0, 3).map(slimEp),
    });
  }

  function slimItem(item) {
    var out = {};
    for (var k in item) {
      if (STRIP[k]) continue;
      var v = item[k];
      if (v === null || v === undefined) continue;
      if (k === 'overview')  { v = ov(v); if (!v) continue; }
      if (k === 'genres')    { v = v.slice(0, 2); }
      if (k === 'seasons')   { v = v.slice(0, 1).map(slimSeason); }
      out[k] = v;
    }
    return out;
  }

  function compact(obj) {
    var out = {};
    for (var k in obj) {
      var v = obj[k];
      if (v !== null && v !== undefined) out[k] = v;
    }
    return out;
  }

  data.categories = data.categories.map(function(cat) {
    return { key: cat.key, title: cat.title, items: cat.items.map(slimItem) };
  });

  return { data: data };
}
