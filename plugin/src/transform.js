/**
 * Slim the backend payload before TRMNL stores it (must be < 100 KB).
 *
 * Uses an allowlist approach — only fields the templates actually reference
 * are kept. Also caps items per category at 12 (max ever rendered across
 * all layout × breakpoint combinations).
 */
function transform(input) {
  var data = input.data;
  if (!data || !data.categories) return { data: data || {} };

  var OV_MAX = 150;
  var MAX_ITEMS = 12;

  function ov(text) {
    if (!text) return undefined;
    var s = String(text);
    return s.length > OV_MAX ? s.slice(0, OV_MAX) : s;
  }

  function slimEp(ep) {
    var out = { episode: ep.episode, title: ep.title };
    var o = ov(ep.overview); if (o)   out.overview   = o;
    if (ep.watched_at) out.watched_at = ep.watched_at;
    if (ep.airs_at)    out.airs_at    = ep.airs_at;
    if (ep.progress)   out.progress   = ep.progress;
    return out;
  }

  function slimSeason(s) {
    var out = { number: s.number };
    if (s.progress) out.progress = { completed: s.progress.completed, aired: s.progress.aired };
    out.episodes = (s.episodes || []).slice(0, 3).map(slimEp);
    return out;
  }

  function slimItem(item) {
    var t = item.type;
    var out = { type: t };
    if (item.image_url) out.image_url = item.image_url;
    if (item.favorited) out.favorited = item.favorited;

    if (t === 'stat') {
      if (item.label)  out.label  = item.label;
      if (item.title)  out.title  = item.title;
      if (item.values) out.values = item.values;
      return out;
    }

    if (t === 'movie') {
      if (item.title)        out.title        = item.title;
      if (item.year)         out.year         = item.year;
      if (item.genres)       out.genres       = item.genres.slice(0, 2);
      if (item.rating)       out.rating       = item.rating;
      if (item.watchers)     out.watchers     = item.watchers;
      if (item.progress)     out.progress     = item.progress;
      if (item.watched_at)   out.watched_at   = item.watched_at;
      if (item.collected_at) out.collected_at = item.collected_at;
      if (item.released)     out.released     = item.released;
      var o = ov(item.overview); if (o) out.overview = o;
      return out;
    }

    if (t === 'show' || t === 'show_group') {
      if (item.title)         out.title         = item.title;
      if (item.show)          out.show          = item.show;
      if (item.year)          out.year          = item.year;
      if (item.genres)        out.genres        = item.genres.slice(0, 2);
      if (item.rating)        out.rating        = item.rating;
      if (item.network)       out.network       = item.network;
      if (item.watchers)      out.watchers      = item.watchers;
      if (item.progress)      out.progress      = item.progress;
      if (item.watched_at)    out.watched_at    = item.watched_at;
      if (item.collected_at)  out.collected_at  = item.collected_at;
      if (item.total_seasons) out.total_seasons = item.total_seasons;
      if (item.show_progress) out.show_progress = { completed: item.show_progress.completed, aired: item.show_progress.aired };
      if (item.seasons)       out.seasons       = item.seasons.slice(0, 1).map(slimSeason);
      var o = ov(item.overview); if (o) out.overview = o;
      return out;
    }

    return out;
  }

  data.categories = data.categories.map(function(cat) {
    return { key: cat.key, title: cat.title, items: (cat.items || []).slice(0, MAX_ITEMS).map(slimItem) };
  });

  if (data.user) {
    data.user = { username: data.user.username, avatar: data.user.avatar };
  }

  return { data: data };
}
