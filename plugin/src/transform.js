function transform(input) {
  const IMAGE_BASE_URL = 'https://trmnl.bettens.dev/trakttv';

  // Determine active categories from user settings (deduped, in priority order).
  // The polling_url emits 2 URLs per unique category in this same order, so
  // IDX_0/1 = category 0 (shows/movies), IDX_2/3 = category 1, etc.
  const settings = input.trmnl?.plugin_settings?.custom_fields_values || {};
  const p1 = settings.priority_1 || 'continue_watching';
  const p2 = settings.priority_2 || 'recently_watched';
  const p3 = settings.priority_3 || 'upcoming';
  const p4 = settings.priority_4 || 'recommended';

  const seenCats = new Set();
  const activeCats = [p1, p2, p3, p4].filter(cat => {
    if (cat === 'none') return false;
    if (seenCats.has(cat)) return false;
    seenCats.add(cat);
    return true;
  });

  const catBase = {};
  activeCats.forEach((cat, i) => { catBase[cat] = i * 2; });

  function idx(cat, offset) {
    const base = catBase[cat];
    if (base === undefined) return [];
    return input[`IDX_${base + offset}`]?.data || [];
  }

  function groupEpisodesByShow(items, episodeMapper) {
    const showMap = new Map();
    const seenEpisodes = new Set();
    items.forEach(item => {
      const showKey = item.show?.title || 'Unknown';
      const epKey = `${showKey}-S${item.episode?.season}E${item.episode?.number}`;
      if (seenEpisodes.has(epKey)) return;
      seenEpisodes.add(epKey);
      if (!showMap.has(showKey)) {
        showMap.set(showKey, {
          type: 'show_group',
          show: item.show?.title,
          tmdb_id: item.show?.ids?.tmdb || null,
          media_type: 'show',
          episodes: []
        });
      }
      showMap.get(showKey).episodes.push(episodeMapper(item));
    });
    return Array.from(showMap.values());
  }

  function dedupeMovies(items, mapper) {
    const seen = new Map();
    items.forEach(item => {
      const key = `${item.movie?.title}-${item.movie?.year}`;
      if (!seen.has(key)) seen.set(key, mapper(item));
    });
    return Array.from(seen.values());
  }

  const continueWatching = [
    ...groupEpisodesByShow(idx('continue_watching', 0), item => ({
      season: item.episode?.season, episode: item.episode?.number,
      title: item.episode?.title, overview: item.episode?.overview || null,
      progress: Math.round(item.progress),
    })),
    ...idx('continue_watching', 1).map(item => ({
      type: 'movie', title: item.movie?.title, year: item.movie?.year,
      overview: item.movie?.overview || null,
      progress: Math.round(item.progress),
      tmdb_id: item.movie?.ids?.tmdb || null, media_type: 'movie',
    })),
  ];

  const recentlyWatched = [
    ...groupEpisodesByShow(idx('recently_watched', 0), item => ({
      season: item.episode?.season, episode: item.episode?.number,
      title: item.episode?.title, overview: item.episode?.overview || null,
      watched_at: item.watched_at,
    })),
    ...dedupeMovies(idx('recently_watched', 1), item => ({
      type: 'movie', title: item.movie?.title, year: item.movie?.year,
      overview: item.movie?.overview || null,
      watched_at: item.watched_at,
      tmdb_id: item.movie?.ids?.tmdb || null, media_type: 'movie',
    })),
  ];

  const upcoming = [
    ...groupEpisodesByShow(idx('upcoming', 0), item => ({
      season: item.episode?.season, episode: item.episode?.number,
      title: item.episode?.title, overview: item.episode?.overview || null,
      airs_at: item.first_aired,
    })),
    ...dedupeMovies(idx('upcoming', 1), item => ({
      type: 'movie', title: item.movie?.title, year: item.movie?.year,
      overview: item.movie?.overview || null,
      released: item.released,
      tmdb_id: item.movie?.ids?.tmdb || null, media_type: 'movie',
    })),
  ];

  const recommendedShowsSeen = new Map();
  idx('recommended', 0).forEach(item => {
    if (!recommendedShowsSeen.has(item.title)) {
      recommendedShowsSeen.set(item.title, {
        type: 'show', title: item.title, year: item.year,
        genres: item.genres || [],
        overview: item.overview || null,
        rating: item.rating ? Math.round(item.rating * 10) / 10 : null,
        network: item.network,
        tmdb_id: item.ids?.tmdb || null, media_type: 'show',
      });
    }
  });
  const recommendedMoviesSeen = new Map();
  idx('recommended', 1).forEach(item => {
    const key = `${item.title}-${item.year}`;
    if (!recommendedMoviesSeen.has(key)) {
      recommendedMoviesSeen.set(key, {
        type: 'movie', title: item.title, year: item.year,
        genres: item.genres || [],
        overview: item.overview || null,
        rating: item.rating ? Math.round(item.rating * 10) / 10 : null,
        tmdb_id: item.ids?.tmdb || null, media_type: 'movie',
      });
    }
  });
  const _recShows = Array.from(recommendedShowsSeen.values());
  const _recMovies = Array.from(recommendedMoviesSeen.values());
  const recommended = [];
  for (let i = 0; i < Math.max(_recShows.length, _recMovies.length); i++) {
    if (i < _recShows.length)  recommended.push(_recShows[i]);
    if (i < _recMovies.length) recommended.push(_recMovies[i]);
  }

  const watchlistShowsSeen = new Map();
  idx('watchlist', 0).forEach(item => {
    if (!watchlistShowsSeen.has(item.show?.title)) {
      watchlistShowsSeen.set(item.show?.title, {
        type: 'show', title: item.show?.title, year: item.show?.year,
        genres: item.show?.genres || [],
        overview: item.show?.overview || null,
        rating: item.show?.rating ? Math.round(item.show.rating * 10) / 10 : null,
        network: item.show?.network,
        tmdb_id: item.show?.ids?.tmdb || null, media_type: 'show',
      });
    }
  });
  const watchlistMoviesSeen = new Map();
  idx('watchlist', 1).forEach(item => {
    const key = `${item.movie?.title}-${item.movie?.year}`;
    if (!watchlistMoviesSeen.has(key)) {
      watchlistMoviesSeen.set(key, {
        type: 'movie', title: item.movie?.title, year: item.movie?.year,
        genres: item.movie?.genres || [],
        overview: item.movie?.overview || null,
        rating: item.movie?.rating ? Math.round(item.movie.rating * 10) / 10 : null,
        tmdb_id: item.movie?.ids?.tmdb || null, media_type: 'movie',
      });
    }
  });
  const watchlist = [
    ...Array.from(watchlistShowsSeen.values()),
    ...Array.from(watchlistMoviesSeen.values()),
  ];

  const collectionShowsSeen = new Map();
  idx('collection', 0).forEach(item => {
    if (!collectionShowsSeen.has(item.show?.title)) {
      collectionShowsSeen.set(item.show?.title, {
        type: 'show', title: item.show?.title, year: item.show?.year,
        genres: item.show?.genres || [],
        overview: item.show?.overview || null,
        rating: item.show?.rating ? Math.round(item.show.rating * 10) / 10 : null,
        network: item.show?.network,
        tmdb_id: item.show?.ids?.tmdb || null, media_type: 'show',
      });
    }
  });
  const collectionMoviesSeen = new Map();
  idx('collection', 1).forEach(item => {
    const key = `${item.movie?.title}-${item.movie?.year}`;
    if (!collectionMoviesSeen.has(key)) {
      collectionMoviesSeen.set(key, {
        type: 'movie', title: item.movie?.title, year: item.movie?.year,
        genres: item.movie?.genres || [],
        overview: item.movie?.overview || null,
        rating: item.movie?.rating ? Math.round(item.movie.rating * 10) / 10 : null,
        tmdb_id: item.movie?.ids?.tmdb || null, media_type: 'movie',
      });
    }
  });
  const collection = [
    ...Array.from(collectionShowsSeen.values()),
    ...Array.from(collectionMoviesSeen.values()),
  ];

  // Output as a keyed object — template iterates using user-defined priority order
  return {
    data: {
      image_base_url: IMAGE_BASE_URL,
      categories: {
        continue_watching: { key: 'continue_watching', title: 'Watching',    items: continueWatching },
        recently_watched:  { key: 'recently_watched',  title: 'History',     items: recentlyWatched },
        upcoming:          { key: 'upcoming',           title: 'Upcoming',    items: upcoming },
        recommended:       { key: 'recommended',        title: 'Picks',       items: recommended },
        watchlist:         { key: 'watchlist',          title: 'Watchlist',   items: watchlist },
        collection:        { key: 'collection',         title: 'Collection',  items: collection },
      },
      has_content: continueWatching.length > 0 || recentlyWatched.length > 0 || upcoming.length > 0 || recommended.length > 0 || watchlist.length > 0 || collection.length > 0,
    }
  };
}
