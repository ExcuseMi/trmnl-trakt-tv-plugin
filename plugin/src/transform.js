function transform(input) {
  // Configure your backend URL here (no trailing slash)
  const IMAGE_BASE_URL = 'https://trmnl.bettens.dev/trakttv';

  const TITLES = {
    continue_watching: 'Continue Watching',
    recently_watched:  'Recently Watched',
    upcoming:          'Upcoming',
    recommended:       'Recommended For You',
  };

  // Helper: group episodes by show, deduplicating individual episodes
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

  // Helper: deduplicate movies by title+year
  function dedupeMovies(items, mapper) {
    const seen = new Map();
    items.forEach(item => {
      const key = `${item.movie?.title}-${item.movie?.year}`;
      if (!seen.has(key)) seen.set(key, mapper(item));
    });
    return Array.from(seen.values());
  }

  // Detect which Trakt category landed in a pair of IDX slots by examining data shape.
  // IDX_A = shows/episodes endpoint, IDX_B = movies endpoint for the chosen category.
  function detectCategory(idxA, idxB) {
    const sample = idxA[0] || idxB[0] || null;
    if (!sample) return null;
    if ('progress' in sample)                                                    return 'continue_watching';
    if ('watched_at' in sample)                                                  return 'recently_watched';
    if ('first_aired' in sample || ('released' in sample && 'movie' in sample)) return 'upcoming';
    if ('title' in sample && !('show' in sample) && !('episode' in sample))     return 'recommended';
    return null;
  }

  // Extract and shape the items for a given category from its two IDX arrays.
  function processCategory(category, idxA, idxB, limit) {
    if (category === 'continue_watching') {
      const epGroups = groupEpisodesByShow(idxA, item => ({
        season:    item.episode?.season,
        episode:   item.episode?.number,
        title:     item.episode?.title,
        progress:  Math.round(item.progress),
        paused_at: item.paused_at
      }));
      const movies = idxB.map(item => ({
        type:      'movie',
        title:     item.movie?.title,
        year:      item.movie?.year,
        progress:  Math.round(item.progress),
        paused_at: item.paused_at,
        tmdb_id:   item.movie?.ids?.tmdb || null,
        media_type:'movie'
      }));
      return [...epGroups, ...movies].slice(0, limit);
    }

    if (category === 'recently_watched') {
      const epGroups = groupEpisodesByShow(idxA, item => ({
        season:     item.episode?.season,
        episode:    item.episode?.number,
        title:      item.episode?.title,
        watched_at: item.watched_at
      }));
      const movies = dedupeMovies(idxB, item => ({
        type:       'movie',
        title:      item.movie?.title,
        year:       item.movie?.year,
        watched_at: item.watched_at,
        tmdb_id:    item.movie?.ids?.tmdb || null,
        media_type: 'movie'
      }));
      return [...epGroups, ...movies].slice(0, limit);
    }

    if (category === 'upcoming') {
      const showGroups = groupEpisodesByShow(idxA, item => ({
        season:  item.episode?.season,
        episode: item.episode?.number,
        title:   item.episode?.title,
        airs_at: item.first_aired
      }));
      const movies = dedupeMovies(idxB, item => ({
        type:      'movie',
        title:     item.movie?.title,
        year:      item.movie?.year,
        released:  item.released,
        tmdb_id:   item.movie?.ids?.tmdb || null,
        media_type:'movie'
      }));
      return [...showGroups, ...movies].slice(0, limit);
    }

    if (category === 'recommended') {
      const showsMap = new Map();
      idxA.forEach(item => {
        if (!showsMap.has(item.title)) {
          showsMap.set(item.title, {
            type:      'show',
            title:     item.title,
            year:      item.year,
            rating:    item.rating ? Math.round(item.rating * 10) / 10 : null,
            network:   item.network,
            tmdb_id:   item.ids?.tmdb || null,
            media_type:'show'
          });
        }
      });
      const moviesMap = new Map();
      idxB.forEach(item => {
        const key = `${item.title}-${item.year}`;
        if (!moviesMap.has(key)) {
          moviesMap.set(key, {
            type:      'movie',
            title:     item.title,
            year:      item.year,
            rating:    item.rating ? Math.round(item.rating * 10) / 10 : null,
            tmdb_id:   item.ids?.tmdb || null,
            media_type:'movie'
          });
        }
      });
      return [...Array.from(showsMap.values()), ...Array.from(moviesMap.values())].slice(0, limit);
    }

    return [];
  }

  // Primary: IDX_0 (shows/episodes) + IDX_1 (movies)
  // Secondary: IDX_2 + IDX_3 — only present when the user selected >1 category
  const idx0 = input.IDX_0?.data || [];
  const idx1 = input.IDX_1?.data || [];
  const idx2 = input.IDX_2?.data || [];
  const idx3 = input.IDX_3?.data || [];

  const primaryCategory   = detectCategory(idx0, idx1);
  const hasSecondary      = idx2.length > 0 || idx3.length > 0;
  const secondaryCategory = hasSecondary ? detectCategory(idx2, idx3) : null;

  const primaryItems   = primaryCategory   ? processCategory(primaryCategory,   idx0, idx1, 10) : [];
  const secondaryItems = secondaryCategory ? processCategory(secondaryCategory, idx2, idx3, 4)  : [];

  // Spread items into named category keys so the template conditionals work unchanged
  const empty = [];
  const primary = {
    continue_watching: primaryCategory === 'continue_watching' ? primaryItems : empty,
    recently_watched:  primaryCategory === 'recently_watched'  ? primaryItems : empty,
    upcoming:          primaryCategory === 'upcoming'          ? primaryItems : empty,
    recommended:       primaryCategory === 'recommended'       ? primaryItems : empty,
  };
  const secondary = {
    continue_watching: secondaryCategory === 'continue_watching' ? secondaryItems : empty,
    recently_watched:  secondaryCategory === 'recently_watched'  ? secondaryItems : empty,
    upcoming:          secondaryCategory === 'upcoming'          ? secondaryItems : empty,
    recommended:       secondaryCategory === 'recommended'       ? secondaryItems : empty,
  };

  return {
    data: {
      image_base_url: IMAGE_BASE_URL,

      // Primary category
      display_category:  primaryCategory,
      display_title:     TITLES[primaryCategory] || null,
      continue_watching: primary.continue_watching,
      recently_watched:  primary.recently_watched,
      upcoming:          primary.upcoming,
      recommended:       primary.recommended,

      // Secondary category (only present when >1 category selected)
      secondary_category:          secondaryCategory,
      secondary_title:             TITLES[secondaryCategory] || null,
      secondary_continue_watching: secondary.continue_watching,
      secondary_recently_watched:  secondary.recently_watched,
      secondary_upcoming:          secondary.upcoming,
      secondary_recommended:       secondary.recommended,

      fetched_at:  new Date().toISOString(),
      has_content: primaryItems.length > 0 || secondaryItems.length > 0,
      counts: {
        continue_watching: primary.continue_watching.length,
        recently_watched:  primary.recently_watched.length,
        upcoming:          primary.upcoming.length,
        recommended:       primary.recommended.length,
      }
    }
  };
}
