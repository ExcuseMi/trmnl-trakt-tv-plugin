function transform(input) {
  const IMAGE_BASE_URL = 'https://trmnl.bettens.dev/trakttv';

  // Fixed endpoint order matches the polling_url:
  //   IDX_0  sync/playback/episodes     → continue_watching shows
  //   IDX_1  sync/playback/movies       → continue_watching movies
  //   IDX_2  sync/history/episodes      → recently_watched shows
  //   IDX_3  sync/history/movies        → recently_watched movies
  //   IDX_4  calendars/my/shows         → upcoming shows
  //   IDX_5  calendars/my/movies        → upcoming movies
  //   IDX_6  recommendations/shows      → recommended shows
  //   IDX_7  recommendations/movies     → recommended movies

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

  const idx0 = input.IDX_0?.data || [];
  const idx1 = input.IDX_1?.data || [];
  const idx2 = input.IDX_2?.data || [];
  const idx3 = input.IDX_3?.data || [];
  const idx4 = input.IDX_4?.data || [];
  const idx5 = input.IDX_5?.data || [];
  const idx6 = input.IDX_6?.data || [];
  const idx7 = input.IDX_7?.data || [];

  const continueWatching = [
    ...groupEpisodesByShow(idx0, item => ({
      season: item.episode?.season, episode: item.episode?.number,
      title: item.episode?.title, overview: item.episode?.overview || null,
      progress: Math.round(item.progress),
    })),
    ...idx1.map(item => ({
      type: 'movie', title: item.movie?.title, year: item.movie?.year,
      overview: item.movie?.overview || null,
      progress: Math.round(item.progress),
      tmdb_id: item.movie?.ids?.tmdb || null, media_type: 'movie',
    })),
  ];

  const recentlyWatched = [
    ...groupEpisodesByShow(idx2, item => ({
      season: item.episode?.season, episode: item.episode?.number,
      title: item.episode?.title, overview: item.episode?.overview || null,
      watched_at: item.watched_at,
    })),
    ...dedupeMovies(idx3, item => ({
      type: 'movie', title: item.movie?.title, year: item.movie?.year,
      overview: item.movie?.overview || null,
      watched_at: item.watched_at,
      tmdb_id: item.movie?.ids?.tmdb || null, media_type: 'movie',
    })),
  ];

  const upcoming = [
    ...groupEpisodesByShow(idx4, item => ({
      season: item.episode?.season, episode: item.episode?.number,
      title: item.episode?.title, overview: item.episode?.overview || null,
      airs_at: item.first_aired,
    })),
    ...dedupeMovies(idx5, item => ({
      type: 'movie', title: item.movie?.title, year: item.movie?.year,
      overview: item.movie?.overview || null,
      released: item.released,
      tmdb_id: item.movie?.ids?.tmdb || null, media_type: 'movie',
    })),
  ];

  const recommendedShowsSeen = new Map();
  idx6.forEach(item => {
    if (!recommendedShowsSeen.has(item.title)) {
      recommendedShowsSeen.set(item.title, {
        type: 'show', title: item.title, year: item.year,
        genres: item.genres || [],
        rating: item.rating ? Math.round(item.rating * 10) / 10 : null,
        network: item.network,
        tmdb_id: item.ids?.tmdb || null, media_type: 'show',
      });
    }
  });
  const recommendedMoviesSeen = new Map();
  idx7.forEach(item => {
    const key = `${item.title}-${item.year}`;
    if (!recommendedMoviesSeen.has(key)) {
      recommendedMoviesSeen.set(key, {
        type: 'movie', title: item.title, year: item.year,
        genres: item.genres || [],
        rating: item.rating ? Math.round(item.rating * 10) / 10 : null,
        tmdb_id: item.ids?.tmdb || null, media_type: 'movie',
      });
    }
  });
  const recommended = [
    ...Array.from(recommendedShowsSeen.values()),
    ...Array.from(recommendedMoviesSeen.values()),
  ];

  // Output as a keyed object — template iterates using user-defined priority order
  return {
    data: {
      image_base_url: IMAGE_BASE_URL,
      categories: {
        continue_watching: { key: 'continue_watching', title: 'Watching', items: continueWatching },
        recently_watched:  { key: 'recently_watched',  title: 'History',  items: recentlyWatched },
        upcoming:          { key: 'upcoming',           title: 'Upcoming', items: upcoming },
        recommended:       { key: 'recommended',        title: 'Picks',          items: recommended },
      },
      has_content: continueWatching.length > 0 || recentlyWatched.length > 0 || upcoming.length > 0 || recommended.length > 0,
    }
  };
}
