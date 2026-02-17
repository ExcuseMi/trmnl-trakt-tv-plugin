function transform(input) {
  // Configure your backend URL here (no trailing slash)
  const IMAGE_BASE_URL = 'https://trmnl.bettens.dev/trakttv';

  const getMovieKey = (item) =>
    `${item.movie?.title}-${item.movie?.year}`;

  const getShowKey = (item) =>
    `${item.title}`;

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

  // In-progress episodes (grouped by show)
  const episodesInProgress = groupEpisodesByShow(input.IDX_0?.data || [], item => ({
    season: item.episode?.season,
    episode: item.episode?.number,
    title: item.episode?.title,
    progress: Math.round(item.progress),
    paused_at: item.paused_at
  }));

  // In-progress movies
  const moviesInProgress = (input.IDX_1?.data || []).map(item => ({
    type: 'movie',
    title: item.movie?.title,
    year: item.movie?.year,
    progress: Math.round(item.progress),
    paused_at: item.paused_at,
    tmdb_id: item.movie?.ids?.tmdb || null,
    media_type: 'movie'
  }));

  // Recently watched episodes (grouped by show)
  const recentEpisodes = groupEpisodesByShow(input.IDX_2?.data || [], item => ({
    season: item.episode?.season,
    episode: item.episode?.number,
    title: item.episode?.title,
    watched_at: item.watched_at
  }));

  // Recently watched movies (with deduplication)
  const recentMoviesMap = new Map();
  (input.IDX_3?.data || []).forEach(item => {
    const key = getMovieKey(item);
    if (!recentMoviesMap.has(key)) {
      recentMoviesMap.set(key, {
        type: 'movie',
        title: item.movie?.title,
        year: item.movie?.year,
        watched_at: item.watched_at,
        tmdb_id: item.movie?.ids?.tmdb || null,
        media_type: 'movie'
      });
    }
  });
  const recentMovies = Array.from(recentMoviesMap.values());

  // Upcoming episodes (grouped by show)
  const upcomingShows = groupEpisodesByShow(input.IDX_4?.data || [], item => ({
    season: item.episode?.season,
    episode: item.episode?.number,
    title: item.episode?.title,
    airs_at: item.first_aired
  }));

  // Upcoming movies (with deduplication)
  const upcomingMoviesMap = new Map();
  (input.IDX_5?.data || []).forEach(item => {
    const key = getMovieKey(item);
    if (!upcomingMoviesMap.has(key)) {
      upcomingMoviesMap.set(key, {
        type: 'movie',
        title: item.movie?.title,
        year: item.movie?.year,
        released: item.released,
        tmdb_id: item.movie?.ids?.tmdb || null,
        media_type: 'movie'
      });
    }
  });
  const upcomingMovies = Array.from(upcomingMoviesMap.values());

  // Recommended shows (with deduplication)
  const recommendedShowsMap = new Map();
  (input.IDX_6?.data || []).forEach(item => {
    const key = getShowKey(item);
    if (!recommendedShowsMap.has(key)) {
      recommendedShowsMap.set(key, {
        type: 'show',
        title: item.title,
        year: item.year,
        rating: item.rating ? Math.round(item.rating * 10) / 10 : null,
        network: item.network,
        tmdb_id: item.ids?.tmdb || null,
        media_type: 'show'
      });
    }
  });
  const recommendedShows = Array.from(recommendedShowsMap.values());

  // Recommended movies (with deduplication)
  const recommendedMoviesMap = new Map();
  (input.IDX_7?.data || []).forEach(item => {
    const key = getMovieKey(item);
    if (!recommendedMoviesMap.has(key)) {
      recommendedMoviesMap.set(key, {
        type: 'movie',
        title: item.title,
        year: item.year,
        rating: item.rating ? Math.round(item.rating * 10) / 10 : null,
        tmdb_id: item.ids?.tmdb || null,
        media_type: 'movie'
      });
    }
  });
  const recommendedMovies = Array.from(recommendedMoviesMap.values());

  // Combine sections
  const continueWatching = [
    ...episodesInProgress,
    ...moviesInProgress
  ].slice(0, 5);

  const recentlyWatched = [
    ...recentEpisodes,
    ...recentMovies
  ].slice(0, 10);

  const upcoming = [
    ...upcomingShows,
    ...upcomingMovies
  ].slice(0, 10);

  const recommended = [
    ...recommendedShows,
    ...recommendedMovies
  ].slice(0, 10);

  return {
    data: {
      image_base_url: IMAGE_BASE_URL,
      continue_watching: continueWatching,
      recently_watched: recentlyWatched,
      upcoming: upcoming,
      recommended: recommended,
      fetched_at: new Date().toISOString(),
      has_content: (
        continueWatching.length > 0 ||
        recentlyWatched.length > 0 ||
        upcoming.length > 0 ||
        recommended.length > 0
      ),
      counts: {
        continue_watching: continueWatching.length,
        recently_watched: recentlyWatched.length,
        upcoming: upcoming.length,
        recommended: recommended.length
      }
    }
  };
}
