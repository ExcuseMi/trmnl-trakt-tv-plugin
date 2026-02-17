function transform(input) {
  // Configure your backend URL here (no trailing slash)
  const IMAGE_BASE_URL = '';

  // Helper to create unique key for deduplication
  const getEpisodeKey = (item) =>
    `${item.show?.title}-${item.episode?.season}-${item.episode?.number}`;

  const getMovieKey = (item) =>
    `${item.movie?.title}-${item.movie?.year}`;

  const getShowKey = (item) =>
    `${item.title}`;

  // In-progress episodes
  const episodesInProgress = (input.IDX_0?.data || []).map(item => ({
    type: 'episode',
    show: item.show?.title,
    season: item.episode?.season,
    episode: item.episode?.number,
    title: item.episode?.title,
    progress: Math.round(item.progress),
    paused_at: item.paused_at,
    tmdb_id: item.show?.ids?.tmdb || null,
    media_type: 'show'
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

  // Recently watched episodes (with deduplication)
  const recentEpisodesMap = new Map();
  (input.IDX_2?.data || []).forEach(item => {
    const key = getEpisodeKey(item);
    if (!recentEpisodesMap.has(key)) {
      recentEpisodesMap.set(key, {
        type: 'episode',
        show: item.show?.title,
        season: item.episode?.season,
        episode: item.episode?.number,
        title: item.episode?.title,
        watched_at: item.watched_at,
        tmdb_id: item.show?.ids?.tmdb || null,
        media_type: 'show'
      });
    }
  });
  const recentEpisodes = Array.from(recentEpisodesMap.values());

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

  // Upcoming shows (with deduplication)
  const upcomingShowsMap = new Map();
  (input.IDX_4?.data || []).forEach(item => {
    const key = getEpisodeKey(item);
    if (!upcomingShowsMap.has(key)) {
      upcomingShowsMap.set(key, {
        type: 'episode',
        show: item.show?.title,
        season: item.episode?.season,
        episode: item.episode?.number,
        title: item.episode?.title,
        airs_at: item.first_aired,
        tmdb_id: item.show?.ids?.tmdb || null,
        media_type: 'show'
      });
    }
  });
  const upcomingShows = Array.from(upcomingShowsMap.values());

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
