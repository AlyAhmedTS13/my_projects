"""Recommender helpers for similar movies and user-based recommendations.

This module is designed to be safe: if the model files or CSV mapping
are missing or inconsistent, the public functions will simply return
empty lists instead of raising errors.
"""

from __future__ import annotations

import csv
import json
from functools import lru_cache
from pathlib import Path
from typing import List

from django.conf import settings

from .models import Movie, Like
from .utils import fetch_tmdb_data

try:
    import joblib  # type: ignore
    from sklearn.neighbors import NearestNeighbors  # type: ignore
except Exception:  # pragma: no cover - if sklearn/joblib missing in env
    joblib = None  # type: ignore
    NearestNeighbors = None  # type: ignore


def _get_project_root() -> Path:
    """Return the project root where manage.py lives (BASE_DIR)."""
    base_dir = getattr(settings, "BASE_DIR", None)
    if base_dir is not None:
        return Path(base_dir)
    # Fallback: resolve relative to this file
    return Path(__file__).resolve().parents[2]


@lru_cache(maxsize=1)
def _load_models():
    """Load TF-IDF vectorizer, NearestNeighbors model, and movie-id mapping.

    Returns (tfidf, nn_model, movie_ids). If anything goes wrong,
    returns (None, None, None) and logs nothing noisy to avoid
    breaking the site.
    """

    if joblib is None or NearestNeighbors is None:
        return None, None, None

    root = _get_project_root()
    # Model files live one level above the website folder
    models_dir = root.parent

    tfidf_path = models_dir / "tfidf_vectorizer.pkl"
    nn_path = models_dir / "recommendation system.pkl"
    csv_path = models_dir / "movies_DB_cleaned.csv"

    if not tfidf_path.exists() or not nn_path.exists() or not csv_path.exists():
        return None, None, None

    try:
        tfidf = joblib.load(tfidf_path)
        nn_model = joblib.load(nn_path)
    except Exception:
        return None, None, None

    # Build mapping from training index -> tmdb_id (movie_id column)
    movie_ids: List[int] = []
    try:
        with csv_path.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                raw_id = row.get("movie_id") or row.get("tmdb_id") or row.get("id")
                if not raw_id:
                    continue
                try:
                    movie_ids.append(int(raw_id))
                except (TypeError, ValueError):
                    continue
    except Exception:
        return None, None, None

    fit_x = getattr(nn_model, "_fit_X", None)
    if fit_x is not None and len(movie_ids) != fit_x.shape[0]:
        # Mismatch between training data and mapping: fail silently
        return None, None, None

    return tfidf, nn_model, movie_ids


def _build_combined_text(movie: Movie) -> str:
    """Recreate the combined text features for a Movie instance."""

    import json

    def join_from_json(value, key: str = "name", limit: int | None = None) -> str:
        if not value:
            return ""
        # value may be a list of dicts, or a dict with a "keywords" key, or a JSON string
        try:
            if isinstance(value, str):
                try:
                    parsed = json.loads(value)
                    value = parsed
                except Exception:
                    # keep original string if it isn't valid JSON
                    pass
            if isinstance(value, dict) and "keywords" in value:
                items = value["keywords"]
            else:
                items = value
        except Exception:
            items = []
        names = []
        for i, item in enumerate(items or []):
            if limit is not None and i >= limit:
                break
            if isinstance(item, dict):
                name = item.get(key)
            else:
                name = str(item)
            if name:
                names.append(str(name))
        return " ".join(names)

    overview = movie.overview or ""
    tagline = movie.tagline or ""
    genres_text = join_from_json(movie.genres)
    keywords_text = join_from_json(movie.keywords)
    cast_text = join_from_json(movie.cast, key="name", limit=5)
    director_text = movie.director or ""

    parts = [overview, tagline, genres_text, keywords_text, cast_text, director_text]
    combined = " ".join(p for p in parts if p).strip()
    return combined



def get_similar_movies(tmdb_id: int, n: int = 10) -> List[int]:
    """Return up to n similar movie TMDB IDs for the given movie.

    Results are sorted by distance (closest/most similar first).
    If the models or mapping are unavailable, returns an empty list.
    """

    tfidf, nn_model, movie_ids = _load_models()
    if tfidf is None or nn_model is None or not movie_ids:
        return []

    try:
        movie = Movie.objects.get(tmdb_id=tmdb_id)
    except Movie.DoesNotExist:
        # If movie not in local DB, we can't build text features
        return []

    text = _build_combined_text(movie)
    if not text:
        return []

    try:
        query_vec = tfidf.transform([text])
        # Ask for a few extra to account for the movie itself
        n_neighbors = min(len(movie_ids), max(n + 5, n))
        # Return both distances and indices for proper sorting
        distances, indices = nn_model.kneighbors(query_vec, n_neighbors=n_neighbors, return_distance=True)
        distances = distances[0]
        indices = indices[0]
    except Exception:
        return []

    # Build list sorted by distance (closest first)
    similar_ids: List[int] = []
    for i, idx in enumerate(indices):
        try:
            candidate_id = movie_ids[int(idx)]
        except (IndexError, ValueError, TypeError):
            continue
        if candidate_id == tmdb_id:
            continue
        if candidate_id not in similar_ids:
            similar_ids.append(candidate_id)
        if len(similar_ids) >= n:
            break

    return similar_ids


def get_user_recommendations(user, n_per_movie: int = 5) -> List[int]:
    """Return recommended TMDB IDs based on a user's liked movies.
    
    Gets n_per_movie recommendations for each liked movie and combines them.
    Results are sorted by popularity within the local DB.
    """

    if user is None or not user.is_authenticated:
        return []

    liked_ids = list(Like.objects.filter(user=user).values_list("movie_id", flat=True))
    if not liked_ids:
        return []

    # Collect recommendations for each liked movie
    all_recommendations = []
    seen_ids = set(liked_ids)  # Don't recommend movies user already liked
    
    for mid in liked_ids:
        # Get top 5 similar movies for each liked movie (already sorted by distance)
        similar_ids = get_similar_movies(mid, n=n_per_movie)
        for sim_id in similar_ids:
            if sim_id not in seen_ids:
                all_recommendations.append(sim_id)
                seen_ids.add(sim_id)

    if not all_recommendations:
        return []

    # Sort by popularity from local DB
    movies_qs = Movie.objects.filter(tmdb_id__in=all_recommendations).order_by('-popularity')
    sorted_ids = [m.tmdb_id for m in movies_qs]
    
    return sorted_ids
