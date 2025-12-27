# In movies/views.py

from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth.decorators import login_required
from django.contrib.auth.forms import UserCreationForm 
from django.db import IntegrityError # Needed for error handling
from django.db.models import F # Used for potential future updates, good to have
from datetime import datetime
# CRITICAL: Import all necessary models and forms

from .models import Watchlist, Review, Like, Movie
from .forms import WatchlistForm, ReviewForm, LikeForm 


from .utils import fetch_tmdb_data, get_poster_url 

# Recommender helpers (safe: they fail closed and return empty lists)
from .recommender import get_similar_movies, get_user_recommendations


def get_optimized_movie_display_data(movie_obj):
    """
    Helper function to get poster_path and vote_average from local DB.
    Only fetches from API if either field is missing (null/empty).
    SAVES the fetched data to DB so next time it's faster!
    Returns: (poster_path, vote_average)
    """
    poster_path = movie_obj.poster_path
    vote_average = movie_obj.vote_average
    updated = False
    
    # Only fetch from API if poster_path OR vote_average is missing
    if not poster_path or vote_average is None:
        print(f"API call for movie {movie_obj.title} ({movie_obj.tmdb_id})")
        api_data = fetch_tmdb_data(f'movie/{movie_obj.tmdb_id}')
        if api_data:
            if not poster_path:
                poster_path = api_data.get('poster_path')
                movie_obj.poster_path = poster_path
                updated = True
            if vote_average is None:
                vote_average = api_data.get('vote_average')
                movie_obj.vote_average = vote_average
                updated = True
            if updated:
                movie_obj.save()  # Save to DB for next time
                print(f"Saved to DB: {movie_obj.title}")
    else:
        print(f"Using cached data for {movie_obj.title} ({movie_obj.tmdb_id})")
    
    return poster_path, vote_average


## Helper Function
def get_full_language_name(code):
    """
    Converts a 2-letter ISO 639-1 code (e.g., 'en') to the full language name.
    """
    language_map = {
    'en': 'English', 'es': 'Spanish', 'fr': 'French', 'de': 'German',
    'it': 'Italian', 'ja': 'Japanese', 'ko': 'Korean', 'zh': 'Mandarin',
    'ru': 'Russian', 'pt': 'Portuguese', 'hi': 'Hindi', 'ar': 'Arabic',
    'cn': 'Cantonese', 'no': 'Norwegian', 'sv': 'Swedish', 'tr': 'Turkish',
    'pl': 'Polish', 'nl': 'Dutch', 'th': 'Thai', 'id': 'Indonesian',
    'da': 'Danish', 'fi': 'Finnish', 'he': 'Hebrew', 'hu': 'Hungarian',
    'cs': 'Czech', 'el': 'Greek', 'vi': 'Vietnamese',

    # Additional languages
    'ro': 'Romanian', 'bg': 'Bulgarian', 'uk': 'Ukrainian', 'sr': 'Serbian',
    'hr': 'Croatian', 'sk': 'Slovak', 'lt': 'Lithuanian', 'lv': 'Latvian',
    'et': 'Estonian', 'fa': 'Persian (Farsi)', 'bn': 'Bengali', 'ta': 'Tamil',
    'te': 'Telugu', 'ml': 'Malayalam', 'mr': 'Marathi', 'gu': 'Gujarati',
    'pa': 'Punjabi', 'ur': 'Urdu', 'si': 'Sinhala', 'my': 'Burmese',
    'km': 'Khmer', 'lo': 'Lao', 'mn': 'Mongolian', 'ka': 'Georgian',
    'hy': 'Armenian', 'az': 'Azerbaijani', 'kk': 'Kazakh', 'uz': 'Uzbek',
    'tk': 'Turkmen', 'sw': 'Swahili', 'am': 'Amharic', 'so': 'Somali',
    'yo': 'Yoruba', 'ig': 'Igbo', 'ha': 'Hausa', 'zu': 'Zulu',
    'xh': 'Xhosa', 'af': 'Afrikaans', 'ms': 'Malay', 'fil': 'Filipino',
    "kn": "Kannada",
}

    return language_map.get(code, code.upper())

def _extract_director(credits_data):
    """Safely extracts the director's name from the credits data."""
    if credits_data and 'crew' in credits_data:
        director_crew = next((crew for crew in credits_data['crew'] if crew['job'] == 'Director'), None)
        return director_crew['name'] if director_crew else None
        
    return None


# In movies/views.py

def movie_cast_view(request, movie_id):
    """
    Fetches and displays the full cast details for a given movie ID.
    OPTIMIZED: Uses local DB for cast data, only falls back to API if not cached.
    """
    movie_id = int(movie_id)
    full_cast = []
    movie_title = None
    movie_poster_url = None
    
    # 1. Try to get cast from local DB first
    try:
        movie_obj = Movie.objects.get(tmdb_id=movie_id)
        movie_title = movie_obj.title
        movie_poster_url = get_poster_url(movie_obj.poster_path)
        
        # Get cast from cached JSON field
        if movie_obj.cast and isinstance(movie_obj.cast, list):
            for member in movie_obj.cast:
                # Handle both dict format (from API) and legacy formats
                if isinstance(member, dict):
                    # Handle both 'profile_path' (from API) and 'profile_url' (from CSV import)
                    profile = member.get('profile_path') or member.get('profile_url')
                    full_cast.append({
                        'name': member.get('name', 'Unknown'),
                        'character': member.get('character', ''),
                        'profile_full_url': get_poster_url(profile) if profile else None,
                    })
                elif isinstance(member, str):
                    # Legacy: just actor name as string
                    full_cast.append({
                        'name': member,
                        'character': '',
                        'profile_full_url': None,
                    })
                    
    except Movie.DoesNotExist:
        # 2. Fallback: Fetch from API if movie not in DB
        credits_data = fetch_tmdb_data(f'movie/{movie_id}/credits')
        movie_data = fetch_tmdb_data(f'movie/{movie_id}')
        
        if not credits_data or not movie_data:
            return render(request, 'movies/detail.html', {'page_title': 'Movie or Cast data not found.'})
        
        movie_title = movie_data.get('title')
        movie_poster_url = get_poster_url(movie_data.get('poster_path'))
        
        for member in credits_data.get('cast', []):
            full_cast.append({
                'name': member.get('name', 'Unknown'),
                'character': member.get('character', ''),
                'profile_full_url': get_poster_url(member.get('profile_path')),
            })
        
    # Context to pass to the template
    context = {
        'movie_id': movie_id,
        'movie_title': movie_title,
        'movie_poster_url': movie_poster_url,
        'cast': full_cast,
        'page_title': f'Full Cast & Crew for {movie_title}',
    }

    # Render the template
    return render(request, 'movies/cast_detail.html', context)


## View Functions
def register_view(request):
    """
    Handles user registration using Django's built-in UserCreationForm.
    """
    if request.method == 'POST':
        form = UserCreationForm(request.POST)
        if form.is_valid():
            user = form.save()
            return redirect('login') 
    else:
        form = UserCreationForm()
        
    context = {'form': form, 'page_title': 'Register'}
    return render(request, 'registration/register.html', context)


def home_view(request):
    """
    Displays movies based on the selected filter (Now Trending, Top Rated, Now Playing, For You).
    """

    filter_type = request.GET.get('filter')
    movies = []
    endpoint = None
    page_title_suffix = 'Movies'

    if filter_type is None:
        endpoint = 'trending/movie/week'
        page_title_suffix = 'Now Trending'
        movies = []
        for page_num in range(1, 5): 
            data = fetch_tmdb_data(endpoint, params={'page': page_num})
            if data and 'results' in data:
                movies.extend(data['results'])
        for movie in movies:
            movie['poster_full_url'] = get_poster_url(movie.get('poster_path'))


    
    if filter_type == 'top_rated':
        endpoint = 'movie/top_rated'
        page_title_suffix = 'Top Rated'
        movies = []
        for page_num in range(1, 5):  
            data = fetch_tmdb_data(endpoint, params={'page': page_num})
            if data and 'results' in data:
                movies.extend(data['results'])
            
        
        for movie in movies:
            movie['poster_full_url'] = get_poster_url(movie.get('poster_path'))

    elif filter_type == 'now_playing':
        endpoint = 'movie/now_playing'
        page_title_suffix = 'Now Playing'
        movies = []
        for page_num in range(1, 5):
            data = fetch_tmdb_data(endpoint, params={'page': page_num})
            if data and 'results' in data:
                movies.extend(data['results'])
            
        
        for movie in movies:
            movie['poster_full_url'] = get_poster_url(movie.get('poster_path'))

    elif filter_type == 'popular':
        page_title_suffix = 'Most Popular'
        qs = Movie.objects.filter(tmdb_id__isnull=False).order_by('-popularity')[:60]
        movies = []
        for obj in qs:
            # OPTIMIZED: Use helper function which also SAVES to DB
            poster_path, vote_average = get_optimized_movie_display_data(obj)
            
            movies.append({
                'id': obj.tmdb_id,
                'title': obj.title,
                'poster_full_url': get_poster_url(poster_path),
                'vote_average': vote_average or 0.0,
            })

    elif filter_type == 'for_you':
        page_title_suffix = 'For You'
        # Recommendation message handled below

    # API fetch for trending (default)
    if endpoint and not movies:
        data = fetch_tmdb_data(endpoint)
        if data and 'results' in data:
            movies = data['results']
            for movie in movies:
                movie['poster_full_url'] = get_poster_url(movie.get('poster_path'))

    # "For You" messaging (unchanged)
    recommendation_message = None
    if filter_type == 'for_you':
        if not request.user.is_authenticated:
            recommendation_message = "Log in or register to start seeing personalized movie recommendations."
        else:
            if not Like.objects.filter(user=request.user).exists():
                recommendation_message = "Personalized picks coming soon! Like movies to help us get to know your style."
            else:
                recommended_ids = get_user_recommendations(request.user)
                if recommended_ids:
                    local_movies = Movie.objects.filter(tmdb_id__in=recommended_ids)
                    movie_map = {m.tmdb_id: m for m in local_movies}
                    movies = []
                    for mid in recommended_ids:
                        if mid in movie_map:
                            movie_obj = movie_map[mid]
                            # OPTIMIZED: Use helper function which also SAVES to DB
                            poster_path, vote_average = get_optimized_movie_display_data(movie_obj)
                            
                            if movie_obj.tmdb_id:
                                movies.append({
                                    'id': movie_obj.tmdb_id,
                                    'title': movie_obj.title,
                                    'poster_full_url': get_poster_url(poster_path),
                                    'vote_average': vote_average or 0.0,
                                })
                    if not movies:
                        recommendation_message = "We have recommendations but couldn't find their details in our database."
                else:
                    recommendation_message = "We couldn't generate recommendations for you yet. Try liking more movies!"

    context = {
        'movies': movies,
        'current_filter': filter_type,
        'recommendation_message': recommendation_message,
        'page_title': page_title_suffix,
    }

    return render(request, 'movies/home.html', context)


def movie_detail_view(request, movie_id):
    
    movie_id = int(movie_id) 
    data = None
    
    # --- 1. LOCAL CACHE LOOKUP (Cache Hit - Everything from DB) ---
    try:
        movie_obj = Movie.objects.get(tmdb_id=movie_id)
        
        # Load ALL cached data from the Movie object
        data = {
            'id': movie_obj.tmdb_id,
            'title': movie_obj.title,
            'overview': movie_obj.overview,
            'runtime': movie_obj.runtime,
            'poster_path': movie_obj.poster_path,
            'tagline': movie_obj.tagline,
            'release_date': movie_obj.release_date,  
            'genres': movie_obj.genres,        # JSON Field
            'popularity': movie_obj.popularity,
            'budget': movie_obj.budget,
            'revenue': movie_obj.revenue,
            'director': movie_obj.director, 
            'certification': movie_obj.certification, 
            'external_ids': movie_obj.external_ids, # JSON Field
            'keywords': movie_obj.keywords,         # JSON Field
            'cast': movie_obj.cast,                 # JSON Field (ALL cast members)
            'original_language': movie_obj.original_language,  # Now stored in DB
            'vote_average': movie_obj.vote_average,             # Now stored in DB
        }
        
        # Only fetch from API if original_language OR vote_average is missing
        if not data.get('original_language') or data.get('vote_average') is None:
            base_api_data = fetch_tmdb_data(f'movie/{movie_id}')
            if base_api_data:
                if not data.get('original_language'):
                    data['original_language'] = base_api_data.get('original_language', 'en')
                    # Update DB for next time
                    movie_obj.original_language = data['original_language']
                if data.get('vote_average') is None:
                    data['vote_average'] = base_api_data.get('vote_average')
                    # Update DB for next time
                    movie_obj.vote_average = data['vote_average']
                movie_obj.save() 

    except Movie.DoesNotExist:
        # --- 2. CACHE MISS: FETCH ALL DATA FROM API ---
        # We MUST include 'videos' and 'images' in the API call here to get the data for display
        api_params = {'append_to_response': 'keywords,release_dates,external_ids,credits,videos,images'} 
        data = fetch_tmdb_data(f'movie/{movie_id}', params=api_params) 

        if not data:
            return render(request, 'movies/detail.html', {'page_title': 'Movie Not Found'})
        
        # --- 3. EXTRACT AND CACHE DATA ---
        
        # Extract US Certification (Logic unchanged)
        certification = 'N/A'
        if 'release_dates' in data and 'results' in data['release_dates']:
            for release in data['release_dates']['results']:
                if release['iso_3166_1'] == 'US':
                    for date in release['release_dates']:
                        if date.get('certification'):
                            certification = date['certification']
                    if certification != 'N/A': break
        
        # Extract Director Name (Logic unchanged)
        director_name = _extract_director(data.get('credits'))

        # Prepare external IDs (Logic unchanged)
        external_ids = data.get('external_ids', {})
        
        # Prepare ALL cast for caching (not just top 10)
        all_cast = [{'name': c['name'], 'character': c.get('character', ''), 'profile_path': c.get('profile_path')} 
                    for c in data.get('credits', {}).get('cast', [])]
        
        # Extract keywords for caching
        cached_keywords = data.get('keywords', {}).get('keywords', data.get('keywords', {}))
        
        # Create and save the Movie object (NOW includes original_language and vote_average)
        Movie.objects.create(
            tmdb_id=data.get('id'),
            title=data.get('title'),
            overview=data.get('overview'),
            tagline=data.get('tagline'),
            release_date=datetime.strptime(data.get('release_date'), '%Y-%m-%d').date() if data.get('release_date') else None,
            runtime=data.get('runtime'),
            popularity=data.get('popularity'),
            budget=data.get('budget'),
            revenue=data.get('revenue'),
            poster_path=data.get('poster_path'),
            vote_average=data.get('vote_average'),  # NOW SAVED
            original_language=data.get('original_language'),  # NOW SAVED
            
            # Complex/JSON fields
            genres=data.get('genres'),
            keywords=cached_keywords,          
            cast=all_cast,  # ALL CAST MEMBERS NOW                 
            director=director_name,            
            external_ids=external_ids,
            certification=certification
            # Note: video_keys and poster_urls removed - always fetch fresh from API
        )
        
        # The 'data' dict already contains everything needed for context preparation

    # --- 4. PREPARE CONTEXT (Common to both Cache Hit and Miss) ---
    
    # --- NEW: MEDIA PROCESSING FOR TEMPLATE ---
    
    # ALWAYS fetch images and videos from API (they're lightweight and may be missing from old cache)
    media_api_data = fetch_tmdb_data(f'movie/{movie_id}', params={'append_to_response': 'images,videos'})
    
    # 1. Process Images from API
    media_backdrops = []
    if media_api_data and media_api_data.get('images'):
        backdrops = media_api_data['images'].get('backdrops', [])[:10]
        posters = media_api_data['images'].get('posters', [])[:10]
        
        for img in (backdrops + posters)[:20]:
            if img.get('file_path'):
                media_backdrops.append({
                    'file_path': img['file_path'], 
                    'full_url': get_poster_url(img['file_path'])
                })

    # 2. Process Videos from API
    media_videos = []
    trailer_key = None
    
    if media_api_data and media_api_data.get('videos'):
        videos = media_api_data['videos'].get('results', [])
        for video in videos:
            if video.get('site') == 'YouTube' and video.get('key'):
                media_videos.append({
                    'key': video['key'], 
                    'name': video.get('name', 'Video'),
                    'type': video.get('type', 'Clip')
                })
        
        # Set trailer key for header button
        if media_videos:
            trailer_key = media_videos[0]['key']

    # --- UPDATE POST-PROCESSING LOGIC ---
    data['poster_full_url'] = get_poster_url(data.get('poster_path'))
    
    # The cast is now loaded from the cached 'cast' JSON field on cache hit
    top_cast = []
    
    # Use the cached cast list if available (Cache Hit)
    if isinstance(data.get('cast'), list):
        top_cast = data['cast'][:6] 
        for actor in top_cast:
            # Handle both 'profile_path' (from API) and 'profile_url' (from CSV import)
            profile = actor.get('profile_path') or actor.get('profile_url')
            actor['profile_full_url'] = get_poster_url(profile) if profile else None
    # Use the API data if available (Cache Miss)
    elif 'credits' in data:
        top_cast = data['credits']['cast'][:6] 
        for actor in top_cast:
            actor['profile_full_url'] = get_poster_url(actor.get('profile_path'))

    # Director is loaded from the cached 'director' field
    director = data.get('director')
    
    certification = data.get('certification', 'N/A')
    
    # Extract External Links (Logic unchanged)
    links = {
        'imdb_id': data.get('external_ids', {}).get('imdb_id'),
        'facebook_id': data.get('external_ids', {}).get('facebook_id'),
        'twitter_id': data.get('external_ids', {}).get('twitter_id'),
        'instagram_id': data.get('external_ids', {}).get('instagram_id'),
    }
    
    # This relies on 'original_language' being present in 'data' from the base API call
    full_language_name = get_full_language_name(data.get('original_language', 'en'))
    
    # --- USER INTERACTION DATA (Existing Logic) ---
    reviews = Review.objects.filter(movie_id=movie_id).order_by('-created_at')
    # ... (rest of user interaction data and form instantiation remains the same)

    user_liked = False
    user_watched = False
    user_review = None 
    
    if request.user.is_authenticated:
        user_liked = Like.objects.filter(user=request.user, movie_id=movie_id).exists()
        user_watched = Watchlist.objects.filter(user=request.user, movie_id=movie_id).exists()
        
        try:
            user_review = Review.objects.get(user=request.user, movie_id=movie_id)
        except Review.DoesNotExist:
            pass

    if user_review:
        review_form = ReviewForm(initial={'movie_id': movie_id, 'rating': user_review.rating, 'text': user_review.text})
    else:
        review_form = ReviewForm(initial={'movie_id': movie_id})
        
    watchlist_form = WatchlistForm(initial={'movie_id': movie_id})
    like_form = LikeForm(initial={'movie_id': movie_id})

    # --- SIMILAR MOVIES via recommender (with graceful fallback) ---
    # OPTIMIZED: Use cached poster_path and vote_average, only fetch from API if null
    similar_movies = []
    try:
        similar_ids = get_similar_movies(movie_id, n=6)
        
        if similar_ids:
            sim_qs = Movie.objects.filter(tmdb_id__in=similar_ids)
            id_to_movie = {m.tmdb_id: m for m in sim_qs}

            for mid in similar_ids:
                obj = id_to_movie.get(mid)
                if not obj:
                    continue
                
                # OPTIMIZED: Use helper function which also SAVES to DB
                poster_path, vote_average = get_optimized_movie_display_data(obj)
                poster_full_url = get_poster_url(poster_path)

                similar_movies.append({
                    'tmdb_id': obj.tmdb_id,
                    'title': obj.title,
                    'poster_full_url': poster_full_url,
                    'vote_average': vote_average or 0.0,
                })
        print(f"DEBUG: Total similar movies: {len(similar_movies)}")
    except Exception as e:
        # Fail silently: if recommender or API has issues, we just hide similar section
        print(f"DEBUG: Exception in similar movies: {e}")
        similar_movies = []

    context = {
        'movie': data,
        'cast': top_cast,
        'director': director,
        'trailer_key': trailer_key,
        'certification': certification, 
        'links': links,
        'full_language_name': full_language_name, 
        'page_title': data.get('title', 'Movie Details'),
        
        # MEDIA CONTEXT ITEMS
        'media_backdrops': media_backdrops, # <--- NEW
        'media_videos': media_videos,       # <--- NEW
        
        # SIMILAR MOVIES CONTEXT ITEM
        'similar_movies': similar_movies, 
        
        # Interaction data (Existing Logic)
        'watchlist_form': watchlist_form,
        'review_form': review_form,
        'like_form': like_form,
        'user_liked': user_liked,
        'user_watched': user_watched,
        'user_review': user_review,
        'reviews': reviews, 
    }
    return render(request, 'movies/detail.html', context)


@login_required
def submit_interaction(request):
    """
    Handles POST requests for Liking/Disliking, Adding/Removing from Watchlist, 
    and Submitting/Updating/Deleting a Review.
    """
    if request.method == 'POST':
        movie_id = request.POST.get('movie_id')
        
        # --- 1. LIKE/DISLIKE TOGGLE LOGIC (NEW) ---
        if 'like_submit' in request.POST:
            try:
                # Attempt to find the existing Like entry
                existing_like = Like.objects.get(user=request.user, movie_id=movie_id)
                
                # If found, DELETE (Dislike/Remove)
                existing_like.delete()
            except Like.DoesNotExist:
                # If not found, CREATE (Like)
                form = LikeForm(request.POST) 
                if form.is_valid():
                    interaction = form.save(commit=False)
                    interaction.user = request.user 
                    try:
                        interaction.save() 
                    except IntegrityError:
                         pass
            
            return redirect('movie_detail', movie_id=movie_id)

        # --- 2. WATCHLIST ADD/REMOVE TOGGLE LOGIC (UPDATED for proper toggle) ---
        elif 'watchlist_submit' in request.POST:
            try:
                # Attempt to find the existing Watchlist entry
                existing_watchlist = Watchlist.objects.get(user=request.user, movie_id=movie_id)
                
                # If found, DELETE (Remove from list)
                existing_watchlist.delete()
            except Watchlist.DoesNotExist:
                # If not found, CREATE (Add to list)
                form = WatchlistForm(request.POST) 
                if form.is_valid():
                    interaction = form.save(commit=False)
                    interaction.user = request.user 
                    try:
                        interaction.save() 
                    except IntegrityError:
                         pass
                    
            return redirect('movie_detail', movie_id=movie_id)

        # --- 3. REVIEW SUBMIT/UPDATE LOGIC (UPDATED for update) ---
        elif 'review_submit' in request.POST:
            form = ReviewForm(request.POST)
            
            if form.is_valid():
                movie_id = form.cleaned_data.get('movie_id')
                rating = form.cleaned_data.get('rating')
                text = form.cleaned_data.get('text')
                
                try:
                    # Attempt to get existing review
                    existing_review = Review.objects.get(user=request.user, movie_id=movie_id)
                    
                    # If found, UPDATE it
                    existing_review.rating = rating
                    existing_review.text = text
                    existing_review.save()
                    
                except Review.DoesNotExist:
                    # If not found, CREATE a new one
                    interaction = form.save(commit=False)
                    interaction.user = request.user 
                    interaction.save()

            return redirect('movie_detail', movie_id=movie_id)
            
        # --- 4. REVIEW DELETE BUTTON LOGIC (User-initiated deletion) ---
        elif 'review_delete' in request.POST:
            review_id = request.POST.get('review_id') # Hidden field from template
            movie_id = request.POST.get('movie_id') 
            
            # Find and delete the review, ensuring it belongs to the current user
            Review.objects.filter(pk=review_id, user=request.user).delete()
            
            if movie_id:
                return redirect('movie_detail', movie_id=movie_id) 

    # Fallback redirect if no movie_id found
    movie_id = request.POST.get('movie_id') 
    if movie_id:
        return redirect('movie_detail', movie_id=movie_id) 

    return redirect('home')


def search_view(request):
    """Search using the local Movie DB with substring first, then fuzzy fallback pattern.
    OPTIMIZED: Uses cached poster_path and vote_average from DB, only fetches from API if null.
    """
    from rapidfuzz import process as fuzzy_process
    
    query = request.GET.get('query', '').strip()
    search_type = request.GET.get('type', 'title')  # 'title', 'actor', 'director', 'genre'
    selected_id = request.GET.get('selected', '')  # User-selected movie ID for title search
    search_results = []
    selected_movie = None
    recommendations = []

    if query:
        if search_type == 'title':
            # Title search: substring first, fuzzy fallback
            # Show 10 results for dropdown selection
            qs = Movie.objects.filter(title__icontains=query).order_by('-popularity')
            
            if qs.exists():
                # Substring matches found - limit to 10 for dropdown
                for obj in qs[:10]:
                    poster_path, vote_average = get_optimized_movie_display_data(obj)
                    search_results.append({
                        'id': obj.tmdb_id,
                        'title': obj.title,
                        'release_date': obj.release_date,
                        'poster_full_url': get_poster_url(poster_path),
                        'vote_average': vote_average or 0.0,
                    })
                    
            else:
                # Fuzzy fallback for title - limit to 5 results
                all_titles = list(Movie.objects.values_list('tmdb_id', 'title', 'popularity').order_by('-popularity'))
                if all_titles:
                    titles_list = [title for tmdb_id, title, pop in all_titles]
                    id_by_title = {title: tmdb_id for tmdb_id, title, pop in all_titles}
                    
                    # Use rapidfuzz extract to get best matches
                    matches = fuzzy_process.extract(query, titles_list, limit=5)
                    
                    if matches:
                        for title, score, idx in matches:
                            if score >= 60:  # Score threshold
                                tmdb_id = id_by_title[title]
                                obj = Movie.objects.get(tmdb_id=tmdb_id)
                                poster_path, vote_average = get_optimized_movie_display_data(obj)
                                search_results.append({
                                    'id': obj.tmdb_id,
                                    'title': obj.title,
                                    'release_date': obj.release_date,
                                    'poster_full_url': get_poster_url(poster_path),
                                    'vote_average': vote_average or 0.0,
                                })
            
            # Check if user selected a specific movie
            if selected_id:
                try:
                    selected_tmdb_id = int(selected_id)
                    obj = Movie.objects.get(tmdb_id=selected_tmdb_id)
                    poster_path, vote_average = get_optimized_movie_display_data(obj)
                    
                    selected_movie = {
                        'id': obj.tmdb_id,
                        'title': obj.title,
                        'release_date': obj.release_date,
                        'poster_full_url': get_poster_url(poster_path),
                        'vote_average': vote_average or 0.0,
                    }
                    
                    # Get 10 recommendations for this movie (sorted by NN distance)
                    rec_ids = get_similar_movies(selected_tmdb_id, n=10)
                    if rec_ids:
                        rec_qs = Movie.objects.filter(tmdb_id__in=rec_ids)
                        id_to_movie = {m.tmdb_id: m for m in rec_qs}
                        # Preserve order from rec_ids (already sorted by distance)
                        for mid in rec_ids:
                            obj = id_to_movie.get(mid)
                            if not obj:
                                continue
                            poster_path, vote_average = get_optimized_movie_display_data(obj)
                            recommendations.append({
                                'id': obj.tmdb_id,
                                'title': obj.title,
                                'release_date': obj.release_date,
                                'poster_full_url': get_poster_url(poster_path),
                                'vote_average': vote_average or 0.0,
                            })
                except (ValueError, Movie.DoesNotExist):
                    pass

        elif search_type == 'director':
            # Director search: substring first, fuzzy fallback - limit to 50 results
            qs = Movie.objects.filter(director__icontains=query).order_by('-popularity')[:50]
            
            if qs.exists():
                for obj in qs:
                    poster_path, vote_average = get_optimized_movie_display_data(obj)
                    search_results.append({
                        'id': obj.tmdb_id,
                        'title': obj.title,
                        'release_date': obj.release_date,
                        'poster_full_url': get_poster_url(poster_path),
                        'vote_average': vote_average or 0.0,
                    })
            else:
                # Fuzzy fallback for director
                all_directors_objs = Movie.objects.exclude(director__isnull=True).exclude(director__exact='').values_list('director', flat=True).distinct().order_by('-popularity')
                directors_list = list(set(all_directors_objs))  # unique directors
                
                if directors_list:
                    matches = fuzzy_process.extract(query, directors_list, limit=3)
                    
                    if matches:
                        matched_directors = [directors_list[idx] for _, score, idx in matches if score >= 60]
                        if matched_directors:
                            # Get movies by matched directors, limit to 50 total
                            qs = Movie.objects.filter(director__in=matched_directors).order_by('-popularity')[:50]
                            for obj in qs:
                                poster_path, vote_average = get_optimized_movie_display_data(obj)
                                search_results.append({
                                    'id': obj.tmdb_id,
                                    'title': obj.title,
                                    'release_date': obj.release_date,
                                    'poster_full_url': get_poster_url(poster_path),
                                    'vote_average': vote_average or 0.0,
                                })

        elif search_type == 'actor':
            # Actor search: substring first via cast JSON, fuzzy fallback
            all_movies = Movie.objects.filter(cast__isnull=False).order_by('-popularity')
            qs_matches = []
            
            for obj in all_movies:
                if obj.cast and isinstance(obj.cast, list):
                    for cast_item in obj.cast:
                        actor_name = cast_item.get('name', '') if isinstance(cast_item, dict) else str(cast_item)
                        if query.lower() in actor_name.lower():
                            qs_matches.append(obj)
                            break
            
            if qs_matches:
                # Substring matches found - limit to 50
                for obj in qs_matches[:50]:
                    poster_path, vote_average = get_optimized_movie_display_data(obj)
                    search_results.append({
                        'id': obj.tmdb_id,
                        'title': obj.title,
                        'release_date': obj.release_date,
                        'poster_full_url': get_poster_url(poster_path),
                        'vote_average': vote_average or 0.0,
                    })
            else:
                # Fuzzy fallback: extract all actor names and fuzzy match
                all_actors = set()
                for obj in all_movies:
                    if obj.cast and isinstance(obj.cast, list):
                        for cast_item in obj.cast:
                            actor_name = cast_item.get('name', '') if isinstance(cast_item, dict) else str(cast_item)
                            if actor_name:
                                all_actors.add(actor_name)
                
                if all_actors:
                    actors_list = list(all_actors)
                    matches = fuzzy_process.extract(query, actors_list, limit=3)
                    
                    if matches:
                        matched_actors = [actors_list[idx] for _, score, idx in matches if score >= 60]
                        if matched_actors:
                            # Get movies with matched actors, limit to 50
                            for obj in all_movies:
                                if len(search_results) >= 50:
                                    break
                                if obj.cast and isinstance(obj.cast, list):
                                    for cast_item in obj.cast:
                                        cast_name = cast_item.get('name', '') if isinstance(cast_item, dict) else str(cast_item)
                                        if cast_name and cast_name.lower() in [a.lower() for a in matched_actors]:
                                            poster_path, vote_average = get_optimized_movie_display_data(obj)
                                            search_results.append({
                                                'id': obj.tmdb_id,
                                                'title': obj.title,
                                                'release_date': obj.release_date,
                                                'poster_full_url': get_poster_url(poster_path),
                                                'vote_average': vote_average or 0.0,
                                            })
                                            break

        elif search_type == 'genre':
            # Genre search: substring first, fuzzy fallback
            all_movies = Movie.objects.filter(genres__isnull=False).order_by('-popularity')[:10000]
            qs_matches = []
            
            for obj in all_movies:
                if obj.genres and isinstance(obj.genres, list):
                    for genre_item in obj.genres:
                        genre_name = genre_item.get('name', '') if isinstance(genre_item, dict) else str(genre_item)
                        if query.lower() in genre_name.lower():
                            qs_matches.append(obj)
                            break
            
            if qs_matches:
                # Substring matches found - limit to 50
                for obj in qs_matches[:50]:
                    poster_path, vote_average = get_optimized_movie_display_data(obj)
                    search_results.append({
                        'id': obj.tmdb_id,
                        'title': obj.title,
                        'release_date': obj.release_date,
                        'poster_full_url': get_poster_url(poster_path),
                        'vote_average': vote_average or 0.0,
                    })
            else:
                # Fuzzy fallback: extract all genre names and fuzzy match
                all_genres = set()
                for obj in all_movies:
                    if obj.genres and isinstance(obj.genres, list):
                        for genre_item in obj.genres:
                            genre_name = genre_item.get('name', '') if isinstance(genre_item, dict) else str(genre_item)
                            if genre_name:
                                all_genres.add(genre_name)
                
                if all_genres:
                    genres_list = list(all_genres)
                    matches = fuzzy_process.extract(query, genres_list, limit=3)
                    
                    if matches:
                        matched_genres = [genres_list[idx] for _, score, idx in matches if score >= 60]
                        if matched_genres:
                            # Get movies with matched genres, limit to 50
                            for obj in all_movies:
                                if len(search_results) >= 50:
                                    break
                                if obj.genres and isinstance(obj.genres, list):
                                    for genre_item in obj.genres:
                                        g_name = genre_item.get('name', '') if isinstance(genre_item, dict) else str(genre_item)
                                        if g_name and g_name.lower() in [g.lower() for g in matched_genres]:
                                            poster_path, vote_average = get_optimized_movie_display_data(obj)
                                            search_results.append({
                                                'id': obj.tmdb_id,
                                                'title': obj.title,
                                                'release_date': obj.release_date,
                                                'poster_full_url': get_poster_url(poster_path),
                                                'vote_average': vote_average or 0.0,
                                            })
                                            break

    context = {
        'query': query,
        'search_type': search_type,
        'search_results': search_results,
        'selected_movie': selected_movie,
        'recommendations': recommendations,
        'page_title': f'Search Results for "{query}"' if query else 'Search',
    }

    return render(request, 'movies/search.html', context)


@login_required
def user_watchlist_view(request):
    """
    Retrieves the user's Watchlist items from the database.
    OPTIMIZED: Uses local DB for movie data, only fetches from API if missing.
    """
    # 1. Get all Watchlist entries for the logged-in user, ordered newest first
    watchlist_items = Watchlist.objects.filter(user=request.user).order_by('-created_at')
    
    # 2. Extract the movie IDs
    movie_ids = [item.movie_id for item in watchlist_items]
    
    # 3. Get movies from local DB first
    local_movies = Movie.objects.filter(tmdb_id__in=movie_ids)
    movie_map = {m.tmdb_id: m for m in local_movies}
    
    watchlist_movies = []
    
    for item in watchlist_items:
        movie_id = item.movie_id
        movie_obj = movie_map.get(movie_id)
        
        if movie_obj:
            # OPTIMIZED: Use local DB, only fetch from API if poster/vote missing
            poster_path, vote_average = get_optimized_movie_display_data(movie_obj)
            
            watchlist_movies.append({
                'id': movie_obj.tmdb_id,
                'title': movie_obj.title,
                'overview': movie_obj.overview,
                'release_date': movie_obj.release_date,
                'poster_full_url': get_poster_url(poster_path),
                'vote_average': vote_average or 0.0,
                'watchlist_db_id': item.id,
            })
        else:
            # Fallback: Movie not in local DB, fetch from API
            movie_data = fetch_tmdb_data(f'movie/{movie_id}')
            if movie_data:
                movie_data['poster_full_url'] = get_poster_url(movie_data.get('poster_path'))
                movie_data['watchlist_db_id'] = item.id
                watchlist_movies.append(movie_data)
            
    context = {
        'watchlist_movies': watchlist_movies,
        'page_title': f"{request.user.username}'s Watchlist",
    }
    
    return render(request, 'movies/watchlist.html', context)


@login_required
def remove_watchlist_item(request):
    """
    Handles POST request to remove a specific item from the user's watchlist 
    (used on the watchlist page itself).
    """
    if request.method == 'POST':
        # This is the database ID of the Watchlist entry
        watchlist_id = request.POST.get('watchlist_id')

        if watchlist_id:
            try:
                # IMPORTANT: Only delete the item if it belongs to the current user
                Watchlist.objects.filter(id=watchlist_id, user=request.user).delete()
            except Exception as e:
                print(f"Error removing watchlist item: {e}")
        
        # Redirect the user back to the watchlist page
        return redirect('user_watchlist') 
        
    return redirect('home')

    # In movies/views.py

