# movies/utils.py

import requests
from django.conf import settings 
import os

# Base URLs for the TMDb API and images
BASE_URL = "https://api.themoviedb.org/3"
IMAGE_BASE_URL = "https://image.tmdb.org/t/p/" 

def fetch_tmdb_data(endpoint, params=None):
    """
    Handles making a GET request to a specific TMDb endpoint.
    """
    url = f"{BASE_URL}/{endpoint}"
    
    default_params = {
        'api_key': settings.TMDB_API_KEY, 
        'language': 'en-US'
    }
    if params:
        default_params.update(params)

    try:
        response = requests.get(url, params=default_params)
        response.raise_for_status() # Check for errors (4XX or 5XX status codes)
        
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from TMDb: {e}")
        return None

def get_poster_url(path, size='w500'):
    """
    Constructs the full, displayable URL for an image.
    """
    if path:
        return f"{IMAGE_BASE_URL}{size}{path}"
    return None