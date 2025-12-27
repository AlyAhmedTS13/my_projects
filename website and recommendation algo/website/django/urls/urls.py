# In movies/urls.py

from django.urls import path
from . import views

urlpatterns = [
    path('', views.home_view, name='home'),
    path('register/', views.register_view, name='register'),
    path('movie/<int:movie_id>/', views.movie_detail_view, name='movie_detail'),
    path('search/', views.search_view, name='search'),
    
    # --- USER INTERACTION PATHS ---
    path('submit_interaction/', views.submit_interaction, name='submit_interaction'), 
    
    # --- USER LIST PATHS (NEW) ---
    path('my-watchlist/', views.user_watchlist_view, name='user_watchlist'),
    path('remove-watchlist-item/', views.remove_watchlist_item, name='remove_watchlist_item'),
   path('movie/<int:movie_id>/cast/', views.movie_cast_view, name='movie_cast_detail'),
]
