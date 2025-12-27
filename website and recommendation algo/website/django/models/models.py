# In movies/models.py

from django.db import models
from django.contrib.auth.models import User
from django.core.validators import MinValueValidator, MaxValueValidator

# Base class to handle common fields for all interactions
class BaseInteraction(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    movie_id = models.IntegerField(help_text="The TMDB ID of the movie.")
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        abstract = True
        # Ensures a user can only have one of this interaction type per movie
        unique_together = (('user', 'movie_id'),) 

# 1. Watchlist Model
class Watchlist(BaseInteraction):
    # No extra fields needed
    class Meta(BaseInteraction.Meta):
        verbose_name_plural = "Watchlists"
        
    def __str__(self):
        return f"{self.user.username}'s Watchlist Item: {self.movie_id}"

# 2. Like Model (NEW)
class Like(BaseInteraction):
    # No extra fields needed, it's just a record of the interaction
    class Meta(BaseInteraction.Meta):
        verbose_name_plural = "Likes"
        
    def __str__(self):
        return f"{self.user.username} liked Movie {self.movie_id}"

# 3. Review Model
class Review(BaseInteraction):
    rating = models.IntegerField(
        validators=[MinValueValidator(1), MaxValueValidator(10)],
        help_text="Rating from 1 to 10."
    )
    text = models.TextField(blank=True, null=True, help_text="Your detailed review.")

    class Meta(BaseInteraction.Meta):
        ordering = ['-created_at']

    def __str__(self):
        return f"{self.user.username}'s Review for Movie {self.movie_id} ({self.rating}/10)"
    
    # In your app's models.py


# In your app's models.py

from django.db import models

class Movie(models.Model):
    # --- CORE MOVIE DATA ---
    # TMDb ID serves as the primary key for direct lookups
    tmdb_id = models.IntegerField(unique=True, primary_key=True) 
    title = models.CharField(max_length=255)
    overview = models.TextField()
    tagline = models.CharField(max_length=255, null=True, blank=True)
    
    # --- NUMERIC DATA ---
    release_date = models.DateField(null=True, blank=True)
    runtime = models.IntegerField(null=True, blank=True)
    popularity = models.FloatField(null=True, blank=True)
    budget = models.BigIntegerField(null=True, blank=True)
    revenue = models.BigIntegerField(null=True, blank=True)
    vote_average = models.FloatField(null=True, blank=True)
    
    # --- IMAGE/VIDEO DATA ---
    poster_path = models.CharField(max_length=255, null=True, blank=True)
    # Note: video_keys and poster_urls removed - always fetch fresh from API
    
    # --- LANGUAGE DATA ---
    original_language = models.CharField(max_length=10, null=True, blank=True, help_text="ISO 639-1 language code.")

    # --- LIST/COMPLEX DATA (Using JSONField) ---
    genres = models.JSONField(default=list, help_text="List of genre IDs/names.")
    keywords = models.JSONField(default=list, help_text="List of keyword IDs/names.")
    
    # To store ALL cast members with name and profile_path
    cast = models.JSONField(default=list, help_text="List of ALL cast members with name and profile_path.")
    director = models.CharField(max_length=255, null=True, blank=True)
    
    # --- EXTERNAL/MISC DATA ---
    external_ids = models.JSONField(default=dict, help_text="External IDs (IMDb, etc.).")
    certification = models.CharField(max_length=10, null=True, blank=True)

    def __str__(self):
        return self.title

    class Meta:
        # Ensures your custom models are ordered logically in admin/queries
        ordering = ['-release_date', 'title']

        
