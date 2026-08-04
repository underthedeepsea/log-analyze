from django.urls import include, path


urlpatterns = [
    path("logrisk/", include("logrisk_django.urls")),
]
