from django.urls import path, include

from django.contrib import admin

admin.autodiscover()

import hello.views

#URLS.PY IN This project
# To add a new path, first import the app:
# import blog
#
# Then add the new path:
# path('blog/', blog.urls, name="blog")
#
# Learn more here: https://docs.djangoproject.com/en/2.1/topics/http/urls/

urlpatterns = [
    # path("", hello.views.index, name="index"),
    # path("", hello.views.index, name="index"),
    # path("db/", hello.views.db, name="db"),
    # path("comtrade/", hello.views.comtrade, name="comtrade"),
    path("admin/", admin.site.urls),
]
