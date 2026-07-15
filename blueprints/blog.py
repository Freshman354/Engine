"""
blueprints/blog.py
-------------------
Lumvi Knowledge Hub — standalone blog blueprint.

Follows the same shape as the other no-dependency blueprints in this
package (account_bp, email_domains_bp, client_settings_bp): no init_*()
call needed, just import and register.

  from blueprints.blog import blog_bp
  app.register_blueprint(blog_bp)

Templates live in templates/blog/, assets in static/blog/ — both are
picked up automatically since blueprints fall back to the main app's
template_folder="templates" and static_folder="static" by default.
"""

from flask import Blueprint, render_template, abort

blog_bp = Blueprint(
    "blog",
    __name__,
    url_prefix="/blog",
    # No custom template_folder/static_folder: Flask blueprints fall back
    # to the main app's templates/ and static/ directories, which is why
    # templates/blog/*.html and static/blog/* resolve correctly without
    # any extra config on the main app.
)

# Slugs this blueprint recognizes. In this starter version the article
# metadata lives in static/blog/data.js (client-side) so the guide list
# below only needs to know which slugs are valid — swap this for a real
# lookup (DB, CMS, etc.) when the content source moves server-side.
CORNERSTONE_SLUG = "complete-guide-white-label-ai-agents"


@blog_bp.route("/")
def index():
    """Blog hub — /blog"""
    return render_template("blog/index.html")


@blog_bp.route("/<slug>")
def article(slug):
    """Individual article — /blog/<slug>

    Article content and metadata are rendered client-side from
    static/blog/data.js and static/blog/cornerstone-content.js — this
    route's job is just to serve the shell and pass the slug through.
    If you move article content server-side later, this is the place
    to fetch it and pass it into the template instead.
    """
    if not slug or not slug.replace("-", "").isalnum():
        abort(404)
    return render_template("blog/article.html", slug=slug)
