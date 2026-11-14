import os

SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "agri-dwh-demo-secret-change-me")
SQLALCHEMY_DATABASE_URI = "postgresql+psycopg2://superset:superset@postgres/superset"

WTF_CSRF_ENABLED = False
TALISMAN_ENABLED = False
PUBLIC_ROLE_LIKE = "Gamma"
ENABLE_PROXY_FIX = True
DASHBOARD_AUTO_REFRESH_INTERVAL = 60
SQLLAB_TIMEOUT = 120
SUPERSET_WEBSERVER_TIMEOUT = 120
