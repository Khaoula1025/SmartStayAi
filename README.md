# SmartStay Intelligence

> Système IA de Revenue Management Hôtelier — Projet Fil Rouge Simplon Maghreb

**Hôtel pilote :** The Hickstead Hotel (52 chambres) — Bolney, West Sussex, Angleterre  
**Client :** UNO Hotels  
**Données :** Propriétaires réelles (PMS, Budget, Compset, TripAdvisor) — pas Kaggle

---

## Aperçu

SmartStay Intelligence est un système end-to-end de revenue management alimenté par l'IA. Il prédit le taux d'occupation, recommande des tarifs dynamiques, explique chaque décision via SHAP, et analyse le sentiment des avis clients avec NLP.

| Métrique | Valeur |
|----------|--------|
| Précision opérationnelle | **88%** |
| MAE opérationnel | **0.12 (12pp)** |
| Lignes d'entraînement | 638 (Avr 2024 – Déc 2025) |
| Dates prédites | 307 (2026) |
| Avis TripAdvisor analysés | 686 (2004–2026) |

---

## Stack Technique

| Couche | Technologies |
|--------|-------------|
| **Backend** | FastAPI · PostgreSQL (port 5433) · JWT Auth |
| **Modèles IA** | LightGBM · Random Forest · Prophet · VADER NLP |
| **Frontend** | Next.js 14 (App Router) · Tailwind CSS · Recharts |
| **Orchestration** | Apache Airflow · Docker · DAG quotidien 06:00 UTC |
| **MLOps** | MLflow · pytest · GitHub Actions CI/CD |
| **NLP & Insights** | VADER Sentiment · Google Gemini 1.5 Flash · Apify |

---

## Structure du Projet

```
smartstay-intelligence/
├── backend/
│   ├── app/
│   │   ├── api/v1/endpoints/     # Routes FastAPI
│   │   ├── models/               # SQLAlchemy models
│   │   ├── schemas/              # Pydantic schemas
│   │   ├── services/             # Business logic
│   │   ├── db/                   # Session & init
│   │   └── main.py
│   └── .env                      # Variables d'environnement
├── scripts/
│   ├── 01_load_occupancy.py      # Parse exports PMS
│   ├── 02_load_budget.py         # Parse fichier budget Excel
│   ├── 03_load_compset.py        # Parse données compétiteurs
│   ├── 04_clean_pickup.py        # Nettoyage fichiers BOB pickup
│   ├── 05_build_training_matrix.py
│   ├── 06_build_prediction_matrix.py
│   ├── 07_train_model.py         # Entraînement GBM + RF ensemble
│   ├── 07b_prophet.py            # Entraînement Prophet
│   ├── 07_train_model_mlflow.py  # Wrapper MLflow (ne modifie pas 07)
│   ├── 08_evaluate_model.py      # MAE, accuracy metrics
│   ├── 09_airflow_dag.py         # DAG Airflow
│   ├── daily_rescore.py          # Rescore journalier (importé par Airflow)
│   ├── 11_shap_explainability.py
│   ├── 12_prophet_seasonality.py
│   └── 13_sentiment_analysis.py
├── data/
│   ├── raw/                      # Données brutes (non versionnées)
│   ├── processed/                # Matrices d'entraînement/prédiction
│   ├── models/                   # gbm_model.joblib, rf_model.joblib
│   ├── prediction/               # predictions_2026.csv, model_metrics.json
│   ├── shap/                     # shap_values.csv, shap_summary.json
│   ├── prophet/                  # seasonality_*.json
│   └── sentiment/                # reviews_scored.csv, sentiment_summary.json
├── airflow/
│   ├── dags/
│   └── Dockerfile
├── tests/
│   ├── conftest.py
│   ├── test_api.py               # Tests endpoints FastAPI
│   ├── test_model.py             # Sanity checks modèles ML
│   └── test_pipeline.py         # Intégrité fichiers pipeline
├── .github/workflows/
│   └── ci.yml                    # GitHub Actions CI
└── mlruns/                       # MLflow tracking (local)
```

---

## Installation & Démarrage

### Prérequis

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) (gestionnaire de paquets)
- PostgreSQL 15+
- Docker & Docker Compose (pour Airflow)
- Node.js 18+ (pour le frontend)

### 1. Cloner le repo

```bash
git clone https://github.com/Khaoula1025/SmartStayAi.git
cd SmartStayAi
```

### 2. Installer les dépendances Python

```bash
uv sync
```

### 3. Configurer les variables d'environnement

Créer `backend/.env` :

```dotenv
DB_USER=postgres
DB_PASSWORD=0000
DB_HOST=localhost
DB_PORT=5433
DB_NAME=SmartStayAi
secret=votre_secret_jwt

GOOGLE_API_KEY=votre_cle_google
APIFY_TOKEN=votre_token_apify

AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=admin123
AIRFLOW_FERNET_KEY=<genere>
AIRFLOW_SECRET_KEY=<genere>
AIRFLOW_DB=airflow
```

### 4. Initialiser la base de données

```bash
# Créer la DB dans PostgreSQL
createdb -U postgres SmartStayAi

# Initialiser les tables
uv run python -c "
import sys; sys.path.insert(0, 'backend')
from app.db.session import engine, Base
Base.metadata.create_all(bind=engine)
print('Tables créées')
"
```

### 5. Lancer le pipeline de données

```bash
# Exécuter les scripts dans l'ordre
$env:PYTHONUTF8=1  # Windows uniquement
uv run python scripts/01_load_occupancy.py
uv run python scripts/02_load_budget.py
uv run python scripts/03_load_compset.py
uv run python scripts/04_clean_pickup.py
uv run python scripts/05_build_training_matrix.py
uv run python scripts/06_build_prediction_matrix.py
uv run python scripts/07_train_model.py
uv run python scripts/07b_prophet.py
uv run python scripts/08_evaluate_model.py
uv run python scripts/11_shap_explainability.py
uv run python scripts/12_prophet_seasonality.py
uv run python scripts/13_sentiment_analysis.py
```

### 6. Démarrer le backend FastAPI

```bash
cd backend
uv run uvicorn app.main:app --reload --port 8000
```

API disponible sur `http://localhost:8000`  
Documentation Swagger : `http://localhost:8000/docs`

### 7. Démarrer le frontend Next.js

```bash
# Dans le répertoire du projet Next.js
npm run dev
```

Frontend disponible sur `http://localhost:3000`

### 8. Démarrer Airflow (optionnel)

```bash
docker compose --env-file ./backend/.env up -d
```

Interface Airflow : `http://localhost:8080`

---

## API Endpoints

| Méthode | Endpoint | Description |
|---------|----------|-------------|
| `GET` | `/health` | Santé de l'API |
| `POST` | `/api/v1/auth/login` | Authentification JWT |
| `GET` | `/api/v1/dashboard/` | KPIs & tendances |
| `GET` | `/api/v1/predictions/` | Prédictions 2026 |
| `GET` | `/api/v1/analytics/model/metrics` | Métriques du modèle |
| `GET` | `/api/v1/analytics/model/history` | Historique des runs |
| `GET` | `/api/v1/analytics/seasonality/yearly` | Saisonnalité mensuelle |
| `GET` | `/api/v1/analytics/seasonality/weekly` | Saisonnalité hebdomadaire |
| `GET` | `/api/v1/explain/summary` | Importance globale SHAP |
| `GET` | `/api/v1/explain/{date}` | Explication SHAP par date |
| `GET` | `/api/v1/sentiment/summary` | Résumé sentiment |
| `GET` | `/api/v1/sentiment/insights` | Rapport Gemini AI |

---

## Modèles IA

### Ensemble Champion (GBM 60% + RF 40%)

```
Entraînement : 638 lignes (Avr 2024 – Déc 2025)
Features     : 13 (month, dow, cs_adr, b_adr, season, ...)
MAE opérationnel : 0.1196 (12pp)
Précision    : 88.0% (±10pp threshold)
```

**Top features (SHAP) :**
1. `month` — 32% (saisonnalité mensuelle dominante)
2. `cs_adr` — 17% (tarif concurrent)
3. `b_adr` — 15% (tarif budget)

**Insight clé :** Bank holidays, événements culturels et locaux → impact SHAP quasi nul.

### Prophet (Saisonnalité)

- Juillet : **+27.3pp** (pic estival)
- Janvier : **-49.4pp** (creux hivernal)
- Mardi : **+14.6pp** meilleur jour (segment corporate)
- Dimanche : **-35.9pp** pire jour
- Ramp-up hôtel : 17.8% → 70.6% en 20 mois

### VADER NLP + Gemini AI (Sentiment)

- 686 avis TripAdvisor (2004–2026)
- Score compound moyen : **79.8%**
- 92.1% positifs | 7.6% négatifs
- ⚠️ Déclin fin 2025 : Juil 25 = 7.7%, Déc 25 = 20.9%

---

## MLflow

```bash
# Lancer l'interface MLflow
mlflow ui

# Ouvrir http://localhost:5000
# Expérience : smartstay_training
```

Pour tracker un run d'entraînement :

```bash
$env:PYTHONUTF8=1
uv run python scripts/07_train_model_mlflow.py
```

---

## Tests

```bash
$env:PYTHONUTF8=1  # Windows uniquement
uv run pytest tests/ -v
```

| Fichier | Tests | Couverture |
|---------|-------|------------|
| `test_api.py` | Endpoints + auth JWT | Routes FastAPI |
| `test_model.py` | Sanity checks ML | Modèles joblib |
| `test_pipeline.py` | Intégrité fichiers | Outputs pipeline |

**Coverage actuelle : 58%**

---

## CI/CD

GitHub Actions déclenché sur chaque push :

1. Spin-up PostgreSQL 15 (container)
2. Écriture du `.env` CI
3. Création tables + seed utilisateur test
4. `pytest tests/test_api.py -v`

Voir `.github/workflows/ci.yml`

---

## Pages Frontend

| Route | Description |
|-------|-------------|
| `/` | Landing page |
| `/login` | Authentification |
| `/dashboard` | KPIs, tendance demande, BOB quality |
| `/forecast` | Prévisions 60 jours + bande IC + tarifs |
| `/rates` | Rate Decisions + tooltip SHAP "Pourquoi ?" |
| `/analytics` | Accuracy + Saisonnalité (Prophet) |
| `/pipeline` | Architecture + statut pipeline |
| `/sentiment` | KPIs NLP + rapport Gemini AI + avis |

**Design system :** Navy `#1C2B4A` · Gold `#C9A84C` · Surface `#F7F8FC`

---

## Variables d'Environnement

| Variable | Description |
|----------|-------------|
| `DB_USER` | Utilisateur PostgreSQL |
| `DB_PASSWORD` | Mot de passe PostgreSQL |
| `DB_HOST` | Hôte PostgreSQL |
| `DB_PORT` | Port PostgreSQL (5433 local, 5432 CI) |
| `DB_NAME` | Nom de la base (`SmartStayAi`) |
| `secret` | Clé secrète JWT |
| `GOOGLE_API_KEY` | Clé API Google (Gemini) |
| `APIFY_TOKEN` | Token Apify (scraping TripAdvisor) |

---

## Licence

Projet académique — Simplon Maghreb · Mars 2026  
Données propriétaires UNO Hotels — usage interne uniquement
