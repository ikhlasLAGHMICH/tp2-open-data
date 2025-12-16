
# 🏭 Pipeline Open Data (TP2)

Ce projet est un pipeline Data Engineering complet capable d'ingérer, d'enrichir, de nettoyer et de valider des données issues de l'Open Data.

Il croise les données produits (**OpenFoodFacts**) avec des données géographiques (**API Adresse Data.gouv**) pour produire des datasets enrichis de haute qualité.

## 🎯 Fonctionnalités

- **📥 Acquisition Multi-Sources** : Récupération résiliente (retry, rate-limit) depuis OpenFoodFacts.
- **🔄 Chargement Incrémental** : Détection intelligente des nouveaux enregistrements pour éviter les doublons et économiser la bande passante.
- **🌍 Enrichissement Géographique** : Géocodage automatique des lieux de vente/fabrication (API Adresse).
- **🔧 Transformation & Nettoyage** :
  - Détection et suppression des doublons.
  - Correction automatique des types de données (ex: texte dans colonnes numériques).
  - Normalisation du texte et remplissage des valeurs manquantes.
- **✅ Qualité des Données (Data Quality)** :
  - Scoring automatique (A, B, C...).
  - Génération de rapport en Markdown avec recommandations IA.
- **📊 Dashboard Interactif** : Visualisation des résultats avec Streamlit.
- **💾 Stockage Optimisé** : Sauvegarde au format Parquet (compression Snappy).


## 🚀 Installation

1. **Prérequis** : Avoir Python installé (et `uv` recommandé, sinon `pip`).
2. **Initialisation** :
   ```bash
   # Avec uv (recommandé)
   uv sync
   
   # Avec pip standard
   pip install -r requirements.txt
   ```
3. **Configuration (Optionnel)** :
   Créez un fichier `.env` à la racine pour l'IA (si utilisée) :
   ```env
   # Pour utiliser Google Gemini
   GEMINI_API_KEY=votre_cle_ici
   
   # OU pour utiliser Ollama (Local)
   # Assurez-vous que Ollama tourne sur le port 11434
   ```

## 🛠️ Utilisation

### 1. Lancer le Pipeline 
Le script principal permet de choisir la catégorie et le volume de données.

**Mode Standard (Téléchargement complet) :**
```bash
# Exemple : Récupérer 100 pizzas
uv run python -m pipeline.main --category "pizzas" --max-items 100
```

**Mode Incrémental (Uniquement les nouveaux produits) :**
Utilisez l'option `--incremental` (ou `-i`) pour ne pas retélécharger les produits déjà existants.
```bash
uv run python -m pipeline.main --category "chocolats" --max-items 100 --incremental
```

### 2. Visualiser les Données (Dashboard)
Lancez l'interface web pour explorer vos datasets et voir les graphiques.

```bash
uv run streamlit run dashboard.py
```

## 🧪 Tests et Qualité

Le projet inclut une suite de tests unitaires couvrant l'acquisition et la transformation.

```bash
# Lancer tous les tests
uv run pytest tests/ -v
```

### Exemple de Rapport de Qualité
Un rapport est généré automatiquement dans `data/reports/` après chaque exécution :
> **Note Globale : A**
> - Complétude : 100%
> - Doublons : 0%
> - Géocodage : 85% de succès

## Auteur
Ikhlas LAGHMICH
