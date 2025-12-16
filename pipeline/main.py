#!/usr/bin/env python3
"""Script principal du pipeline."""
import argparse
from datetime import datetime
from pathlib import Path
import pandas as pd
import sys

# Import des modules internes
from .fetchers.openfoodfacts import OpenFoodFactsFetcher
from .enricher import DataEnricher
from .transformer import DataTransformer
from .quality import QualityAnalyzer
from .storage import save_raw_json, save_parquet
from .config import MAX_ITEMS, PROCESSED_DIR
from .logger import setup_logger

# Initialisation du logger
logger = setup_logger("Orchestrator")

def get_existing_ids(category: str) -> set:
    """
    Récupère les IDs (codes barres) des produits déjà stockés en Parquet.
    Permet d'éviter de traiter deux fois le même produit.
    """
    # On cherche tous les fichiers parquet correspondant à la catégorie
    files = list(PROCESSED_DIR.glob(f"{category}_*.parquet"))
    
    existing_ids = set()
    logger.info(f"🔄 Vérification de l'historique dans {len(files)} fichiers...")

    for f in files:
        try:
            # On lit uniquement la colonne 'code' pour aller très vite
            df = pd.read_parquet(f, columns=['code'])
            # On ajoute les codes à notre set (ensemble unique)
            existing_ids.update(df['code'].astype(str).tolist())
        except Exception as e:
            logger.warning(f"Impossible de lire {f.name}: {e}")
            continue
            
    if existing_ids:
        logger.info(f"ℹ️ {len(existing_ids)} produits déjà existants en base.")
    return existing_ids

def run_pipeline(
    category: str,
    max_items: int = MAX_ITEMS,
    skip_enrichment: bool = False,
    incremental: bool = False,  # <--- NOUVEAU PARAMÈTRE
    verbose: bool = True
) -> dict:
    """
    Exécute le pipeline complet.
    """
    stats = {"start_time": datetime.now()}
    
    logger.info("=" * 60)
    logger.info(f"🚀 PIPELINE OPEN DATA - Catégorie : {category.upper()}")
    if incremental:
        logger.info("🔄 Mode Incrémental : ACTIVÉ")
    logger.info("=" * 60)
    
    # === ÉTAPE 0 : Chargement de l'historique (Si incrémental) ===
    existing_ids = set()
    if incremental:
        existing_ids = get_existing_ids(category)

    # === ÉTAPE 1 : Acquisition ===
    logger.info("📥 ÉTAPE 1 : Acquisition des données (OpenFoodFacts)")
    fetcher = OpenFoodFactsFetcher()
    
    # On récupère les données brutes
    # Note : Fetcher ne filtre pas en amont (l'API OFF ne le permet pas facilement par ID)
    raw_products = list(fetcher.fetch_all(category, max_items, verbose))
    
    if not raw_products:
        logger.error("❌ Aucun produit récupéré.")
        return {"error": "No data fetched"}
    
    # --- FILTRAGE INCRÉMENTAL ---
    products = raw_products
    if incremental and existing_ids:
        # On ne garde que les produits dont le code n'est PAS dans l'historique
        products = [p for p in raw_products if str(p.get('code')) not in existing_ids]
        
        skipped_count = len(raw_products) - len(products)
        if skipped_count > 0:
            logger.info(f"⏩ {skipped_count} produits déjà connus ignorés.")
        
        if not products:
            logger.info("✅ Aucun NOUVEAU produit à traiter. Pipeline terminé.")
            return {"status": "skipped_no_new_data"}
    # -----------------------------
    
    # Sauvegarde de sécurité
    raw_path = save_raw_json(products, f"{category}_raw")
    logger.info(f"💾 Sauvegarde brute ({len(products)} items) : {raw_path.name}")
    stats["fetcher"] = fetcher.get_stats()
    
    # === ÉTAPE 2 : Enrichissement ===
    if not skip_enrichment:
        logger.info("🌍 ÉTAPE 2 : Enrichissement (Géocodage API Adresse)")
        enricher = DataEnricher()
        
        addresses = enricher.extract_addresses(products, "stores")
        
        if addresses:
            limit_geo = 100
            logger.info(f"   Géocodage des {min(len(addresses), limit_geo)} premières adresses uniques...")
            geo_cache = enricher.build_geocoding_cache(addresses[:limit_geo])
            
            products = enricher.enrich_products(products, geo_cache, "stores")
            stats["enricher"] = enricher.get_stats()
        else:
            logger.warning("⚠️ Pas d'adresses trouvées dans le champ 'stores'.")
    else:
        logger.info("⏭️ ÉTAPE 2 : Enrichissement ignoré")
    
    # === ÉTAPE 3 : Transformation ===
    logger.info("🔧 ÉTAPE 3 : Transformation et nettoyage")
    df = pd.DataFrame(products)
    
    transformer = DataTransformer(df)
    df_clean = (
        transformer
        .remove_duplicates(subset=['code'])
        .handle_missing_values(numeric_strategy='median', text_strategy='unknown')
        .normalize_text_columns(['brands', 'categories', 'stores'])
        .add_derived_columns()
        .get_result()
    )
    
    logger.info(f"   Transformations appliquées : {len(transformer.transformations_applied)}")
    stats["transformer"] = {"transformations": transformer.transformations_applied}
    
    # === ÉTAPE 4 : Qualité ===
    logger.info("📊 ÉTAPE 4 : Analyse de qualité")
    analyzer = QualityAnalyzer(df_clean)
    metrics = analyzer.analyze()
    
    logger.info(f"   📝 Note globale : {metrics.quality_grade}")
    logger.info(f"   ✅ Complétude : {metrics.completeness_score * 100:.1f}%")
    
    report_path = analyzer.generate_report(f"{category}_quality")
    stats["quality"] = metrics.model_dump()
    
    # === ÉTAPE 5 : Stockage ===
    logger.info("💾 ÉTAPE 5 : Stockage final (Parquet)")
    output_path = save_parquet(df_clean, category)
    stats["output_path"] = str(output_path)
    
    # === FIN ===
    stats["end_time"] = datetime.now()
    stats["duration_seconds"] = (stats["end_time"] - stats["start_time"]).seconds
    
    logger.info("=" * 60)
    logger.info("✅ PIPELINE TERMINÉ AVEC SUCCÈS")
    logger.info("=" * 60)
    logger.info(f"⏱️  Durée : {stats['duration_seconds']} secondes")
    logger.info(f"📦 Nouveaux produits : {len(df_clean)}")
    logger.info(f"📂 Fichier final : {output_path}")
    
    return stats

def main():
    """Point d'entrée CLI."""
    parser = argparse.ArgumentParser(description="Pipeline Open Data TP2")
    parser.add_argument("--category", "-c", default="chocolats", help="Catégorie")
    parser.add_argument("--max-items", "-m", type=int, default=50, help="Nombre max")
    parser.add_argument("--skip-enrichment", "-s", action="store_true", help="Sauter géocodage")
    parser.add_argument("--incremental", "-i", action="store_true", help="Ne traiter que les nouveaux produits")
    parser.add_argument("--verbose", "-v", action="store_true", default=True)
    
    args = parser.parse_args()
    
    try:
        run_pipeline(
            category=args.category,
            max_items=args.max_items,
            skip_enrichment=args.skip_enrichment,
            incremental=args.incremental, # <--- Passage de l'argument
            verbose=args.verbose
        )
    except KeyboardInterrupt:
        logger.warning("🛑 Pipeline arrêté par l'utilisateur.")
        sys.exit(0)
    except Exception as e:
        logger.critical(f"❌ Erreur critique : {e}", exc_info=True)
        raise e

if __name__ == "__main__":
    main()