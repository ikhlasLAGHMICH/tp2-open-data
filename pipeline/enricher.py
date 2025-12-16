"""Module d'enrichissement des données."""
import pandas as pd
from typing import Optional, List, Dict, Any
from tqdm import tqdm

from .fetchers.adresse import AdresseFetcher
from .models import GeocodingResult

class DataEnricher:
    """Enrichit les données en croisant plusieurs sources."""
    
    def __init__(self):
        self.geocoder = AdresseFetcher()
        self.enrichment_stats = {
            "total_processed": 0,
            "successfully_enriched": 0,
            "failed_enrichment": 0,
        }
    
    def extract_addresses(self, products: List[Dict[str, Any]], address_field: str = "stores") -> List[str]:
        """
        Extrait les adresses/magasins uniques des produits.
        Ex: "Carrefour, Auchan" -> ["Carrefour", "Auchan"]
        """
        addresses = set()
        
        for product in products:
            addr_str = product.get(address_field, "")
            # Vérification que ce n'est pas None et que c'est une chaine
            if addr_str and isinstance(addr_str, str) and addr_str.strip():
                # Les magasins sont souvent séparés par des virgules
                for part in addr_str.split(","):
                    cleaned = part.strip()
                    # On garde seulement si ça a l'air d'un vrai nom (> 2 lettres)
                    if len(cleaned) > 2:  
                        addresses.add(cleaned)
        
        return list(addresses)
    
    def build_geocoding_cache(self, addresses: List[str]) -> Dict[str, GeocodingResult]:
        """
        Construit un cache de géocodage pour éviter les requêtes en double.
        Retourne un dictionnaire : { "Nom Magasin": GeocodingResult(...) }
        """
        cache = {}
        
        print(f"🌍 Géocodage de {len(addresses)} adresses uniques...")
        
        # On utilise le fetcher d'adresse créé à l'étape précédente
        # fetch_all gère déjà la barre de progression (tqdm)
        for result in self.geocoder.fetch_all(addresses):
            cache[result.original_address] = result
        
        # Petit calcul de stats
        success_count = sum(1 for r in cache.values() if r.is_valid)
        total = len(cache) if cache else 1
        print(f"✅ Taux de succès géocodage: {success_count / total * 100:.1f}%")
        
        return cache
    
    def enrich_products(
        self, 
        products: List[Dict[str, Any]], 
        geocoding_cache: Dict[str, GeocodingResult],
        address_field: str = "stores"
    ) -> List[Dict[str, Any]]:
        """
        Enrichit les produits avec les données de géocodage du cache.
        """
        enriched = []
        
        for product in tqdm(products, desc="Enrichissement"):
            self.enrichment_stats["total_processed"] += 1
            
            # On copie le produit pour ne pas modifier l'original
            enriched_product = product.copy()
            
            # On récupère le champ magasin
            addr_str = product.get(address_field, "")
            found_match = False

            if addr_str and isinstance(addr_str, str):
                # On essaie de trouver le premier magasin de la liste dans notre cache
                parts = [p.strip() for p in addr_str.split(",")]
                
                for part in parts:
                    if part in geocoding_cache:
                        geo = geocoding_cache[part]
                        
                        # Si on a trouvé une coordonnée, on l'ajoute
                        enriched_product["store_address"] = geo.label
                        enriched_product["latitude"] = geo.latitude
                        enriched_product["longitude"] = geo.longitude
                        enriched_product["city"] = geo.city
                        enriched_product["postal_code"] = geo.postal_code
                        enriched_product["geocoding_score"] = geo.score
                        
                        if geo.is_valid:
                            self.enrichment_stats["successfully_enriched"] += 1
                            found_match = True
                        break # On s'arrête au premier magasin trouvé
            
            if not found_match:
                self.enrichment_stats["failed_enrichment"] += 1
                
            enriched.append(enriched_product)
        
        return enriched
    
    def get_stats(self) -> Dict[str, Any]:
        """Retourne les statistiques d'enrichissement."""
        stats = self.enrichment_stats.copy()
        stats["geocoder_stats"] = self.geocoder.get_stats()
        
        total = stats["total_processed"]
        if total > 0:
            stats["success_rate"] = stats["successfully_enriched"] / total * 100
        else:
            stats["success_rate"] = 0
        
        return stats