import pytest
from pipeline.fetchers.openfoodfacts import OpenFoodFactsFetcher
from pipeline.fetchers.adresse import AdresseFetcher


class TestOpenFoodFactsFetcher:
    """Tests pour OpenFoodFactsFetcher."""
    
    def test_fetch_batch_returns_list(self):
        """Test que fetch_batch retourne une liste."""
        fetcher = OpenFoodFactsFetcher()
        result = fetcher.fetch_batch("chocolats", page=1, page_size=5)
        
        assert isinstance(result, list)
        assert len(result) <= 5
    
    def test_fetch_batch_has_required_fields(self):
        """Test que les produits ont les champs requis."""
        fetcher = OpenFoodFactsFetcher()
        products = fetcher.fetch_batch("chocolats", page=1, page_size=3)
        
        if products:
            product = products[0]
            assert "code" in product


class TestAdresseFetcher:
    """Tests pour AdresseFetcher."""
    
    def test_geocode_single_valid_address(self):
        """Test le géocodage d'une adresse valide."""
        fetcher = AdresseFetcher()
        result = fetcher.geocode_single("20 avenue de ségur paris")
        
        assert result.original_address == "20 avenue de ségur paris"
        assert result.score > 0.5
        assert result.latitude is not None
        assert result.longitude is not None
    
    def test_geocode_single_invalid_address(self):
        """Test le géocodage d'une adresse invalide."""
        fetcher = AdresseFetcher()
        result = fetcher.geocode_single("xyzabc123456")
        
        assert result.score < 0.5 or result.latitude is None
    
    def test_geocode_empty_address(self):
        """Test le géocodage d'une adresse vide."""
        fetcher = AdresseFetcher()
        result = fetcher.geocode_single("")
        
        assert result.score == 0