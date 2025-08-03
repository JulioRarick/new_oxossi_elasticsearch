#!/usr/bin/env python3
import sys
import os
sys.path.insert(0, os.path.abspath('.'))

def test_imports():
    try:
        from src.elasticsearch_manager import ElasticsearchManager
        print("✓ ElasticsearchManager")
        
        from src.config_manager import ConfigManager
        print("✓ ConfigManager")
        
        from src.pdf_processor import PDFProcessor  
        print("✓ PDFProcessor")
        
        from api.services.search_service import SearchService
        print("✓ SearchService")
        
        print("\n🎉 Todas as importações funcionam!")
        return True
    except Exception as e:
        print(f"❌ Erro: {e}")
        return False

if __name__ == "__main__":
    test_imports()
