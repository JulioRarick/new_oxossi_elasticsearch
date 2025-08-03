#!/usr/bin/env python3
"""
Teste simples do sistema - com correção de imports
"""

import sys
import os
from pathlib import Path

# Adicionar diretório atual ao path PRIMEIRO
sys.path.insert(0, os.path.abspath('.'))

def test_elasticsearch():
    """Testa Elasticsearch"""
    print("🔍 Testando Elasticsearch...")
    
    try:
        import requests
        response = requests.get("http://localhost:9200", timeout=5)
        if response.status_code == 200:
            info = response.json()
            print(f"✅ Elasticsearch OK - Versão: {info.get('version', {}).get('number', 'unknown')}")
            return True
        else:
            print("❌ Elasticsearch não responde corretamente")
            return False
    except Exception as e:
        print("❌ Elasticsearch não está rodando")
        print("   Execute: ./scripts/start_elasticsearch.sh")
        return False

def test_imports():
    """Testa importações"""
    print("📦 Testando importações...")
    
    success = True
    
    try:
        from src.elasticsearch_manager import ElasticsearchManager
        print("✓ ElasticsearchManager")
    except Exception as e:
        print(f"❌ ElasticsearchManager: {e}")
        success = False
    
    try:
        from src.config_manager import ConfigManager
        print("✓ ConfigManager")
    except Exception as e:
        print(f"❌ ConfigManager: {e}")
        success = False
    
    try:
        from src.pdf_processor import PDFProcessor
        print("✓ PDFProcessor")
    except Exception as e:
        print(f"❌ PDFProcessor: {e}")
        success = False
    
    try:
        from api.services.search_service import SearchService
        print("✓ SearchService")
    except Exception as e:
        print(f"❌ SearchService: {e}")
        success = False
    
    return success

def test_elasticsearch_connection():
    """Testa conexão com Elasticsearch via código"""
    print("🔗 Testando conexão Elasticsearch via código...")
    
    try:
        from src.elasticsearch_manager import ElasticsearchManager
        
        es_manager = ElasticsearchManager()
        health = es_manager.health_check()
        
        if health.get("connection") == "healthy":
            print("✅ Conexão Elasticsearch OK via código")
            return True
        else:
            print(f"❌ Conexão falhou: {health.get('error', 'Unknown error')}")
            return False
    except Exception as e:
        print(f"❌ Erro na conexão: {e}")
        return False

def test_config_files():
    """Verifica arquivos de configuração"""
    print("📁 Verificando arquivos de configuração...")
    
    required_files = [
        "config/date_config.json",
        "config/names.json", 
        "config/places.txt",
        "config/themes.json"
    ]
    
    missing = []
    for file_path in required_files:
        if not Path(file_path).exists():
            missing.append(file_path)
        else:
            print(f"✓ {file_path}")
    
    if missing:
        print("❌ Arquivos de configuração faltando:")
        for file_path in missing:
            print(f"   - {file_path}")
        return False
    
    return True

def create_index():
    """Cria índice se não existir"""
    print("🗂️  Verificando/criando índice...")
    
    try:
        from src.elasticsearch_manager import ElasticsearchManager
        
        es_manager = ElasticsearchManager()
        
        if es_manager.create_index(force_recreate=False):
            print("✅ Índice criado/verificado")
            return True
        else:
            print("✅ Índice já existe")
            return True
    except Exception as e:
        print(f"❌ Erro ao criar índice: {e}")
        return False

def main():
    """Função principal"""
    print("=" * 60)
    print("🧪 TESTE SIMPLES DO SISTEMA OXOSSI")
    print("=" * 60)
    
    # Lista de testes
    tests = [
        ("Importações", test_imports),
        ("Arquivos de Configuração", test_config_files),
        ("Elasticsearch Externo", test_elasticsearch),
        ("Conexão Elasticsearch", test_elasticsearch_connection),
        ("Criação de Índice", create_index)
    ]
    
    results = []
    
    for test_name, test_func in tests:
        print(f"\n{test_name}:")
        result = test_func()
        results.append((test_name, result))
    
    # Resumo
    print("\n" + "=" * 60)
    print("📊 RESUMO DOS TESTES")
    print("=" * 60)
    
    passed = 0
    for test_name, result in results:
        status = "✅ PASSOU" if result else "❌ FALHOU"
        print(f"{test_name:25} - {status}")
        if result:
            passed += 1
    
    print("=" * 60)
    print(f"🎯 RESULTADO: {passed}/{len(results)} testes passaram")
    
    if passed == len(results):
        print("\n🎉 SISTEMA PRONTO!")
        print("Execute: python api/app.py")
        print("Docs em: http://localhost:8000/docs")
    else:
        print("\n⚠️  CORRIJA OS PROBLEMAS ACIMA")
        if not results[2][1]:  # Elasticsearch externo
            print("💡 Inicie o Elasticsearch primeiro:")
            print("   ./scripts/start_elasticsearch.sh")
    
    return passed == len(results)

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
