#!/usr/bin/env python3
"""
Teste completo do sistema
Testa todos os componentes principais
"""

import sys
import os
from pathlib import Path

# Adicionar diretório raiz ao path
sys.path.append(str(Path(__file__).parent))

def test_imports():
    """Testa todas as importações"""
    print("=== Testando Importações ===")
    
    try:
        from src.elasticsearch_manager import ElasticsearchManager
        print("✓ ElasticsearchManager importado")
    except Exception as e:
        print(f"❌ ElasticsearchManager: {e}")
        return False
    
    try:
        from src.pdf_processor import PDFProcessor
        print("✓ PDFProcessor importado")
    except Exception as e:
        print(f"❌ PDFProcessor: {e}")
        return False
    
    try:
        from src.config_manager import ConfigManager
        print("✓ ConfigManager importado")
    except Exception as e:
        print(f"❌ ConfigManager: {e}")
        return False
    
    try:
        from src.data_extractor import DataExtractor
        print("✓ DataExtractor importado")
    except Exception as e:
        print(f"❌ DataExtractor: {e}")
        return False
    
    try:
        from api.services.search_service import SearchService
        print("✓ SearchService importado")
    except Exception as e:
        print(f"❌ SearchService: {e}")
        return False
    
    return True

def test_elasticsearch():
    """Testa conexão com Elasticsearch"""
    print("\n=== Testando Elasticsearch ===")
    
    try:
        from src.elasticsearch_manager import ElasticsearchManager
        
        es_manager = ElasticsearchManager()
        health = es_manager.health_check()
        
        if health.get("connection") == "healthy":
            print("✓ Conexão com Elasticsearch OK")
            
            # Teste de criação de índice
            if es_manager.create_index(force_recreate=True):
                print("✓ Índice criado com sucesso")
                return True
            else:
                print("❌ Falha na criação do índice")
                return False
        else:
            print("❌ Elasticsearch não está saudável")
            print("Execute: ./scripts/start_elasticsearch.sh")
            return False
            
    except Exception as e:
        print(f"❌ Erro no teste Elasticsearch: {e}")
        return False

def test_config_manager():
    """Testa ConfigManager"""
    print("\n=== Testando ConfigManager ===")
    
    try:
        from src.config_manager import ConfigManager
        
        config_manager = ConfigManager()
        
        # Testar carregamento de configurações
        date_config = config_manager.load_date_config()
        print("✓ Configurações de data carregadas")
        
        names_config = config_manager.load_names_config()
        print("✓ Configurações de nomes carregadas")
        
        places_config = config_manager.load_places_config()
        print("✓ Configurações de lugares carregadas")
        
        themes_config = config_manager.load_themes_config()
        print("✓ Configurações de temas carregadas")
        
        # Validar configurações
        if config_manager.validate_configs():
            print("✓ Todas as configurações são válidas")
            return True
        else:
            print("❌ Configurações inválidas")
            return False
            
    except Exception as e:
        print(f"❌ Erro no teste ConfigManager: {e}")
        return False

def test_pdf_processor():
    """Testa PDFProcessor"""
    print("\n=== Testando PDFProcessor ===")
    
    try:
        from src.pdf_processor import PDFProcessor
        
        pdf_processor = PDFProcessor()
        print("✓ PDFProcessor instanciado")
        
        # Verificar se há PDFs de teste
        pdf_dir = Path("src/pdfs")
        if pdf_dir.exists():
            pdf_files = list(pdf_dir.glob("*.pdf"))
            if pdf_files:
                test_pdf = pdf_files[0]
                print(f"✓ Testando com PDF: {test_pdf.name}")
                
                if pdf_processor.validate_pdf(str(test_pdf)):
                    print("✓ PDF válido")
                    
                    # Tentar extrair metadados
                    metadata = pdf_processor.extract_metadata(str(test_pdf))
                    print(f"✓ Metadados extraídos: {metadata['page_count']} páginas")
                    
                    return True
                else:
                    print("❌ PDF inválido")
                    return False
            else:
                print("⚠️  Nenhum PDF encontrado para teste")
                return True
        else:
            print("⚠️  Diretório de PDFs não existe")
            return True
            
    except Exception as e:
        print(f"❌ Erro no teste PDFProcessor: {e}")
        return False

def test_api_imports():
    """Testa importações da API"""
    print("\n=== Testando API ===")
    
    try:
        from api.app import app
        print("✓ API importada com sucesso")
        
        # Testar se pode criar app
        if app:
            print("✓ App FastAPI criado")
            return True
        else:
            print("❌ Falha na criação do app")
            return False
            
    except Exception as e:
        print(f"❌ Erro no teste da API: {e}")
        return False

def create_missing_files():
    """Cria arquivos necessários se não existirem"""
    print("\n=== Criando Arquivos Necessários ===")
    
    # Criar diretórios
    directories = [
        "src/pdfs",
        "logs",
        "config"
    ]
    
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
        print(f"✓ Diretório criado/verificado: {directory}")
    
    # Criar __init__.py se não existir
    init_files = [
        "src/__init__.py",
        "api/__init__.py",
        "api/services/__init__.py",
        "api/utils/__init__.py"
    ]
    
    for init_file in init_files:
        init_path = Path(init_file)
        if not init_path.exists():
            init_path.parent.mkdir(parents=True, exist_ok=True)
            init_path.touch()
            print(f"✓ Criado: {init_file}")

def main():
    """Função principal"""
    print("=== TESTE COMPLETO DO SISTEMA ===")
    print("Este script testa todos os componentes do sistema\n")
    
    # Criar arquivos necessários
    create_missing_files()
    
    # Executar testes
    tests = [
        ("Importações", test_imports),
        ("ConfigManager", test_config_manager),
        ("PDFProcessor", test_pdf_processor),
        ("Elasticsearch", test_elasticsearch),
        ("API", test_api_imports),
    ]
    
    results = []
    
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"❌ Erro crítico em {test_name}: {e}")
            results.append((test_name, False))
    
    # Resumo dos resultados
    print("\n" + "="*50)
    print("RESUMO DOS TESTES")
    print("="*50)
    
    passed = 0
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASSOU" if result else "❌ FALHOU"
        print(f"{test_name:20} - {status}")
        if result:
            passed += 1
    
    print("="*50)
    print(f"RESULTADO FINAL: {passed}/{total} testes passaram")
    
    if passed == total:
        print("🎉 TODOS OS TESTES PASSARAM!")
        print("\nSistema pronto para uso:")
        print("1. Para processar PDFs: python src/main_processor.py --local-only")
        print("2. Para iniciar API: python api/app.py")
        print("3. Para acessar docs: http://localhost:8000/docs")
    else:
        print("⚠️  ALGUNS TESTES FALHARAM")
        print("\nVerifique os erros acima e:")
        print("1. Certifique-se que o Elasticsearch está rodando")
        print("2. Verifique se todos os arquivos de configuração existem")
        print("3. Execute: pip install -r requirements.txt")
    
    return passed == total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)