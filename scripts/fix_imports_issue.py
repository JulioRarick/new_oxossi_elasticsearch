#!/usr/bin/env python3
"""
Script para corrigir problemas de importação
"""

import os
import sys
from pathlib import Path

def fix_all_issues():
    """Corrige todos os problemas de uma vez"""
    print("🔧 Corrigindo todos os problemas...")
    
    # 1. Criar arquivos __init__.py
    print("1. Criando arquivos __init__.py...")
    init_dirs = [
        "src",
        "api", 
        "api/services",
        "api/utils"
    ]
    
    for directory in init_dirs:
        init_file = Path(directory) / "__init__.py"
        init_file.parent.mkdir(parents=True, exist_ok=True)
        
        if not init_file.exists():
            init_file.write_text("# Auto-generated __init__.py\n")
            print(f"   ✓ Criado: {init_file}")
    
    # 2. Criar diretórios necessários
    print("2. Criando diretórios...")
    directories = [
        "src/pdfs",
        "logs", 
        "config",
        "scripts"
    ]
    
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
        print(f"   ✓ {directory}")
    
    # 3. Corrigir arquivo .env
    print("3. Corrigindo arquivo .env...")
    env_content = """# Elasticsearch
ELASTICSEARCH_HOST=localhost
ELASTICSEARCH_PORT=9200
ELASTICSEARCH_INDEX=oxossi_docs_index

# API
API_HOST=0.0.0.0
API_PORT=8000
LOG_LEVEL=INFO

# Processamento
PDF_DIR=src/pdfs
BATCH_SIZE=10
MAX_WORKERS=4

# Ambiente
ENVIRONMENT=development
"""
    
    with open(".env", "w") as f:
        f.write(env_content)
    print("   ✓ Arquivo .env corrigido")
    
    # 4. Atualizar src/elasticsearch_manager.py
    print("4. Corrigindo elasticsearch_manager.py...")
    
    # Substituir as linhas problemáticas no arquivo existente
    es_manager_path = Path("src/elasticsearch_manager.py")
    if es_manager_path.exists():
        content = es_manager_path.read_text()
        
        # Corrigir a configuração do Elasticsearch
        old_config = """            self.es = Elasticsearch(
                hosts=[f"http://{self.host}:{self.port}"],
                timeout=30,
                max_retries=3,
                retry_on_timeout=True,
                # Desabilitar SSL para desenvolvimento local
                verify_certs=False,
                ssl_show_warn=False,
                # Configurações de autenticação desabilitadas para desenvolvimento
                request_timeout=30
            )"""
        
        new_config = """            self.es = Elasticsearch(
                hosts=[f"http://{self.host}:{self.port}"],
                request_timeout=30,
                max_retries=3,
                retry_on_timeout=True,
                verify_certs=False,
                ssl_show_warn=False
            )"""
        
        if old_config in content:
            content = content.replace(old_config, new_config)
            es_manager_path.write_text(content)
            print("   ✓ elasticsearch_manager.py corrigido")
        else:
            print("   ⚠️  elasticsearch_manager.py já estava correto")
    
    # 5. Criar script de teste simples
    print("5. Criando script de teste...")
    test_script = """#!/usr/bin/env python3
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
        
        print("\\n🎉 Todas as importações funcionam!")
        return True
    except Exception as e:
        print(f"❌ Erro: {e}")
        return False

if __name__ == "__main__":
    test_imports()
"""
    
    with open("test_imports.py", "w") as f:
        f.write(test_script)
    os.chmod("test_imports.py", 0o755)
    print("   ✓ test_imports.py criado")
    
    print("\n✅ Todas as correções aplicadas!")
    print("\n📋 Próximos passos:")
    print("1. python test_imports.py")
    print("2. python scripts/create_index.py")
    print("3. python api/app.py")

if __name__ == "__main__":
    fix_all_issues()