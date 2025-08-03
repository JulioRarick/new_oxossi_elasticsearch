#!/usr/bin/env python3
"""
Script para testar a conexão com Elasticsearch
"""

import sys
import os
from pathlib import Path

# Adicionar diretório raiz ao path
sys.path.append(str(Path(__file__).parent.parent))

try:
    from src.elasticsearch_manager import ElasticsearchManager
    print("✓ Importação do ElasticsearchManager bem-sucedida")
except ImportError as e:
    print(f"❌ Erro na importação: {e}")
    sys.exit(1)

def test_connection():
    """Testa a conexão com Elasticsearch"""
    print("Testando conexão com Elasticsearch...")
    
    try:
        # Criar instância do manager
        es_manager = ElasticsearchManager()
        print("✓ ElasticsearchManager instanciado")
        
        # Testar health check
        health = es_manager.health_check()
        print(f"Health check: {health}")
        
        if health.get("connection") == "healthy":
            print("✅ Conexão com Elasticsearch estabelecida com sucesso!")
            
            # Mostrar informações do cluster
            cluster_info = es_manager.es.info()
            print(f"Versão do Elasticsearch: {cluster_info['version']['number']}")
            print(f"Nome do cluster: {cluster_info['cluster_name']}")
            
            # Testar criação de índice
            print("\nTestando criação de índice...")
            if es_manager.create_index(force_recreate=True):
                print("✓ Índice criado com sucesso")
                
                # Verificar se índice existe
                if es_manager.es.indices.exists(index=es_manager.index_name):
                    print(f"✓ Índice '{es_manager.index_name}' confirmado")
                else:
                    print(f"❌ Índice '{es_manager.index_name}' não encontrado")
            else:
                print("❌ Falha na criação do índice")
                
        else:
            print("❌ Falha na conexão com Elasticsearch")
            print("Certifique-se de que o Elasticsearch está rodando em localhost:9200")
            print("Para iniciar: ./scripts/start_elasticsearch.sh")
            
    except Exception as e:
        print(f"❌ Erro: {e}")
        print("\nSoluções possíveis:")
        print("1. Verifique se o Elasticsearch está rodando:")
        print("   curl http://localhost:9200")
        print("2. Inicie o Elasticsearch:")
        print("   ./scripts/start_elasticsearch.sh")
        print("3. Ou use Docker Compose:")
        print("   docker-compose up elasticsearch")

def test_indexing():
    """Testa indexação de documento"""
    print("\nTestando indexação de documento...")
    
    try:
        es_manager = ElasticsearchManager()
        
        # Documento de teste
        test_doc = {
            "titulo": "Documento de Teste",
            "autor": "Sistema de Teste",
            "descricao": "Este é um documento de teste para verificar a indexação",
            "capitania": "Teste",
            "ano": 2024,
            "tipo": "Teste",
            "texto_completo": "Este é o texto completo do documento de teste.",
            "metadata": {
                "file_size": 1024,
                "page_count": 1
            }
        }
        
        # Indexar documento
        doc_id = es_manager.index_document(test_doc, doc_id="test_doc_001")
        print(f"✓ Documento indexado com ID: {doc_id}")
        
        # Buscar documento
        retrieved_doc = es_manager.get_by_id("test_doc_001")
        if retrieved_doc:
            print("✓ Documento recuperado com sucesso")
            print(f"  Título: {retrieved_doc.get('titulo')}")
            print(f"  Autor: {retrieved_doc.get('autor')}")
        else:
            print("❌ Falha ao recuperar documento")
            
        # Busca simples
        search_query = {
            "query": {
                "match": {
                    "titulo": "teste"
                }
            }
        }
        
        search_results = es_manager.search(search_query)
        hits = search_results.get('hits', {}).get('total', {}).get('value', 0)
        print(f"✓ Busca realizada: {hits} resultado(s) encontrado(s)")
        
        # Limpar documento de teste
        if es_manager.delete_document("test_doc_001"):
            print("✓ Documento de teste removido")
        
        print("✅ Teste de indexação concluído com sucesso!")
        
    except Exception as e:
        print(f"❌ Erro no teste de indexação: {e}")

if __name__ == "__main__":
    print("=== Teste de Conexão Elasticsearch ===")
    test_connection()
    
    # Só faz teste de indexação se conexão estiver ok
    try:
        es_manager = ElasticsearchManager()
        health = es_manager.health_check()
        if health.get("connection") == "healthy":
            test_indexing()
    except:
        pass
    
    print("\n=== Teste Concluído ===")