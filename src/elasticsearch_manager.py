"""
Gerenciador do Elasticsearch
Responsável pela conexão, indexação e operações com Elasticsearch
"""

import json
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import ConnectionError, RequestError, NotFoundError

logger = logging.getLogger(__name__)

class ElasticsearchManager:
    def __init__(self, host: str = "localhost", port: int = 9200, index_name: str = "oxossi_docs_index"):
        self.host = host
        self.port = port
        self.index_name = index_name
        self.es = None
        self._connect()
    
    def _connect(self):
        """Estabelece conexão com Elasticsearch"""
        try:
            self.es = Elasticsearch(
                [{"host": self.host, "port": self.port}],
                timeout=30,
                max_retries=3,
                retry_on_timeout=True
            )
            
            # Testar conexão
            if not self.es.ping():
                raise ConnectionError("Não foi possível conectar ao Elasticsearch")
            
            logger.info(f"Conectado ao Elasticsearch em {self.host}:{self.port}")
            
        except Exception as e:
            logger.error(f"Erro ao conectar ao Elasticsearch: {e}")
            raise
    
    def create_index(self, force_recreate: bool = False) -> bool:
        """Cria o índice com configurações otimizadas"""
        try:
            # Verificar se índice já existe
            if self.es.indices.exists(index=self.index_name):
                if force_recreate:
                    logger.info(f"Removendo índice existente: {self.index_name}")
                    self.es.indices.delete(index=self.index_name)
                else:
                    logger.info(f"Índice {self.index_name} já existe")
                    return True
            
            # Configurações do índice
            index_config = {
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0,
                    "analysis": {
                        "analyzer": {
                            "portuguese_analyzer": {
                                "type": "custom",
                                "tokenizer": "standard",
                                "filter": [
                                    "lowercase",
                                    "asciifolding",
                                    "portuguese_stop",
                                    "portuguese_stemmer"
                                ]
                            },
                            "autocomplete_analyzer": {
                                "type": "custom",
                                "tokenizer": "keyword",
                                "filter": ["lowercase", "asciifolding"]
                            }
                        },
                        "filter": {
                            "portuguese_stop": {
                                "type": "stop",
                                "stopwords": "_portuguese_"
                            },
                            "portuguese_stemmer": {
                                "type": "stemmer",
                                "language": "portuguese"
                            }
                        }
                    }
                },
                "mappings": {
                    "properties": {
                        "id": {"type": "keyword"},
                        "filename": {"type": "keyword"},
                        "titulo": {
                            "type": "text",
                            "analyzer": "portuguese_analyzer",
                            "fields": {
                                "keyword": {"type": "keyword"},
                                "autocomplete": {
                                    "type": "text",
                                    "analyzer": "autocomplete_analyzer"
                                }
                            }
                        },
                        "autor": {
                            "type": "text",
                            "analyzer": "portuguese_analyzer",
                            "fields": {
                                "keyword": {"type": "keyword"},
                                "autocomplete": {
                                    "type": "text",
                                    "analyzer": "autocomplete_analyzer"
                                }
                            }
                        },
                        "descricao": {
                            "type": "text",
                            "analyzer": "portuguese_analyzer"
                        },
                        "capitania": {
                            "type": "keyword",
                            "fields": {
                                "text": {
                                    "type": "text",
                                    "analyzer": "portuguese_analyzer"
                                }
                            }
                        },
                        "data": {
                            "type": "date",
                            "format": "yyyy||yyyy-MM||yyyy-MM-dd||strict_date_optional_time"
                        },
                        "ano": {"type": "integer"},
                        "tipo": {
                            "type": "keyword",
                            "fields": {
                                "text": {
                                    "type": "text",
                                    "analyzer": "portuguese_analyzer"
                                }
                            }
                        },
                        "pdf_url": {"type": "keyword", "index": False},
                        "full_text": {
                            "type": "text",
                            "analyzer": "portuguese_analyzer"
                        },
                        "extracted_data": {
                            "properties": {
                                "dates": {
                                    "type": "nested",
                                    "properties": {
                                        "type": {"type": "keyword"},
                                        "year": {"type": "integer"},
                                        "year_end": {"type": "integer"},
                                        "century": {"type": "keyword"},
                                        "period": {"type": "keyword"},
                                        "original_text": {"type": "text"},
                                        "position": {"type": "integer"},
                                        "confidence": {"type": "float"},
                                        "context": {"type": "text"}
                                    }
                                },
                                "names": {
                                    "type": "nested",
                                    "properties": {
                                        "first_name": {"type": "keyword"},
                                        "last_name": {"type": "keyword"},
                                        "full_name": {
                                            "type": "text",
                                            "analyzer": "portuguese_analyzer",
                                            "fields": {"keyword": {"type": "keyword"}}
                                        },
                                        "position": {"type": "integer"},
                                        "confidence": {"type": "float"},
                                        "context": {"type": "text"}
                                    }
                                },
                                "places": {
                                    "type": "nested",
                                    "properties": {
                                        "location": {
                                            "type": "keyword",
                                            "fields": {
                                                "text": {
                                                    "type": "text",
                                                    "analyzer": "portuguese_analyzer"
                                                }
                                            }
                                        },
                                        "capitania": {"type": "keyword"},
                                        "position": {"type": "integer"},
                                        "confidence": {"type": "float"},
                                        "match_type": {"type": "keyword"},
                                        "context": {"type": "text"}
                                    }
                                },
                                "themes": {
                                    "type": "nested",
                                    "properties": {
                                        "category": {"type": "keyword"},
                                        "keywords_found": {"type": "keyword"},
                                        "keyword_count": {"type": "integer"},
                                        "total_occurrences": {"type": "integer"},
                                        "relevance_score": {"type": "float"},
                                        "context": {"type": "text"}
                                    }
                                }
                            }
                        },
                        "metadata": {
                            "properties": {
                                "file_size": {"type": "long"},
                                "page_count": {"type": "integer"},
                                "processed_at": {"type": "date"},
                                "processing_time_ms": {"type": "long"},
                                "extraction_summary": {
                                    "type": "object",
                                    "enabled": False  # Não indexar, apenas armazenar
                                }
                            }
                        }
                    }
                }
            }
            
            # Criar índice
            self.es.indices.create(index=self.index_name, body=index_config)
            logger.info(f"Índice {self.index_name} criado com sucesso")
            return True
            
        except RequestError as e:
            logger.error(f"Erro ao criar índice: {e}")
            return False
        except Exception as e:
            logger.error(f"Erro inesperado ao criar índice: {e}")
            return False
    
    def index_document(self, document: Dict[str, Any], doc_id: str = None) -> str:
        """Indexa um documento individual"""
        try:
            # Preparar documento
            prepared_doc = self._prepare_document(document)
            
            # Gerar ID se não fornecido
            if not doc_id:
                doc_id = self._generate_document_id(document)
            
            # Indexar documento
            response = self.es.index(
                index=self.index_name,
                id=doc_id,
                body=prepared_doc
            )
            
            logger.info(f"Documento indexado: {doc_id}")
            return response['_id']
            
        except Exception as e:
            logger.error(f"Erro ao indexar documento {doc_id}: {e}")
            raise
    
    def bulk_index(self, documents: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Indexa múltiplos documentos em lote"""
        try:
            # Preparar documentos para bulk
            actions = []
            for doc in documents:
                prepared_doc = self._prepare_document(doc)
                doc_id = self._generate_document_id(doc)
                
                action = {
                    "_index": self.index_name,
                    "_id": doc_id,
                    "_source": prepared_doc
                }
                actions.append(action)
            
            # Executar bulk indexing
            success_count, failed_docs = helpers.bulk(
                self.es,
                actions,
                chunk_size=100,
                request_timeout=60
            )
            
            logger.info(f"Bulk indexing concluído: {success_count} sucessos, {len(failed_docs)} falhas")
            
            return {
                "success_count": success_count,
                "failed_count": len(failed_docs),
                "failed_docs": failed_docs
            }
            
        except Exception as e:
            logger.error(f"Erro no bulk indexing: {e}")
            raise
    
    def search(self, query: Dict[str, Any]) -> Dict[str, Any]:
        """Executa busca no índice"""
        try:
            response = self.es.search(
                index=self.index_name,
                body=query
            )
            return response
            
        except Exception as e:
            logger.error(f"Erro na busca: {e}")
            raise
    
    def get_by_id(self, doc_id: str) -> Optional[Dict[str, Any]]:
        """Recupera documento por ID"""
        try:
            response = self.es.get(index=self.index_name, id=doc_id)
            return response['_source']
            
        except NotFoundError:
            logger.warning(f"Documento não encontrado: {doc_id}")
            return None
        except Exception as e:
            logger.error(f"Erro ao buscar documento {doc_id}: {e}")
            raise
    
    def delete_document(self, doc_id: str) -> bool:
        """Remove documento por ID"""
        try:
            self.es.delete(index=self.index_name, id=doc_id)
            logger.info(f"Documento removido: {doc_id}")
            return True
            
        except NotFoundError:
            logger.warning(f"Documento não encontrado para remoção: {doc_id}")
            return False
        except Exception as e:
            logger.error(f"Erro ao remover documento {doc_id}: {e}")
            return False
    
    def get_index_stats(self) -> Dict[str, Any]:
        """Retorna estatísticas do índice"""
        try:
            stats = self.es.indices.stats(index=self.index_name)
            count_response = self.es.count(index=self.index_name)
            
            return {
                "document_count": count_response['count'],
                "index_size_bytes": stats['indices'][self.index_name]['total']['store']['size_in_bytes'],
                "index_size_mb": round(stats['indices'][self.index_name]['total']['store']['size_in_bytes'] / (1024 * 1024), 2)
            }
            
        except Exception as e:
            logger.error(f"Erro ao obter estatísticas: {e}")
            return {}
    
    def _prepare_document(self, document: Dict[str, Any]) -> Dict[str, Any]:
        """Prepara documento para indexação"""
        prepared = document.copy()
        
        # Adicionar timestamp de processamento se não existir
        if 'metadata' not in prepared:
            prepared['metadata'] = {}
        
        if 'processed_at' not in prepared['metadata']:
            prepared['metadata']['processed_at'] = datetime.utcnow().isoformat()
        
        # Garantir que campos obrigatórios existam
        required_fields = {
            'titulo': '',
            'autor': '',
            'descricao': '',
            'capitania': '',
            'tipo': '',
            'ano': 0
        }
        
        for field, default_value in required_fields.items():
            if field not in prepared or prepared[field] is None:
                prepared[field] = default_value
        
        # Converter data para formato ISO se necessário
        if prepared.get('data') and isinstance(prepared['data'], str):
            try:
                # Se é só ano, converter para data ISO
                if len(prepared['data']) == 4 and prepared['data'].isdigit():
                    prepared['data'] = f"{prepared['data']}-01-01"
            except:
                pass
        
        return prepared
    
    def _generate_document_id(self, document: Dict[str, Any]) -> str:
        """Gera ID único para o documento"""
        filename = document.get('filename', 'unknown')
        # Remover extensão e caracteres especiais
        doc_id = filename.replace('.pdf', '').replace(' ', '_')
        
        # Adicionar hash se necessário para garantir unicidade
        import hashlib
        content_hash = hashlib.md5(str(document).encode()).hexdigest()[:8]
        
        return f"{doc_id}_{content_hash}"
    
    def health_check(self) -> Dict[str, Any]:
        """Verifica saúde da conexão Elasticsearch"""
        try:
            cluster_health = self.es.cluster.health()
            index_exists = self.es.indices.exists(index=self.index_name)
            
            return {
                "connection": "healthy",
                "cluster_status": cluster_health['status'],
                "index_exists": index_exists,
                "nodes": cluster_health['number_of_nodes']
            }
            
        except Exception as e:
            return {
                "connection": "unhealthy",
                "error": str(e)
            }