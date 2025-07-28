"""
Serviço de Busca
Implementa a lógica de busca e integração com Elasticsearch
"""

import sys
from pathlib import Path
from typing import Dict, List, Any, Optional
import logging

# Adicionar diretório raiz ao path
sys.path.append(str(Path(__file__).parent.parent.parent))

from src.elasticsearch_manager import ElasticsearchManager
from api.utils.query_builder import QueryBuilder
from api.utils.response_formatter import ResponseFormatter

logger = logging.getLogger(__name__)

class SearchService:
    def __init__(self):
        self.es_manager = ElasticsearchManager()
        self.query_builder = QueryBuilder()
        self.formatter = ResponseFormatter()
        self.index_name = self.es_manager.index_name
    
    async def simple_search(
        self, 
        query: str, 
        page: int = 1, 
        size: int = 20,
        sort: str = "relevance",
        order: str = "desc"
    ) -> Dict[str, Any]:
        """Executa busca simples"""
        try:
            es_query = self.query_builder.build_simple_query(
                query, page, size, sort, order
            )
            
            response = self.es_manager.search(es_query)
            return self.formatter.format_search_response(response)
            
        except Exception as e:
            logger.error(f"Erro na busca simples: {e}")
            raise
    
    async def advanced_search(
        self,
        query: Optional[str],
        filters: Dict[str, Any],
        page: int = 1,
        size: int = 20,
        sort: str = "relevance",
        order: str = "desc",
        include_aggregations: bool = False
    ) -> Dict[str, Any]:
        """Executa busca avançada com filtros"""
        try:
            es_query = self.query_builder.build_advanced_query(
                query, filters, page, size, sort, order, include_aggregations
            )
            
            response = self.es_manager.search(es_query)
            return self.formatter.format_search_response(response, include_aggregations)
            
        except Exception as e:
            logger.error(f"Erro na busca avançada: {e}")
            raise
    
    async def autocomplete(self, field: str, query: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Fornece sugestões de autocomplete"""
        try:
            es_query = self.query_builder.build_autocomplete_query(field, query, limit)
            response = self.es_manager.search(es_query)
            return self.formatter.format_autocomplete_response(response, field)
            
        except Exception as e:
            logger.error(f"Erro no autocomplete: {e}")
            raise
    
    async def get_document(self, doc_id: str) -> Optional[Dict[str, Any]]:
        """Retorna documento específico"""
        try:
            return self.es_manager.get_by_id(doc_id)
        except Exception as e:
            logger.error(f"Erro ao buscar documento {doc_id}: {e}")
            raise
    
    async def get_available_filters(self) -> Dict[str, Any]:
        """Retorna filtros disponíveis para busca avançada"""
        try:
            aggs_query = {
                "size": 0,
                "aggs": {
                    "autores": {
                        "terms": {"field": "autor.keyword", "size": 1000}
                    },
                    "capitanias": {
                        "terms": {"field": "capitania", "size": 50}
                    },
                    "tipos": {
                        "terms": {"field": "tipo", "size": 50}
                    },
                    "anos": {
                        "stats": {"field": "ano"}
                    },
                    "decadas": {
                        "date_histogram": {
                            "field": "data",
                            "calendar_interval": "decade",
                            "format": "yyyy"
                        }
                    }
                }
            }
            
            response = self.es_manager.search(aggs_query)
            return self.formatter.format_filters_response(response)
            
        except Exception as e:
            logger.error(f"Erro ao buscar filtros: {e}")
            raise
    
    async def get_collection_stats(self) -> Dict[str, Any]:
        """Retorna estatísticas gerais da coleção"""
        try:
            stats_query = {
                "size": 0,
                "aggs": {
                    "total_docs": {"value_count": {"field": "_id"}},
                    "periodo_range": {"stats": {"field": "ano"}},
                    "tipos_distribution": {"terms": {"field": "tipo", "size": 20}},
                    "capitanias_distribution": {"terms": {"field": "capitania", "size": 30}},
                    "avg_pages": {"avg": {"field": "metadata.page_count"}},
                    "total_size": {"sum": {"field": "metadata.file_size"}}
                }
            }
            
            response = self.es_manager.search(stats_query)
            return self.formatter.format_stats_response(response)
            
        except Exception as e:
            logger.error(f"Erro ao buscar estatísticas: {e}")
            raise
    
    async def get_search_suggestions(self, query: str, limit: int = 5) -> List[str]:
        """Retorna sugestões de busca baseadas no conteúdo"""
        try:
            # Busca por termos similares no título e descrição
            suggestion_query = {
                "size": 0,
                "aggs": {
                    "title_terms": {
                        "terms": {
                            "field": "titulo.keyword",
                            "include": f".*{query}.*",
                            "size": limit
                        }
                    },
                    "author_terms": {
                        "terms": {
                            "field": "autor.keyword",
                            "include": f".*{query}.*",
                            "size": limit
                        }
                    }
                }
            }
            
            response = self.es_manager.search(suggestion_query)
            
            suggestions = []
            
            # Extrair sugestões de títulos
            if 'aggregations' in response:
                for bucket in response['aggregations'].get('title_terms', {}).get('buckets', []):
                    suggestions.append(bucket['key'])
                
                for bucket in response['aggregations'].get('author_terms', {}).get('buckets', []):
                    suggestions.append(f"autor:{bucket['key']}")
            
            return suggestions[:limit]
            
        except Exception as e:
            logger.error(f"Erro nas sugestões de busca: {e}")
            return []
    
    async def list_all_documents(self, page: int = 1, size: int = 20, sort: str = "filename") -> Dict[str, Any]:
        """Lista todos os documentos com paginação"""
        try:
            query = {
                "query": {"match_all": {}},
                "from": (page - 1) * size,
                "size": size,
                "sort": [
                    {sort + ".keyword": {"order": "asc"}} if sort != "relevance" 
                    else {"_score": {"order": "desc"}}
                ],
                "_source": {
                    "excludes": ["full_text", "extracted_data.themes.context"]
                }
            }
            
            response = self.es_manager.search(query)
            return self.formatter.format_search_response(response)
            
        except Exception as e:
            logger.error(f"Erro ao listar documentos: {e}")
            raise
    
    async def get_documents_batch(self, document_ids: List[str]) -> List[Dict[str, Any]]:
        """Retorna múltiplos documentos por IDs"""
        try:
            documents = []
            for doc_id in document_ids:
                doc = self.es_manager.get_by_id(doc_id)
                if doc:
                    doc['id'] = doc_id
                    documents.append(doc)
            
            return documents
            
        except Exception as e:
            logger.error(f"Erro na busca em lote: {e}")
            raise
    
    async def check_elasticsearch_health(self) -> str:
        """Verifica saúde do Elasticsearch"""
        try:
            health = self.es_manager.health_check()
            return health.get("connection", "unhealthy")
        except Exception as e:
            logger.error(f"Erro no health check: {e}")
            return "unhealthy"
    
    async def get_detailed_index_stats(self) -> Dict[str, Any]:
        """Retorna estatísticas detalhadas do índice"""
        try:
            return self.es_manager.get_index_stats()
        except Exception as e:
            logger.error(f"Erro ao buscar estatísticas detalhadas: {e}")
            raise
    
    async def delete_document(self, doc_id: str) -> bool:
        """Remove um documento do índice"""
        try:
            return self.es_manager.delete_document(doc_id)
        except Exception as e:
            logger.error(f"Erro ao deletar documento: {e}")
            raise
    
    async def trigger_reindex(self) -> Dict[str, Any]:
        """Dispara reindexação (placeholder para implementação futura)"""
        try:
            # Aqui seria implementada a lógica para disparar reprocessamento
            # de todos os PDFs. Por enquanto, retorna um placeholder.
            logger.info("Reindexação solicitada")
            return {"task_id": "reindex_task_placeholder"}
        except Exception as e:
            logger.error(f"Erro ao disparar reindexação: {e}")
            raise
    
    async def cleanup(self):
        """Limpeza de recursos"""
        try:
            # Cleanup se necessário
            logger.info("Limpeza do SearchService concluída")
        except Exception as e:
            logger.error(f"Erro na limpeza: {e}")
    
    # Métodos para desenvolvimento
    
    async def create_test_documents(self) -> int:
        """Cria documentos de teste para desenvolvimento"""
        try:
            test_docs = [
                {
                    "filename": "test_doc_1.pdf",
                    "titulo": "Carta de Doação da Capitania de São Vicente",
                    "autor": "Manuel da Silva",
                    "descricao": "Documento de doação de terras na capitania de São Vicente",
                    "capitania": "São Vicente",
                    "data": "1532-01-20",
                    "ano": 1532,
                    "tipo": "Carta Régia",
                    "pdf_url": "/pdfs/test_doc_1.pdf",
                    "full_text": "Este é um documento de teste sobre a doação de terras...",
                    "extracted_data": {
                        "dates": [{"year": 1532, "century": "XVI", "confidence": 0.9}],
                        "names": [{"full_name": "Manuel da Silva", "confidence": 0.8}],
                        "places": [{"location": "São Vicente", "capitania": "São Vicente"}],
                        "themes": [{"category": "Economia", "relevance_score": 0.7}]
                    },
                    "metadata": {
                        "file_size": 1024,
                        "page_count": 2,
                        "processed_at": "2024-01-01T00:00:00Z"
                    }
                },
                {
                    "filename": "test_doc_2.pdf",
                    "titulo": "Testamento de João Santos",
                    "autor": "João Santos",
                    "descricao": "Testamento com disposições sobre bens e escravos",
                    "capitania": "Bahia",
                    "data": "1598-12-15",
                    "ano": 1598,
                    "tipo": "Testamento",
                    "pdf_url": "/pdfs/test_doc_2.pdf",
                    "full_text": "Testamento de João Santos, morador da Cidade da Bahia...",
                    "extracted_data": {
                        "dates": [{"year": 1598, "century": "XVI", "confidence": 0.9}],
                        "names": [{"full_name": "João Santos", "confidence": 0.9}],
                        "places": [{"location": "Cidade da Bahia", "capitania": "Bahia"}],
                        "themes": [{"category": "Relacionamentos", "relevance_score": 0.8}]
                    },
                    "metadata": {
                        "file_size": 2048,
                        "page_count": 3,
                        "processed_at": "2024-01-01T00:00:00Z"
                    }
                }
            ]
            
            result = self.es_manager.bulk_index(test_docs)
            return result["success_count"]
            
        except Exception as e:
            logger.error(f"Erro ao criar dados de teste: {e}")
            raise
    
    async def clear_all_documents(self) -> int:
        """Limpa todos os documentos do índice"""
        try:
            delete_query = {
                "query": {"match_all": {}}
            }
            
            response = self.es_manager.es.delete_by_query(
                index=self.index_name,
                body=delete_query
            )
            
            return response.get("deleted", 0)
            
        except Exception as e:
            logger.error(f"Erro ao limpar índice: {e}")
            raise