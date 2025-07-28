"""
Construtor de Queries Elasticsearch
Constrói queries complexas para diferentes tipos de busca,
agora incluindo os campos enriquecidos do JSON.
"""

from typing import Dict, List, Any, Optional
import logging

logger = logging.getLogger(__name__)

class QueryBuilder:
    def __init__(self):
        # Adicionados 'titulo' e 'autor' com pesos maiores para relevância
        self.default_fields = [
            "titulo^4",
            "autor^3", 
            "texto_completo",
            "dados_extraidos.nomes.nome_completo^2",
            "dados_extraidos.locais.local^1.5"
        ]
    
    def build_simple_query(
        self, 
        query_text: str, 
        page: int = 1, 
        size: int = 20,
        sort: str = "relevance",
        order: str = "desc"
    ) -> Dict[str, Any]:
        """Constrói query para busca simples"""
        
        query = {
            "query": {
                "multi_match": {
                    "query": query_text,
                    "fields": self.default_fields,
                    "type": "best_fields",
                    "fuzziness": "AUTO",
                    "operator": "or",
                    "minimum_should_match": "75%"
                }
            },
            "highlight": {
                "fields": {
                    "titulo": {"fragment_size": 100},
                    "autor": {"fragment_size": 100},
                    "texto_completo": {"fragment_size": 200, "number_of_fragments": 3}
                },
                "pre_tags": ["<em>"],
                "post_tags": ["</em>"],
                "require_field_match": False
            },
            "from": (page - 1) * size,
            "size": size
        }

        # Lógica de ordenação
        sort_mapping = {
            "relevance": "_score",
            "year_asc": {"ano_publicacao": "asc"},
            "year_desc": {"ano_publicacao": "desc"},
            "author_asc": {"autor.keyword": "asc"},
            "author_desc": {"autor.keyword": "desc"}
        }

        if sort in sort_mapping:
            if sort != "relevance":
                 query["sort"] = [sort_mapping[sort]]
        
        return query

    def build_advanced_query(
        self, 
        query_text: Optional[str], 
        filters: Dict[str, Any],
        page: int = 1, 
        size: int = 20,
        sort: str = "relevance",
        order: str = "desc",
        include_aggregations: bool = False
    ) -> Dict[str, Any]:
        """Constrói query para busca avançada com filtros e agregações."""
        
        must_clauses = []
        if query_text:
            must_clauses.append({
                "multi_match": {
                    "query": query_text,
                    "fields": self.default_fields,
                    "fuzziness": "AUTO"
                }
            })

        filter_clauses = []
        if filters:
            # Filtro por autor
            if "autor" in filters and filters["autor"]:
                filter_clauses.append({"term": {"autor.keyword": filters["autor"]}})
            
            # Filtro por capitania
            if "capitania" in filters and filters["capitania"]:
                filter_clauses.append({
                    "nested": {
                        "path": "dados_extraidos.locais",
                        "query": {
                            "term": {"dados_extraidos.locais.capitania": filters["capitania"]}
                        }
                    }
                })
            
            # Filtro por período (ano)
            year_range = {}
            if "ano_inicio" in filters:
                year_range["gte"] = filters["ano_inicio"]
            if "ano_fim" in filters:
                year_range["lte"] = filters["ano_fim"]
            if year_range:
                filter_clauses.append({"range": {"ano_publicacao": year_range}})

        query = {
            "query": {
                "bool": {
                    "must": must_clauses,
                    "filter": filter_clauses
                }
            },
            "from": (page - 1) * size,
            "size": size
        }

        # Lógica de ordenação (similar à busca simples)
        sort_mapping = {
            "relevance": "_score",
            "year_asc": {"ano_publicacao": "asc"},
            "year_desc": {"ano_publicacao": "desc"},
            "author_asc": {"autor.keyword": "asc"},
            "author_desc": {"autor.keyword": "desc"}
        }
        if sort in sort_mapping and sort != "relevance":
            query["sort"] = [sort_mapping[sort]]

        # Adicionar agregações se solicitado
        if include_aggregations:
            query["aggs"] = {
                "autores": {
                    "terms": {"field": "autor.keyword", "size": 50}
                },
                "anos": {
                    "histogram": {
                        "field": "ano_publicacao",
                        "interval": 10, # Agrupa por década
                        "min_doc_count": 1
                    }
                },
                "capitanias": {
                    "nested": {"path": "dados_extraidos.locais"},
                    "aggs": {
                        "nomes_capitanias": {
                            "terms": {"field": "dados_extraidos.locais.capitania", "size": 50}
                        }
                    }
                }
            }

        return query

    def build_autocomplete_query(self, field: str, prefix: str, limit: int = 10) -> Dict[str, Any]:
        """Constrói uma query para sugestões de autocomplete."""
        
        field_mapping = {
            "autor": "autor.keyword",
            "titulo": "titulo.keyword",
            "local": "dados_extraidos.locais.local"
        }

        if field not in field_mapping:
            return {} # Campo inválido

        es_field = field_mapping[field]

        if field == "local": # Campo aninhado
            return {
                "query": {
                    "nested": {
                        "path": "dados_extraidos.locais",
                        "query": {
                            "prefix": {es_field: prefix}
                        }
                    }
                },
                "_source": False,
                "aggs": {
                    "sugestoes": {
                        "nested": {"path": "dados_extraidos.locais"},
                        "aggs": {
                            "termos": {
                                "terms": {"field": es_field, "size": limit}
                            }
                        }
                    }
                },
                "size": 0
            }
        else: # Campo de nível superior
            return {
                "query": {
                    "prefix": {es_field: prefix}
                },
                "_source": False,
                "aggs": {
                    "sugestoes": {
                        "terms": {"field": es_field, "size": limit}
                    }
                },
                "size": 0
            }
