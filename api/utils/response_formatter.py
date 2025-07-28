"""
Formatador de Respostas
Formata respostas do Elasticsearch para o formato da API
"""

from typing import Dict, List, Any
import logging

logger = logging.getLogger(__name__)

class ResponseFormatter:
    def format_search_response(
        self, 
        es_response: Dict[str, Any], 
        include_aggregations: bool = False
    ) -> Dict[str, Any]:
        """Formata resposta de busca do Elasticsearch"""
        try:
            hits = []
            
            for hit in es_response['hits']['hits']:
                doc = hit['_source'].copy()
                doc['id'] = hit['_id']
                doc['score'] = hit.get('_score', 0)
                
                # Adicionar highlights se existirem
                if 'highlight' in hit:
                    doc['highlights'] = hit['highlight']
                
                # Limpar campos internos se necessário
                doc = self._clean_document_for_response(doc)
                
                hits.append(doc)
            
            result = {
                'hits': hits,
                'total': es_response['hits']['total']['value']
            }
            
            # Adicionar agregações se solicitado
            if include_aggregations and 'aggregations' in es_response:
                result['aggregations'] = self._format_aggregations(
                    es_response['aggregations']
                )
            
            return result
            
        except Exception as e:
            logger.error(f"Erro ao formatar resposta de busca: {e}")
            return {'hits': [], 'total': 0}
    
    def format_autocomplete_response(
        self, 
        es_response: Dict[str, Any], 
        field: str
    ) -> List[Dict[str, Any]]:
        """Formata resposta de autocomplete"""
        try:
            suggestions = []
            
            if 'aggregations' in es_response and 'suggestions' in es_response['aggregations']:
                buckets = es_response['aggregations']['suggestions']['buckets']
                for bucket in buckets:
                    suggestions.append({
                        'text': bucket['key'],
                        'count': bucket['doc_count'],
                        'type': field
                    })
            
            return suggestions
            
        except Exception as e:
            logger.error(f"Erro ao formatar autocomplete: {e}")
            return []
    
    def format_filters_response(self, es_response: Dict[str, Any]) -> Dict[str, Any]:
        """Formata resposta de filtros disponíveis"""
        try:
            aggs = es_response.get('aggregations', {})
            
            # Processar anos
            anos_stats = aggs.get('anos', {})
            ano_min = int(anos_stats.get('min', 1500)) if anos_stats.get('min') else 1500
            ano_max = int(anos_stats.get('max', 1822)) if anos_stats.get('max') else 1822
            
            # Criar intervalos de décadas
            intervalos_anos = []
            for inicio in range(ano_min, ano_max + 1, 10):
                fim = min(inicio + 9, ano_max)
                intervalos_anos.append({
                    'range': f"{inicio}-{fim}",
                    'start': inicio,
                    'end': fim
                })
            
            return {
                'autores': [
                    {'value': b['key'], 'count': b['doc_count']}
                    for b in aggs.get('autores', {}).get('buckets', [])
                ],
                'capitanias': [
                    {'value': b['key'], 'count': b['doc_count']}
                    for b in aggs.get('capitanias', {}).get('buckets', [])
                ],
                'tipos': [
                    {'value': b['key'], 'count': b['doc_count']}
                    for b in aggs.get('tipos', {}).get('buckets', [])
                ],
                'anos': {
                    'min': ano_min,
                    'max': ano_max,
                    'intervals': intervalos_anos
                }
            }
            
        except Exception as e:
            logger.error(f"Erro ao formatar filtros: {e}")
            return {'autores': [], 'capitanias': [], 'tipos': [], 'anos': {}}
    
    def format_stats_response(self, es_response: Dict[str, Any]) -> Dict[str, Any]:
        """Formata estatísticas da coleção"""
        try:
            aggs = es_response.get('aggregations', {})
            
            return {
                'total_documentos': aggs.get('total_docs', {}).get('value', 0),
                'periodo': {
                    'inicio': int(aggs.get('periodo_range', {}).get('min', 0)) or None,
                    'fim': int(aggs.get('periodo_range', {}).get('max', 0)) or None
                },
                'tipos_documento': [
                    {'tipo': b['key'], 'quantidade': b['doc_count']}
                    for b in aggs.get('tipos_distribution', {}).get('buckets', [])
                ],
                'capitanias': [
                    {'capitania': b['key'], 'quantidade': b['doc_count']}
                    for b in aggs.get('capitanias_distribution', {}).get('buckets', [])
                ],
                'media_paginas': round(
                    aggs.get('avg_pages', {}).get('value', 0), 1
                ) if aggs.get('avg_pages', {}).get('value') else 0,
                'tamanho_total_mb': round(
                    aggs.get('total_size', {}).get('value', 0) / (1024 * 1024), 2
                ) if aggs.get('total_size', {}).get('value') else 0
            }
            
        except Exception as e:
            logger.error(f"Erro ao formatar estatísticas: {e}")
            return {
                'total_documentos': 0,
                'periodo': {'inicio': None, 'fim': None},
                'tipos_documento': [],
                'capitanias': [],
                'media_paginas': 0,
                'tamanho_total_mb': 0
            }
    
    def format_document_response(self, document: Dict[str, Any]) -> Dict[str, Any]:
        """Formata resposta de documento individual"""
        try:
            # Criar cópia do documento
            formatted_doc = document.copy()
            
            # Adicionar informações derivadas
            if 'extracted_data' in formatted_doc:
                formatted_doc['extraction_summary'] = self._create_extraction_summary(
                    formatted_doc['extracted_data']
                )
            
            return formatted_doc
            
        except Exception as e:
            logger.error(f"Erro ao formatar documento: {e}")
            return document
    
    def _format_aggregations(self, aggs: Dict[str, Any]) -> Dict[str, Any]:
        """Formata agregações para resposta"""
        try:
            formatted = {}
            
            for key, agg in aggs.items():
                if 'buckets' in agg:
                    formatted[key] = [
                        {'key': bucket['key'], 'doc_count': bucket['doc_count']}
                        for bucket in agg['buckets']
                    ]
                elif 'value' in agg:
                    formatted[key] = agg['value']
                elif key == 'temas' and 'categorias' in agg:
                    # Tratar agregação nested de temas
                    formatted[key] = [
                        {'key': bucket['key'], 'doc_count': bucket['doc_count']}
                        for bucket in agg['categorias']['buckets']
                    ]
                elif key == 'lugares_principais' and 'locais' in agg:
                    # Tratar agregação nested de lugares
                    formatted[key] = [
                        {'key': bucket['key'], 'doc_count': bucket['doc_count']}
                        for bucket in agg['locais']['buckets']
                    ]
            
            return formatted
            
        except Exception as e:
            logger.error(f"Erro ao formatar agregações: {e}")
            return {}
    
    def _clean_document_for_response(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """Limpa documento para resposta da API"""
        try:
            # Remover campos muito grandes se não necessário
            if 'full_text' in doc and len(doc['full_text']) > 1000:
                # Manter apenas um preview do texto completo
                doc['full_text_preview'] = doc['full_text'][:500] + "..."
                del doc['full_text']
            
            # Limitar contextos em dados extraídos
            if 'extracted_data' in doc:
                extracted = doc['extracted_data']
                
                # Limitar contextos de temas
                if 'themes' in extracted:
                    for theme in extracted['themes']:
                        if 'context' in theme and isinstance(theme['context'], list):
                            theme['context'] = theme['context'][:2]  # Máximo 2 contextos
                
                # Limitar número de itens extraídos na resposta de lista
                for data_type in ['names', 'places', 'dates']:
                    if data_type in extracted and isinstance(extracted[data_type], list):
                        if len(extracted[data_type]) > 10:
                            extracted[data_type] = extracted[data_type][:10]
            
            return doc
            
        except Exception as e:
            logger.error(f"Erro ao limpar documento: {e}")
            return doc
    
    def _create_extraction_summary(self, extracted_data: Dict[str, Any]) -> Dict[str, Any]:
        """Cria resumo dos dados extraídos"""
        try:
            summary = {
                'total_items': 0,
                'confidence_avg': 0,
                'top_categories': []
            }
            
            total_items = 0
            confidence_scores = []
            
            # Contar itens e coletar scores de confiança
            for data_type in ['names', 'places', 'dates', 'themes']:
                if data_type in extracted_data:
                    items = extracted_data[data_type]
                    if isinstance(items, list):
                        total_items += len(items)
                        
                        # Coletar scores de confiança
                        for item in items:
                            if isinstance(item, dict):
                                if 'confidence' in item:
                                    confidence_scores.append(item['confidence'])
                                elif 'relevance_score' in item:
                                    confidence_scores.append(item['relevance_score'])
            
            summary['total_items'] = total_items
            
            if confidence_scores:
                summary['confidence_avg'] = round(
                    sum(confidence_scores) / len(confidence_scores), 2
                )
            
            # Top categorias de temas
            if 'themes' in extracted_data:
                themes = extracted_data['themes']
                if isinstance(themes, list):
                    sorted_themes = sorted(
                        themes, 
                        key=lambda x: x.get('relevance_score', 0), 
                        reverse=True
                    )
                    summary['top_categories'] = [
                        t['category'] for t in sorted_themes[:3]
                    ]
            
            return summary
            
        except Exception as e:
            logger.error(f"Erro ao criar resumo de extração: {e}")
            return {}
    
    def format_export_data(
        self, 
        documents: List[Dict[str, Any]], 
        format_type: str = "json"
    ) -> Any:
        """Formata dados para exportação"""
        try:
            if format_type == "csv":
                return self._format_for_csv(documents)
            elif format_type == "excel":
                return self._format_for_excel(documents)
            else:  # JSON
                return documents
                
        except Exception as e:
            logger.error(f"Erro ao formatar para exportação: {e}")
            return documents
    
    def _format_for_csv(self, documents: List[Dict[str, Any]]) -> List[Dict[str, str]]:
        """Formata documentos para CSV"""
        csv_rows = []
        
        for doc in documents:
            row = {
                'id': doc.get('id', ''),
                'titulo': doc.get('titulo', ''),
                'autor': doc.get('autor', ''),
                'descricao': doc.get('descricao', ''),
                'capitania': doc.get('capitania', ''),
                'data': doc.get('data', ''),
                'ano': str(doc.get('ano', '')),
                'tipo': doc.get('tipo', ''),
                'filename': doc.get('filename', '')
            }
            
            # Adicionar dados extraídos simplificados
            if 'extracted_data' in doc:
                extracted = doc['extracted_data']
                
                # Nomes extraídos (primeiros 3)
                names = extracted.get('names', [])
                if names:
                    row['nomes_extraidos'] = ', '.join([
                        n.get('full_name', '') for n in names[:3] if n.get('full_name')
                    ])
                
                # Lugares extraídos (primeiros 3)
                places = extracted.get('places', [])
                if places:
                    row['lugares_extraidos'] = ', '.join([
                        p.get('location', '') for p in places[:3] if p.get('location')
                    ])
                
                # Temas principais
                themes = extracted.get('themes', [])
                if themes:
                    row['temas_principais'] = ', '.join([
                        t.get('category', '') for t in themes[:3] if t.get('category')
                    ])
            
            csv_rows.append(row)
        
        return csv_rows
    
    def _format_for_excel(self, documents: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Formata documentos para Excel com múltiplas abas"""
        return {
            'documentos': self._format_for_csv(documents),
            'estatisticas': self._generate_export_stats(documents)
        }
    
    def _generate_export_stats(self, documents: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Gera estatísticas para exportação"""
        try:
            total_docs = len(documents)
            
            if total_docs == 0:
                return {}
            
            # Contar por tipo
            tipos_count = {}
            capitanias_count = {}
            anos = []
            
            for doc in documents:
                tipo = doc.get('tipo', 'Não especificado')
                tipos_count[tipo] = tipos_count.get(tipo, 0) + 1
                
                capitania = doc.get('capitania', 'Não especificado')
                capitanias_count[capitania] = capitanias_count.get(capitania, 0) + 1
                
                ano = doc.get('ano')
                if ano and isinstance(ano, (int, str)) and str(ano).isdigit():
                    anos.append(int(ano))
            
            stats = {
                'total_documentos': total_docs,
                'tipos_distribuicao': tipos_count,
                'capitanias_distribuicao': capitanias_count
            }
            
            if anos:
                stats['periodo'] = {
                    'inicio': min(anos),
                    'fim': max(anos),
                    'media': round(sum(anos) / len(anos), 1)
                }
            
            return stats
            
        except Exception as e:
            logger.error(f"Erro ao gerar estatísticas de exportação: {e}")
            return {}