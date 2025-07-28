"""
Extrator de Dados
Extrai informações estruturadas dos textos baseado nas configurações
"""

import re
import logging
from typing import Dict, List, Any, Tuple, Optional
from fuzzywuzzy import fuzz, process
from unidecode import unidecode
from config_manager import ConfigManager

logger = logging.getLogger(__name__)

class DataExtractor:
    def __init__(self, config_manager: ConfigManager = None):
        self.config_manager = config_manager or ConfigManager()
        self.date_config = self.config_manager.load_date_config()
        self.names_config = self.config_manager.load_names_config()
        self.places_config = self.config_manager.load_places_config()
        self.themes_config = self.config_manager.load_themes_config()
        
        # Compilar regex patterns
        self._compile_patterns()
        
        # Preparar listas para busca otimizada
        self._prepare_search_lists()
    
    def _compile_patterns(self):
        """Compila os padrões regex para melhor performance"""
        self.year_pattern = re.compile(
            self.date_config['regex_patterns']['year'], 
            re.IGNORECASE
        )
        self.textual_phrase_pattern = re.compile(
            self.date_config['regex_patterns']['textual_phrase'], 
            re.IGNORECASE
        )
    
    def _prepare_search_lists(self):
        """Prepara listas otimizadas para busca"""
        # Normalizar nomes para busca
        self.first_names_normalized = [
            unidecode(name.lower()) for name in self.names_config['first_names']
        ]
        self.second_names_normalized = [
            unidecode(name.lower()) for name in self.names_config['second_names']
        ]
        
        # Preparar lugares para busca fuzzy
        self.places_normalized = []
        for place in self.places_config:
            self.places_normalized.append({
                'original': place,
                'normalized': unidecode(place['location'].lower())
            })
    
    def extract_all(self, text: str) -> Dict[str, Any]:
        """Extrai todas as informações do texto"""
        logger.info("Iniciando extração de dados...")
        
        extracted = {
            'dates': self.extract_dates(text),
            'names': self.extract_names(text),
            'places': self.extract_places(text),
            'themes': self.classify_themes(text)
        }
        
        # Estatísticas de extração
        stats = {
            'total_dates': len(extracted['dates']),
            'total_names': len(extracted['names']),
            'total_places': len(extracted['places']),
            'total_themes': len(extracted['themes'])
        }
        
        logger.info(f"Extração concluída: {stats}")
        return extracted
    
    def extract_dates(self, text: str) -> List[Dict[str, Any]]:
        """Extrai datas do texto"""
        dates = []
        
        # Buscar anos específicos
        year_matches = self.year_pattern.finditer(text)
        for match in year_matches:
            year = int(match.group('year'))
            dates.append({
                'type': 'year',
                'year': year,
                'century': self._get_century_from_year(year),
                'original_text': match.group(0),
                'position': match.start(),
                'confidence': 0.9,
                'context': self._get_context(text, match.start(), match.end())
            })
        
        # Buscar frases textuais de séculos
        textual_matches = self.textual_phrase_pattern.finditer(text)
        for match in textual_matches:
            century_text = match.group('century')
            part_text = match.group('part') if match.group('part') else None
            
            # Mapear século para ano base
            century_year = self._map_century_to_year(century_text)
            if century_year:
                # Aplicar modificador de período se existir
                year_range = self._apply_period_modifier(century_year, part_text)
                
                dates.append({
                    'type': 'textual',
                    'year': year_range[0],  # Ano inicial do período
                    'year_end': year_range[1],  # Ano final do período
                    'century': self._get_century_from_year(year_range[0]),
                    'period': part_text,
                    'original_text': match.group(0),
                    'position': match.start(),
                    'confidence': 0.7,
                    'context': self._get_context(text, match.start(), match.end())
                })
        
        # Remover duplicatas e ordenar por posição
        dates = self._deduplicate_dates(dates)
        dates.sort(key=lambda x: x['position'])
        
        return dates
    
    def extract_names(self, text: str) -> List[Dict[str, Any]]:
        """Extrai nomes de pessoas do texto"""
        names = []
        
        # Padrão para identificar nomes: [Primeiro] [de/da/do/dos/das] [Sobrenome]
        name_pattern = r'\b([A-ZÁÀÂÃÉÊÍÓÔÕÚÇ][a-záàâãéêíóôõúç]+)(?:\s+(?:' + '|'.join(self.names_config['prepositions']) + r')\s+)?([A-ZÁÀÂÃÉÊÍÓÔÕÚÇ][a-záàâãéêíóôõúç]+(?:\s+[A-ZÁÀÂÃÉÊÍÓÔÕÚÇ][a-záàâãéêíóôõúç]+)*)'
        
        matches = re.finditer(name_pattern, text)
        
        for match in matches:
            potential_first = match.group(1)
            potential_last = match.group(2)
            
            # Verificar se primeiro nome está na lista
            first_confidence = self._check_name_confidence(
                potential_first, self.first_names_normalized
            )
            
            # Verificar se sobrenome está na lista
            last_confidence = self._check_name_confidence(
                potential_last, self.second_names_normalized
            )
            
            # Calcular confiança geral
            overall_confidence = (first_confidence + last_confidence) / 2
            
            # Só incluir se confiança for razoável
            if overall_confidence > 0.6:
                full_name = match.group(0)
                names.append({
                    'first_name': potential_first,
                    'last_name': potential_last,
                    'full_name': full_name,
                    'position': match.start(),
                    'confidence': overall_confidence,
                    'context': self._get_context(text, match.start(), match.end())
                })
        
        # Remover duplicatas
        names = self._deduplicate_names(names)
        names.sort(key=lambda x: x['confidence'], reverse=True)
        
        return names
    
    def extract_places(self, text: str) -> List[Dict[str, Any]]:
        """Extrai lugares do texto"""
        places = []
        text_normalized = unidecode(text.lower())
        
        for place_data in self.places_normalized:
            location = place_data['normalized']
            original_place = place_data['original']
            
            # Busca exata
            if location in text_normalized:
                start_pos = text_normalized.find(location)
                places.append({
                    'location': original_place['location'],
                    'capitania': original_place['capitania'],
                    'position': start_pos,
                    'confidence': 1.0,
                    'match_type': 'exact',
                    'context': self._get_context(text, start_pos, start_pos + len(location))
                })
            else:
                # Busca fuzzy para variações
                words = text_normalized.split()
                for i, word in enumerate(words):
                    similarity = fuzz.ratio(location, word)
                    if similarity > 80:  # 80% de similaridade
                        # Encontrar posição no texto original
                        word_start = text_normalized.find(word)
                        places.append({
                            'location': original_place['location'],
                            'capitania': original_place['capitania'],
                            'position': word_start,
                            'confidence': similarity / 100,
                            'match_type': 'fuzzy',
                            'context': self._get_context(text, word_start, word_start + len(word))
                        })
        
        # Remover duplicatas e ordenar por confiança
        places = self._deduplicate_places(places)
        places.sort(key=lambda x: x['confidence'], reverse=True)
        
        return places
    
    def classify_themes(self, text: str) -> List[Dict[str, Any]]:
        """Classifica temas do documento"""
        themes = []
        text_lower = text.lower()
        
        for category, keywords in self.themes_config.items():
            found_keywords = []
            keyword_positions = []
            
            for keyword in keywords:
                keyword_lower = keyword.lower()
                if keyword_lower in text_lower:
                    found_keywords.append(keyword)
                    # Encontrar todas as posições desta palavra-chave
                    start = 0
                    while True:
                        pos = text_lower.find(keyword_lower, start)
                        if pos == -1:
                            break
                        keyword_positions.append(pos)
                        start = pos + 1
            
            if found_keywords:
                # Calcular score de relevância
                relevance_score = self._calculate_theme_relevance(
                    found_keywords, len(text.split()), keyword_positions
                )
                
                themes.append({
                    'category': category,
                    'keywords_found': found_keywords,
                    'keyword_count': len(found_keywords),
                    'total_occurrences': len(keyword_positions),
                    'relevance_score': relevance_score,
                    'context': self._get_themes_context(text, keyword_positions[:3])  # Top 3 ocorrências
                })
        
        # Filtrar temas com score muito baixo e ordenar por relevância
        themes = [t for t in themes if t['relevance_score'] > 0.1]
        themes.sort(key=lambda x: x['relevance_score'], reverse=True)
        
        return themes
    
    def _get_century_from_year(self, year: int) -> str:
        """Converte ano para século"""
        if 1500 <= year <= 1599:
            return "XVI"
        elif 1600 <= year <= 1699:
            return "XVII"
        elif 1700 <= year <= 1799:
            return "XVIII"
        elif 1800 <= year <= 1899:
            return "XIX"
        else:
            return f"{(year // 100) + 1}"
    
    def _map_century_to_year(self, century_text: str) -> Optional[int]:
        """Mapeia texto de século para ano base"""
        century_text = century_text.lower()
        for key, year in self.date_config['century_map'].items():
            if key in century_text:
                return year
        return None
    
    def _apply_period_modifier(self, base_year: int, period_text: str) -> Tuple[int, int]:
        """Aplica modificador de período ao ano base"""
        if not period_text:
            return (base_year, base_year + 99)  # Século inteiro
        
        period_text = period_text.lower()
        
        for period, (start_pct, end_pct) in self.date_config['part_map'].items():
            if period in period_text:
                start_year = base_year + (start_pct * 99 // 100)
                end_year = base_year + (end_pct * 99 // 100)
                return (start_year, end_year)
        
        return (base_year, base_year + 99)
    
    def _check_name_confidence(self, name: str, name_list: List[str]) -> float:
        """Verifica confiança de um nome contra uma lista"""
        name_normalized = unidecode(name.lower())
        
        # Busca exata
        if name_normalized in name_list:
            return 1.0
        
        # Busca fuzzy
        best_match = process.extractOne(name_normalized, name_list)
        if best_match and best_match[1] > 80:
            return best_match[1] / 100
        
        return 0.0
    
    def _calculate_theme_relevance(self, keywords: List[str], total_words: int, positions: List[int]) -> float:
        """Calcula score de relevância do tema"""
        if not keywords or total_words == 0:
            return 0.0
        
        # Fatores para o cálculo:
        # 1. Número de palavras-chave únicas encontradas
        unique_keywords = len(set(keywords))
        
        # 2. Frequência total de ocorrências
        total_occurrences = len(positions)
        
        # 3. Densidade (ocorrências / total de palavras)
        density = total_occurrences / total_words
        
        # 4. Distribuição no texto (palavras-chave espalhadas são melhores)
        distribution_score = self._calculate_distribution_score(positions, total_words)
        
        # Combinar fatores
        relevance = (
            (unique_keywords * 0.3) +
            (min(total_occurrences / 10, 1.0) * 0.3) +
            (density * 100 * 0.2) +
            (distribution_score * 0.2)
        )
        
        return min(relevance, 1.0)
    
    def _calculate_distribution_score(self, positions: List[int], total_length: int) -> float:
        """Calcula score de distribuição das palavras-chave no texto"""
        if len(positions) <= 1:
            return 0.5
        
        # Normalizar posições para 0-1
        normalized_positions = [pos / total_length for pos in positions]
        
        # Calcular variância das posições
        mean_pos = sum(normalized_positions) / len(normalized_positions)
        variance = sum((pos - mean_pos) ** 2 for pos in normalized_positions) / len(normalized_positions)
        
        # Converter variância para score (maior variância = melhor distribuição)
        return min(variance * 4, 1.0)
    
    def _get_context(self, text: str, start: int, end: int, context_size: int = 100) -> str:
        """Extrai contexto ao redor de uma posição no texto"""
        context_start = max(0, start - context_size)
        context_end = min(len(text), end + context_size)
        
        context = text[context_start:context_end]
        
        # Adicionar indicadores se o contexto foi truncado
        if context_start > 0:
            context = "..." + context
        if context_end < len(text):
            context = context + "..."
        
        return context.strip()
    
    def _get_themes_context(self, text: str, positions: List[int], context_size: int = 50) -> List[str]:
        """Extrai contextos para múltiplas posições de temas"""
        contexts = []
        for pos in positions:
            context = self._get_context(text, pos, pos + 10, context_size)
            contexts.append(context)
        return contexts
    
    def _deduplicate_dates(self, dates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Remove datas duplicadas"""
        seen = set()
        unique_dates = []
        
        for date in dates:
            # Criar chave única baseada em ano e posição aproximada
            key = (date['year'], date['position'] // 50)  # Agrupamento por proximidade
            if key not in seen:
                seen.add(key)
                unique_dates.append(date)
        
        return unique_dates
    
    def _deduplicate_names(self, names: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Remove nomes duplicados"""
        seen = set()
        unique_names = []
        
        for name in names:
            # Normalizar nome para comparação
            key = unidecode(name['full_name'].lower())
            if key not in seen:
                seen.add(key)
                unique_names.append(name)
        
        return unique_names
    
    def _deduplicate_places(self, places: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Remove lugares duplicados"""
        seen = set()
        unique_places = []
        
        for place in places:
            key = (place['location'], place['capitania'])
            if key not in seen:
                seen.add(key)
                unique_places.append(place)
        
        return unique_places
    
    def get_extraction_summary(self, extracted_data: Dict[str, Any]) -> Dict[str, Any]:
        """Gera resumo da extração"""
        summary = {
            'dates': {
                'total': len(extracted_data['dates']),
                'years_found': [],
                'centuries': set(),
                'earliest_year': None,
                'latest_year': None
            },
            'names': {
                'total': len(extracted_data['names']),
                'high_confidence': 0,
                'unique_first_names': set(),
                'unique_last_names': set()
            },
            'places': {
                'total': len(extracted_data['places']),
                'capitanias': set(),
                'exact_matches': 0,
                'fuzzy_matches': 0
            },
            'themes': {
                'total': len(extracted_data['themes']),
                'categories': [],
                'top_theme': None
            }
        }
        
        # Processar datas
        if extracted_data['dates']:
            years = [d['year'] for d in extracted_data['dates'] if d.get('year')]
            summary['dates']['years_found'] = sorted(set(years))
            summary['dates']['centuries'] = set(d['century'] for d in extracted_data['dates'] if d.get('century'))
            summary['dates']['earliest_year'] = min(years) if years else None
            summary['dates']['latest_year'] = max(years) if years else None
        
        # Processar nomes
        for name in extracted_data['names']:
            if name['confidence'] > 0.8:
                summary['names']['high_confidence'] += 1
            summary['names']['unique_first_names'].add(name['first_name'])
            summary['names']['unique_last_names'].add(name['last_name'])
        
        # Processar lugares
        for place in extracted_data['places']:
            summary['places']['capitanias'].add(place['capitania'])
            if place['match_type'] == 'exact':
                summary['places']['exact_matches'] += 1
            else:
                summary['places']['fuzzy_matches'] += 1
        
        # Processar temas
        if extracted_data['themes']:
            summary['themes']['categories'] = [t['category'] for t in extracted_data['themes']]
            summary['themes']['top_theme'] = extracted_data['themes'][0]['category']
        
        # Converter sets para listas para serialização JSON
        summary['dates']['centuries'] = list(summary['dates']['centuries'])
        summary['names']['unique_first_names'] = list(summary['names']['unique_first_names'])
        summary['names']['unique_last_names'] = list(summary['names']['unique_last_names'])
        summary['places']['capitanias'] = list(summary['places']['capitanias'])
        
        return summary