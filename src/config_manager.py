"""
Gerenciador de Configurações
Carrega e gerencia todas as configurações JSON para extração de dados
"""

import json
import os
from pathlib import Path
from typing import Dict, List, Any
import logging

logger = logging.getLogger(__name__)

class ConfigManager:
    def __init__(self, config_dir: str = "config"):
        self.config_dir = Path(config_dir)
        self._date_config = None
        self._names_config = None
        self._places_config = None
        self._themes_config = None
        
        # Verificar se diretório existe
        if not self.config_dir.exists():
            raise FileNotFoundError(f"Diretório de configuração não encontrado: {config_dir}")
    
    def load_date_config(self) -> Dict[str, Any]:
        """Carrega configurações de datas"""
        if self._date_config is None:
            config_path = self.config_dir / "date_config.json"
            try:
                with open(config_path, 'r', encoding='utf-8') as f:
                    self._date_config = json.load(f)
                logger.info("Configurações de data carregadas com sucesso")
            except FileNotFoundError:
                logger.error(f"Arquivo de configuração de datas não encontrado: {config_path}")
                raise
            except json.JSONDecodeError as e:
                logger.error(f"Erro ao decodificar JSON de datas: {e}")
                raise
        
        return self._date_config
    
    def load_names_config(self) -> Dict[str, List[str]]:
        """Carrega configurações de nomes"""
        if self._names_config is None:
            config_path = self.config_dir / "names.json"
            try:
                with open(config_path, 'r', encoding='utf-8') as f:
                    self._names_config = json.load(f)
                logger.info(f"Configurações de nomes carregadas: {len(self._names_config.get('first_names', []))} primeiros nomes, {len(self._names_config.get('second_names', []))} sobrenomes")
            except FileNotFoundError:
                logger.error(f"Arquivo de configuração de nomes não encontrado: {config_path}")
                raise
            except json.JSONDecodeError as e:
                logger.error(f"Erro ao decodificar JSON de nomes: {e}")
                raise
        
        return self._names_config
    
    def load_places_config(self) -> List[Dict[str, str]]:
        """Carrega configurações de lugares"""
        if self._places_config is None:
            config_path = self.config_dir / "places.txt"
            try:
                places = []
                with open(config_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if line and ',' in line:
                            location, capitania = line.split(',', 1)
                            places.append({
                                'location': location.strip(),
                                'capitania': capitania.strip()
                            })
                
                self._places_config = places
                logger.info(f"Configurações de lugares carregadas: {len(places)} localidades")
            except FileNotFoundError:
                logger.error(f"Arquivo de configuração de lugares não encontrado: {config_path}")
                raise
        
        return self._places_config
    
    def load_themes_config(self) -> Dict[str, List[str]]:
        """Carrega configurações de temas"""
        if self._themes_config is None:
            config_path = self.config_dir / "themes.json"
            try:
                with open(config_path, 'r', encoding='utf-8') as f:
                    self._themes_config = json.load(f)
                logger.info(f"Configurações de temas carregadas: {len(self._themes_config)} categorias")
            except FileNotFoundError:
                logger.error(f"Arquivo de configuração de temas não encontrado: {config_path}")
                raise
            except json.JSONDecodeError as e:
                logger.error(f"Erro ao decodificar JSON de temas: {e}")
                raise
        
        return self._themes_config
    
    def get_all_configs(self) -> Dict[str, Any]:
        """Retorna todas as configurações carregadas"""
        return {
            'dates': self.load_date_config(),
            'names': self.load_names_config(),
            'places': self.load_places_config(),
            'themes': self.load_themes_config()
        }
    
    def reload_configs(self):
        """Recarrega todas as configurações"""
        self._date_config = None
        self._names_config = None
        self._places_config = None
        self._themes_config = None
        logger.info("Configurações recarregadas")
    
    def validate_configs(self) -> bool:
        """Valida se todas as configurações são válidas"""
        try:
            # Testar carregamento de todas as configurações
            date_config = self.load_date_config()
            names_config = self.load_names_config()
            places_config = self.load_places_config()
            themes_config = self.load_themes_config()
            
            # Validações básicas
            assert 'regex_patterns' in date_config, "regex_patterns não encontrado em date_config"
            assert 'century_map' in date_config, "century_map não encontrado em date_config"
            
            assert 'first_names' in names_config, "first_names não encontrado em names_config"
            assert 'second_names' in names_config, "second_names não encontrado em names_config"
            
            assert len(places_config) > 0, "Nenhum lugar configurado"
            assert len(themes_config) > 0, "Nenhum tema configurado"
            
            logger.info("Todas as configurações são válidas")
            return True
            
        except Exception as e:
            logger.error(f"Erro na validação das configurações: {e}")
            return False