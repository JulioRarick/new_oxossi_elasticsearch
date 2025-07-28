import sys
from pathlib import Path
import logging

# Adicionar diretório raiz ao path para importações corretas
sys.path.append(str(Path(__file__).parent.parent))

from src.elasticsearch_manager import ElasticsearchManager

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_index_with_mapping(force_recreate: bool = False):
    """
    Cria o índice no Elasticsearch com um mapeamento detalhado e otimizado.
    """
    try:
        es_manager = ElasticsearchManager()
        
        # Definição do analisador customizado para português
        settings = {
            "analysis": {
                "analyzer": {
                    "brazilian_analyzer": {
                        "type": "custom",
                        "tokenizer": "standard",
                        "filter": [
                            "lowercase",
                            "brazilian_stop",
                            "brazilian_stemmer"
                        ]
                    }
                },
                "filter": {
                    "brazilian_stop": {
                        "type": "stop",
                        "stopwords": "_brazilian_"
                    },
                    "brazilian_stemmer": {
                        "type": "stemmer",
                        "language": "brazilian"
                    }
                }
            }
        }

        # Definição do mapeamento dos campos do documento
        mappings = {
            "properties": {
                "id_original": {"type": "keyword"},
                "nome_arquivo": {"type": "keyword"},
                # Campos adicionados do JSON
                "titulo": {
                    "type": "text", 
                    "analyzer": "brazilian_analyzer",
                    "fields": {"keyword": {"type": "keyword"}}
                },
                "autor": {
                    "type": "text", 
                    "analyzer": "brazilian_analyzer",
                    "fields": {"keyword": {"type": "keyword"}}
                },
                "ano_publicacao": {"type": "keyword"},
                "url_origem": {"type": "keyword"},
                "link_pdf": {"type": "keyword"},
                # Campos originais
                "texto_completo": {"type": "text", "analyzer": "brazilian_analyzer"},
                "data_processamento": {"type": "date"},
                "dados_extraidos": {
                    "properties": {
                        "datas": {
                            "properties": {
                                "anos_encontrados": {"type": "integer"},
                                "seculos": {"type": "keyword"}
                            }
                        },
                        "nomes": {
                            "type": "nested",
                            "properties": {
                                "nome_completo": {
                                    "type": "text",
                                    "analyzer": "brazilian_analyzer",
                                    "fields": {"keyword": {"type": "keyword"}}
                                }
                            }
                        },
                        "locais": {
                            "type": "nested",
                            "properties": {
                                "local": {"type": "keyword"},
                                "capitania": {"type": "keyword"}
                            }
                        },
                        "temas": {
                            "type": "nested",
                            "properties": {
                                "tema": {"type": "keyword"},
                                "categoria": {"type": "keyword"}
                            }
                        }
                    }
                },
                "metadados_pdf": {
                    "properties": {
                        "author": {"type": "text"},
                        "creationDate": {"type": "date"},
                        "title": {"type": "text"}
                    }
                }
            }
        }

        logger.info("Tentando criar/recriar o índice...")
        if es_manager.create_index_with_mapping(settings, mappings, force_recreate=force_recreate):
            logger.info(f"Índice '{es_manager.index_name}' criado/atualizado com sucesso.")
        else:
            logger.warning(f"Índice '{es_manager.index_name}' já existia e não foi recriado. Use --force para recriar.")

    except Exception as e:
        logger.error(f"Ocorreu um erro ao criar o índice: {e}", exc_info=True)

if __name__ == "__main__":
    force = "--force" in sys.argv
    create_index_with_mapping(force_recreate=force)
