"""
Processador Principal
Orquestra o processamento completo de PDFs para Elasticsearch,
enriquecendo os dados com um arquivo JSON de metadados.
"""

import os
import sys
import time
import asyncio
import json
import requests
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any
import logging
from tqdm import tqdm

# Adicionar diretório raiz ao path para importações corretas
sys.path.append(str(Path(__file__).parent.parent))

from src.config_manager import ConfigManager
from src.pdf_processor import PDFProcessor
from src.data_extractor import DataExtractor
from src.elasticsearch_manager import ElasticsearchManager

# Configurar logging
# Criar diretório de logs se não existir
log_dir = Path('logs')
log_dir.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_dir / 'processing.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

class DocumentProcessor:
    def __init__(self, config_dir: str = "config", pdf_dir: str = "src/pdfs", source_json_path: str = "scraped_items.json"):
        """Inicializa o processador de documentos"""
        self.config_dir = config_dir
        self.pdf_dir = Path(pdf_dir)
        self.source_json_path = Path(source_json_path)
        self.source_data = []

        # Criar diretório de PDFs se não existir
        self.pdf_dir.mkdir(exist_ok=True)
        
        # Inicializar componentes
        try:
            self.config_manager = ConfigManager(config_dir)
            self.pdf_processor = PDFProcessor()
            self.data_extractor = DataExtractor(self.config_manager)
            self.es_manager = ElasticsearchManager()
            
            logger.info("Processador de documentos inicializado com sucesso")
            
        except Exception as e:
            logger.error(f"Erro na inicialização: {e}")
            raise
        
        # Estatísticas de processamento
        self.stats = {
            'total_files': 0,
            'processed': 0,
            'errors': 0,
            'skipped': 0,
            'start_time': None,
            'end_time': None,
            'errors_detail': []
        }

    def _load_source_data(self):
        """Carrega os metadados do arquivo JSON de origem."""
        if not self.source_json_path.exists():
            logger.warning(f"Arquivo de metadados não encontrado: {self.source_json_path}")
            return
        
        try:
            with open(self.source_json_path, 'r', encoding='utf-8') as f:
                self.source_data = json.load(f)
            logger.info(f"{len(self.source_data)} registros carregados de {self.source_json_path}")
        except json.JSONDecodeError as e:
            logger.error(f"Erro ao decodificar o JSON de {self.source_json_path}: {e}")
        except Exception as e:
            logger.error(f"Erro ao ler o arquivo {self.source_json_path}: {e}")

    def _download_pdf(self, url: str, destination_path: Path) -> bool:
        """Baixa um PDF de uma URL para um caminho de destino."""
        if destination_path.exists():
            logger.info(f"PDF já existe localmente: {destination_path.name}")
            return True
        
        try:
            logger.info(f"Baixando PDF de: {url}")
            response = requests.get(url, timeout=60, stream=True, headers={'User-Agent': 'Mozilla/5.0'})
            response.raise_for_status()
            
            with open(destination_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            logger.info(f"PDF salvo em: {destination_path.name}")
            return True
        except requests.exceptions.RequestException as e:
            logger.error(f"Erro ao baixar PDF de {url}: {e}")
            return False
        except Exception as e:
            logger.error(f"Erro inesperado ao salvar PDF {destination_path.name}: {e}")
            return False

    def setup(self, force_recreate_index: bool = False) -> bool:
        """Configura o ambiente para processamento"""
        try:
            self.es_manager.create_index(force_recreate=force_recreate_index)
            self._load_source_data()
            return True
        except Exception as e:
            logger.error(f"Erro durante o setup: {e}")
            return False

    def process_single_pdf_file(self, pdf_path: Path) -> None:
        """Processa um único arquivo PDF local sem download"""
        pdf_filename = pdf_path.name
        try:
            if not self.pdf_processor.validate_pdf(str(pdf_path)):
                self.stats['skipped'] += 1
                self.stats['errors_detail'].append({pdf_filename: "PDF inválido ou corrompido"})
                logger.warning(f"PDF inválido, pulando: {pdf_filename}")
                return

            # Extração de texto e metadados do PDF
            text = self.pdf_processor.extract_text(str(pdf_path))
            metadata = self.pdf_processor.extract_metadata(str(pdf_path))
            
            if not text or len(text) < 100:
                self.stats['skipped'] += 1
                self.stats['errors_detail'].append({pdf_filename: "Texto extraído é muito curto ou vazio"})
                logger.warning(f"Texto extraído de {pdf_filename} é muito curto, pulando.")
                return

            # Extração de dados estruturados do texto
            extracted_data = self.data_extractor.extract_all(text)

            # Montagem do documento para indexação (sem dados do JSON)
            document = {
                "id_original": pdf_filename.replace('.pdf', ''),
                "nome_arquivo": pdf_filename,
                "titulo": metadata.get('title', 'N/A') or pdf_filename.replace('.pdf', ''),
                "autor": metadata.get('author', 'N/A') or 'Desconhecido',
                "ano_publicacao": None,
                "url_origem": None,
                "link_pdf": f"/pdfs/{pdf_filename}",
                "texto_completo": text,
                "dados_extraidos": extracted_data,
                "metadados_pdf": metadata,
                "data_processamento": datetime.utcnow().isoformat()
            }

            # Indexar o documento
            doc_id = pdf_filename.replace('.pdf', '').replace(' ', '_')
            self.es_manager.index_document(document, doc_id=doc_id)
            self.stats['processed'] += 1
            logger.info(f"Documento processado com sucesso: {pdf_filename}")
            
        except Exception as e:
            self.stats['errors'] += 1
            self.stats['errors_detail'].append({pdf_filename: str(e)})
            logger.error(f"Erro ao processar {pdf_filename}: {e}", exc_info=True)

    async def _process_single_pdf(self, pdf_path: Path, source_item: Dict[str, Any]) -> None:
        """Processa um único arquivo PDF e o indexa no Elasticsearch."""
        pdf_filename = pdf_path.name
        try:
            if not self.pdf_processor.validate_pdf(str(pdf_path)):
                self.stats['skipped'] += 1
                self.stats['errors_detail'].append({pdf_filename: "PDF inválido ou corrompido"})
                logger.warning(f"PDF inválido, pulando: {pdf_filename}")
                return

            # Extração de texto e metadados do PDF
            text = self.pdf_processor.extract_text(str(pdf_path))
            metadata = self.pdf_processor.extract_metadata(str(pdf_path))
            
            if not text or len(text) < 100:
                self.stats['skipped'] += 1
                self.stats['errors_detail'].append({pdf_filename: "Texto extraído é muito curto ou vazio"})
                logger.warning(f"Texto extraído de {pdf_filename} é muito curto, pulando.")
                return

            # Extração de dados estruturados do texto
            extracted_data = self.data_extractor.extract_all(text)

            # Montagem do documento para indexação
            source_id = source_item.get('_id', {}).get('$oid', pdf_filename)
            
            document = {
                "id_original": source_id,
                "nome_arquivo": pdf_filename,
                "titulo": source_item.get('titulo', metadata.get('title', 'N/A')),
                "autor": source_item.get('autor', metadata.get('author', 'N/A')),
                "ano_publicacao": source_item.get('ano_publicacao'),
                "url_origem": source_item.get('url'),
                "link_pdf": source_item.get('pdf_links'),
                "texto_completo": text,
                "dados_extraidos": extracted_data,
                "metadados_pdf": metadata,
                "data_processamento": datetime.utcnow().isoformat()
            }

            # Indexar o documento
            self.es_manager.index_document(document, doc_id=source_id)
            self.stats['processed'] += 1
            
        except Exception as e:
            self.stats['errors'] += 1
            self.stats['errors_detail'].append({pdf_filename: str(e)})
            logger.error(f"Erro ao processar {pdf_filename}: {e}", exc_info=True)

    def process_local_pdfs(self, batch_size: int = 10) -> None:
        """Processa todos os PDFs locais na pasta sem usar JSON"""
        self.stats['start_time'] = time.time()
        
        # Encontrar todos os PDFs na pasta
        pdf_files = list(self.pdf_dir.glob("*.pdf"))
        
        if not pdf_files:
            logger.warning(f"Nenhum arquivo PDF encontrado em {self.pdf_dir}")
            return

        self.stats['total_files'] = len(pdf_files)
        logger.info(f"Encontrados {len(pdf_files)} arquivos PDF para processar")
        
        # Processar em lotes para feedback
        for i in range(0, len(pdf_files), batch_size):
            batch_files = pdf_files[i:i+batch_size]
            
            for pdf_path in tqdm(batch_files, desc=f"Processando lote {i//batch_size + 1}"):
                self.process_single_pdf_file(pdf_path)

        self.stats['end_time'] = time.time()
        self.print_stats()

    async def run_processing(self, batch_size: int = 10, max_workers: int = 4) -> None:
        """Executa o processamento em lote dos PDFs a partir da fonte JSON."""
        self.stats['start_time'] = time.time()
        
        if not self.source_data:
            logger.warning("Nenhum dado de origem encontrado no arquivo JSON. Tentando processar PDFs locais.")
            self.process_local_pdfs(batch_size)
            return

        self.stats['total_files'] = len(self.source_data)
        
        # Usar um semáforo para limitar a concorrência
        semaphore = asyncio.Semaphore(max_workers)

        async def process_with_semaphore(item):
            async with semaphore:
                pdf_url = item.get("pdf_links")
                source_id = item.get('_id', {}).get('$oid')

                if not pdf_url or not source_id:
                    logger.warning(f"Item sem 'pdf_links' ou '_id' válido, pulando: {item.get('titulo')}")
                    self.stats['skipped'] += 1
                    return

                # Define um nome de arquivo único e determinístico
                file_extension = Path(pdf_url.split('?')[0]).suffix or '.pdf'
                if not file_extension.lower() == '.pdf':
                    file_extension = '.pdf' # Garante que a extensão seja .pdf
                
                pdf_path = self.pdf_dir / f"{source_id}{file_extension}"

                if self._download_pdf(pdf_url, pdf_path):
                    await self._process_single_pdf(pdf_path, item)
                else:
                    self.stats['errors'] += 1
                    self.stats['errors_detail'].append({item.get('titulo', 'N/A'): f"Falha no download de {pdf_url}"})

        # Processar em lotes para melhor gerenciamento de memória e feedback
        for i in range(0, len(self.source_data), batch_size):
            batch_items = self.source_data[i:i+batch_size]
            tasks = [process_with_semaphore(item) for item in batch_items]
            
            # Usar tqdm para a barra de progresso
            for f in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc=f"Processando lote {i//batch_size + 1}"):
                await f

        self.stats['end_time'] = time.time()
        self.print_stats()

    def print_stats(self) -> None:
        """Imprime as estatísticas finais do processamento."""
        duration = self.stats['end_time'] - self.stats['start_time']
        logger.info("\n--- Estatísticas de Processamento ---")
        logger.info(f"Tempo total: {duration:.2f} segundos")
        logger.info(f"Total de arquivos: {self.stats['total_files']}")
        logger.info(f"Processados com sucesso: {self.stats['processed']}")
        logger.info(f"Com erros: {self.stats['errors']}")
        logger.info(f"Ignorados (inválidos/sem texto): {self.stats['skipped']}")
        if self.stats['errors'] > 0:
            logger.warning("Detalhes dos erros:")
            for error in self.stats['errors_detail'][:10]:  # Limitar a 10 erros
                logger.warning(f" - {error}")
        logger.info("--- Fim das Estatísticas ---\n")


async def main():
    """Função principal"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Processador de Documentos Históricos')
    parser.add_argument('--recreate-index', action='store_true', 
                       help='Recriar o índice Elasticsearch')
    parser.add_argument('--local-only', action='store_true',
                       help='Processar apenas PDFs locais (sem JSON)')
    parser.add_argument('--batch-size', type=int, default=10,
                       help='Tamanho do lote para processamento')
    parser.add_argument('--max-workers', type=int, default=4,
                       help='Número máximo de workers concorrentes')
    
    args = parser.parse_args()
    
    processor = DocumentProcessor()
    
    if processor.setup(force_recreate_index=args.recreate_index):
        if args.local_only:
            processor.process_local_pdfs(batch_size=args.batch_size)
        else:
            await processor.run_processing(
                batch_size=args.batch_size, 
                max_workers=args.max_workers
            )

if __name__ == "__main__":
    asyncio.run(main())