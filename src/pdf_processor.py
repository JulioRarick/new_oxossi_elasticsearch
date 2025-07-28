"""
Processador de PDFs
Responsável pela extração de texto e metadados dos arquivos PDF
"""

import os
import logging
from pathlib import Path
from typing import Dict, Any, Optional
import PyPDF2
import pdfplumber
from io import BytesIO

logger = logging.getLogger(__name__)

class PDFProcessor:
    def __init__(self):
        self.max_file_size = 50 * 1024 * 1024  # 50MB
        self.timeout_seconds = 300  # 5 minutos
    
    def validate_pdf(self, pdf_path: str) -> bool:
        """Valida se o PDF pode ser processado"""
        try:
            pdf_file = Path(pdf_path)
            
            # Verificar se arquivo existe
            if not pdf_file.exists():
                logger.error(f"Arquivo não encontrado: {pdf_path}")
                return False
            
            # Verificar tamanho do arquivo
            file_size = pdf_file.stat().st_size
            if file_size > self.max_file_size:
                logger.warning(f"Arquivo muito grande ({file_size} bytes): {pdf_path}")
                return False
            
            if file_size == 0:
                logger.error(f"Arquivo vazio: {pdf_path}")
                return False
            
            # Verificar se é um PDF válido
            with open(pdf_path, 'rb') as file:
                try:
                    pdf_reader = PyPDF2.PdfReader(file)
                    if len(pdf_reader.pages) == 0:
                        logger.error(f"PDF sem páginas: {pdf_path}")
                        return False
                except Exception as e:
                    logger.error(f"PDF corrompido {pdf_path}: {e}")
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"Erro na validação do PDF {pdf_path}: {e}")
            return False
    
    def extract_text(self, pdf_path: str) -> str:
        """Extrai texto do PDF usando múltiplas estratégias"""
        if not self.validate_pdf(pdf_path):
            raise ValueError(f"PDF inválido: {pdf_path}")
        
        # Tentar com pdfplumber primeiro (melhor qualidade)
        text = self._extract_with_pdfplumber(pdf_path)
        
        # Se falhar, tentar com PyPDF2
        if not text or len(text.strip()) < 100:
            logger.warning(f"pdfplumber falhou para {pdf_path}, tentando PyPDF2")
            text = self._extract_with_pypdf2(pdf_path)
        
        # Limpar e normalizar texto
        text = self._clean_text(text)
        
        if not text or len(text.strip()) < 50:
            raise ValueError(f"Não foi possível extrair texto significativo de {pdf_path}")
        
        logger.info(f"Texto extraído de {pdf_path}: {len(text)} caracteres")
        return text
    
    def _extract_with_pdfplumber(self, pdf_path: str) -> str:
        """Extrai texto usando pdfplumber"""
        try:
            text_parts = []
            
            with pdfplumber.open(pdf_path) as pdf:
                for page_num, page in enumerate(pdf.pages):
                    try:
                        page_text = page.extract_text()
                        if page_text:
                            text_parts.append(page_text)
                    except Exception as e:
                        logger.warning(f"Erro na página {page_num + 1} de {pdf_path}: {e}")
                        continue
            
            return '\n\n'.join(text_parts)
            
        except Exception as e:
            logger.error(f"Erro com pdfplumber em {pdf_path}: {e}")
            return ""
    
    def _extract_with_pypdf2(self, pdf_path: str) -> str:
        """Extrai texto usando PyPDF2"""
        try:
            text_parts = []
            
            with open(pdf_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                
                for page_num, page in enumerate(pdf_reader.pages):
                    try:
                        page_text = page.extract_text()
                        if page_text:
                            text_parts.append(page_text)
                    except Exception as e:
                        logger.warning(f"Erro na página {page_num + 1} de {pdf_path}: {e}")
                        continue
            
            return '\n\n'.join(text_parts)
            
        except Exception as e:
            logger.error(f"Erro com PyPDF2 em {pdf_path}: {e}")
            return ""
    
    def _clean_text(self, text: str) -> str:
        """Limpa e normaliza o texto extraído"""
        if not text:
            return ""
        
        # Remover caracteres de controle
        text = ''.join(char for char in text if ord(char) >= 32 or char in '\n\t')
        
        # Normalizar quebras de linha
        text = text.replace('\r\n', '\n').replace('\r', '\n')
        
        # Remover linhas em branco excessivas
        lines = text.split('\n')
        cleaned_lines = []
        empty_line_count = 0
        
        for line in lines:
            line = line.strip()
            if line:
                cleaned_lines.append(line)
                empty_line_count = 0
            else:
                empty_line_count += 1
                if empty_line_count <= 2:  # Máximo 2 linhas vazias consecutivas
                    cleaned_lines.append('')
        
        # Juntar linhas e normalizar espaços
        text = '\n'.join(cleaned_lines)
        
        # Normalizar espaços múltiplos mas preservar quebras de linha
        lines = text.split('\n')
        normalized_lines = []
        for line in lines:
            # Normalizar apenas espaços dentro da linha
            normalized_line = ' '.join(line.split())
            normalized_lines.append(normalized_line)
        
        text = '\n'.join(normalized_lines)
        
        return text
    
    def extract_metadata(self, pdf_path: str) -> Dict[str, Any]:
        """Extrai metadados do PDF - nome atualizado"""
        return self.get_metadata(pdf_path)
    
    def get_metadata(self, pdf_path: str) -> Dict[str, Any]:
        """Extrai metadados do PDF"""
        metadata = {
            'filename': Path(pdf_path).name,
            'file_size': 0,
            'page_count': 0,
            'title': '',
            'author': '',
            'subject': '',
            'creator': '',
            'creation_date': None,
            'modification_date': None
        }
        
        try:
            # Metadados do arquivo
            file_stats = Path(pdf_path).stat()
            metadata['file_size'] = file_stats.st_size
            
            # Metadados do PDF
            with open(pdf_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                metadata['page_count'] = len(pdf_reader.pages)
                
                # Informações do documento
                if pdf_reader.metadata:
                    pdf_metadata = pdf_reader.metadata
                    metadata['title'] = str(pdf_metadata.get('/Title', '')) if pdf_metadata.get('/Title') else ''
                    metadata['author'] = str(pdf_metadata.get('/Author', '')) if pdf_metadata.get('/Author') else ''
                    metadata['subject'] = str(pdf_metadata.get('/Subject', '')) if pdf_metadata.get('/Subject') else ''
                    metadata['creator'] = str(pdf_metadata.get('/Creator', '')) if pdf_metadata.get('/Creator') else ''
                    
                    # Datas (podem estar em formatos diferentes)
                    creation_date = pdf_metadata.get('/CreationDate')
                    if creation_date:
                        metadata['creation_date'] = str(creation_date)
                    
                    mod_date = pdf_metadata.get('/ModDate')
                    if mod_date:
                        metadata['modification_date'] = str(mod_date)
        
        except Exception as e:
            logger.warning(f"Erro ao extrair metadados de {pdf_path}: {e}")
        
        return metadata
    
    def get_text_statistics(self, text: str) -> Dict[str, Any]:
        """Calcula estatísticas do texto extraído"""
        if not text:
            return {
                'char_count': 0,
                'word_count': 0,
                'line_count': 0,
                'paragraph_count': 0
            }
        
        words = text.split()
        lines = text.split('\n')
        paragraphs = [p.strip() for p in text.split('\n\n') if p.strip()]
        
        return {
            'char_count': len(text),
            'word_count': len(words),
            'line_count': len(lines),
            'paragraph_count': len(paragraphs),
            'avg_words_per_line': len(words) / len(lines) if lines else 0,
            'avg_chars_per_word': len(text) / len(words) if words else 0
        }
    
    def extract_text_by_page(self, pdf_path: str) -> List[str]:
        """Extrai texto página por página"""
        if not self.validate_pdf(pdf_path):
            raise ValueError(f"PDF inválido: {pdf_path}")
        
        pages = []
        
        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page_num, page in enumerate(pdf.pages):
                    try:
                        page_text = page.extract_text() or ""
                        pages.append(self._clean_text(page_text))
                    except Exception as e:
                        logger.warning(f"Erro na página {page_num + 1}: {e}")
                        pages.append("")
        
        except Exception as e:
            logger.error(f"Erro ao extrair páginas de {pdf_path}: {e}")
            raise
        
        return pages