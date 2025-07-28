"""
API Principal - FastAPI
Aplicação principal da API de busca de documentos históricos
"""

import os
import sys
import time
import logging
from typing import List, Optional, Dict, Any
from pathlib import Path

from fastapi import FastAPI, Query, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

# Adicionar diretório raiz ao path
sys.path.append(str(Path(__file__).parent.parent))

from api.services.search_service import SearchService
from api.utils.response_formatter import ResponseFormatter

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Criar aplicação FastAPI
app = FastAPI(
    title="Historical Documents Search API",
    description="API para busca de documentos históricos brasileiros",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Configurar CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Em produção, especificar domínios
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["*"],
)

# Modelos Pydantic
class SearchRequest(BaseModel):
    query: Optional[str] = Field(None, description="Texto de busca")
    filters: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Filtros de busca")
    page: int = Field(1, ge=1, description="Número da página")
    size: int = Field(20, ge=1, le=100, description="Itens por página")
    sort: str = Field("relevance", description="Campo de ordenação")
    order: str = Field("desc", pattern="^(asc|desc)$", description="Direção da ordenação")
    include_aggregations: bool = Field(False, description="Incluir agregações na resposta")

class SearchResponse(BaseModel):
    results: List[Dict[str, Any]]
    total: int
    page: int
    size: int
    total_pages: int
    execution_time_ms: int
    aggregations: Optional[Dict[str, Any]] = None

class DocumentResponse(BaseModel):
    document: Dict[str, Any]

class FiltersResponse(BaseModel):
    autores: List[Dict[str, Any]]
    capitanias: List[Dict[str, Any]]
    tipos: List[Dict[str, Any]]
    anos: Dict[str, Any]

class StatsResponse(BaseModel):
    total_documentos: int
    periodo: Dict[str, Any]
    tipos_documento: List[Dict[str, Any]]
    capitanias: List[Dict[str, Any]]
    media_paginas: float
    tamanho_total_mb: float

class HealthResponse(BaseModel):
    status: str
    checks: Dict[str, Any]
    timestamp: float

# Instância global do serviço de busca
search_service = SearchService()

# Middleware para logging de requests
@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = time.time()
    
    response = await call_next(request)
    
    process_time = time.time() - start_time
    logger.info(
        f"{request.method} {request.url.path} - "
        f"Status: {response.status_code} - "
        f"Time: {process_time:.3f}s"
    )
    
    return response

# Exception handlers
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Erro não tratado em {request.url.path}: {exc}")
    return JSONResponse(
        status_code=500,
        content={"detail": "Erro interno do servidor"}
    )

# Endpoints da API

@app.get("/", tags=["Root"])
async def root():
    """Endpoint raiz da API"""
    return {
        "message": "Historical Documents Search API",
        "version": "1.0.0",
        "docs": "/docs"
    }

@app.get("/health", response_model=HealthResponse, tags=["Health"])
async def health_check():
    """Verifica saúde do sistema"""
    checks = {
        "api": "healthy",
        "elasticsearch": await search_service.check_elasticsearch_health()
    }
    
    overall_status = "healthy" if all(
        status == "healthy" for status in checks.values()
    ) else "unhealthy"
    
    return HealthResponse(
        status=overall_status,
        checks=checks,
        timestamp=time.time()
    )

@app.get("/api/search/simple", response_model=SearchResponse, tags=["Search"])
async def simple_search(
    q: str = Query(..., description="Texto de busca"),
    page: int = Query(1, ge=1, description="Número da página"),
    size: int = Query(20, ge=1, le=100, description="Itens por página"),
    sort: str = Query("relevance", pattern="^(relevance|date|title|author)$", description="Campo de ordenação"),
    order: str = Query("desc", pattern="^(asc|desc)$", description="Direção da ordenação")
):
    """Busca simples por texto"""
    start_time = time.time()
    
    try:
        results = await search_service.simple_search(q, page, size, sort, order)
        
        execution_time = int((time.time() - start_time) * 1000)
        
        return SearchResponse(
            results=results['hits'],
            total=results['total'],
            page=page,
            size=size,
            total_pages=(results['total'] + size - 1) // size,
            execution_time_ms=execution_time
        )
    except Exception as e:
        logger.error(f"Erro na busca simples: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/search/advanced", response_model=SearchResponse, tags=["Search"])
async def advanced_search(request: SearchRequest):
    """Busca avançada com filtros"""
    start_time = time.time()
    
    try:
        results = await search_service.advanced_search(
            request.query,
            request.filters,
            request.page,
            request.size,
            request.sort,
            request.order,
            request.include_aggregations
        )
        
        execution_time = int((time.time() - start_time) * 1000)
        
        return SearchResponse(
            results=results['hits'],
            total=results['total'],
            page=request.page,
            size=request.size,
            total_pages=(results['total'] + request.size - 1) // request.size,
            execution_time_ms=execution_time,
            aggregations=results.get('aggregations')
        )
    except Exception as e:
        logger.error(f"Erro na busca avançada: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/search/autocomplete", tags=["Search"])
async def autocomplete(
    field: str = Query(..., pattern="^(autor|titulo|capitania|tipo)$", description="Campo para autocomplete"),
    q: str = Query(..., min_length=1, description="Texto para sugestões"),
    limit: int = Query(10, ge=1, le=50, description="Número máximo de sugestões")
):
    """Autocomplete para campos específicos"""
    try:
        suggestions = await search_service.autocomplete(field, q, limit)
        return {"suggestions": suggestions}
    except Exception as e:
        logger.error(f"Erro no autocomplete: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/documents/{doc_id}", response_model=DocumentResponse, tags=["Documents"])
async def get_document(doc_id: str):
    """Retorna documento específico por ID"""
    try:
        document = await search_service.get_document(doc_id)
        if not document:
            raise HTTPException(status_code=404, detail="Documento não encontrado")
        return DocumentResponse(document=document)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao buscar documento: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/search/filters", response_model=FiltersResponse, tags=["Search"])
async def get_filters():
    """Retorna todos os filtros disponíveis para busca avançada"""
    try:
        filters = await search_service.get_available_filters()
        return FiltersResponse(**filters)
    except Exception as e:
        logger.error(f"Erro ao buscar filtros: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/stats", response_model=StatsResponse, tags=["Statistics"])
async def get_stats():
    """Retorna estatísticas gerais do acervo"""
    try:
        stats = await search_service.get_collection_stats()
        return StatsResponse(**stats)
    except Exception as e:
        logger.error(f"Erro ao buscar estatísticas: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/search/suggest", tags=["Search"])
async def search_suggestions(
    q: str = Query(..., min_length=2, description="Texto para sugestões"),
    limit: int = Query(5, ge=1, le=20, description="Número de sugestões")
):
    """Sugestões de busca baseadas no conteúdo indexado"""
    try:
        suggestions = await search_service.get_search_suggestions(q, limit)
        return {"suggestions": suggestions}
    except Exception as e:
        logger.error(f"Erro nas sugestões de busca: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/documents", tags=["Documents"])
async def list_documents(
    page: int = Query(1, ge=1, description="Número da página"),
    size: int = Query(20, ge=1, le=100, description="Itens por página"),
    sort: str = Query("filename", description="Campo de ordenação")
):
    """Lista todos os documentos com paginação"""
    try:
        results = await search_service.list_all_documents(page, size, sort)
        
        return {
            "documents": results['hits'],
            "total": results['total'],
            "page": page,
            "size": size,
            "total_pages": (results['total'] + size - 1) // size
        }
    except Exception as e:
        logger.error(f"Erro ao listar documentos: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/documents/batch", tags=["Documents"])
async def get_documents_batch(document_ids: List[str]):
    """Retorna múltiplos documentos por IDs"""
    try:
        if len(document_ids) > 100:
            raise HTTPException(
                status_code=400, 
                detail="Máximo de 100 documentos por requisição"
            )
        
        documents = await search_service.get_documents_batch(document_ids)
        return {"documents": documents}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro na busca em lote: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Endpoints administrativos (podem ser protegidos em produção)

@app.post("/api/admin/reindex", tags=["Admin"])
async def trigger_reindex():
    """Dispara reindexação de todos os documentos"""
    try:
        # Este endpoint pode ser usado para disparar reprocessamento
        # Em produção, deve ser protegido com autenticação
        result = await search_service.trigger_reindex()
        return {"message": "Reindexação iniciada", "task_id": result.get("task_id")}
    except Exception as e:
        logger.error(f"Erro ao iniciar reindexação: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/admin/index-stats", tags=["Admin"])
async def get_index_statistics():
    """Retorna estatísticas detalhadas do índice"""
    try:
        stats = await search_service.get_detailed_index_stats()
        return stats
    except Exception as e:
        logger.error(f"Erro ao buscar estatísticas do índice: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/admin/documents/{doc_id}", tags=["Admin"])
async def delete_document(doc_id: str):
    """Remove um documento do índice"""
    try:
        success = await search_service.delete_document(doc_id)
        if not success:
            raise HTTPException(status_code=404, detail="Documento não encontrado")
        return {"message": f"Documento {doc_id} removido com sucesso"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao deletar documento: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Startup e shutdown events

@app.on_event("startup")
async def startup_event():
    """Evento executado na inicialização da API"""
    logger.info("Iniciando Historical Documents Search API...")
    
    try:
        # Verificar conexão com Elasticsearch
        health = await search_service.check_elasticsearch_health()
        if health != "healthy":
            logger.error("Elasticsearch não está saudável!")
        else:
            logger.info("Conexão com Elasticsearch estabelecida")
        
        # Outras inicializações se necessário
        logger.info("API iniciada com sucesso")
        
    except Exception as e:
        logger.error(f"Erro na inicialização: {e}")

@app.on_event("shutdown")
async def shutdown_event():
    """Evento executado no encerramento da API"""
    logger.info("Encerrando Historical Documents Search API...")
    
    try:
        # Cleanup se necessário
        await search_service.cleanup()
        logger.info("API encerrada com sucesso")
        
    except Exception as e:
        logger.error(f"Erro no encerramento: {e}")

# Rotas de desenvolvimento (remover em produção)
if os.getenv("ENVIRONMENT") == "development":
    
    @app.get("/api/dev/test-data", tags=["Development"])
    async def create_test_data():
        """Cria dados de teste para desenvolvimento"""
        try:
            result = await search_service.create_test_documents()
            return {"message": "Dados de teste criados", "count": result}
        except Exception as e:
            logger.error(f"Erro ao criar dados de teste: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.get("/api/dev/clear-index", tags=["Development"])
    async def clear_index():
        """Limpa todos os documentos do índice"""
        try:
            result = await search_service.clear_all_documents()
            return {"message": "Índice limpo", "deleted_count": result}
        except Exception as e:
            logger.error(f"Erro ao limpar índice: {e}")
            raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    
    # Configurações para desenvolvimento
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )