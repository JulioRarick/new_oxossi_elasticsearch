#!/bin/bash

# Script para iniciar Elasticsearch com configurações adequadas
echo "Iniciando Elasticsearch para desenvolvimento..."

# Verificar se Docker está instalado
if ! command -v docker &> /dev/null; then
    echo "Docker não está instalado. Por favor, instale o Docker primeiro."
    exit 1
fi

# Verificar se Docker está rodando
if ! docker info &> /dev/null; then
    echo "Docker não está rodando. Iniciando..."
    sudo systemctl start docker
fi

# Parar container existente se estiver rodando
if docker ps -q --filter "name=elasticsearch-dev" | grep -q .; then
    echo "Parando container Elasticsearch existente..."
    docker stop elasticsearch-dev
    docker rm elasticsearch-dev
fi

# Criar rede se não existir
docker network create elastic-net 2>/dev/null || true

echo "Iniciando Elasticsearch..."

# Iniciar Elasticsearch com configurações para desenvolvimento
docker run -d \
    --name elasticsearch-dev \
    --net elastic-net \
    -p 9200:9200 \
    -p 9300:9300 \
    -e "discovery.type=single-node" \
    -e "xpack.security.enabled=false" \
    -e "xpack.security.enrollment.enabled=false" \
    -e "xpack.security.http.ssl.enabled=false" \
    -e "xpack.security.transport.ssl.enabled=false" \
    -e "ES_JAVA_OPTS=-Xms1g -Xmx1g" \
    -e "cluster.name=historical-docs-cluster" \
    -e "node.name=es-node-dev" \
    -v es-data-dev:/usr/share/elasticsearch/data \
    docker.elastic.co/elasticsearch/elasticsearch:8.11.0

echo "Aguardando Elasticsearch inicializar..."

# Aguardar Elasticsearch estar pronto
max_attempts=30
attempt=1

while [ $attempt -le $max_attempts ]; do
    if curl -s http://localhost:9200 >/dev/null 2>&1; then
        echo "✓ Elasticsearch está pronto!"
        
        # Mostrar informações do cluster
        echo
        echo "=== Informações do Cluster ==="
        curl -s http://localhost:9200 | jq '.' 2>/dev/null || curl -s http://localhost:9200
        echo
        echo "=== Health do Cluster ==="
        curl -s http://localhost:9200/_cluster/health | jq '.' 2>/dev/null || curl -s http://localhost:9200/_cluster/health
        echo
        echo
        echo "Elasticsearch disponível em: http://localhost:9200"
        echo "Para parar: docker stop elasticsearch-dev"
        echo "Para logs: docker logs -f elasticsearch-dev"
        
        exit 0
    fi
    
    echo "Tentativa $attempt/$max_attempts - aguardando..."
    sleep 2
    ((attempt++))
done

echo "❌ Timeout: Elasticsearch não iniciou dentro do tempo esperado"
echo "Verificando logs do container..."
docker logs elasticsearch-dev
exit 1