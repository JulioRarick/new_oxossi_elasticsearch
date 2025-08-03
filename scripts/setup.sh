#!/bin/bash

# Script de Setup do Sistema de Documentos Históricos
# Este script configura o ambiente completo para desenvolvimento e produção

set -e  # Exit on any error

echo "=== Setup do Sistema de Documentos Históricos ==="
echo "Data: $(date)"
echo

# Cores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Função para imprimir mensagens coloridas
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Verificar se está sendo executado como root
if [[ $EUID -eq 0 ]]; then
   print_error "Este script não deve ser executado como root"
   exit 1
fi

# Função para verificar dependências
check_dependencies() {
    print_status "Verificando dependências..."
    
    local deps=("python3" "pip3" "docker" "docker-compose" "curl" "git")
    local missing_deps=()
    
    for dep in "${deps[@]}"; do
        if ! command -v "$dep" &> /dev/null; then
            missing_deps+=("$dep")
        fi
    done
    
    if [ ${#missing_deps[@]} -ne 0 ]; then
        print_error "Dependências não encontradas: ${missing_deps[*]}"
        print_status "Por favor, instale as dependências antes de continuar:"
        echo "  Ubuntu/Debian: sudo apt update && sudo apt install python3 python3-pip docker.io docker-compose curl git"
        echo "  CentOS/RHEL: sudo yum install python3 python3-pip docker docker-compose curl git"
        echo "  macOS: brew install python3 docker docker-compose curl git"
        exit 1
    fi
    
    print_success "Todas as dependências estão instaladas"
}

# Função para verificar versões
check_versions() {
    print_status "Verificando versões..."
    
    # Python version
    PYTHON_VERSION=$(python3 --version | cut -d' ' -f2)
    if ! python3 -c "import sys; exit(0 if sys.version_info >= (3, 8) else 1)"; then
        print_error "Python 3.8+ é necessário. Versão atual: $PYTHON_VERSION"
        exit 1
    fi
    print_success "Python $PYTHON_VERSION ✓"
    
    # Docker version
    DOCKER_VERSION=$(docker --version | cut -d' ' -f3 | tr -d ',')
    print_success "Docker $DOCKER_VERSION ✓"
    
    # Docker Compose version
    COMPOSE_VERSION=$(docker compose --version | cut -d' ' -f3 | tr -d ',')
    print_success "Docker Compose $COMPOSE_VERSION ✓"
}

# Função para criar estrutura de diretórios
create_directories() {
    print_status "Criando estrutura de diretórios..."
    
    local dirs=(
        "src/pdfs"
        "config"
        "logs"
        "data/elasticsearch"
        "data/backups"
        "static"
        "nginx"
        "scripts"
    )
    
    for dir in "${dirs[@]}"; do
        mkdir -p "$dir"
        print_success "Diretório criado: $dir"
    done
}

# Função para criar arquivo .env
create_env_file() {
    print_status "Criando arquivo de configuração .env..."
    
    if [ ! -f .env ]; then
        cat > .env << EOF
# Configurações do Elasticsearch
ELASTICSEARCH_HOST=localhost
ELASTICSEARCH_PORT=9200
ELASTICSEARCH_INDEX=historical_documents

# Configurações da API
API_HOST=0.0.0.0
API_PORT=8000
LOG_LEVEL=INFO
ENVIRONMENT=development

# Configurações de Processamento
PDF_DIR=src/pdfs
CONFIG_DIR=config
BATCH_SIZE=10
MAX_WORKERS=4

# Configurações do Frontend
NEXT_PUBLIC_API_URL=http://localhost:8000

# Configurações de Backup
BACKUP_DIR=data/backups
BACKUP_RETENTION_DAYS=30
EOF
        print_success "Arquivo .env criado"
    else
        print_warning "Arquivo .env já existe"
    fi
}

# Função para instalar dependências Python
install_python_deps() {
    print_status "Instalando dependências Python..."
    
    # Criar ambiente virtual se não existir
    if [ ! -d "venv" ]; then
        print_status "Criando ambiente virtual Python..."
        python3 -m venv venv
        print_success "Ambiente virtual criado"
    fi
    
    # Ativar ambiente virtual
    source venv/bin/activate
    
    # Atualizar pip
    pip install --upgrade pip
    
    # Instalar dependências
    if [ -f "../requirements.txt" ]; then
        pip install -r ../requirements.txt
        print_success "Dependências Python instaladas"
    else
        print_error "Arquivo requirements.txt não encontrado"
        exit 1
    fi
}

# Função para verificar arquivos de configuração
check_config_files() {
    print_status "Verificando arquivos de configuração..."
    
    local config_files=(
        "config/date_config.json"
        "config/names.json"
        "config/places.txt"
        "config/themes.json"
    )
    
    local missing_configs=()
    
    for config in "${config_files[@]}"; do
        if [ ! -f "$config" ]; then
            missing_configs+=("$config")
        fi
    done
    
    if [ ${#missing_configs[@]} -ne 0 ]; then
        print_error "Arquivos de configuração não encontrados: ${missing_configs[*]}"
        print_status "Por favor, copie os arquivos de configuração fornecidos para o diretório config/"
        exit 1
    fi
    
    print_success "Todos os arquivos de configuração estão presentes"
}

# Função para configurar Docker
setup_docker() {
    print_status "Configurando Docker..."
    
    # Verificar se Docker está rodando
    if ! docker info &> /dev/null; then
        print_error "Docker não está rodando. Por favor, inicie o serviço Docker:"
        echo "  sudo systemctl start docker"
        exit 1
    fi
    
    # Verificar se usuário está no grupo docker
    if ! groups | grep -q docker; then
        print_warning "Usuário não está no grupo docker. Adicionando..."
        sudo usermod -aG docker "$USER"
        print_warning "Por favor, faça logout e login novamente para aplicar as mudanças"
    fi
    
    print_success "Docker configurado"
}

# Função para iniciar Elasticsearch
start_elasticsearch() {
    print_status "Iniciando Elasticsearch..."
    
    # Verificar se já está rodando
    if curl -s http://localhost:9200 &> /dev/null; then
        print_warning "Elasticsearch já está rodando"
        return 0
    fi
    
    # Iniciar com Docker Compose
    docker-compose up -d elasticsearch
    
    print_status "Aguardando Elasticsearch inicializar..."
    local max_attempts=30
    local attempt=1
    
    while [ $attempt -le $max_attempts ]; do
        if curl -s http://localhost:9200/_cluster/health &> /dev/null; then
            print_success "Elasticsearch iniciado com sucesso"
            return 0
        fi
        
        echo -n "."
        sleep 2
        ((attempt++))
    done
    
    print_error "Timeout aguardando Elasticsearch"
    exit 1
}

# Função para criar índice Elasticsearch
create_elasticsearch_index() {
    print_status "Criando índice no Elasticsearch..."
    
    # Ativar ambiente virtual
    source venv/bin/activate
    
    # Executar script de criação de índice
    if python scripts/create_index.py; then
        print_success "Índice criado com sucesso"
    else
        print_error "Falha ao criar índice"
        exit 1
    fi
}

# Função para executar testes
run_tests() {
    print_status "Executando testes básicos..."
    
    source venv/bin/activate
    
    # Testar importação dos módulos principais
    if python -c "
from src.config_manager import ConfigManager
from src.pdf_processor import PDFProcessor
from src.data_extractor import DataExtractor
from src.elasticsearch_manager import ElasticsearchManager
print('✓ Todos os módulos importados com sucesso')
"; then
        print_success "Testes de importação passaram"
    else
        print_error "Falha nos testes de importação"
        exit 1
    fi
    
    # Testar conexão com Elasticsearch
    if python -c "
from src.elasticsearch_manager import ElasticsearchManager
es = ElasticsearchManager()
health = es.health_check()
print(f'✓ Elasticsearch health: {health.get(\"connection\", \"unknown\")}')
"; then
        print_success "Teste de conexão Elasticsearch passou"
    else
        print_error "Falha no teste de conexão Elasticsearch"
        exit 1
    fi
}

# Função para criar script de inicialização da API
create_api_start_script() {
    print_status "Criando script de inicialização da API..."
    
    cat > scripts/start-api.sh << 'EOF'
#!/bin/bash

# Script de inicialização da API
echo "Iniciando Historical Documents API..."

# Ativar ambiente virtual se existir
if [ -d "venv" ]; then
    source venv/bin/activate
fi

# Aguardar Elasticsearch estar disponível
echo "Verificando conexão com Elasticsearch..."
while ! curl -s http://${ELASTICSEARCH_HOST:-localhost}:${ELASTICSEARCH_PORT:-9200}/_cluster/health > /dev/null; do
    echo "Aguardando Elasticsearch..."
    sleep 5
done

echo "Elasticsearch disponível, iniciando API..."

# Iniciar API
exec uvicorn api.app:app \
    --host ${API_HOST:-0.0.0.0} \
    --port ${API_PORT:-8000} \
    --log-level ${LOG_LEVEL:-info} \
    --reload
EOF
    
    chmod +x scripts/start-api.sh
    print_success "Script de inicialização da API criado"
}

# Função para criar configuração do Nginx
create_nginx_config() {
    print_status "Criando configuração do Nginx..."
    
    cat > nginx/default.conf << 'EOF'
server {
    listen 80;
    server_name localhost;
    
    # API
    location /api/ {
        proxy_pass http://api:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_read_timeout 300s;
        proxy_connect_timeout 75s;
    }
    
    # Health check
    location /health {
        proxy_pass http://api:8000/health;
        proxy_set_header Host $host;
    }
    
    # Servir PDFs
    location /pdfs/ {
        alias /usr/share/nginx/html/pdfs/;
        add_header Access-Control-Allow-Origin *;
        add_header Cache-Control "public, max-age=31536000";
        expires 1y;
    }
    
    # Servir arquivos estáticos
    location /static/ {
        alias /usr/share/nginx/html/static/;
        add_header Cache-Control "public, max-age=86400";
        expires 1d;
    }
    
    # Documentação da API
    location /docs {
        proxy_pass http://api:8000/docs;
        proxy_set_header Host $host;
    }
    
    location /redoc {
        proxy_pass http://api:8000/redoc;
        proxy_set_header Host $host;
    }
    
    # Root - pode servir frontend ou redirecionar para docs
    location / {
        return 301 /docs;
    }
}
EOF
    
    print_success "Configuração do Nginx criada"
}

# Função principal de setup
main() {
    print_status "Iniciando setup do sistema..."
    
    # Verificações iniciais
    check_dependencies
    check_versions
    
    # Configuração do ambiente
    create_directories
    create_env_file
    check_config_files
    
    # Instalação de dependências
    install_python_deps
    
    # Configuração Docker
    setup_docker
    
    # Elasticsearch
    start_elasticsearch
    create_elasticsearch_index
    
    # Scripts e configurações
    create_api_start_script
    create_nginx_config
    
    # Testes
    run_tests
    
    print_success "Setup concluído com sucesso!"
    echo
    echo "=== Próximos Passos ==="
    echo "1. Coloque seus arquivos PDF em: src/pdfs/"
    echo "2. Para processar PDFs: python src/main_processor.py"
    echo "3. Para iniciar API: ./scripts/start-api.sh"
    echo "4. Para ambiente completo: docker-compose up"
    echo "5. Acesse a documentação: http://localhost:8000/docs"
    echo
    echo "Para mais informações, consulte o README.md"
}

# Verificar argumentos
if [[ "$1" == "--help" || "$1" == "-h" ]]; then
    echo "Uso: $0 [opções]"
    echo
    echo "Opções:"
    echo "  --help, -h          Mostra esta ajuda"
    echo "  --skip-tests        Pula os testes de verificação"
    echo "  --no-elasticsearch  Não inicia Elasticsearch automaticamente"
    echo "  --production        Configura para ambiente de produção"
    echo
    exit 0
fi

# Variáveis de controle baseadas em argumentos
SKIP_TESTS=false
NO_ELASTICSEARCH=false
PRODUCTION=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --skip-tests)
            SKIP_TESTS=true
            shift
            ;;
        --no-elasticsearch)
            NO_ELASTICSEARCH=true
            shift
            ;;
        --production)
            PRODUCTION=true
            shift
            ;;
        *)
            print_error "Opção desconhecida: $1"
            exit 1
            ;;
    esac
done

# Modificar função main para suportar argumentos
main() {
    print_status "Iniciando setup do sistema..."
    
    if [ "$PRODUCTION" = true ]; then
        print_status "Configurando para ambiente de produção..."
        export ENVIRONMENT=production
    fi
    
    # Verificações iniciais
    check_dependencies
    check_versions
    
    # Configuração do ambiente
    create_directories
    create_env_file
    check_config_files
    
    # Instalação de dependências
    install_python_deps
    
    # Configuração Docker
    setup_docker
    
    # Elasticsearch (se não for pulado)
    if [ "$NO_ELASTICSEARCH" = false ]; then
        start_elasticsearch
        create_elasticsearch_index
    fi
    
    # Scripts e configurações
    create_api_start_script
    
    # Testes (se não for pulado)
    if [ "$SKIP_TESTS" = false ]; then
        run_tests
    fi
    
    print_success "Setup concluído com sucesso!"
    echo
    echo "=== Próximos Passos ==="
    echo "1. Coloque seus arquivos PDF em: src/pdfs/"
    echo "2. Para processar PDFs: python src/main_processor.py"
    echo "3. Para iniciar API: ./scripts/start-api.sh"
    echo "4. Para ambiente completo: docker-compose up"
    echo "5. Acesse a documentação: http://localhost:8000/docs"
    echo
    
    if [ "$PRODUCTION" = true ]; then
        echo "=== Configuração de Produção ==="
        echo "- Configure SSL/TLS no Nginx"
        echo "- Ajuste variáveis de ambiente em .env"
        echo "- Configure backup automático"
        echo "- Monitore logs em logs/"
        echo
    fi
    
    echo "Para mais informações, consulte o README.md"
}

# Função de cleanup em caso de erro
cleanup() {
    print_error "Setup interrompido"
    
    # Parar containers se foram iniciados
    if docker-compose ps | grep -q "Up"; then
        print_status "Parando containers..."
        docker-compose down
    fi
    
    exit 1
}

# Configurar trap para cleanup
trap cleanup INT TERM

# Executar função principal
main "$@"
