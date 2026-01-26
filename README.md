# Airflow ETL – Projeto de Portfólio

## Visão Geral
Projeto de engenharia de dados utilizando Apache Airflow para orquestração de pipelines ETL, Postgres como banco de dados e Docker para padronização do ambiente. O objetivo é demonstrar boas práticas de organização, versionamento, automação e observabilidade, com foco em portfólio profissional (LinkedIn/GitHub).

## Tecnologias Utilizadas
- Apache Airflow 2.9.0
- Python 3.10
- PostgreSQL 15
- Docker e Docker Compose
- Pandas
- Requests
- psycopg2-binary
- GitHub Actions (CI)
- Black e Flake8 (qualidade de código)

## Estrutura do Projeto
- dags/: DAGs do Airflow (orquestração)
- dags/sql/: scripts SQL versionados
  - raw/
  - staging/
  - analytics/
- etl/: código Python de extract, transform e load
- docker/: infraestrutura Docker (docker-compose.yml, Dockerfile)
- .github/workflows/: pipelines de CI
- requirements.txt: dependências de runtime
- requirements-dev.txt: dependências de desenvolvimento
- README.md

## Funcionalidades Implementadas
- Criação automática de schemas e tabelas no Postgres
- Execução de SQL versionado via Airflow
- Separação de camadas (raw, staging, analytics)
- Uso de template_searchpath para SQL
- Checks pós-criação (validação de existência de schemas/tabelas)
- Ambiente reproduzível com Docker
- CI para lint, parsing de DAGs e validações básicas

## Boas Práticas Aplicadas
- Infraestrutura como código
- Versionamento de SQL
- Separação de dependências (runtime vs dev)
- Padrões profissionais de DAG
- Código pronto para evolução incremental
- Projeto estruturado para portfólio

## Como Executar Localmente
1. Clonar o repositório
2. Entrar na pasta docker
3. Subir o Postgres e inicializar o Airflow:
   docker compose --profile init up airflow-init
4. Subir os serviços:
   docker compose up -d
5. Acessar o Airflow em http://localhost:8080

## CI / Qualidade
- Lint com flake8
- Formatação com black
- Parsing de DAGs no CI
- Instalação via requirements-dev.txt

## Próximos Passos
- Tasks de extract usando APIs reais
- Transformações com Pandas
- Data quality checks
- Documentação de métricas
- Evolução para ambiente cloud

## Autor
Projeto desenvolvido para fins de estudo e portfólio em Engenharia de Dados.
