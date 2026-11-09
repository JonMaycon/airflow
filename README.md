# Airflow - DAG de Exemplo

Projeto de estudo com Apache Airflow rodando via Docker Compose, contendo uma DAG de exemplo (`dags/dag_1.py`) que demonstra o uso básico de operadores, dependências e configuração padrão de tarefas.

## Estrutura do projeto

```
airflow/
├── dags/              # DAGs do Airflow
│   └── dag_1.py
├── config/            # Configurações do Airflow
├── plugins/           # Plugins customizados
├── logs/              # Logs de execução (gerados em runtime)
└── docker-compose.yaml
```

## DAG: `dag_1.py`

### Imports

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
```

- `datetime` e `timedelta`: usados para definir a data de início da DAG e o intervalo entre tentativas de retry.
- `DAG`: classe principal do Airflow, usada para declarar a DAG.
- `EmptyOperator`: operador "vazio", usado como marcador de início/fim do fluxo, sem executar nenhuma ação.
- `PythonOperator`: executa uma função Python.
- `BashOperator`: executa um comando shell.

### Função da tarefa Python

```python
def greetings():
    print("Hello, Airflow! Joniel - Engenheiro de Dados")
```

Função simples que imprime uma mensagem no log da tarefa. É chamada pela task `greeting_task`.

### Argumentos padrão (`defalt_args`)

```python
defalt_args = {
    "owner": "JMelo",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}
```

Dicionário com os argumentos padrão aplicados a todas as tasks da DAG:
- `owner`: responsável pela DAG.
- `retries`: número de tentativas em caso de falha da task.
- `retry_delay`: tempo de espera entre cada tentativa (5 minutos).

### Declaração da DAG

```python
with DAG(
    dag_id="dag_1",
    description="DAG de exemplo para demonstrar o uso do Airflow",
    tags=["first_dag", "airflow"],
    schedule_interval="@daily",
    start_date=datetime(2026, 11, 8),
    catchup=False,
    default_args=defalt_args,
) as dag:
```

- `dag_id`: identificador único da DAG no Airflow.
- `description`: descrição exibida na interface web.
- `tags`: etiquetas usadas para filtrar DAGs na UI.
- `schedule_interval`: frequência de execução (`@daily` = uma vez por dia).
- `start_date`: data a partir da qual a DAG passa a ser agendada.
- `catchup`: quando `False`, evita a execução retroativa de todas as execuções perdidas desde o `start_date`.
- `default_args`: aplica o dicionário `defalt_args` a todas as tasks.

### Tasks

```python
start_execution = EmptyOperator(task_id="start_execution")

bash_command = BashOperator(
    task_id="bash_command",
    bash_command="echo 'Hello, Airflow!'"
)

greeting_task = PythonOperator(
    task_id="greetings",
    python_callable=greetings
)

finish_execution = EmptyOperator(task_id="finish_execution")
```

- `start_execution`: marca o início do fluxo (não executa nada).
- `bash_command`: executa o comando shell `echo 'Hello, Airflow!'`.
- `greeting_task`: executa a função Python `greetings`.
- `finish_execution`: marca o fim do fluxo (não executa nada).

### Dependências entre tasks

```python
#executar as tarefas em paralelo:
start_execution >> [bash_command >> greeting_task] >> finish_execution
```

Define a ordem de execução das tasks:
1. `start_execution` é executada primeiro.
2. Em seguida, `bash_command` executa e depois `greeting_task` (sequencial entre si).
3. Por fim, `finish_execution` é executada ao término do bloco anterior.

Há também uma linha comentada com a alternativa de execução puramente sequencial:

```python
#start_execution >> bash_command >> greeting_task >> finish_execution
```

## Como executar

Com o Docker e o Docker Compose instalados:

```bash
docker-compose up -d
```

A interface web do Airflow ficará disponível em `http://localhost:8080`.
