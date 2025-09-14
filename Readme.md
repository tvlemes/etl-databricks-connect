# 📚 ETL com Databricks Connect 


Este repositório contém:
- 📄 Um guia com comandos úteis para manipular arquivos no **Databricks File System (DBFS)**.
- 📓 Notebooks prontos para uso que ajudam na criação de tabelas, limpeza de cache e configuração de catálogos, criação de uma arquitetura medalhão com as camadas Bronze, Silver e Gold.

---


[![Python](https://img.shields.io/badge/Python-3.11%2B-blue.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](https://github.com/tvlemes/etl-databricks-connect/blob/main/LICENSE)
[![Status](https://img.shields.io/badge/status-Concluído-green.svg)]()


## 📂 Estrutura do Repositório

├── .databricks/\
├── .venv\
├── .vscode\
├── dev/ # ambiente de desenvolvimento\
├── docs\
├── prod/ # ambiente de produção \
├── typings/\
├── .env \
├── .gitignore\
├── databricks.yml # arquivo responsável pelas configurações do Databricks\
├── Readme.md \
└── requeriments.txt

---

## 🔧 Pré-requisitos

Antes de utilizar os notebooks e os comandos deste repositório:

- Tenha o **Databricks Connect** instalado.
- Instale o Databricks CLI e configure um profile válido:
```bash
databricks configure --token
```
Crie um ambiente virtual atráves da ferramenta do **Databricks Connect** e logo em seguida, dentro do ambiente criado rode o comando:
```
pip install -r requirements.txt
```
Esse comando irá instalar as bibliotecas necessárias para os notebooks.

---

📓 Notebooks

Todos os notebooks estão na pasta dev/notebooks.

| Notebook | Descrição |
|:---:|:---:|
| drop_catalog.ipynb | Apaga o catalogo do Databricks. | 
| clear_cache.ipynb | Limpa o cache do Spark. | 
| create_catatoq.ipynb	| Configuração e criação do catálogo e schemas no Databricks para organização de dados. |
| create_bronze_table.ipynb | Criação da tabela Bronze no Data Lake usando Spark. |
| create_silver_table.ipynb | Criação da tabela Silver no Data Lake usando Spark. |
| create_catatoq.ipynb	| Configuração e criação do catálogo e schemas no Databricks para organização de dados. |

--- 

## 📜 Comandos Importantes
### ✅ Validação do Código
```
databricks bundle validate

# ou caso não encontre o profile

databricks bundle validate --profile <e-mail_databricks_ou_token_url> --debug
```

### 📦 Deploy para os Ambientes
```
# Deploy para DEV
databricks bundle deploy --target dev 

# ou caso não encontre o profile 

databricks bundle deploy --target dev --profile <e-mail_databricks_ou_token_url> --debug

# Deploy para PROD
databricks bundle deploy --target prod

# ou caso não encontre o profile

databricks bundle deploy --target prod --profile <e-mail_databricks_ou_token_url> --debug
```

### 🔄 Sincronização de Arquivos
```
databricks bundle sync

# ou caso não encontre o profile

databricks bundle sync --target dev --profile <e-mail_databricks_ou_token_url> --debug
```

💡 Dica:
Na primeira vez que for sincronizar, utilize o databricks_connect.
Sempre que for sincronizar, altere o target do databricks_connect para o ambiente correto (dev ou prod).\
A tag `--debug` é para mostrar os passos do deploy.

---

## 🧹 Arquivos Ignorados no Git

O arquivo `.gitignore` está configurado para ignorar arquivos para o **Databricks**:
```
.databricks
typings
.gitignore
Readme.md
prod/
databricks.yml
.databricks.env
.venv
.env_example
.env
anotações
test.ipynb
```

---

## 🛠️ Troubleshooting (Erros Comuns)

Aqui estão alguns problemas comuns e como resolvê-los:
### ❌ `Metadata Service returned empty token`
Esse erro indica que o Databricks CLI não conseguiu autenticar automaticamente.
### ✅ Solução: Reconfigure o token manualmente:
```
databricks configure --token
```

--- 

### ❌ `multiple profiles matched: ... please set DATABRICKS_CONFIG_PROFILE`
O CLI encontrou múltiplos perfis no seu arquivo `~/.databrickscfg`.
### ✅ Solução: Defina o perfil explicitamente antes de rodar os comandos:
```
set DATABRICKS_CONFIG_PROFILE=nome_do_perfil  # Windows PowerShell
export DATABRICKS_CONFIG_PROFILE=nome_do_perfil  # Linux/MacOS
```

---

### ❌ `ModuleNotFoundError: No module named 'app'`
Se ocorrer ao rodar testes no VSCode, certifique-se de que o PYTHONPATH está configurado corretamente.
### ✅ Solução: No launch.json, adicione:
```
"env": {
  "PYTHONPATH": "${workspaceFolder}"
}
```

---

### ❌ `Problemas de Sincronização`

* Verifique se o `databricks bundle sync` está usando o target correto (dev ou prod).
* Sempre rode `databricks bundle validate` antes de sincronizar para detectar problemas de configuração.

## 👨‍💻 Sobre

Autor: Thiago Vilarinho Lemes <br>
Home: https://thiagolemes.netlify.app/ \
LinkedIn <a href="https://www.linkedin.com/in/thiago-v-lemes-b1232727" target="_blank">Thiago Lemes</a><br>
e-mail: contatothiagolemes@gmail.com | lemes_vilarinho@yahoo.com.br