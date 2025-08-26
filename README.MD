# 🤖 Automação VR/VA

Sistema automatizado para processamento mensal de Vale Refeição, aplicando todas as regras de negócio e sindicatos.

## 📁 Estrutura do Projeto

```
projeto-vr-automation/
│
├── src/                          # Código fonte
│   ├── __init__.py
│   └── vr_automation.py          # Lógica principal
│
├── data/                         # Arquivos Excel de entrada
│   ├── ADMISSÃO ABRIL.xlsx
│   ├── AFASTAMENTOS.xlsx
│   ├── APRENDIZ.xlsx
│   ├── ATIVOS.xlsx
│   ├── Base dias uteis.xlsx
│   ├── Base sindicato x valor.xlsx
│   ├── DESLIGADOS.xlsx
│   ├── ESTÁGIO.xlsx
│   ├── EXTERIOR.xlsx
│   └── FÉRIAS.xlsx
│
├── output/                       # Arquivos gerados
│
├── main.py                       # Script de execução
├── requirements.txt              # Dependências
└── README.md                     # Este arquivo
```

## 🚀 Instalação

1. **Clone ou baixe o projeto**
2. **Instale as dependências:**

   ```bash
   pip install -r requirements.txt
   ```

3. **Coloque os arquivos Excel na pasta `data/`**

## ▶️ Como Executar

### Execução simples:

```bash
python main.py
```

### Para alterar competência, edite no `main.py`:

```python
MES_COMPETENCIA = 6   # Junho
ANO_COMPETENCIA = 2025
```

## 📋 Arquivos Necessários na pasta `data/`

| Arquivo                       | Descrição                     |
| ----------------------------- | ----------------------------- |
| `ADMISSÃO ABRIL.xlsx`         | Funcionários admitidos no mês |
| `AFASTAMENTOS.xlsx`           | Licenças e afastamentos       |
| `APRENDIZ.xlsx`               | Base de aprendizes            |
| `ATIVOS.xlsx`                 | Funcionários ativos           |
| `Base dias uteis.xlsx`        | Dias úteis por sindicato      |
| `Base sindicato x valor.xlsx` | Valores por estado            |
| `DESLIGADOS.xlsx`             | Funcionários desligados       |
| `ESTÁGIO.xlsx`                | Base de estagiários           |
| `EXTERIOR.xlsx`               | Funcionários no exterior      |
| `FÉRIAS.xlsx`                 | Funcionários em férias        |

## ⚙️ Regras Implementadas

### ❌ **Exclusões Automáticas:**

- Diretores (cargo contém "DIRETOR")
- Estagiários e Aprendizes
- Afastados (licenças)
- Funcionários no exterior

### 📅 **Regras de Desligamento:**

- **Até dia 15**: Se comunicado = "OK", não pagar
- **Após dia 15**: Pagar proporcional

### 💰 **Cálculo Financeiro:**

- **Base**: Dias úteis do sindicato
- **Descontos**: Férias
- **Proporcionais**: Admissões/demissões no meio do mês
- **Rateio**: 80% empresa / 20% funcionário

## 📊 Saída

O sistema gera na pasta `output/`:

- **VR_MENSAL_MM_AAAA.xlsx**: Planilha final para envio à operadora

## 🔍 Validações

✅ Dias válidos (não negativos)  
✅ Valores consistentes  
✅ Rateio correto (80%/20%)  
✅ Matrículas únicas

## 🛠️ Solução de Problemas

**Erro "Pasta data não encontrada"**

- Criar pasta `data/` e colocar os arquivos Excel

**Erro ao carregar arquivo**

- Verificar se arquivos não estão abertos no Excel
- Conferir nomes exatos dos arquivos

**Validações falharam**

- Conferir dados de entrada
- Verificar regras de negócio aplicadas

## 📈 Performance

- ⏱️ **Tempo**: ~10-30 segundos (1000 funcionários)
- 💾 **Memória**: ~50-100MB
- 📁 **Saída**: ~100-500KB

## 🎯 Próximos Passos

1. Colocar arquivos na pasta `data/`
2. Executar `python main.py`
3. Conferir arquivo gerado em `output/`
4. Enviar para operadora

---

_Sistema desenvolvido para automação do processo mensal de VR/VA_
