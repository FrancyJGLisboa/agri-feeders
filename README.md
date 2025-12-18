# Agri Feeders - Dados Agrícolas Brasileiros

Dados históricos de soja e milho para municípios brasileiros (2000-2024), consolidados em formato JSON para uso em ferramentas de análise climática.

## 📊 Dataset

### Cobertura
- **4.108 municípios** brasileiros únicos
- **25 anos** de dados históricos (2000-2024)
- **Culturas**: soja e milho
- **148.463 registros** de área e produção
- **99.8% com coordenadas** (lat/lon)

### Fonte
Dados extraídos do IBGE/SIDRA (Sistema IBGE de Recuperação Automática):
- Tabela 5457 - PAM (Produção Agrícola Municipal)
- Tabela 1301 - Área Territorial dos Municípios

## 📁 Estrutura do Repositório

```
agri-feeders/
├── README.md                 # Este arquivo
├── data/
│   ├── ibge-data.json       # Dados consolidados (10MB)
│   └── metadata.json        # Metadados do dataset
├── scripts/
│   └── csv_to_json.py       # Script de conversão CSV→JSON
└── examples/
    └── crop-weighted-climate/ # Exemplos de uso
```

## 🔗 URLs dos Dados

### Dados Consolidados
```
https://raw.githubusercontent.com/FrancyJGLisboa/agri-feeders/main/data/ibge-data.json
```

### Metadados
```
https://raw.githubusercontent.com/FrancyJGLisboa/agri-feeders/main/data/metadata.json
```

## 📋 Estrutura do JSON

```json
{
  "metadata": {
    "fonte": "CSV Histórico IBGE/SIDRA",
    "extraido_em": "2024-12-18T10:00:00",
    "periodo": "2000-2024",
    "culturas": ["soja", "milho"]
  },
  "municipios": {
    "sorriso-mt": {
      "lat": -12.5425,
      "lon": -55.7211,
      "cod_ibge": "5107925"
    }
  },
  "areaTotal": {
    "sorriso-mt": 845.48
  },
  "area": {
    "soja": {
      "2024": { "sorriso-mt": 600.0 },
      "2000": { "sorriso-mt": 360.0 }
    },
    "milho": {
      "2024": { "sorriso-mt": 0.0 }
    }
  },
  "producao": {
    "soja": {
      "2024": { "sorriso-mt": 2244.4 }
    }
  }
}
```

### Unidades
- `area`: mil hectares
- `producao`: mil toneladas
- `areaTotal`: mil hectares

## 🚀 Como Usar

### JavaScript (Frontend)
```javascript
// Carregar dados do GitHub
const response = await fetch('https://raw.githubusercontent.com/FrancyJGLisboa/agri-feeders/main/data/ibge-data.json');
const data = await response.json();

// Usar os dados
console.log(data.area.soja['2024']['sorriso-mt']); // 600.0
console.log(data.municipios['sorriso-mt']); // {lat: -12.5425, lon: -55.7211}
```

### Python
```python
import requests

url = 'https://raw.githubusercontent.com/FrancyJGLisboa/agri-feeders/main/data/ibge-data.json'
response = requests.get(url)
data = response.json()

# Acessar dados
area_soja_2024 = data['area']['soja']['2024']['sorriso-mt']
print(f"Área de soja em Sorriso-MT (2024): {area_soja_2024} mil hectares")
```

## 🔧 Ferramentas Compatíveis

### Crop-Weighted Climate Tool
Ferramenta de análise climática que utiliza estes dados para calcular médias ponderadas por área agrícola.

**URL Raw para uso na ferramenta:**
```
https://raw.githubusercontent.com/FrancyJGLisboa/agri-feeders/main/data/ibge-data.json
```

## 📈 Estatísticas de Validação

- ✅ **4100/4108 municípios** com coordenadas
- ✅ **25 anos** de dados históricos completos
- ✅ **2 warnings** apenas (casos extremos de área plantada > área total)
- ✅ **Formatação compatível** com ferramentas de análise

## 🛠️ Scripts de Conversão

Para converter novos dados CSV para JSON, use o script incluído:

```bash
# Instalar dependências
uv add pandas click requests

# Converter CSV para JSON
uv run scripts/csv_to_json.py --input-soja dataset_soja.csv --input-milho dataset_milho.csv --output data/ibge-data.json
```

## 📝 Histórico de Versões

### v1.0 (2024-12-18)
- Dataset inicial com dados 2000-2024
- 4.108 municípios brasileiros
- Soja e milho
- 99.8% cobertura de coordenadas

## 📄 Licença

Dados públicos do IBGE/SIDRA. Consulte os termos de uso em:
https://www.ibge.gov.br/acessoainformacao/licencas.html

## 🤝 Contribuições

Para atualizar ou corrigir dados:
1. Fork este repositório
2. Execute a conversão com novos dados CSV
3. Envie pull request com atualização

## 📧 Contato

Para questões sobre os dados ou ferramentas, abra uma issue neste repositório.

---

**Atualizado em:** 2024-12-18
**Dataset:** v1.0