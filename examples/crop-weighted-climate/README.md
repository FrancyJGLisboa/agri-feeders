# Crop-Weighted Climate Tool - Integração com Agri Feeders

Este diretório contém exemplos de como usar os dados do repositório agri-feeders com a ferramenta de análise climática ponderada por área.

## 🔗 URL dos Dados

```
https://raw.githubusercontent.com/FrancyJGLisboa/agri-feeders/main/data/ibge-data.json
```

## 📋 Como Modificar a Ferramenta HTML

### 1. Alterar URL dos Dados

No arquivo `crop-weighted-climate.html`, localize a configuração de URL:

```javascript
// ANTES (arquivo local)
const IBGE_DATA_URL = './ibge-data.json';

// DEPOIS (GitHub com fallback)
const IBGE_DATA_URL = 'https://raw.githubusercontent.com/FrancyJGLisboa/agri-feeders/main/data/ibge-data.json';
```

### 2. Implementar Fallback Local

Adicione tratamento de erro para usar arquivo local se o GitHub falhar:

```javascript
async function loadIBGEData() {
    try {
        // Tentar GitHub primeiro
        console.log('Carregando dados do GitHub...');
        const response = await fetch(IBGE_DATA_URL);
        if (!response.ok) throw new Error('GitHub failed');
        return await response.json();
    } catch (error) {
        // Fallback para arquivo local
        console.log('GitHub falhou, usando arquivo local...');
        const localResponse = await fetch('./ibge-data.json');
        return await localResponse.json();
    }
}
```

### 3. Exemplo Completo

```javascript
// Configuração
const IBGE_DATA_URL = 'https://raw.githubusercontent.com/FrancyJGLisboa/agri-feeders/main/data/ibge-data.json';

// Função de carregamento com fallback
async function loadIBGEData() {
    try {
        console.log('🌐 Carregando dados do GitHub...');
        const response = await fetch(IBGE_DATA_URL);
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        const data = await response.json();
        console.log('✅ Dados carregados do GitHub');
        return data;
    } catch (error) {
        console.log('⚠️ GitHub indisponível, usando arquivo local...');
        try {
            const localResponse = await fetch('./ibge-data.json');
            if (!localResponse.ok) throw new Error(`Local HTTP ${localResponse.status}`);
            const data = await localResponse.json();
            console.log('✅ Dados carregados localmente');
            return data;
        } catch (localError) {
            console.error('❌ Erro ao carregar dados:', localError);
            throw new Error('Impossível carregar dados agrícolas');
        }
    }
}

// Uso na aplicação
let ibgeData = null;

async function initializeApp() {
    try {
        ibgeData = await loadIBGEData();
        console.log(`📊 ${Object.keys(ibgeData.municipios).length} municípios carregados`);
        // Continuar inicialização da aplicação...
    } catch (error) {
        console.error('Falha ao inicializar:', error);
        // Mostrar mensagem de erro ao usuário
    }
}
```

## 📊 Estrutura dos Dados na Ferramenta

### Municipios
```javascript
// Acessar informações do município
const municipio = 'sorriso-mt';
const info = ibgeData.municipios[municipio];
// { lat: -12.5425, lon: -55.7211, cod_ibge: "5107925" }
```

### Área Plantada
```javascript
// Acessar área plantada
const area = ibgeData.area.soja['2024']['sorriso-mt'];
// 600.0 (mil hectares)
```

### Produção
```javascript
// Acessar produção
const producao = ibgeData.producao.soja['2024']['sorriso-mt'];
// 2244.4 (mil toneladas)
```

### Área Total do Município
```javascript
// Acessar área total
const areaTotal = ibgeData.areaTotal['sorriso-mt'];
// 845.48 (mil hectares)
```

## 🔍 Validação de Dados

A ferramenta pode validar automaticamente:

```javascript
function validarDados(municipio, cultura, ano) {
    const areaPlantada = ibgeData.area[cultura]?.[ano]?.[municipio] || 0;
    const areaTotal = ibgeData.areaTotal[municipio] || 0;

    if (areaTotal > 0 && areaPlantada > areaTotal) {
        console.warn(`⚠️ ${municipio}: área plantada (${areaPlantada}) > área total (${areaTotal})`);
        return false;
    }
    return true;
}
```

## 📈 Exemplos de Uso

### Listar Todos os Municípios
```javascript
const municipios = Object.keys(ibgeData.municipios);
console.log(`${municipios.length} municípios disponíveis`);
```

### Buscar por Estado
```javascript
function municipiosPorEstado(uf) {
    return Object.keys(ibgeData.municipios)
        .filter(key => key.endsWith(`-${uf.toLowerCase()}`));
}

const municipiosMT = municipiosPorEstado('mt');
console.log(`Mato Grosso: ${municipiosMT.length} municípios`);
```

### Análise por Período
```javascript
function analisarPeriodo(cultura, anoInicio, anoFim) {
    const municipios = Object.keys(ibgeData.area[cultura]);
    const resultados = [];

    for (let ano = anoInicio; ano <= anoFim; ano++) {
        const anoStr = ano.toString();
        for (const municipio of municipios) {
            const area = ibgeData.area[cultura]?.[anoStr]?.[municipio];
            const producao = ibgeData.producao[cultura]?.[anoStr]?.[municipio];

            if (area && producao) {
                resultados.push({ municipio, ano, area, producao });
            }
        }
    }
    return resultados;
}

// Exemplo: Soja de 2020 a 2024
const soja2020_2024 = analisarPeriodo('soja', 2020, 2024);
console.log(`${soja2020_2024.length} registros encontrados`);
```

## 🚀 Vantagens da Abordagem GitHub

### ✅ Benefícios
- **Centralização**: Dados em repositório público
- **Versionamento**: Histórico de atualizações
- **CDN**: GitHub raw funciona como CDN
- **CORS**: Suporte nativo para acesso via browser
- **Atualizações**: Fácil de manter dados atualizados

### ⚠️ Considerações
- **Offline**: Requer conexão com internet
- **Rate limits**: GitHub tem limites para downloads
- **Fallback**: Manter arquivo local para contingência

## 📝 Checklist de Implementação

- [ ] Alterar `IBGE_DATA_URL` para URL do GitHub
- [ ] Implementar fallback para arquivo local
- [ ] Testar carregamento com conexão internet
- [ ] Testar fallback sem conexão internet
- [ ] Validar estrutura dos dados carregados
- [ ] Atualizar documentação da ferramenta

---

**URL dos Dados:** https://raw.githubusercontent.com/FrancyJGLisboa/agri-feeders/main/data/ibge-data.json
**Última atualização:** 2024-12-18