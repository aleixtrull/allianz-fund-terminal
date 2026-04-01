# Allianz Fund Terminal — Lite

Dashboard de NAV para distribuidores Allianz España. Versión simplificada con tres páginas: **Home**, **Performance** y **Comparador Fondos**.

Tecnología: HTML + JS estático servido por GitHub Pages. Sin backend, sin login. Los datos se actualizan automáticamente 3 veces al día mediante GitHub Actions.

---

## Puesta en producción (GitHub corporativo Allianz)

### 1. Crear el repositorio

En GitHub (o GitHub Enterprise corporativo):

- **New repository** → nombre sugerido: `allianz-fund-terminal`
- Visibilidad: **Private** o Internal
- Sin README, .gitignore ni licencia — se sube el código tal cual

### 2. Subir el código

```bash
cd allianz-lite
git remote add origin https://github.com/ORGANIZACION/allianz-fund-terminal.git
git push -u origin main
```

### 3. Dar permisos de escritura al workflow

En el repositorio:

**Settings → Actions → General → Workflow permissions**
- Seleccionar: **Read and write permissions**
- Guardar

> Sin esto el workflow no podrá hacer `git push` de los CSVs y fallará con error 403.

### 4. Activar GitHub Pages

**Settings → Pages**
- Source: **Deploy from a branch**
- Branch: `main` · Carpeta: `/ (root)`
- Guardar

La URL del sitio aparece en unos segundos:
`https://ORGANIZACION.github.io/allianz-fund-terminal/`

> Si GitHub Pages está desactivado a nivel de organización, pide al administrador de GitHub que lo habilite para este repositorio en *Organization Settings → Policies*.

### 5. Primera ejecución manual

El repositorio recién subido no tiene datos. Hay que generarlos la primera vez:

**Actions → "Update NAV Data" → Run workflow → Run workflow**

El workflow tarda entre 15 y 30 minutos. Al terminar aparece un commit automático (`chore: update NAV data`) con los archivos `data/nav_all_funds.csv`, `data/nav_benchmarks.csv` y `data/nav_summary.csv`.

### 6. Verificar

Abrir la URL de GitHub Pages. Si aparece "No se encontró nav_all_funds.csv" es que el paso anterior aún no terminó — esperar y recargar.

---

## Actualización automática de datos

El workflow `update_nav_data.yml` se ejecuta automáticamente con la misma periodicidad que el proyecto completo:

| Hora UTC | Hora Madrid | Motivo |
|----------|-------------|--------|
| 07:00 Lun–Sáb | 09:00 | NAV de ayer publicado por Morningstar overnight |
| 13:00 Lun–Vie | 15:00 | Actualización de mediodía |
| 20:00 Lun–Vie | 22:00 | Fondos que publican el NAV del día rápido |

En cada ejecución: descarga NAV → actualiza los CSVs en `data/` → commit automático → GitHub Pages sirve los datos actualizados.

**No se necesitan secrets ni credenciales.** Los datos se obtienen de Morningstar (mstarpy), Yahoo Finance (yfinance) y la web pública de AllianzGI. Todo funciona con el `GITHUB_TOKEN` estándar del repositorio.

---

## Añadir o quitar fondos

El catálogo de fondos está definido en **dos archivos** que deben mantenerse sincronizados.

### Archivo 1 — `fetch_funds_full_history.py`

Controla **qué fondos se descargan**. Hay dos listas:

**`FUNDS` (línea ~52)** — fondos descargados vía Morningstar o Yahoo Finance:

```python
FUNDS = [
    {"isin": "LU1548497699", "name": "Allianz Global Artificial Intelligence", "cat": "Equity Thematic"},
    # ...
]
```

**`ALLIANZGI_FUNDS` (línea ~169)** — fondos ActiveInvest con fuente exclusiva AllianzGI web. Solo tocar si cambian sus URLs internas:

```python
ALLIANZGI_FUNDS = {
    "LU2208987508": {
        "slug":     "allianz-activeinvest-balanced-ct-eur",
        "page_id":  "43dc692d-f6cc-41f1-b44e-9f91901d0a5e",
        "table_id": "aab8dd70-0a03-4c7e-bd94-c0ae051b8b75",
    },
    # ...
}
```

**Categorías válidas** (usadas para filtros y colores en el frontend):

| Valor `cat` | Descripción |
|-------------|-------------|
| `"Equity Global"` | Renta variable global |
| `"Equity EU"` | Renta variable Europa |
| `"Equity US"` | Renta variable EE.UU. |
| `"Equity ES"` | Renta variable España |
| `"Equity EM"` | Renta variable emergentes |
| `"Equity JP"` | Renta variable Japón |
| `"Equity Thematic"` | Temáticos (IA, agua, ciberseguridad…) |
| `"Fixed Income"` | Renta fija |
| `"Mixed"` | Mixtos |

### Archivo 2 — `index.html`

Controla **cómo se muestran** los fondos (nombre y categoría en el frontend). Buscar `FUND_META` (línea ~1350):

```javascript
const FUND_META = {
  'LU1548497699': {name:'Allianz Global Artificial Intelligence', cat:'Equity Thematic'},
  // ...
};
```

---

### Añadir un fondo nuevo

**Paso 1 — `fetch_funds_full_history.py`**

Añadir una entrada al array `FUNDS`:

```python
{"isin": "LU9999999999", "name": "Nuevo Fondo Allianz", "cat": "Equity Global"},
```

> Si el fondo **no está disponible en Morningstar ni Yahoo Finance**, hay que añadirlo también a `ALLIANZGI_FUNDS` con su slug y los UUIDs de la tabla histórica de es.allianzgi.com.

**Paso 2 — `index.html`**

Añadir la misma entrada a `FUND_META`:

```javascript
'LU9999999999': {name:'Nuevo Fondo Allianz', cat:'Equity Global'},
```

**Paso 3 — Ejecutar el workflow**

En GitHub: **Actions → Update NAV Data → Run workflow**

La próxima ejecución descargará el historial completo del fondo nuevo y lo incluirá en los CSVs. A partir de ahí aparecerá automáticamente en el dashboard.

---

### Quitar un fondo

**Paso 1 — `fetch_funds_full_history.py`**

Eliminar o comentar la línea correspondiente en `FUNDS`:

```python
# {"isin": "LU9999999999", "name": "Fondo a quitar", "cat": "Equity Global"},
```

**Paso 2 — `index.html`**

Eliminar la entrada en `FUND_META`:

```javascript
// 'LU9999999999': {name:'Fondo a quitar', cat:'Equity Global'},
```

**Paso 3 — Ejecutar el workflow**

La próxima ejecución regenerará los CSVs sin ese fondo. Desaparecerá del dashboard automáticamente.

> Los CSVs históricos existentes en `data/` se sobreescriben en cada ejecución — no hace falta borrar nada manualmente.

---

## Estructura del repositorio

```
allianz-fund-terminal/
├── index.html                        # Frontend completo (HTML + CSS + JS, sin build)
├── fetch_funds_full_history.py       # Pipeline de descarga de NAV
├── requirements.txt                  # Dependencias Python
├── data/                             # CSVs generados automáticamente por el workflow
│   ├── nav_all_funds.csv             # NAV histórico de todos los fondos
│   ├── nav_benchmarks.csv            # NAV histórico de benchmarks
│   ├── nav_summary.csv               # Resumen de métricas
│   └── fund_composition.json         # Composición sectorial (opcional)
├── .github/
│   └── workflows/
│       └── update_nav_data.yml       # Workflow de actualización automática
└── README.md
```

---

## Solución de problemas

**El workflow falla con error 403 en el push**
→ Verificar que en Settings → Actions → General los permisos son "Read and write".

**El workflow falla descargando datos de algún fondo**
→ Normal de forma ocasional (Morningstar/Yahoo Finance tienen rate limits). El workflow reintenta 3 veces. Si falla repetidamente, ejecutar manualmente fuera de horario de mercado.

**Un fondo aparece con datos desactualizados (icono ⚠)**
→ El fondo lleva más de 7 días sin actualización. Puede indicar que la fuente de datos (Morningstar/yfinance) no tiene NAV reciente para ese ISIN. Verificar manualmente en Morningstar si el fondo sigue activo.

**GitHub Pages no está disponible en la organización**
→ Contactar al administrador de GitHub Enterprise para habilitar Pages en el repositorio. Alternativamente, los archivos estáticos se pueden servir con cualquier servidor web (Apache, Nginx, IIS) apuntando al raíz del repo.
