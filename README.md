# Stage 1 - Data Layer: Search Engine Project

**Curso:** Big Data - Grado en Ciencia e Ingeniería de Datos  
**Universidad:** Universidad de Las Palmas de Gran Canaria  
**Grupo:** `<Nombre del Grupo>`  
**Repositorio:** `https://github.com/DREAM-TEAM-ULPGC/stage_1`

---

## 1. Introducción

En esta primera etapa, el objetivo es construir la **capa de datos (Data Layer)** del motor de búsqueda. Esta capa incluye:

- **Datalake:** Almacena los libros descargados de Project Gutenberg en formato sin procesar (texto plano) y JSON estructurado.  
- **Datamarts:** Contiene datos estructurados (metadata de libros e índices invertidos).  
- **Control Layer:** Coordina descargas e indexaciones evitando duplicados.

---

## 2. Estructura del Proyecto

```text
stage_1/
├── crawler_books.py       # Crawler de libros con JSON bonito
├── writer.py              # Clase para escribir JSON estructurado
├── utils.py               # Funciones de normalización y dominios
├── link_extractor.py      # Extrae enlaces de HTML (opcional)
├── robots_cache.py        # Manejo de robots.txt
├── polite_limiter.py      # Respeta crawl-delay por host
├── fetcher.py             # Fetcher de páginas HTML/texto
├── control_layer.py       # Coordinación de descargas e indexación
├── datalake/              # Carpeta donde se guardan libros descargados
│   └── YYYYMMDD/
│       └── HH/
│           ├── <BOOK_ID>.body.txt
│           └── <BOOK_ID>.header.txt
├── control/               # Archivos de control de estado
│   ├── downloaded_books.txt
│   └── indexed_books.txt
├── README.md
└── requirements.txt       # Librerías necesarias
