# Proyecto Stage_1 — Crawler / Ingesta a Datalake

Este repositorio implementa la **parte del crawler** (fase 1) del proyecto, encargada de:

- Descargar textos desde Gutenberg (por ID)  
- Identificar marcadores de inicio/fin del contenido útil  
- Separar *header* y *body*  
- Guardar los archivos en un datalake organizado por fecha y hora:  
  `datalake/YYYYMMDD/HH/<BOOK_ID>_header.txt` y `<BOOK_ID>_body.txt`  

---

## Estructura del proyecto (estado actual)

    BD-PROJECT/
    crawler/
        config.py
        utils.py
        parsing.py
        downloader.py
        cli.py
        init.py # (puede estar vacío)
        datalake/ # se crea automáticamente al ejecutar el crawler
        README.md

- **crawler/config.py**: contiene rutas, URLs y constantes (marcadores START/END, reintentos, etc.)  
- **crawler/utils.py**: funciones utilitarias (creación de directorios, horario UTC, backoff)  
- **crawler/parsing.py**: lógica para separar header / body / footer a partir de los marcadores  
- **crawler/downloader.py**: función principal para descargar y guardar los textos en el datalake  
- **crawler/cli.py**: interfaz de línea de comandos para invocar el crawler con IDs, rangos o listas  

---

## Instalación y dependencias

Requisitos mínimos:

- Python 3.7+ (recomendado 3.10+)
- Paquete `requests`

Instala la dependencia con:

```bash
pip install requests
```

## Cómo ejecutar el crawler

Desde la raíz del proyecto (stage_1/), usa el módulo crawler.cli. Aquí algunos ejemplos:


python -m crawler.cli --id 1342
python -m crawler.cli --range 1300 1310
python -m crawler.cli --list ids.txt
El fichero ids.txt debe contener un ID por línea.

## Siguientes pasos (pendientes para fases siguientes)
Capa de metadata (base de datos) con título, autor, idioma.

Construcción del índice invertido (JSON / MongoDB / FS).

Controlador maestro que gestione descargas e indexaciones.

Benchmarks y validaciones.