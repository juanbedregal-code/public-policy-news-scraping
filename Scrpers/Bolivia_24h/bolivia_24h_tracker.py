"""
==============================================================================
PROJECT: Alternative High-Frequency Data Pipeline (Bolivia)
MODULE: 24-Hour News Tracker & Scraper
AUTHOR: Juan José Bedregal
DESCRIPTION: 
Highly optimized web scraper designed for daily execution. It extracts 
contemporary news updates within a 24-hour window from 13 major Bolivian 
news outlets, bypassing complex nested sitemaps and normalizing metadata.
Outputs are directly pushed to the 'Data/Raw' DIME-standard folder.
==============================================================================
"""

import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
import datetime
import time
import re
from typing import List, Dict, Any
from urllib.parse import urlparse
import gzip

# ==============================================================================
# CONFIGURACIÓN INICIAL Y DE ENTORNO
# ==============================================================================

# DIME Standard: Generación automática del directorio de datos crudos
DIR_RAW = os.path.join('..', '..', 'Data', 'Raw')
os.makedirs(DIR_RAW, exist_ok=True)

# Cálculo dinámico: Por defecto extrae las noticias de "Ayer"
TARGET_DATE = datetime.datetime.today().date() - datetime.timedelta(days=1)
TARGET_DATE_STR = TARGET_DATE.strftime("%Y-%m-%d")

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                  'AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/91.0.4472.124 Safari/537.36'
}

# ==============================================================================
# DICCIONARIO DE CONFIGURACIÓN DE SITIOS (13 Medios)
# ==============================================================================

SITE_CONFIGS = {
    "La Razón": {
        "sitemap_index": "https://larazon.bo/sitemap_index.xml",
        "exclusiones": ["/tags/", "/autores/", "/bloque/", "/categoria/", "/ciudades/"],
        "crawl_delay_seconds": 1.5,
        "selectores": {
            "titulo": {'tag': 'h1', 'attrs': {'class': 'jeg_post_title'}},
            "cuerpo_contenedor": ['div.entry-content', 'div.content-inner'],
            "fecha_contenedor": {'tag': 'div', 'attrs': {'class': 'jeg_meta_date'}},
            "seccion_contenedor": {'tag': 'div', 'attrs': {'class': 'jeg_meta_category'}},
        }
    },
    "El Deber": {
        "sitemap_index": "https://eldeber.com.bo/sitemap-news.xml",
        "exclusiones": ["/partido_detalle/", "/portadas/", "/buscar/", ".pdf$", "?utm_"],
        "crawl_delay_seconds": 1.5,
        "selectores": {
            "titulo": {'tag': 'h1', 'attrs': {'class': 'articulo__titulo'}},
            "cuerpo_contenedor": ['main.articulo__cuerpo'],
            "fecha_contenedor": {'tag': 'div', 'attrs': {'class': 'articulo__fecha'}},
            "seccion_contenedor": {'tag': 'div', 'attrs': {'class': 'articulo__volanta'}},
        }
    },
    "Red Uno": {
        "sitemap_index": [
            "https://www.reduno.com.bo/sitemap.xml",
            "https://www.reduno.com.bo/sitemap_lite.xml",
            "https://www.reduno.com.bo/sitemap-news.xml"
        ],
        "exclusiones": ["/embebidos/iframe/", "/buscar/", ".pdf", "?utm_", "/programa", "/series", "/salud-belleza", "/tecnologia",
                        "/feicobol", "/podcast", "/fotogaleria", "/zoomcast", "/festichela", "/recetas", "/zona-anime",
                        "/unicef", "/carnaval-2022", "/ucrania", "/no-es-normal", "/conectados", "/pelicula", "/la-turista",
                        "/fexco", "/miss-universo-2023", "/carnaval-2024", "/show-de-copas", "/espacio-empresarial", "/carnaval-2025",
                        "/scz-despide-a-percy", "/uno-de-feria"],
        "crawl_delay_seconds": 1.5,
        "selectores": {
            "titulo": {"tag": "h1", "attrs": {"class": "titulo"}},
            "cuerpo_contenedor": ["div.body__cuerpo"],
            "fecha_contenedor": {"tag": "p", "attrs": {"class": "fecha"}},
            "seccion_contenedor": {"tag": "a", "attrs": {}} # Fallback a URL
        }
    },
    "Éxito Noticias Bolivia": {
        "sitemap_index": "https://exitonoticias.com.bo/sitemap_index.xml",
        "exclusiones": ["/category/", "/tag/", "/author/", "/page/"],
        "crawl_delay_seconds": 1.5,
        "selectores": {
            "titulo": {"tag": "h1", "attrs": {"class": "tdb-title-text"}},
            "cuerpo_contenedor": ["div.tdb-block-inner.td-fix-index"],
            "fecha_contenedor": {"tag": "time", "attrs": {"class": "entry-date"}},
            "seccion_contenedor": {"tag": "div", "attrs": {"class": "tdb-category"}},
        }
    },
    "Opinión": {
        "sitemap_index": "https://www.opinion.com.bo/sitemap.xml",
        "exclusiones": ["/api", "/admin", "/ads/", "/comments/", "/blog/", "/section/", "/author/"],
        "crawl_delay_seconds": 1.5,
        "selectores": {
            "titulo": {'tag': 'h2', 'attrs': {'class': 'title'}},
            "cuerpo_contenedor": ['div.body'],
            "fecha_contenedor": {'tag': 'span', 'attrs': {'class': 'content-time'}},
            "seccion_contenedor": {'tag': 'div', 'attrs': {'class': 'metadata'}},
        }
    },
    "Unitel": {
        "sitemap_index": "https://unitel.bo/sitemapforgoogle.xml",
        "exclusiones": ["/amp/", "/buscar/", "/tag/", "/television/", "/yo-me-llamo/", "/portada/", "/canal-rural/"],
        "crawl_delay_seconds": 1.5,
        "selectores": {
            "titulo": {'tag': 'h1', 'attrs': {'class': 'title'}},
            "cuerpo_contenedor": ['div.vu-td-cdn'],
            "fecha_contenedor": {'tag': 'span', 'attrs': {'class': 'dateTime'}},
            "seccion_contenedor": {'tag': 'div', 'attrs': {'class': 'vu-td-seccion'}},
        }
    },
    "Sol de Pando": {
        "sitemap_index": "https://www.soldepando.com/sitemap_index.xml",
        "exclusiones": ["/category/", "/ngg_tag/"],
        "crawl_delay_seconds": 1.5,
        "selectores": {
            "titulo": {'tag': 'h1', 'attrs': {}},
            "cuerpo_contenedor": ['div.entry'],
            "fecha_contenedor": {'tag': 'div', 'attrs': {'id': 'datemeta_l'}},
            "seccion_contenedor": {'tag': 'div', 'attrs': {'id': 'datemeta_r'}},
        }
    },
    "El País Tarija": {
        "sitemap_index": [
            "https://elpais.bo/sitemap.xml",
            "https://elpais.bo/sitemap-google-news.xml"
        ],
        "exclusiones": ["/cache_css/", "/js/", "/media/", "/plugins/"],
        "crawl_delay_seconds": 1.5,
        "selectores": {
            "titulo": {'tag': 'h1', 'attrs': {'class': 'ep_post_title'}},
            "cuerpo_contenedor": ['div.note-body'],
            "fecha_contenedor": {},
            "seccion_contenedor": {'tag': 'span', 'attrs': {'class': 'section-name'}},
        }
    },
    "Noticias Violeta": {
        "sitemap_index": "https://noticiasvioleta.com/sitemap_index.xml",
        "exclusiones": ["/categorías/"],
        "crawl_delay_seconds": 1.5,
        "selectores": {
            "titulo": {'tag': 'h1', 'attrs': {}},
            "cuerpo_contenedor": ['div.JoB2wd'],
            "fecha_contenedor": {'tag': 'time', 'attrs': {'class': 'entry-date'}},
            "seccion_contenedor": {'tag': 'span', 'attrs': {'class': 'cat-links'}},
        }
    },
    "La Época": {
        "sitemap_index": "https://www.la-epoca.com.bo/sitemaps.xml",
        "exclusiones": ["/categoria/"],
        "crawl_delay_seconds": 1.5,
        "selectores": {
            "titulo": {'tag': 'h1', 'attrs': {'class': 'entry-title'}},
            "cuerpo_contenedor": ['div.entry-content'],
            "fecha_contenedor": {'tag': 'span', 'attrs': {'class': 'posted-on'}},
            "seccion_contenedor": {'tag': 'a', 'attrs': {'rel': 'category tag'}},
        }
    },
    "Radio Panamericana": {
        "sitemap_index": ["https://www.panamericana.bo/sitemap.xml"],
        "exclusiones": ["/api/", "/admin/", "/blog/", "/contacto/", "/publicidad/"],
        "crawl_delay_seconds": 1.5,
        "selectores": {
            "titulo": {'tag': 'h1', 'attrs': {'class': 'title'}},
            "cuerpo_contenedor": ['div.body'],
            "fecha_contenedor": {'tag': 'span', 'attrs': {'class': 'content-time'}},
            "seccion_contenedor": {'tag': 'body', 'attrs': {}},
        }
    },
    "Visión 360": {
        "sitemap_index": "https://www.vision360.bo/sitemap.xml",
        "exclusiones": [],
        "crawl_delay_seconds": 1.5,
        "selectores": {
            "titulo": {'tag': 'h1', 'attrs': {'class': 'noticia-titulo'}},
            "cuerpo_contenedor": ['div.noticia-contenido'],
            "fecha_contenedor": {'tag': 'div', 'attrs': {'class': 'noticia-fecha'}},
            "seccion_contenedor": {'tag': 'div', 'attrs': {'class': 'noticia-categoria'}},
        }
    },
    "Radio FM Bolivia": {
        "sitemap_index": [
            "https://fmbolivia.com.bo/sitemap_index.xml",
            "https://fmbolivia.com.bo/news-sitemap.xml"
        ],
        "exclusiones": ["/wp-admin/", "/comments/feed/", "?replytocom", "/secciones/"],
        "crawl_delay_seconds": 1.5,
        "selectores": {
            "titulo": {'tag': 'span', 'attrs': {'class': 'post-title', 'itemprop': 'headline'}},
            "cuerpo_contenedor": ['div.entry-content'],
            "fecha_contenedor": {'tag': 'time', 'attrs': {'class': 'post-published'}},
            "seccion_contenedor": {'tag': 'a', 'attrs': {'rel': 'category tag'}},
        }
    }
}

# ==============================================================================
# FUNCIONES AUXILIARES DE EXTRACCIÓN Y LIMPIEZA
# ==============================================================================

def get_sitemap_urls(sitemap_index_url: Any) -> List[str]:
    """
    Descomprime y parsea sitemaps (incluyendo .gz) recursivamente para obtener URLs.
    """
    sitemap_list = []
    urls = sitemap_index_url if isinstance(sitemap_index_url, list) else [sitemap_index_url]

    for sitemap_url in urls:
        print(f"🔎 Buscando sitemaps en: {sitemap_url}")
        try:
            response = requests.get(sitemap_url, headers=HEADERS, timeout=15)
            response.raise_for_status()
            content = gzip.decompress(response.content) if sitemap_url.endswith('.gz') else response.content
            soup = BeautifulSoup(content, 'lxml-xml')

            if soup.find('sitemap'):
                for sitemap_tag in soup.find_all('sitemap'):
                    if loc_tag := sitemap_tag.find('loc'):
                        sitemap_list.append(loc_tag.text)
            else:
                sitemap_list.append(sitemap_url)
        except Exception as e:
            print(f"  ⚠️ Error al procesar sitemap {sitemap_url}: {e}")

    print(f"✅ {len(sitemap_list)} sitemaps listos para rastrear.")
    return sitemap_list

def find_article_links_by_date(sitemap_url: str, target_date: datetime.date, exclusiones: List[str]) -> List[str]:
    """
    Filtra los enlaces dentro de un sitemap buscando coincidencia estricta 
    con la fecha objetivo (target_date).
    """
    print(f"  Buscando en: {sitemap_url}")
    links = []
    try:
        response = requests.get(sitemap_url, headers=HEADERS, timeout=15)
        response.raise_for_status()
        content = gzip.decompress(response.content) if sitemap_url.endswith('.gz') else response.content
        soup = BeautifulSoup(content, 'lxml-xml')

        for url in soup.find_all('url'):
            loc = url.find('loc').text if url.find('loc') else None
            if not loc or any(ex in loc for ex in exclusiones):
                continue

            lastmod = url.find('lastmod').text if url.find('lastmod') else None
            if not lastmod:
                links.append(loc)
                continue

            try:
                article_date = datetime.datetime.fromisoformat(lastmod.replace('Z', '+00:00')).date()
                if article_date == target_date:
                    links.append(loc)
            except ValueError:
                try:
                    article_date = datetime.datetime.strptime(lastmod.split('T')[0], "%Y-%m-%d").date()
                    if article_date == target_date:
                        links.append(loc)
                except Exception:
                    links.append(loc)
    except Exception as e:
        print(f"  ⚠️ Error procesando {sitemap_url}: {e}")
    
    return list(dict.fromkeys(links))

def _clean_section_text(s: str) -> str:
    """Limpia cadenas redundantes en las categorías de sección."""
    if not s: return s
    s = s.strip()
    s = re.sub(r'^(en[:\s]*)+', '', s, flags=re.IGNORECASE)
    return s.strip()

def _try_meta(soup: BeautifulSoup, prop_names: List[str]) -> str:
    """Busca atributos en las meta-etiquetas HTML si el selector primario falla."""
    for p in prop_names:
        if tag := soup.find("meta", attrs={"property": p}) or soup.find("meta", attrs={"name": p}):
            if content := tag.get("content"):
                return content.strip()
    return ""

def _largest_text_block(soup: BeautifulSoup) -> Any:
    """Fallback heurístico: Encuentra el div/article con mayor cantidad de etiquetas 'p'."""
    candidates = soup.find_all(['article', 'div'])
    best = None
    max_p = -1
    for c in candidates:
        if c:
            p_count = len(c.find_all('p'))
            if p_count > max_p:
                max_p = p_count
                best = c
    return best

def _extract_date_from_text(text: str) -> datetime.datetime:
    """
    Aplica Expresiones Regulares (Regex) complejas para estandarizar
    fechas en formato español e ISO 8601 encontradas en el HTML.
    """
    if not text or not isinstance(text, str): return None
    
    meses = {'enero':1,'febrero':2,'marzo':3,'abril':4,'mayo':5,'junio':6,'julio':7,'agosto':8,'septiembre':9,'octubre':10,'noviembre':11,'diciembre':12}

    # Formato YYYY-MM-DD HH:MM:SS
    if m := re.search(r'(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})', text):
        try: return datetime.datetime.strptime(m.group(1), '%Y-%m-%d %H:%M:%S')
        except: pass

    # Formato ISO 8601
    if m := re.search(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})', text):
        try: return datetime.datetime.fromisoformat(m.group(1))
        except: pass

    # Formato DD/MM/YYYY HH:MM
    if m := re.search(r'(\d{1,2})/(\d{1,2})/(\d{4})\s+(\d{2}:\d{2})', text):
        try: return datetime.datetime.strptime(m.group(0), '%d/%m/%Y %H:%M')
        except: pass
        
    # Formato (DD/MM/YYYY)
    if m := re.search(r'\((\d{1,2})/(\d{1,2})/(\d{4})\)', text):
        try: return datetime.datetime.strptime(m.group(0), '(%d/%m/%Y)')
        except: pass

    # Formato Español Completo (Ej. "14 de junio de 2026")
    if m := re.search(r'(\d{1,2})\s+de\s+([a-zñ]+)\s+de\s+(\d{4})', text, re.IGNORECASE):
        try:
            dia, mes_txt, anio = m.groups()
            if mes := meses.get(mes_txt.lower()):
                dt = datetime.datetime(int(anio), mes, int(dia))
                if m_time := re.search(r'(\d{2}:\d{2})', text):
                    dt = dt.replace(hour=int(m_time.group(1).split(':')[0]), minute=int(m_time.group(1).split(':')[1]))
                return dt
        except: pass
            
    # Formato "Mes DD, YYYY"
    if m := re.search(r'([a-zñ]+)\s+(\d{1,2}),\s*(\d{4})', text, re.IGNORECASE):
        try:
            mes_txt, dia, anio = m.groups()
            if mes := meses.get(mes_txt.lower()):
                return datetime.datetime(int(anio), mes, int(dia))
        except: pass
            
    return None

# ==============================================================================
# EXTRACCIÓN UNIFORME DE NOTICIAS
# ==============================================================================

def scrape_article_data(article_url: str, selectores: Dict[str, Any], site_name: str) -> Dict[str, Any]:
    """Descarga, limpia y estructura el título, sección, fecha y cuerpo del artículo."""
    print(f"    📰 Scrapeando ({site_name}): {article_url}")
    data = {"sitio_noticias": site_name, "fecha": "No encontrado", "seccion": "No encontrado", "titular": "No encontrado", "cuerpo": "No encontrado", "url": article_url}
    
    try:
        response = requests.get(article_url, headers=HEADERS, timeout=20)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')

        # ---- TÍTULO ----
        titulo = ""
        if sel := selectores.get("titulo"):
            if el := soup.find(sel.get('tag'), **sel.get('attrs', {})):
                titulo = el.get_text(strip=True)
        if not titulo: titulo = _try_meta(soup, ["og:title", "twitter:title"])
        if not titulo:
            if h1 := soup.find('h1'): titulo = h1.get_text(strip=True)
        if not titulo and soup.title: titulo = soup.title.string
        data["titular"] = titulo.strip() if titulo else "No encontrado"

        # ---- FECHA ----
        dt_obj = None
        fecha_text_raw = ""
        if sel := selectores.get("fecha_contenedor"):
            if el := soup.find(sel.get('tag'), **sel.get('attrs', {})):
                fecha_text_raw = el.get('datetime', el.get_text(" ", strip=True))
                dt_obj = _extract_date_from_text(fecha_text_raw)
        if not dt_obj:
            if meta_time := _try_meta(soup, ["article:published_time", "pubdate"]):
                dt_obj = _extract_date_from_text(meta_time)
                if dt_obj: fecha_text_raw = meta_time
        if not dt_obj:
            for t in soup.find_all("time"):
                if dt_str := t.get("datetime"):
                    if dt := _extract_date_from_text(dt_str):
                        dt_obj = dt
                        fecha_text_raw = dt_str
                        break
        if not dt_obj:
            # Fallback a URL
            body_for_date = soup.get_text(" ", strip=True)
            dt_obj = _extract_date_from_text(body_for_date[-200:]) or _extract_date_from_text(body_for_date[:500])
            if dt_obj: fecha_text_raw = "Extracted from body"

        data["fecha"] = dt_obj.strftime("%Y-%m-%d %H:%M:%S") if dt_obj else "No encontrado"

        # ---- SECCIÓN ----
        seccion_text = ""
        if sel := selectores.get("seccion_contenedor"):
            if site_name == "Radio Panamericana" and (body_tag := soup.find('body', attrs={'data-category': True})):
                seccion_text = body_tag['data-category']
            elif el := soup.find(sel.get('tag'), **sel.get('attrs', {})):
                seccion_text = ", ".join(a.get_text(strip=True) for a in el.find_all('a') if a.get_text(strip=True)) or el.get_text(" ", strip=True)
        if not seccion_text: seccion_text = _try_meta(soup, ["article:section", "category"])
        if not seccion_text:
            path_segs = [s for s in urlparse(article_url).path.split('/') if s and not s.isdigit() and s not in ['noticias', 'articulo']]
            if path_segs: seccion_text = path_segs[0].replace('-', ' ').capitalize()
        data["seccion"] = _clean_section_text(seccion_text)

        # ---- CUERPO ----
        body_text = ""
        for selector in selectores.get("cuerpo_contenedor", []):
            if node := soup.select_one(selector):
                for bad in node.select("script, style, iframe, ins, figure, .ad-slot, .xqm-post-inline, .bs-irp, .wp-block-image"): bad.decompose()
                paragraphs = [p.get_text(" ", strip=True) for p in node.find_all("p") if p.get_text(strip=True)]
                if len(paragraphs) > 1:
                    body_text = "\n\n".join(paragraphs)
                    break
        if not body_text:
            if biggest := _largest_text_block(soup):
                for bad in biggest.select("script, style, iframe, ins, figure, .ad-slot, .xqm-post-inline, .bs-irp, .wp-block-image"): bad.decompose()
                pars = [p.get_text(" ", strip=True) for p in biggest.find_all("p") if p.get_text(strip=True)]
                if pars: body_text = "\n\n".join(pars)
        data["cuerpo"] = body_text

    except Exception as e:
        print(f"  ⚠️ Error procesando {article_url}: {e}")
    
    return data

# ==============================================================================
# EJECUCIÓN PRINCIPAL
# ==============================================================================

def main():
    print(f"\n[+] FECHA OBJETIVO DEL RASTREO: {TARGET_DATE_STR}")
    all_articles_data = []
    el_pais_seen_titles = set()

    for site_name, config in SITE_CONFIGS.items():
        print(f"\n==============================================")
        print(f"🚀 INICIANDO RASTREO PARA: {site_name}")
        print(f"==============================================")

        sitemap_key = config.get("sitemap_index") or config.get("sitemap_url")
        if not sitemap_key:
            print(f"⚠️ No se encontró 'sitemap_index' para {site_name}. Saltando...")
            continue
        
        sitemap_list = get_sitemap_urls(sitemap_key)

        if site_name in ["Opinión", "Radio Panamericana"]:
            priority_keywords = ['news', 'categories', 'latest', 'authors', 'contents']
            filtered_sitemaps = [s for s in sitemap_list if any(kw in s for kw in priority_keywords) and not re.search(r'\.\d{4}\.\d{2}\.', s)]
            if filtered_sitemaps:
                print(f"  ↳ Filtrando sitemaps. De {len(sitemap_list)} a {len(filtered_sitemaps)} sitemaps prioritarios.")
                sitemap_list = filtered_sitemaps

        site_links = []
        for sitemap_url in sitemap_list:
            links_found = find_article_links_by_date(sitemap_url, TARGET_DATE, config.get("exclusiones", []))
            site_links.extend(links_found)

        print(f"\n✅ Total de URLs encontradas en sitemaps para {site_name}: {len(site_links)}")
        if not site_links:
            continue

        for link in site_links:
            skip = False
            if site_name == "La Razón":
                if match := re.search(r'/(\d{4})/(\d{2})/(\d{2})/', link):
                    if datetime.date(int(match[1]), int(match[2]), int(match[3])) != TARGET_DATE: skip = True
            elif site_name == "El País Tarija":
                if match := re.search(r'/(\d{4})(\d{2})(\d{2})_', link):
                    if datetime.date(int(match[1]), int(match[2]), int(match[3])) != TARGET_DATE: skip = True
                if match := re.search(r'-(\d{8})\d{0,6}$', link): 
                    try:
                        date_str = match.group(1)
                        parsed_date = datetime.datetime.strptime(date_str, '%Y%m%d').date()
                        if parsed_date != TARGET_DATE:
                            skip = True
                    except ValueError:
                        skip = True
            
            if skip: continue

            article_data = scrape_article_data(link, config["selectores"], site_name)

            if site_name == "El País Tarija" and (titular := article_data.get("titular")) in el_pais_seen_titles:
                continue
            el_pais_seen_titles.add(article_data.get("titular"))

            if (fecha_text := article_data.get("fecha", "No encontrado")) != "No encontrado":
                if (dt := _extract_date_from_text(fecha_text)) and dt.date() == TARGET_DATE:
                    all_articles_data.append(article_data)
            
            # Etiqueta Ética: Pausa para no sobrecargar el servidor
            time.sleep(config.get("crawl_delay_seconds", 1))

    if not all_articles_data:
        print("\n[!] No se extrajo ningún artículo de la fecha objetivo.")
        return

    print("\n🔄 Normalizando fechas a formato AAAA-MM-DD...")
    for article in all_articles_data:
        if 'fecha' in article and (dt_obj := _extract_date_from_text(article['fecha'])):
            article['fecha'] = dt_obj.strftime('%Y-%m-%d')

    print(f"\n==============================================")
    print(f"📊 RESUMEN FINAL DEL PIPELINE")
    print(f"==============================================")
    
    df = pd.DataFrame(all_articles_data)
    df.drop_duplicates(subset=['titular', 'sitio_noticias'], keep='first', inplace=True)
    print(f"[+] Se extrajeron y limpiaron {len(df)} artículos únicos.")
    
    # Exportación a la carpeta DIME Raw
    csv_filename = os.path.join(DIR_RAW, f"noticias_bo_{TARGET_DATE_STR}.csv")
    df.to_csv(csv_filename, index=False, encoding='utf-8-sig')
    print(f"💾 Dataset guardado exitosamente en: {csv_filename}")

if __name__ == "__main__":
    main()
