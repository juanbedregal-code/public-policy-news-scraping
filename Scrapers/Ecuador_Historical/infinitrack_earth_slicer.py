"""
==============================================================================
PROJECT: Alternative High-Frequency Data Pipeline (Ecuador)
MODULE: Infinitrack Earth Slicer (OOP Engine)
AUTHOR: Juan José Bedregal
DESCRIPTION: 
Object-Oriented Web Scraping Architecture. Extracts, tunnels, harvests, and 
cleans massive historical news archives using Sitemap mapping and Wayback 
Machine fallback. Incorporates MinHash LSH for semantic deduplication.
==============================================================================
"""

import json
import logging
import cloudscraper
import pandas as pd
from bs4 import BeautifulSoup
import os
import time
import re
import random
import gzip
import io
import sys
from datetime import datetime
from collections import Counter
import nltk
from datasketch import MinHash, MinHashLSH
from tqdm import tqdm

# Directorios Estándar DIME
DIR_INTERIM = os.path.join('..', '..', 'Data', 'Interim')
DIR_CLEANED = os.path.join('..', '..', 'Data', 'Cleaned')

os.makedirs(DIR_INTERIM, exist_ok=True)
os.makedirs(DIR_CLEANED, exist_ok=True)

class InfinitrackEarthSlicer:
    def __init__(self):
        self.UA_CHROME = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/119.0.0.0 Safari/537.36'
        self.UA_GOOGLE = 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)'
        self.MESES_MAP = {'enero':'01','febrero':'02','marzo':'03','abril':'04','mayo':'05','junio':'06','julio':'07','agosto':'08','septiembre':'09','octubre':'10','noviembre':'11','diciembre':'12'}
        
        logging.basicConfig(
            level=logging.INFO,
            format='[%(asctime)s] [EARTH SLICER] %(levelname)s: %(message)s',
            datefmt='%H:%M:%S',
            handlers=[
                logging.StreamHandler(sys.stdout),
                logging.FileHandler(os.path.join(DIR_INTERIM, "earth_slicer_audit.log"), mode='a', encoding='utf-8')
            ]
        )
        self.logger = logging.getLogger("MASTER")
        
    def _identificar_ruido_dinamico(self, df, min_rep=5, umbral_perc=0.10):
        """Generates a dynamic dictionary of repetitive boilerplate text per news outlet."""
        ruido_por_medio = {}
        medios = df['medio'].unique()
        for medio in medios:
            df_medio = df[df['medio'] == medio]
            total = len(df_medio)
            todas_oraciones = []
            for texto in df_medio['cuerpo'].dropna():
                texto_norm = re.sub(r'\s+', ' ', str(texto))
                oraciones = nltk.sent_tokenize(texto_norm, language='spanish')
                todas_oraciones.extend([o.strip() for o in oraciones if len(o.split()) > 4])
            
            conteo = Counter(todas_oraciones)
            frases_ruido = [f for f, frec in conteo.items() if frec >= min_rep and (frec/total) >= umbral_perc]
            if frases_ruido: ruido_por_medio[medio] = frases_ruido
        return ruido_por_medio

    def _get_minhash(self, text):
        """Generates MinHash signature using Trigrams for Semantic LSH Deduplication."""
        m = MinHash(num_perm=128)
        words = str(text).lower().split()
        shingles = set([' '.join(words[i:i+3]) for i in range(len(words)-2)])
        for s in shingles: m.update(s.encode('utf8'))
        return m

    def _limpieza_boilerplate_avanzada(self, text, medio, dic_ruido):
        """Applies dynamic dictionary noise reduction and trigger-phrase removal."""
        if not isinstance(text, str): return ""
        prefix, rest = text[:200], text[200:]
        prefix = re.sub(r'^[A-ZÁÉÍÓÚ\s]+,?\s?\d{0,2}\s?\w*\s?\(.*?\)\s?[\-–—\.]\s?', '', prefix)
        prefix = re.sub(r'^Redacción\s+[\w\s]+\.?\s?[\-–—\.]\s?', '', prefix, flags=re.IGNORECASE)
        text = prefix + rest
        if medio in dic_ruido:
            for frase in dic_ruido[medio]: text = text.replace(frase, "")
        triggers = [r"Lea también:.*?\.", r"Te puede interesar:.*?\.", r"Relacionado:.*?\.", r"Siga leyendo:.*?\."]
        for pat in triggers: text = re.sub(pat, '', text, flags=re.IGNORECASE)
        return re.sub(r'\s+', ' ', text).strip()
    
    def reparar_mojibake(self, text):
        if not text or not isinstance(text, str): return text
        try: return text.encode('latin-1').decode('utf-8')
        except: return text

    def unificar_categorias(self, cat):
        cat = str(cat).strip().replace("['", "").replace("']", "")
        mapping = {
            r'Nacionales|Nacional|Ecuador|Pichincha|Quito|Guayaquil': 'Nacionales',
            r'Economía|Economia|Comercial|Negocios': 'Economía',
            r'Sucesos|Seguridad|Actualidad': 'Sucesos/Seguridad',
            r'Politica|Política|Elecciones|Presidenciales': 'Política',
            r'Entretenimiento|Cine|TV y Streaming|Trending|Tendencias': 'Entretenimiento'
        }
        for pattern, replacement in mapping.items():
            if re.search(pattern, cat, re.IGNORECASE): return replacement
        return "General"

    # =========================================================================
    # PHASE 1: TRENCHER (Source Excavation)
    # =========================================================================
    def trencher(self, target_date):
        logger = logging.getLogger("TRENCHER")
        scraper = cloudscraper.create_scraper()
        SITES_CONFIG = {
            "teleamazonas": {"nombre": "Teleamazonas", "dominio": "teleamazonas.com", "sitemap_raiz": "https://www.teleamazonas.com/sitemap-index.xml", "estrategia": "sitemap_cronologico"},
            "primicias": {"nombre": "Primicias", "dominio": "primicias.ec", "sitemap_raiz": "https://www.primicias.ec/sitemap-index.xml", "estrategia": "sitemap_cronologico"},
            "radio_pichincha": {"nombre": "Radio Pichincha", "dominio": "radiopichincha.com", "sitemap_raiz": "https://www.radiopichincha.com/sitemap_index.xml", "estrategia": "sitemap_lastmod"},
            "metro_ecuador": {"nombre": "Metro Ecuador", "dominio": "metroecuador.com.ec", "sitemap_raiz": "https://www.metroecuador.com.ec/arc/outboundfeeds/sitemap-index/", "estrategia": "sitemap_arc_publishing"},
            "el_telegrafo": {"nombre": "El Telégrafo", "dominio": "eltelegrafo.com.ec", "estrategia": "wayback", "rutas": ["/noticias"]},
            "ecuavisa": {"nombre": "Ecuavisa", "dominio": "ecuavisa.com", "estrategia": "wayback", "rutas": ["/noticias", "/mundo", "/ecuador", "/seguridad"]},
            "el_universo": {"nombre": "El Universo", "dominio": "eluniverso.com", "estrategia": "wayback", "rutas": ["/noticias/politica", "/noticias/ecuador", "/noticias/economia", "/noticias/seguridad"]},
            "la_hora": {"nombre": "La Hora", "dominio": "lahora.com.ec", "estrategia": "wayback", "rutas": ["/pais", "/politica", "/economia", "/sucesos", "/esmeraldas", "/loja"]}
        }
        logger.info(f"Excavating sources for target date: {target_date}")
        year_month = target_date.replace("-", "")[:6]
        target_dt = datetime.strptime(target_date, "%Y-%m-%d")
        target_sitemaps = {}
        for site_id, config in SITES_CONFIG.items():
            if config['estrategia'] == "wayback":
                target_sitemaps[site_id] = {"nombre": config['nombre'], "metodo_descubrimiento": "wayback", "dominio": config['dominio'], "rutas_wayback": config['rutas']}
                continue
            try:
                resp = scraper.get(config['sitemap_raiz'], timeout=20)
                if resp.status_code != 200: continue
                soup = BeautifulSoup(resp.text, 'xml')
                sitemap_tags = soup.find_all('sitemap')
                filtrados = []
                if config['estrategia'] == "sitemap_cronologico":
                    filtrados = [s.find('loc').text for s in sitemap_tags if "noticias" in s.find('loc').text and year_month in s.find('loc').text]
                elif config['estrategia'] == "sitemap_lastmod":
                    for s in sitemap_tags:
                        loc, lastmod = s.find('loc').text, s.find('lastmod')
                        if lastmod and "post-sitemap" in loc and datetime.strptime(lastmod.text[:10], "%Y-%m-%d") >= target_dt: 
                            filtrados.append(loc)
                elif config['estrategia'] == "sitemap_arc_publishing":
                    virtual_url = f"https://www.{config['dominio']}/arc/outboundfeeds/sitemap/{target_date}/?outputType=xml"
                    check = scraper.head(virtual_url, timeout=10)
                    filtrados = [virtual_url] if check.status_code == 200 else [s.find('loc').text for s in sitemap_tags if target_date[:7] in s.find('loc').text]
                target_sitemaps[site_id] = {"nombre": config['nombre'], "metodo_descubrimiento": "sitemap", "dominio": config['dominio'], "sitemaps_especificos": list(set(filtrados))}
            except Exception as e: logger.error(f" [FAILED] {config['nombre']}: {e}")
        
        with open(os.path.join(DIR_INTERIM, "target_sitemaps.json"), "w", encoding="utf-8") as f: 
            json.dump(target_sitemaps, f, indent=4, ensure_ascii=False)

    # =========================================================================
    # PHASE 2: TUNNELLER (Link Extraction & Validation)
    # =========================================================================
    def tunneller(self, target_date):
        logger = logging.getLogger("TUNELLER")
        scraper = cloudscraper.create_scraper()
        EXCLUDE_PATTERNS = ['/categoria/', '/category/', '/tag/', '/tags/', '/etiqueta/', '/autor/', '/author/', '/feed-rss/', '/search/', '/recetas/', '/avisos-judiciales/']
        MULTIMEDIA_NOISE = ['noticiero', 'clima', 'emision', 'matinal', 'estelar', 'primera-emision']
        
        with open(os.path.join(DIR_INTERIM, "target_sitemaps.json"), "r", encoding="utf-8") as f: 
            target_data = json.load(f)
        
        dict_unique_links = {}
        for site_id, info in target_data.items():
            logger.info(f"Tunneling into {info['nombre']}...")
            raw_links = []
            if info['metodo_descubrimiento'] == "wayback":
                date_clean = target_date.replace("-", "")
                for pref in info['rutas_wayback']:
                    for h in range(24):
                        h_str = str(h).zfill(2)
                        url = f"https://web.archive.org/cdx/search/cdx?url={info['dominio']}{pref}&matchType=prefix&from={date_clean}{h_str}0000&to={date_clean}{h_str}5959&output=json&fl=original&filter=statuscode:200&filter=mimetype:text/html&collapse=urlkey"
                        try:
                            resp = scraper.get(url, timeout=15)
                            if resp.status_code == 200:
                                data = resp.json()
                                if len(data) > 1: raw_links.extend([item[0] for item in data[1:]])
                        except: pass
            else:
                for xml_url in info['sitemaps_especificos']:
                    try:
                        resp = scraper.get(xml_url, timeout=20)
                        if resp.status_code == 200:
                            content = gzip.GzipFile(fileobj=io.BytesIO(resp.content)).read() if xml_url.endswith(".gz") or resp.content[:2] == b'\x1f\x8b' else resp.text
                            soup = BeautifulSoup(content, 'xml')
                            for tag in soup.find_all('url'):
                                loc = tag.find('loc').text
                                lastmod = tag.find('lastmod') or tag.find('publication_date')
                                if not lastmod or target_date in lastmod.text: raw_links.append(loc)
                    except: continue
            
            for l in list(set(raw_links)):
                l_clean = l.split('/embed/')[0] + '/' if '/embed/' in l else (l.split('/embed')[0] + '/' if l.endswith('/embed') else l)
                l_lower = l_clean.lower()
                if any(pat in l_lower for pat in EXCLUDE_PATTERNS): continue
                if site_id in ["teleamazonas", "ecuavisa"] and any(n in l_lower for n in MULTIMEDIA_NOISE): continue
                if len(l_clean) < 65: continue
                if l_clean not in dict_unique_links:
                    dict_unique_links[l_clean] = {"url": l_clean, "medio": info['nombre'], "site_id": site_id, "fecha": target_date}
        
        all_discovered_links = list(dict_unique_links.values())
        with open(os.path.join(DIR_INTERIM, "discovered_links.json"), "w", encoding="utf-8") as f: 
            json.dump(all_discovered_links, f, indent=4, ensure_ascii=False)

    # =========================================================================
    # PHASE 3: HARVESTER (Content Mining & Session Isolation)
    # =========================================================================
    def normalizar_fecha_harvester(self, texto_raw):
        if not texto_raw or texto_raw == "N/A": return "N/A"
        texto = str(texto_raw).lower().strip()
        if texto.isdigit() and len(texto) >= 8: return f"{texto[6:8]}-{texto[4:6]}-{texto[0:4]}"
        iso_match = re.search(r'(\d{4})-(\d{1,2})-(\d{1,2})', texto)
        if iso_match: return f"{iso_match.group(3).zfill(2)}-{iso_match.group(2).zfill(2)}-{iso_match.group(1)}"
        for nombre, num in self.MESES_MAP.items():
            if nombre in texto:
                dia_m = re.search(r'\b(\d{1,2})\b', texto)
                anio_m = re.search(r'\b(\d{4})\b', texto)
                if anio_m: return f"{(dia_m.group(1) if dia_m else '01').zfill(2)}-{num}-{anio_m.group(0)}"
        trad_match = re.search(r'(\d{1,2})[-/](\d{1,2})[-/](\d{4})', texto)
        if trad_match: return f"{trad_match.group(1).zfill(2)}-{trad_match.group(2).zfill(2)}-{trad_match.group(3)}"
        return "N/A"

    def extraer_cuerpo_robusto(self, soup):
        best_div, max_p = None, 0
        for div in soup.find_all(['div', 'article', 'section']):
            p_count = len(div.find_all('p')) 
            if p_count > max_p: max_p, best_div = p_count, div
        return " ".join(best_div.get_text(" ", strip=True).split()) if best_div and max_p > 1 else "N/A"

    def logic_la_hora(self, item, scraper):
        url = item['url'].replace('/embed/', '/').replace('/embed', '/')
        html, metodo, url_final = None, None, url
        try:
            resp = scraper.get(url, timeout=12)
            if resp.status_code == 200 and len(resp.text) > 15000: html, metodo = resp.text, "Directo"
        except: pass
        if not html:
            ts = item['fecha'].replace("-", "")
            url_wayback = f"https://web.archive.org/web/{ts}id_/{url}"
            try:
                resp = scraper.get(url_wayback, timeout=15)
                if resp.status_code == 200: html, metodo, url_final = resp.text, "Wayback", url_wayback
            except: pass
        if not html: return None
        soup = BeautifulSoup(html, 'html.parser')
        cuerpo = self.extraer_cuerpo_robusto(soup)
        fec_el = soup.select_one('a[class*="styles_journalistInfoLink"]')
        fecha = fec_el.get_text() if fec_el else (re.search(r'/web/(\d{8})', url_final).group(1) if "web.archive.org" in url_final else "N/A")
        return {"medio": "La Hora", "titular": soup.find('h1').get_text(strip=True) if soup.find('h1') else "N/A", "cuerpo": cuerpo, "seccion": "General", "fecha_raw": fecha}

    def logic_metro_ecuador(self, item, scraper):
        try:
            resp = scraper.get(item['url'], timeout=20)
            soup = BeautifulSoup(resp.text, 'html.parser')
            sel = {"titulo": "h1.b-headline", "cuerpo": "article.b-article-body", "seccion": "a.b-overline", "fecha": "time.b-date"}
            return {"medio": "Metro Ecuador", "titular": soup.select_one(sel['titulo']).get_text(strip=True), "cuerpo": soup.select_one(sel['cuerpo']).get_text(" ", strip=True), "seccion": soup.select_one(sel['seccion']).get_text(strip=True), "fecha_raw": soup.select_one(sel['fecha']).get('datetime')}
        except: return None

    def harvester(self, target_date):
        logger = logging.getLogger("HARVESTER")
        with open(os.path.join(DIR_INTERIM, "discovered_links.json"), "r") as f: links = json.load(f)
        target_norm = datetime.strptime(target_date, "%Y-%m-%d").strftime("%d-%m-%Y")
        SELECTORS = {
            "el_universo": {"titulo": "h1", "cuerpo": "section.article-body", "seccion": "a.primary_section", "fecha": "time"},
            "teleamazonas": {"titulo": "h1.c-detail__title__h1", "cuerpo": "div.body-modules", "seccion": "div.c-detail__tags a", "fecha": ".c-detail__date"},
            "primicias": {"titulo": "h1.c-detail__title__h1", "cuerpo": "div.body-modules", "seccion": ".c-detail__title__label", "fecha": "time.actualizacion-time"},
            "radio_pichincha": {"titulo": "h1.post-title", "cuerpo": "div.entry-content", "seccion": ".meta56__category--fancy a", "fecha": "span.meta56__author__date"},
            "el_telegrafo": {"titulo": "a.box-decoration-clone", "cuerpo": "div.body", "seccion": "a.inline-flex span", "fecha": "div[itemprop='datePublished']"},
            "ecuavisa": {"titulo": "h1[class*='styles_articule']", "cuerpo": "div[id='content-news-section']", "seccion": "a[class*='styles_sectionDescription']", "fecha": "div[class*='styles_journalistInfoLink']"}
        }
        results = []
        logger.info(f"Harvesting {len(links)} articles...")
        for i, item in enumerate(links):
            site_id = item['site_id']
            temp_scraper = cloudscraper.create_scraper(browser={'browser': 'chrome','platform': 'windows','mobile': False})
            if site_id == "ecuavisa":
                temp_scraper.headers.update({'User-Agent': self.UA_GOOGLE})
                temp_scraper.cookies.update({'cookies_accepted': 'true', 'notice_accepted': 'true'})
            res = None
            try:
                if site_id == 'la_hora': res = self.logic_la_hora(item, temp_scraper)
                elif site_id == 'metro_ecuador': 
                    res = self.logic_metro_ecuador(item, temp_scraper)
                    time.sleep(2)
                else:
                    url = item['url']
                    if site_id == 'radio_pichincha': time.sleep(random.uniform(1, 2))
                    resp = temp_scraper.get(url, timeout=20)
                    if site_id == "ecuavisa" and (resp.status_code != 200 or len(resp.text) < 10000):
                        url = f"https://web.archive.org/web/{target_date.replace('-','')}id_/{item['url']}"
                        resp = temp_scraper.get(url, timeout=20)
                    if resp.status_code == 200:
                        soup = BeautifulSoup(resp.text, 'html.parser')
                        sel = SELECTORS.get(site_id)
                        cuerpo_el = soup.select_one(sel['cuerpo'])
                        cuerpo_txt = cuerpo_el.get_text(" ", strip=True) if cuerpo_el else self.extraer_cuerpo_robusto(soup)
                        res = {"medio": item['medio'], "titular": soup.select_one(sel['titulo']).get_text(strip=True) if soup.select_one(sel['titulo']) else "N/A", "cuerpo": cuerpo_txt, "seccion": soup.select_one(sel['seccion']).get_text(strip=True) if soup.select_one(sel['seccion']) else "General", "fecha_raw": soup.select_one(sel['fecha']).get_text() if soup.select_one(sel['fecha']) else "N/A"}
            except Exception as e: logger.error(f"Error on {item['url']}: {e}")
            if res and len(res.get('cuerpo', '')) > 200:
                f_ext = self.normalizar_fecha_harvester(res['fecha_raw'])
                if f_ext == target_norm or f_ext == "N/A":
                    res['fecha'], res['url'] = target_norm, item['url']
                    results.append(res)
                else: logger.warning(f"Discarded by date: {f_ext}")
        with open(os.path.join(DIR_INTERIM, "raw_extracted_data.json"), "w", encoding="utf-8") as f: 
            json.dump(results, f, indent=4, ensure_ascii=False)

    # =========================================================================
    # PHASE 4: CLEANER (Semantic Deduplication & Normalization)
    # =========================================================================
    def cleaner(self, target_date):
        self.logger.info("Executing Phase 4: Earth Slicer Cleaner (MinHash LSH)...")
        try:
            with open(os.path.join(DIR_INTERIM, "raw_extracted_data.json"), "r", encoding="utf-8") as f:
                data = json.load(f)
            df = pd.DataFrame(data)
            if df.empty: return

            df['titular'] = df['titular'].apply(self.reparar_mojibake)
            df['cuerpo'] = df['cuerpo'].apply(self.reparar_mojibake)
            diccionario_ruido = self._identificar_ruido_dinamico(df)
            df['cuerpo'] = df.apply(lambda x: self._limpieza_boilerplate_avanzada(x['cuerpo'], x['medio'], diccionario_ruido), axis=1)
            df['seccion'] = df['seccion'].apply(self.unificar_categorias)

            lsh = MinHashLSH(threshold=0.85, num_perm=128)
            df = df.sort_values(by='cuerpo', key=lambda x: x.str.len(), ascending=False)
            indices_a_mantener = []
            
            for idx, row in tqdm(df.iterrows(), total=len(df), desc="LSH Semantic Deduplication"):
                m = self._get_minhash(row['cuerpo'])
                if not lsh.query(m):
                    lsh.insert(idx, m)
                    indices_a_mantener.append(idx)
            
            df_final = df.loc[indices_a_mantener].copy()
            output_name = os.path.join(DIR_CLEANED, f"dataset_ecuador_{target_date}.csv")
            df_final[['medio', 'fecha', 'seccion', 'titular', 'cuerpo', 'url']].to_csv(output_name, index=False, encoding='utf-8-sig')
            self.logger.info(f"=== PIPELINE SUCCESS: {output_name} ===")
            
        except Exception as e:
            self.logger.error(f"Cleaner Error: {e}")

    def run(self, fecha):
        self.logger.info(f"=== INITIALIZING INFINITRACK FOR DATE: {fecha} ===")
        self.trencher(fecha)
        self.tunneller(fecha)
        self.harvester(fecha)
        self.cleaner(fecha)
