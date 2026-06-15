"""
==============================================================================
PROJECT: Alternative High-Frequency Data Pipeline (Ecuador)
MODULE: Main Orchestrator (Pipeline Execution)
AUTHOR: Juan José Bedregal
DESCRIPTION: 
This script initializes the environment, guarantees NLTK resources, and 
iterates through a specified historical date range. It calls the 
InfinitrackEarthSlicer engine day by day to perform resilient news scraping,
ensuring the pipeline doesn't break if a single day fails.
==============================================================================
"""

import sys
import logging
from datetime import datetime, timedelta
from infinitrack_earth_slicer import InfinitrackEarthSlicer

def preparar_entorno_slicer():
    """Verifies and downloads necessary NLTK corpora for sentence tokenization."""
    print("--- VERIFICANDO DEPENDENCIAS PARA INFINITRACK EARTH SLICER ---")
    
    try:
        import nltk
        nltk.download('punkt', quiet=True)
        nltk.download('punkt_tab', quiet=True)
        print("[OK] Recursos léxicos de NLTK descargados y listos.")
    except Exception as e:
        print(f"[X] ERROR: No se pudieron descargar los recursos de NLTK: {e}")

    print("\n--- ENTORNO LISTO PARA EJECUCIÓN MASIVA ---")

if __name__ == "__main__":
    # 0. Preparar entorno NLP
    preparar_entorno_slicer()

    # 1. Definición del Rango de Fechas (Caso: Derrame de Petróleo Esmeraldas)
    fecha_inicio = "2025-03-14"
    fecha_fin = "2025-04-25"

    # 2. Generación del vector temporal
    rango_fechas = []
    start_dt = datetime.strptime(fecha_inicio, "%Y-%m-%d")
    end_dt = datetime.strptime(fecha_fin, "%Y-%m-%d")
    
    current_dt = start_dt
    while current_dt <= end_dt:
        rango_fechas.append(current_dt.strftime("%Y-%m-%d"))
        current_dt += timedelta(days=1)

    # 3. Inicialización del Motor OOP
    slicer = InfinitrackEarthSlicer()

    # 4. Bucle Iterativo de Alta Resiliencia
    print(f"\n[+] Se procesarán {len(rango_fechas)} días: de {fecha_inicio} a {fecha_fin}")
    
    for fecha in rango_fechas:
        try:
            # Ejecuta Fase 1 (Trencher) a Fase 4 (Cleaner)
            slicer.run(fecha)
        except Exception as e:
            print(f" [!] Fallo crítico detectado en la fecha {fecha}: {e}")
            # El pipeline ignora el error de este día y continúa con el siguiente
            continue
            
    print("\n[✔] ORQUESTACIÓN HISTÓRICA FINALIZADA EXITOSAMENTE.")
