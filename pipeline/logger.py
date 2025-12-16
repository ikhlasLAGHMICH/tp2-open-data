import logging
import sys
from pathlib import Path
from datetime import datetime

# Création du dossier logs s'il n'existe pas
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

def setup_logger(name: str):
    """Configure un logger qui écrit dans la console et dans un fichier."""
    
    # Nom du fichier log du jour
    log_filename = f"pipeline_{datetime.now().strftime('%Y-%m-%d')}.log"
    log_path = LOG_DIR / log_filename

    # Création du logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # Éviter d'ajouter des handlers si le logger existe déjà (doublons)
    if logger.hasHandlers():
        return logger

    # Format du message : [DATE] [NIVEAU] [MODULE] Message
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # 1. Sortie Console (Ce qu'on voit à l'écran)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # 2. Sortie Fichier (Pour garder une trace)
    file_handler = logging.FileHandler(log_path, encoding='utf-8')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger