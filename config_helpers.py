import configparser
import os
from typing import List, Tuple

from app_paths import APP_DIR, CONFIG_FILENAME, BOM_FILENAME, DESCRIPTION_FILENAME, LOGO_FILENAME


def carregar_caminhos() -> Tuple[str, str, str]:
    """Lê config.ini e devolve (pasta_log, caminho_bom, pasta_db) absolutos."""
    config_path = os.path.join(APP_DIR, CONFIG_FILENAME)
    log_dir = APP_DIR
    db_dir = APP_DIR
    bom_cfg = BOM_FILENAME

    if os.path.isfile(config_path):
        try:
            cfg = configparser.ConfigParser()
            cfg.read(config_path, encoding="utf-8")
            if cfg.has_section("paths"):
                log = cfg.get("paths", "log", fallback=".").strip()
                db = cfg.get("paths", "db", fallback=".").strip()
                bom_cfg = cfg.get("paths", "bom", fallback=BOM_FILENAME).strip() or BOM_FILENAME

                log_dir = os.path.join(APP_DIR, log) if not os.path.isabs(log) else log
                db_dir = os.path.join(APP_DIR, db) if not os.path.isabs(db) else db
        except (configparser.Error, OSError):
            pass

    bom_path_raw = os.path.join(APP_DIR, bom_cfg) if not os.path.isabs(bom_cfg) else bom_cfg
    if os.path.isdir(bom_path_raw):
        bom_path = os.path.join(bom_path_raw, BOM_FILENAME)
    else:
        bom_path = bom_path_raw

    # tolerância a erro comum .cvs
    if not os.path.isfile(bom_path):
        if bom_path.lower().endswith(".csv"):
            alt = bom_path[:-4] + ".cvs"
            if os.path.isfile(alt):
                bom_path = alt
        else:
            alt = os.path.join(os.path.dirname(bom_path), "BOM.cvs")
            if os.path.isfile(alt):
                bom_path = alt

    return log_dir, bom_path, db_dir


def carregar_dropdowns() -> Tuple[List[str], List[str], List[str], List[str]]:
    """Lê config.ini e devolve (projetos, turnos, destinos, postos) para os Combobox."""
    config_path = os.path.join(APP_DIR, CONFIG_FILENAME)
    projetos: List[str] = ["Picking"]
    turnos: List[str] = ["A", "B", "C"]
    destinos: List[str] = ["Scrap", "Retrabalho"]
    postos: List[str] = ["Posto 1"]
    if os.path.isfile(config_path):
        try:
            cfg = configparser.ConfigParser()
            cfg.read(config_path, encoding="utf-8")
            if cfg.has_section("dropdowns"):
                p = cfg.get("dropdowns", "projetos_linhas", fallback="").strip()
                if p:
                    projetos = [x.strip() for x in p.split(",") if x.strip()]
                t = cfg.get("dropdowns", "turnos", fallback="").strip()
                if t:
                    turnos = [x.strip() for x in t.split(",") if x.strip()]
                d = cfg.get("dropdowns", "destinos", fallback="").strip()
                if d:
                    destinos = [x.strip() for x in d.split(",") if x.strip()]
                pto = cfg.get("dropdowns", "postos", fallback="").strip()
                if pto:
                    postos = [x.strip() for x in pto.split(",") if x.strip()]
        except (configparser.Error, OSError):
            pass
    return projetos, turnos, destinos, postos


def carregar_caminho_description() -> str:
    """Lê config.ini e devolve caminho absoluto para Description.csv (com tolerância a .cvs)."""
    config_path = os.path.join(APP_DIR, CONFIG_FILENAME)
    desc_cfg = DESCRIPTION_FILENAME

    if os.path.isfile(config_path):
        try:
            cfg = configparser.ConfigParser()
            cfg.read(config_path, encoding="utf-8")
            if cfg.has_section("paths"):
                desc_cfg = cfg.get("paths", "description", fallback=DESCRIPTION_FILENAME).strip() or DESCRIPTION_FILENAME
        except (configparser.Error, OSError):
            pass

    desc_path_raw = os.path.join(APP_DIR, desc_cfg) if not os.path.isabs(desc_cfg) else desc_cfg
    if os.path.isdir(desc_path_raw):
        desc_path = os.path.join(desc_path_raw, DESCRIPTION_FILENAME)
    else:
        desc_path = desc_path_raw

    # tolerância a erro comum .cvs
    if not os.path.isfile(desc_path):
        if desc_path.lower().endswith(".csv"):
            alt = desc_path[:-4] + ".cvs"
            if os.path.isfile(alt):
                desc_path = alt
        else:
            alt = os.path.join(os.path.dirname(desc_path), "Description.cvs")
            if os.path.isfile(alt):
                desc_path = alt

    return desc_path


def carregar_caminho_logo() -> str:
    """Lê config.ini e devolve caminho absoluto para o ficheiro de logo."""
    config_path = os.path.join(APP_DIR, CONFIG_FILENAME)
    logo_cfg = LOGO_FILENAME

    if os.path.isfile(config_path):
        try:
            cfg = configparser.ConfigParser()
            cfg.read(config_path, encoding="utf-8")
            if cfg.has_section("paths"):
                logo_cfg = cfg.get("paths", "logo", fallback=LOGO_FILENAME).strip() or LOGO_FILENAME
        except (configparser.Error, OSError):
            pass

    logo_path_raw = os.path.join(APP_DIR, logo_cfg) if not os.path.isabs(logo_cfg) else logo_cfg
    if os.path.isdir(logo_path_raw):
        return os.path.join(logo_path_raw, LOGO_FILENAME)
    return logo_path_raw

