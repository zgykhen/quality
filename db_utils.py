import os
import sqlite3

from app_paths import APP_DIR


def db_path(db_cfg: str) -> str:
    db_cfg_str = str(db_cfg or "").strip()
    if db_cfg_str.lower().endswith(".db"):
        return db_cfg_str
    return os.path.join(db_cfg_str or APP_DIR, "quality.db")


def db_connect(path: str) -> sqlite3.Connection:
    con = sqlite3.connect(path)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    con.execute("PRAGMA foreign_keys=ON;")
    return con


def db_init(con: sqlite3.Connection) -> None:
    con.execute(
        """
    CREATE TABLE IF NOT EXISTS leituras (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts TEXT NOT NULL,
        operador TEXT NOT NULL,
        projeto TEXT NOT NULL,
        turno TEXT,
        referencia TEXT NOT NULL,
        description TEXT,
        quantidade INTEGER NOT NULL,
        comentario TEXT,
        lote TEXT,
        causa TEXT,
        defeito TEXT,
        destino TEXT,
        posto TEXT,
        sessao_id TEXT NOT NULL
    );
    """
    )
    cols = [row[1] for row in con.execute("PRAGMA table_info(leituras);").fetchall()]
    if "description" not in cols:
        con.execute("ALTER TABLE leituras ADD COLUMN description TEXT;")
    if "causa" not in cols:
        con.execute("ALTER TABLE leituras ADD COLUMN causa TEXT;")
    if "defeito" not in cols:
        con.execute("ALTER TABLE leituras ADD COLUMN defeito TEXT;")
    if "destino" not in cols:
        con.execute("ALTER TABLE leituras ADD COLUMN destino TEXT;")
    if "posto" not in cols:
        con.execute("ALTER TABLE leituras ADD COLUMN posto TEXT;")

    con.execute("CREATE INDEX IF NOT EXISTS idx_leituras_ts ON leituras(ts);")
    con.execute("CREATE INDEX IF NOT EXISTS idx_leituras_ref ON leituras(referencia);")
    con.execute("CREATE INDEX IF NOT EXISTS idx_leituras_sessao ON leituras(sessao_id);")
    con.commit()

