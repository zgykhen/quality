# -*- coding: utf-8 -*-
"""
QualityControl - Registo de scrap e defeitos

ALTERAÇÃO PEDIDA:
- Description e BOM passam a ser CSV (rápido e robusto em PCs de produção)
- Exportação final também em CSV (detalhe + totais por referência do dia)

CSV esperados (delimitador recomendado: ;):
- Description.csv:  Reference;Description
- BOM.csv:          Seat;Component;Quantity

Config.ini (opcional):
[paths]
log=.
db=.
description=Description.csv      ; pode ser caminho absoluto ou relativo
bom=BOM.csv                       ; pode ser caminho absoluto ou relativo
logo=logo.png
"""

import datetime
import csv
import os
import sys
import sqlite3
import uuid
import time
from typing import Dict, List, Tuple, Optional, Any
import tkinter as tk
from tkinter import ttk, messagebox

from app_paths import APP_DIR
from config_helpers import (
    carregar_caminhos,
    carregar_dropdowns,
    carregar_caminho_description,
    carregar_caminho_logo,
)
from db_utils import db_path, db_connect, db_init

# -------------------- Identidade visual --------------------
CORES = {
    "azul": "#0024D3",
    "azul_claro": "#00A9EB",
    "cinza_claro": "#8C8C8C",
    "cinza_escuro": "#575757",
    "branco": "#FFFFFF",
    "fundo": "#F5F5F5",
    "painel_titulo": "#E8F0FE",
    "verde": "#2E7D32",
    "vermelho": "#C62828",
}

_STARTUP_T0 = time.perf_counter()
_STARTUP_LOG = os.path.join(APP_DIR, "startup.log")


def _startup_log(msg: str) -> None:
    try:
        dt = time.perf_counter() - _STARTUP_T0
        with open(_STARTUP_LOG, "a", encoding="utf-8") as f:
            f.write(f"{dt:8.3f}s | {msg}\n")
    except Exception:
        pass


# -------------------- Helpers comuns --------------------
def _normalizar_referencia(valor: Any) -> str:
    if valor is None:
        return ""
    if isinstance(valor, float) and valor.is_integer():
        valor = int(valor)
    return str(valor).strip().upper()


def _detetar_delimitador(path: str) -> str:
    """
    Tenta detetar delimitador. Prioriza ';' (PT), depois ','.
    """
    try:
        with open(path, "r", encoding="utf-8-sig", newline="") as f:
            sample = f.read(4096)
        if ";" in sample and sample.count(";") >= sample.count(","):
            return ";"
        if "," in sample:
            return ","
        # fallback
        return ";"
    except Exception:
        return ";"


# -------------------- CSV reads (rápidos) --------------------
def _carregar_descricoes_csv(description_path=None):
    """
    Lê Description.csv e devolve dict {REF: DESCRIPTION}.
    Aceita headers:
      - Reference / Referencia / Ref
      - Description / Descricao / Desc
    """
    descricoes = {}
    if not description_path:
        description_path = carregar_caminho_description()
    if not description_path or not os.path.isfile(description_path):
        return descricoes

    delim = _detetar_delimitador(description_path)
    t0 = time.perf_counter()

    try:
        with open(description_path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.reader(f, delimiter=delim)
            rows = list(reader)
    except Exception as e:
        _startup_log(f"Falha a ler Description.csv: {e}")
        return descricoes

    if not rows:
        return descricoes

    header = [str(x).strip().lower() for x in rows[0]]
    ref_alias = {"reference", "referencia", "ref"}
    desc_alias = {"description", "descricao", "desc"}

    idx_ref = next((i for i, v in enumerate(header) if v in ref_alias), None)
    idx_desc = next((i for i, v in enumerate(header) if v in desc_alias), None)

    start_row = 1
    # fallback: col A=ref, col B=desc
    if idx_ref is None or idx_desc is None:
        idx_ref = 0
        idx_desc = 1
        start_row = 1  # assume que a 1ª linha pode ser header ou dados; tentamos proteger

        # se a primeira linha "parece header", mantém start_row=1; se não, também funciona
        # porque vamos normalizar e aceitar linhas mesmo assim.

    for r in rows[start_row:]:
        if not r:
            continue
        ref_val = r[idx_ref] if idx_ref < len(r) else ""
        desc_val = r[idx_desc] if idx_desc < len(r) else ""
        ref = _normalizar_referencia(ref_val)
        if not ref:
            continue
        descricoes[ref] = str(desc_val).strip() if desc_val is not None else ""

    _startup_log(f"Description.csv carregado: {len(descricoes)} refs em {time.perf_counter() - t0:.2f}s (delim='{delim}')")
    return descricoes


def _carregar_bom_csv(bom_path=None):
    """
    Lê BOM.csv e devolve lotes no formato {seat: [(component, qty), ...]}.
    Headers esperados: Seat;Component;Quantity (case-insensitive).
    """
    if not bom_path:
        bom_path = os.path.join(APP_DIR, "BOM.csv")
    if not bom_path or not os.path.isfile(bom_path):
        return {}

    delim = _detetar_delimitador(bom_path)
    lotes = {}
    try:
        with open(bom_path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.reader(f, delimiter=delim)
            rows = list(reader)
    except Exception:
        return lotes

    if not rows:
        return lotes

    header = [str(x).strip().lower() for x in rows[0]]
    idx_seat = header.index("seat") if "seat" in header else None
    idx_comp = header.index("component") if "component" in header else None
    idx_qty = header.index("quantity") if "quantity" in header else None

    if idx_seat is None or idx_comp is None or idx_qty is None:
        # fallback: Seat=A, Component=B, Quantity=C
        idx_seat, idx_comp, idx_qty = 0, 1, 2

    for r in rows[1:]:
        if not r:
            continue
        seat_val = r[idx_seat] if idx_seat < len(r) else ""
        comp_val = r[idx_comp] if idx_comp < len(r) else ""
        qty_val = r[idx_qty] if idx_qty < len(r) else ""

        seat = str(seat_val).strip()
        comp = str(comp_val).strip().upper()
        if not seat or not comp:
            continue

        try:
            qty = int(str(qty_val).strip())
            if qty <= 0:
                qty = 1
        except Exception:
            qty = 1

        lotes.setdefault(seat, []).append((comp, qty))

    return lotes


# -------------------- SQLite helpers --------------------
def _db_path(db_cfg):
    db_cfg = str(db_cfg or "").strip()
    if db_cfg.lower().endswith(".db"):
        return db_cfg
    return os.path.join(db_cfg, "quality.db")


def _db_connect(path):
    con = sqlite3.connect(path)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    con.execute("PRAGMA foreign_keys=ON;")
    return con


def _db_init(con):
    con.execute("""
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
    """)
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


# -------------------- App --------------------
class QualityApp:
    def __init__(self) -> None:
        _startup_log("App __init__ start")

        self.root = tk.Tk()
        self.root.title("Quality Control - Registo de scrap e defeitos")
        self.root.state("zoomed")
        self.root.minsize(1020, 1000)
        self.root.configure(bg=CORES["fundo"])

        # Estado da sessão
        self.sessao_iniciada = False
        self.operador = ""
        self.projeto = ""
        self.turno = ""
        self.inicio_sessao = None
        self.consumos: Dict[str, int] = {}
        # (id, referencia, quantidade, timestamp, causa, defeito, destino, posto, description)
        self.ultimas_leituras: List[tuple] = []
        self.logfile: Optional[str] = None

        # paths
        self.log_dir, self.bom_path, self.db_dir = carregar_caminhos()
        self.description_path = carregar_caminho_description()
        self.logo_path = carregar_caminho_logo()

        # descrições (CSV lazy)
        self.descricoes_ref: Dict[str, str] = {}
        self._desc_mtime = None
        self._desc_loading = False

        self._logo_img = None
        self._timer_duracao = None
        self._timer_foco_referencia = None

        # SQLite
        self.db_con: Optional[sqlite3.Connection] = None
        self.db_path: Optional[str] = None
        self.sessao_id: Optional[str] = None

        self.root.protocol("WM_DELETE_WINDOW", self._on_close)
        self._construir_interface()
        _startup_log("UI construída")

        # Lazy-load para não bloquear arranque
        self.root.after(150, lambda: self._carregar_descricoes_se_necessario(force=True))
        _startup_log("Agendado carregamento Description.csv (lazy)")

    def _on_close(self) -> None:
        """Handler de fecho da janela principal."""
        if self.sessao_iniciada:
            if not messagebox.askyesno(
                "Terminar aplicação",
                "Existe uma sessao em curso.\n\n"
                "Deseja terminar a sessao atual e fechar a aplicação?",
            ):
                return
            self._terminar_sessao()
        self.root.destroy()

    # -------------------- UI --------------------
    def _construir_interface(self) -> None:
        c = CORES

        header = tk.Frame(self.root, bg=c["azul"], height=64)
        header.pack(fill=tk.X)
        header.pack_propagate(False)

        if os.path.isfile(self.logo_path):
            try:
                self._logo_img = tk.PhotoImage(file=self.logo_path)
                tk.Label(header, image=self._logo_img, bg=c["azul"]).pack(side=tk.LEFT, padx=16, pady=12)
            except tk.TclError:
                self._logo_img = None

        if self._logo_img is None:
            tk.Label(header, text="FORVIA", font=("Segoe UI", 18, "bold"),
                     fg=c["branco"], bg=c["azul"]).pack(side=tk.LEFT, padx=16, pady=12)

        tk.Label(header, text="Quality Control - Registo de scrap e defeitos",
                 font=("Segoe UI", 14, "bold"), fg=c["branco"], bg=c["azul"]).pack(side=tk.LEFT, padx=20, pady=14)

        main = tk.Frame(self.root, bg=c["fundo"], padx=12, pady=12)
        main.pack(fill=tk.BOTH, expand=True)

        left = tk.Frame(main, bg=c["fundo"])
        left.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        self._painel_titulo(left, "Sessao")
        frame_sessao_linha = tk.Frame(left, bg=c["fundo"])
        frame_sessao_linha.pack(fill=tk.X, pady=(0, 10))

        frame_sessao = tk.Frame(frame_sessao_linha, bg=c["branco"], padx=12, pady=10, relief=tk.FLAT)
        frame_sessao.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        projetos_list, turnos_list, destinos_list, postos_list = carregar_dropdowns()

        tk.Label(frame_sessao, text="Operador:", font=("Segoe UI", 9),
                 fg=c["cinza_escuro"], bg=c["branco"]).grid(row=0, column=0, sticky=tk.W, pady=2)
        self.entry_operador = tk.Entry(frame_sessao, width=18, font=("Segoe UI", 10), relief=tk.SOLID, bd=1)
        self.entry_operador.grid(row=0, column=1, padx=(8, 12), pady=2)

        tk.Label(frame_sessao, text="Projeto/Linha:", font=("Segoe UI", 9),
                 fg=c["cinza_escuro"], bg=c["branco"]).grid(row=1, column=0, sticky=tk.W, pady=2)
        self.combo_projeto = ttk.Combobox(frame_sessao, width=18, font=("Segoe UI", 10),
                                          values=projetos_list, state="readonly")
        if projetos_list:
            self.combo_projeto.set(projetos_list[0])
        self.combo_projeto.grid(row=1, column=1, padx=(8, 12), pady=2)

        tk.Label(frame_sessao, text="Turno:", font=("Segoe UI", 9),
                 fg=c["cinza_escuro"], bg=c["branco"]).grid(row=2, column=0, sticky=tk.W, pady=2)
        self.combo_turno = ttk.Combobox(frame_sessao, width=18, font=("Segoe UI", 10),
                                        values=turnos_list, state="readonly")
        if turnos_list:
            self.combo_turno.set(turnos_list[0])
        self.combo_turno.grid(row=2, column=1, padx=(8, 12), pady=2)

        self.label_sessao = tk.Label(frame_sessao, text="Introduza operador e clique em Iniciar sessao.",
                                     font=("Segoe UI", 9), fg=c["cinza_claro"], bg=c["branco"])
        self.label_sessao.grid(row=3, column=0, columnspan=2, sticky=tk.W, pady=(6, 4))

        self.btn_iniciar = tk.Button(frame_sessao, text="Iniciar sessao", font=("Segoe UI", 10, "bold"),
                                     fg=c["branco"], bg=c["azul"], activebackground=c["azul_claro"],
                                     activeforeground=c["branco"], relief=tk.FLAT, padx=12, pady=4,
                                     cursor="hand2", command=self._iniciar_sessao)
        self.btn_iniciar.grid(row=4, column=0, columnspan=2, pady=(4, 0))

        frame_hora = tk.Frame(frame_sessao_linha, bg=c["azul"], padx=16, pady=12, relief=tk.FLAT)
        frame_hora.pack(side=tk.LEFT, fill=tk.Y)

        self.label_hora = tk.Label(frame_hora, text="--:--:--", font=("Segoe UI", 22, "bold"),
                                   fg=c["branco"], bg=c["azul"])
        self.label_hora.pack()

        self.label_inicio_sessao = tk.Label(frame_hora, text="Sessao iniciada a\n--:--:--",
                                            font=("Segoe UI", 9), fg=c["branco"], bg=c["azul"],
                                            justify=tk.CENTER)
        self.label_inicio_sessao.pack(pady=(4, 0))
        self._atualizar_hora()

        self._painel_titulo(left, "Leitura")
        frame_leitura = tk.Frame(left, bg=c["branco"], padx=12, pady=10, relief=tk.FLAT)
        frame_leitura.pack(fill=tk.X, pady=(0, 10))

        frame_leitura_cols = tk.Frame(frame_leitura, bg=c["branco"])
        frame_leitura_cols.pack(fill=tk.X)

        col_manual = tk.Frame(frame_leitura_cols, bg=c["branco"])
        col_manual.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 10))

        col_lote = tk.Frame(frame_leitura_cols, bg=c["branco"])
        col_lote.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(10, 0))

        tk.Label(col_manual, text="Referencia:", font=("Segoe UI", 9),
                 fg=c["cinza_escuro"], bg=c["branco"]).pack(anchor=tk.W)
        self.entry_referencia = tk.Entry(col_manual, width=36, font=("Segoe UI", 12), relief=tk.SOLID, bd=1)
        self.entry_referencia.pack(fill=tk.X, pady=(2, 8))
        self.entry_referencia.bind("<Return>", lambda e: self._registar_leitura())
        self.entry_referencia.bind("<FocusIn>", lambda e: self.entry_referencia.select_range(0, tk.END))

        row_qty = tk.Frame(col_manual, bg=c["branco"])
        row_qty.pack(fill=tk.X, pady=(0, 8))
        tk.Label(row_qty, text="Quantidade:", font=("Segoe UI", 9),
                 fg=c["cinza_escuro"], bg=c["branco"]).pack(side=tk.LEFT, padx=(0, 8))
        self.var_quantidade = tk.StringVar(value="1")
        self.spin_quantidade = tk.Spinbox(row_qty, from_=1, to=9999, width=8,
                                          textvariable=self.var_quantidade, font=("Segoe UI", 10))
        self.spin_quantidade.pack(side=tk.LEFT, padx=(0, 4))
        self.spin_quantidade.bind("<KeyRelease>", self._validar_quantidade_teclado)
        self.spin_quantidade.bind("<FocusOut>", self._normalizar_quantidade)

        tk.Button(row_qty, text="-", width=2, font=("Segoe UI", 10),
                  fg=c["cinza_escuro"], bg=c["fundo"], relief=tk.FLAT,
                  command=lambda: self._alterar_quantidade(-1)).pack(side=tk.LEFT, padx=2)
        tk.Button(row_qty, text="+", width=2, font=("Segoe UI", 10),
                  fg=c["cinza_escuro"], bg=c["fundo"], relief=tk.FLAT,
                  command=lambda: self._alterar_quantidade(1)).pack(side=tk.LEFT)

        self.btn_registar = tk.Button(col_manual, text="  REGISTAR (Enter)  ", width=20,
                                      font=("Segoe UI", 11, "bold"),
                                      fg=c["branco"], bg=c["verde"], activebackground="#1B5E20",
                                      activeforeground=c["branco"], relief=tk.FLAT, padx=16, pady=6,
                                      cursor="hand2", command=self._registar_leitura)
        self.btn_registar.pack(anchor=tk.W, pady=(4, 0))

        tk.Label(col_lote, text="Causa:", font=("Segoe UI", 9),
                 fg=c["cinza_escuro"], bg=c["branco"]).pack(anchor=tk.W)
        self.entry_causa = tk.Entry(col_lote, width=36, font=("Segoe UI", 10), relief=tk.SOLID, bd=1)
        self.entry_causa.pack(fill=tk.X, pady=(2, 8))
        self.entry_causa.bind("<Return>", lambda e: self._registar_leitura())

        tk.Label(col_lote, text="Defeito:", font=("Segoe UI", 9),
                 fg=c["cinza_escuro"], bg=c["branco"]).pack(anchor=tk.W)
        self.entry_defeito = tk.Entry(col_lote, width=36, font=("Segoe UI", 10), relief=tk.SOLID, bd=1)
        self.entry_defeito.pack(fill=tk.X, pady=(2, 8))
        self.entry_defeito.bind("<Return>", lambda e: self._registar_leitura())

        tk.Label(col_lote, text="Destino:", font=("Segoe UI", 9),
                 fg=c["cinza_escuro"], bg=c["branco"]).pack(anchor=tk.W)
        self.destinos_list = list(destinos_list or [])
        self.combo_destino = ttk.Combobox(col_lote, width=36, font=("Segoe UI", 10),
                                         values=self.destinos_list, state="readonly")
        if self.destinos_list:
            self.combo_destino.set(self.destinos_list[0])
        self.combo_destino.pack(fill=tk.X, pady=(2, 8))

        tk.Label(col_lote, text="Posto:", font=("Segoe UI", 9),
                 fg=c["cinza_escuro"], bg=c["branco"]).pack(anchor=tk.W)
        self.postos_list = list(postos_list or [])
        self.combo_posto = ttk.Combobox(col_lote, width=36, font=("Segoe UI", 10),
                                       values=self.postos_list, state="readonly")
        if self.postos_list:
            self.combo_posto.set(self.postos_list[0])
        self.combo_posto.pack(fill=tk.X, pady=(2, 8))

        tk.Frame(col_lote, bg=c["branco"], height=18).pack(fill=tk.X)

        self._painel_titulo(left, "Leituras da sessao")
        frame_ultimas = tk.Frame(left, bg=c["branco"], padx=8, pady=8, relief=tk.FLAT)
        frame_ultimas.pack(fill=tk.BOTH, expand=True, pady=(0, 10))
        self.list_ultimas = tk.Listbox(frame_ultimas, height=10, font=("Consolas", 10),
                                       bg=c["branco"], fg=c["cinza_escuro"],
                                       selectbackground=c["azul"], selectforeground=c["branco"],
                                       relief=tk.FLAT, highlightthickness=0)
        scroll_ultimas = tk.Scrollbar(frame_ultimas, orient=tk.VERTICAL, command=self.list_ultimas.yview, bg=c["cinza_claro"])
        self.list_ultimas.configure(yscrollcommand=scroll_ultimas.set)
        self.list_ultimas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scroll_ultimas.pack(side=tk.RIGHT, fill=tk.Y)

        tk.Button(frame_ultimas, text="Eliminar leitura selecionada",
                  font=("Segoe UI", 9), fg=c["azul"], bg=c["branco"],
                  activeforeground=c["azul_claro"], relief=tk.FLAT,
                  cursor="hand2", command=self._eliminar_leitura).pack(anchor=tk.W, pady=(6, 0))

        tk.Button(frame_ultimas, text="Editar causa/defeito/destino/posto",
                  font=("Segoe UI", 9), fg=c["azul"], bg=c["branco"],
                  activeforeground=c["azul_claro"], relief=tk.FLAT,
                  cursor="hand2", command=self._editar_detalhes_leitura).pack(anchor=tk.W, pady=(4, 0))

        right = tk.Frame(main, bg=c["fundo"])
        right.pack(side=tk.LEFT, fill=tk.BOTH, expand=False, padx=(16, 0))

        self._painel_titulo(right, "Resumo da sessao")
        frame_resumo = tk.Frame(right, bg=c["branco"], padx=8, pady=8, relief=tk.FLAT)
        frame_resumo.pack(fill=tk.BOTH, expand=True, pady=(0, 10))

        self.text_resumo = tk.Text(frame_resumo, width=36, height=20, font=("Consolas", 10), state=tk.DISABLED,
                                   bg=c["branco"], fg=c["cinza_escuro"], relief=tk.FLAT, wrap=tk.WORD)
        scroll_resumo = tk.Scrollbar(frame_resumo, orient=tk.VERTICAL, command=self.text_resumo.yview, bg=c["cinza_claro"])
        self.text_resumo.configure(yscrollcommand=scroll_resumo.set)
        self.text_resumo.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scroll_resumo.pack(side=tk.RIGHT, fill=tk.Y)

        self._painel_titulo(right, "Relatorios")
        frame_relatorios = tk.Frame(right, bg=c["branco"], padx=10, pady=10, relief=tk.FLAT)
        frame_relatorios.pack(fill=tk.X, pady=(0, 10))
        tk.Label(
            frame_relatorios,
            text="Extracao CSV direta da base de dados.",
            font=("Segoe UI", 9),
            fg=c["cinza_escuro"],
            bg=c["branco"],
        ).pack(anchor=tk.W)
        self.btn_abrir_relatorios = tk.Button(
            frame_relatorios,
            text="  Abrir exportacao CSV  ",
            font=("Segoe UI", 10, "bold"),
            fg=c["branco"],
            bg=c["azul"],
            activebackground=c["azul_claro"],
            activeforeground=c["branco"],
            relief=tk.FLAT,
            padx=10,
            pady=4,
            cursor="hand2",
            command=self._abrir_janela_exportacao_csv_db,
        )
        self.btn_abrir_relatorios.pack(anchor=tk.W, pady=(8, 0))

        footer = tk.Frame(self.root, bg=c["branco"], padx=16, pady=12)
        footer.pack(fill=tk.X)

        self.label_total = tk.Label(footer, text=" Total do dia: 0", font=("Segoe UI", 11, "bold"),
                                    fg=c["azul"], bg=c["branco"])
        self.label_total.pack(side=tk.LEFT)

        self.label_leituras_sessao = tk.Label(footer, text=" Sessao: 0", font=("Segoe UI", 9),
                                              fg=c["cinza_escuro"], bg=c["branco"])
        self.label_leituras_sessao.pack(side=tk.LEFT, padx=(20, 0))

        self.label_refs = tk.Label(footer, text=" Ref. unicas: 0", font=("Segoe UI", 9),
                                   fg=c["cinza_escuro"], bg=c["branco"])
        self.label_refs.pack(side=tk.LEFT, padx=(12, 0))

        self.label_duracao = tk.Label(footer, text=" Duracao: 00:00:00", font=("Segoe UI", 9),
                                      fg=c["cinza_escuro"], bg=c["branco"])
        self.label_duracao.pack(side=tk.LEFT, padx=(12, 0))

        self.btn_terminar = tk.Button(footer, text="  Terminar sessao  ", font=("Segoe UI", 10, "bold"),
                                      fg=c["branco"], bg=c["vermelho"], activebackground="#B71C1C",
                                      activeforeground=c["branco"], relief=tk.FLAT, padx=12, pady=4,
                                      cursor="hand2", command=self._terminar_sessao)
        self.btn_terminar.pack(side=tk.RIGHT)

        footer2 = tk.Frame(self.root, bg=c["branco"], padx=16, pady=6)
        footer2.pack(fill=tk.X)
        tk.Label(footer2, text="Desenvolvido por Bruno Santos - 2026 - v1.8", font=("Segoe UI", 8),
                 fg=c["cinza_claro"], bg=c["branco"]).pack(anchor=tk.W)

        self._atualizar_resumo()
        self.root.bind_all("<FocusIn>", self._on_focus_change, add="+")
        self.entry_referencia.focus_set()
        self._agendar_retorno_referencia()

    def _painel_titulo(self, parent: tk.Widget, texto: str) -> None:
        f = tk.Frame(parent, bg=CORES["painel_titulo"], padx=10, pady=6)
        f.pack(fill=tk.X)
        tk.Label(f, text=texto, font=("Segoe UI", 10, "bold"),
                 fg=CORES["cinza_escuro"], bg=CORES["painel_titulo"]).pack(anchor=tk.W)

    # -------------------- Focus helpers --------------------
    def _on_focus_change(self, event: Optional[tk.Event] = None) -> None:
        self._agendar_retorno_referencia()

    def _agendar_retorno_referencia(self) -> None:
        if self._timer_foco_referencia:
            self.root.after_cancel(self._timer_foco_referencia)
        self._timer_foco_referencia = self.root.after(150000, self._retornar_foco_referencia)

    def _retornar_foco_referencia(self) -> None:
        self._timer_foco_referencia = None
        if self.root.state() == "iconic" or self.root.focus_displayof() is None:
            self._agendar_retorno_referencia()
            return
        if self.root.focus_get() != self.entry_referencia:
            try:
                self.entry_referencia.focus_set()
                self.entry_referencia.icursor(tk.END)
            except tk.TclError:
                pass
        self._agendar_retorno_referencia()

    # -------------------- Hora / duração --------------------
    def _atualizar_hora(self) -> None:
        now = datetime.datetime.now()
        self.label_hora.configure(text=now.strftime("%H:%M:%S"))
        if self.sessao_iniciada and self.inicio_sessao:
            self.label_inicio_sessao.configure(text=f"Sessao iniciada a\n{self.inicio_sessao.strftime('%H:%M:%S')}")
        else:
            self.label_inicio_sessao.configure(text="Sessao iniciada a\n--:--:--")
        self.root.after(1000, self._atualizar_hora)

    def _atualizar_duracao(self) -> None:
        if not self.sessao_iniciada or not self.inicio_sessao:
            self._timer_duracao = None
            return
        delta = datetime.datetime.now() - self.inicio_sessao
        h, r = divmod(int(delta.total_seconds()), 3600)
        m, s = divmod(r, 60)
        self.label_duracao.configure(text=f" Duracao: {h:02d}:{m:02d}:{s:02d}")
        self._timer_duracao = self.root.after(1000, self._atualizar_duracao)

    # -------------------- Descrições CSV (lazy) --------------------
    def _carregar_descricoes_se_necessario(self, force: bool = False) -> None:
        if self._desc_loading:
            return

        path = carregar_caminho_description()
        self.description_path = path

        mtime = None
        if path and os.path.isfile(path):
            try:
                mtime = os.path.getmtime(path)
            except OSError:
                mtime = None

        if not force and mtime == self._desc_mtime:
            return

        self._desc_loading = True
        try:
            self.descricoes_ref = _carregar_descricoes_csv(path)
            self._desc_mtime = mtime
        finally:
            self._desc_loading = False

    # -------------------- Sessão --------------------
    def _iniciar_sessao(self) -> None:
        op = self.entry_operador.get().strip()
        proj = self.combo_projeto.get().strip() if self.combo_projeto.get() else ""
        turno = self.combo_turno.get().strip() if self.combo_turno.get() else ""

        if not op:
            messagebox.showwarning("Campos em falta", "Preencha o Operador antes de iniciar.")
            return
        if not proj:
            messagebox.showwarning("Campos em falta", "Selecione Projeto/Linha antes de iniciar.")
            return

        try:
            if self.db_con is not None:
                self.db_con.close()
        except Exception:
            pass
        self.db_con = None

        self.operador = op
        self.projeto = proj
        self.turno = turno
        self.inicio_sessao = datetime.datetime.now()

        self.log_dir, self.bom_path, self.db_dir = carregar_caminhos()
        self.description_path = carregar_caminho_description()
        self.logo_path = carregar_caminho_logo()

        os.makedirs(self.log_dir, exist_ok=True)
        db_parent = self.db_dir if not str(self.db_dir).lower().endswith(".db") else (os.path.dirname(self.db_dir) or APP_DIR)
        os.makedirs(db_parent, exist_ok=True)
        self.logfile = os.path.join(self.log_dir, f"log_{self.inicio_sessao.date()}.csv")

        try:
            self.sessao_id = uuid.uuid4().hex
            self.db_path = db_path(self.db_dir)
            self.db_con = db_connect(self.db_path)
            db_init(self.db_con)
        except sqlite3.Error as err:
            self.db_con = None
            messagebox.showerror("Erro DB", f"Não foi possível iniciar a base de dados.\n\n{err}")
            return

        self.consumos = {}
        self.ultimas_leituras.clear()
        self.sessao_iniciada = True
        self._atualizar_ultimas()

        # garante descrições em background (não bloqueia)
        self.root.after(50, lambda: self._carregar_descricoes_se_necessario(force=False))

        self.entry_operador.configure(state="disabled")
        self.combo_projeto.configure(state="disabled")
        self.combo_turno.configure(state="disabled")
        self.btn_iniciar.configure(state=tk.DISABLED)

        txt = f"Sessao iniciada as {self.inicio_sessao.strftime('%H:%M:%S')}  {self.operador} | {self.projeto}"
        if self.turno:
            txt += f" | {self.turno}"

        self.label_sessao.configure(text=txt, fg=CORES["verde"])
        self.label_inicio_sessao.configure(text=f"Sessao iniciada a\n{self.inicio_sessao.strftime('%H:%M:%S')}")
        self._atualizar_resumo()
        self._atualizar_duracao()
        self.entry_referencia.focus_set()

    # -------------------- Leitura --------------------
    def _obter_quantidade(self) -> int:
        texto = self.spin_quantidade.get().strip()
        if not texto:
            return 1
        try:
            n = int(texto)
        except ValueError:
            return 1
        return max(1, min(9999, n))

    def _validar_quantidade_teclado(self, event: Optional[tk.Event] = None) -> None:
        texto = self.spin_quantidade.get()
        if not texto:
            return
        filtrado = "".join(ch for ch in texto if ch.isdigit())
        if filtrado != texto:
            self.spin_quantidade.delete(0, tk.END)
            self.spin_quantidade.insert(0, filtrado)

    def _normalizar_quantidade(self, event: Optional[tk.Event] = None) -> None:
        q = self._obter_quantidade()
        self.var_quantidade.set(str(q))

    def _alterar_quantidade(self, delta: int) -> None:
        q = self._obter_quantidade()
        q = max(1, min(9999, q + delta))
        self.var_quantidade.set(str(q))

    def _registar_leitura(self) -> None:
        if not self.sessao_iniciada:
            messagebox.showwarning("Sessao nao iniciada", "Inicie a sessao antes de registar leituras.")
            return

        referencia = self.entry_referencia.get().strip().upper()
        if not referencia:
            return

        if referencia == "EXIT":
            self._terminar_sessao()
            return

        quantidade = self._obter_quantidade()
        causa = self.entry_causa.get().strip().replace(";", ",")
        defeito = self.entry_defeito.get().strip().replace(";", ",")
        destino = self.combo_destino.get().strip()
        posto = self.combo_posto.get().strip()

        if not causa:
            messagebox.showwarning("Campos em falta", "Preencha a Causa.")
            self.entry_causa.focus_set()
            return
        if not defeito:
            messagebox.showwarning("Campos em falta", "Preencha o Defeito.")
            self.entry_defeito.focus_set()
            return
        if not destino:
            messagebox.showwarning("Campos em falta", "Selecione o Destino.")
            self.combo_destino.focus_set()
            return
        if not posto:
            messagebox.showwarning("Campos em falta", "Selecione o Posto.")
            self.combo_posto.focus_set()
            return

        self._registar_item(referencia, quantidade, causa, defeito, destino, posto)

        self.entry_referencia.delete(0, tk.END)
        self.var_quantidade.set("1")
        self.entry_causa.delete(0, tk.END)
        self.entry_defeito.delete(0, tk.END)
        if self.destinos_list:
            self.combo_destino.set(self.destinos_list[0])
        if self.postos_list:
            self.combo_posto.set(self.postos_list[0])

        self._atualizar_ultimas()
        self._atualizar_resumo()
        self.entry_referencia.focus_set()

    def _registar_item(self, referencia: str, quantidade: int = 1, causa: str = "", defeito: str = "", destino: str = "", posto: str = "") -> None:
        """Regista no SQLite (fonte de verdade) + CSV (compatibilidade)."""
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # descrições: se ainda não carregou, não bloqueia (pode ficar vazio)
        self._carregar_descricoes_se_necessario(force=False)
        descricao_registo = self.descricoes_ref.get(_normalizar_referencia(referencia), "").strip()

        causa = str(causa or "").strip().replace(";", ",")
        defeito = str(defeito or "").strip().replace(";", ",")
        destino = str(destino or "").strip().replace(";", ",")
        posto = str(posto or "").strip().replace(";", ",")

        self.consumos[referencia] = self.consumos.get(referencia, 0) + int(quantidade)

        try:
            if self.db_con is None:
                raise sqlite3.Error("Ligação DB não inicializada.")
            cur = self.db_con.execute(
                """INSERT INTO leituras
                   (ts, operador, projeto, turno, referencia, description, quantidade, causa, defeito, destino, posto, sessao_id)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (timestamp, self.operador, self.projeto, self.turno,
                 referencia, descricao_registo, int(quantidade), causa, defeito, destino, posto, self.sessao_id)
            )
            self.db_con.commit()
            row_id = cur.lastrowid
        except sqlite3.Error as err:
            messagebox.showerror("Erro DB", str(err))
            return

        self.ultimas_leituras.insert(0, (row_id, referencia, int(quantidade), timestamp, causa, defeito, destino, posto, descricao_registo))

        # CSV de log
        try:
            escrever_cabecalho = (not os.path.isfile(self.logfile)) or (os.path.getsize(self.logfile) == 0)
            if not escrever_cabecalho:
                self._garantir_cabecalho_csv_atual()

            with open(self.logfile, mode="a", newline="", encoding="utf-8") as f:
                w = csv.writer(f, delimiter=";")
                if escrever_cabecalho:
                    w.writerow(["Data", "Operador", "Projeto", "Turno", "Referencia", "Description",
                                "Quantidade", "Causa", "Defeito", "Destino", "Posto"])
                w.writerow([timestamp, self.operador, self.projeto, self.turno, referencia, descricao_registo,
                            int(quantidade), causa, defeito, destino, posto])
                f.flush()
                os.fsync(f.fileno())
        except OSError as err:
            messagebox.showwarning("Aviso CSV", f"Registo guardado na BD, mas falhou a escrita no CSV:\n\n{err}")

    def _garantir_cabecalho_csv_atual(self):
        if not self.logfile or not os.path.isfile(self.logfile):
            return
        try:
            with open(self.logfile, mode="r", newline="", encoding="utf-8") as f:
                rows = list(csv.reader(f, delimiter=";"))
        except OSError:
            return
        if not rows:
            return

        header = rows[0]
        header_correto = (
            len(header) >= 11
            and header[0] == "Data"
            and header[1] == "Operador"
            and header[5] in ("Description", "Descricao")
            and header[7] == "Causa"
            and header[8] == "Defeito"
            and header[9] == "Destino"
            and header[10] == "Posto"
        )
        if header_correto:
            return
        if len(header) < 2 or header[0] != "Data" or header[1] != "Operador":
            return

        novos_rows = [["Data", "Operador", "Projeto", "Turno", "Referencia", "Description",
                       "Quantidade", "Causa", "Defeito", "Destino", "Posto"]]
        for row in rows[1:]:
            if len(row) >= 11:
                row_n = row[:11]
            elif len(row) == 10:
                row_n = row[:10] + [""]
            elif len(row) == 9:
                row_n = [row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], "", ""]
            elif len(row) == 8:
                row_n = [row[0], row[1], row[2], row[3], row[4], "", row[5], row[6], row[7], "", ""]
            elif len(row) == 7:
                row_n = [row[0], row[1], row[2], row[3], row[4], "", row[5], row[6], "", "", ""]
            elif len(row) == 6:
                row_n = [row[0], row[1], row[2], "", row[3], "", row[4], row[5], "", "", ""]
            elif len(row) >= 5:
                row_n = [row[0], row[1], row[2], "", row[3], "", row[4], "", "", "", ""]
            else:
                continue
            novos_rows.append(row_n)

        try:
            with open(self.logfile, mode="w", newline="", encoding="utf-8") as f:
                w = csv.writer(f, delimiter=";")
                w.writerows(novos_rows)
        except OSError:
            pass

    # -------------------- Listas / Resumo --------------------
    def _atualizar_ultimas(self) -> None:
        self.list_ultimas.delete(0, tk.END)
        self._carregar_descricoes_se_necessario(force=False)
        total = len(self.ultimas_leituras)
        for i, item in enumerate(self.ultimas_leituras, 1):
            row_id, ref, qty, timestamp = item[:4]
            causa_item = item[4] if len(item) > 4 else ""
            defeito_item = item[5] if len(item) > 5 else ""
            destino_item = item[6] if len(item) > 6 else ""
            posto_item = item[7] if len(item) > 7 else ""
            descricao_item = item[8] if len(item) > 8 else ""

            hora = timestamp.rsplit(" ", 1)[-1] if isinstance(timestamp, str) and " " in timestamp else "--------"
            ordem_sessao = total - i + 1
            descricao = descricao_item or self.descricoes_ref.get(_normalizar_referencia(ref), "")

            extras = []
            if causa_item:
                extras.append(f"C:{causa_item}")
            if defeito_item:
                extras.append(f"D:{defeito_item}")
            if destino_item:
                extras.append(f"Dest:{destino_item}")
            if posto_item:
                extras.append(f"P:{posto_item}")

            texto = f"{hora} {ordem_sessao}. {ref} {descricao} {qty} {' | '.join(extras)}".strip()
            self.list_ultimas.insert(tk.END, texto)

    def _total_do_dia(self) -> int:
        dia = datetime.date.today().strftime("%Y-%m-%d")

        # SQLite - usa sempre a BD se existir, mesmo fora de sessão
        con: Optional[sqlite3.Connection] = None
        fechar_con = False
        try:
            try:
                con, fechar_con = self._obter_conexao_db_relatorio()
            except FileNotFoundError:
                con = None

            if con is not None:
                row = con.execute(
                    "SELECT COALESCE(SUM(quantidade), 0) FROM leituras WHERE ts LIKE ?",
                    (dia + "%",),
                ).fetchone()
                return int(row[0] or 0)
        except sqlite3.Error:
            pass
        finally:
            if fechar_con and con is not None:
                try:
                    con.close()
                except Exception:
                    pass

        # fallback CSV
        self.log_dir, _, _ = carregar_caminhos()
        path = os.path.join(self.log_dir, f"log_{datetime.date.today()}.csv")
        if not os.path.isfile(path):
            return 0
        total = 0
        try:
            with open(path, mode="r", newline="", encoding="utf-8") as f:
                reader = csv.reader(f, delimiter=";")
                for row in reader:
                    if len(row) < 7 or row[0] == "Data":
                        continue
                    try:
                        total += int(row[6])
                    except Exception:
                        total += 1
        except OSError:
            pass
        return total

    def _atualizar_resumo(self) -> None:
        self.text_resumo.configure(state=tk.NORMAL)
        self.text_resumo.delete(1.0, tk.END)
        for ref, qty in sorted(self.consumos.items()):
            self.text_resumo.insert(tk.END, f"  {ref}    {qty}\n")
        self.text_resumo.configure(state=tk.DISABLED)

        total_sessao = sum(self.consumos.values())
        total_dia = self._total_do_dia()
        n_refs = len(self.consumos)

        self.label_total.configure(text=f" Total do dia: {total_dia}")
        self.label_leituras_sessao.configure(text=f" Sessao: {total_sessao}")
        self.label_refs.configure(text=f" Ref. unicas: {n_refs}")

        if self.sessao_iniciada and self.inicio_sessao:
            delta = datetime.datetime.now() - self.inicio_sessao
            h, r = divmod(int(delta.total_seconds()), 3600)
            m, s = divmod(r, 60)
            self.label_duracao.configure(text=f" Duracao: {h:02d}:{m:02d}:{s:02d}")
        else:
            self.label_duracao.configure(text=" Duracao: 00:00:00")

    # -------------------- Edição / Eliminar (mantido como estava) --------------------
    # -------------------- Relatorios CSV (DB) --------------------
    def _parse_data_relatorio(self, valor: Any) -> datetime.date:
        texto = str(valor or "").strip()
        if not texto:
            raise ValueError("Data em falta.")
        return datetime.datetime.strptime(texto, "%Y-%m-%d").date()

    def _obter_conexao_db_relatorio(self) -> Tuple[sqlite3.Connection, bool]:
        if self.db_con is not None:
            return self.db_con, False

        self.log_dir, self.bom_path, self.db_dir = carregar_caminhos()
        caminho_db = db_path(self.db_dir)
        if not os.path.isfile(caminho_db):
            raise FileNotFoundError(f"Base de dados nao encontrada:\n{os.path.abspath(caminho_db)}")

        con = db_connect(caminho_db)
        return con, True

    def _gerar_relatorio_csv_db(self, data_ini: datetime.date, data_fim: datetime.date) -> Tuple[str, str, int]:
        inicio_str = data_ini.strftime("%Y-%m-%d")
        fim_str = data_fim.strftime("%Y-%m-%d")
        limite_superior = (data_fim + datetime.timedelta(days=1)).strftime("%Y-%m-%d")

        if inicio_str == fim_str:
            base_nome = f"relatorio_quality_db_{inicio_str}"
        else:
            base_nome = f"relatorio_quality_db_{inicio_str}_a_{fim_str}"

        self.log_dir, _, _ = carregar_caminhos()
        os.makedirs(self.log_dir, exist_ok=True)
        detalhe_path = os.path.join(self.log_dir, f"{base_nome}_detalhe.csv")
        totais_path = os.path.join(self.log_dir, f"{base_nome}_totais.csv")

        con = None
        fechar_con = False
        try:
            con, fechar_con = self._obter_conexao_db_relatorio()

            linhas_detalhe = con.execute(
                """SELECT ts,
                          operador,
                          projeto,
                          turno,
                          referencia,
                          COALESCE(description, ''),
                          quantidade,
                          COALESCE(causa, comentario, '') AS causa,
                          COALESCE(defeito, lote, '') AS defeito,
                          COALESCE(destino, ''),
                          COALESCE(posto, '')
                   FROM leituras
                   WHERE ts >= ? AND ts < ?
                   ORDER BY ts ASC""",
                (inicio_str, limite_superior),
            ).fetchall()

            totais_ref = con.execute(
                """SELECT referencia, COALESCE(MAX(description), ''), SUM(quantidade) AS total
                   FROM leituras
                   WHERE ts >= ? AND ts < ?
                   GROUP BY referencia
                   ORDER BY referencia ASC""",
                (inicio_str, limite_superior),
            ).fetchall()
        finally:
            if fechar_con and con is not None:
                try:
                    con.close()
                except Exception:
                    pass

        with open(detalhe_path, mode="w", newline="", encoding="utf-8") as f:
            w = csv.writer(f, delimiter=";")
            w.writerow(["Data", "Operador", "Projeto", "Turno", "Referencia", "Description",
                        "Quantidade", "Causa", "Defeito", "Destino", "Posto"])
            w.writerows(linhas_detalhe)

        with open(totais_path, mode="w", newline="", encoding="utf-8") as f:
            w = csv.writer(f, delimiter=";")
            w.writerow(["Referencia", "Description", "Total"])

            total_geral = 0
            for ref, descricao, total in totais_ref:
                total_int = int(total or 0)
                w.writerow([ref, descricao, total_int])
                total_geral += total_int

            w.writerow([])
            w.writerow(["TOTAL GERAL", "", total_geral])

        return detalhe_path, totais_path, len(linhas_detalhe)

    def _abrir_janela_exportacao_csv_db(self) -> None:
        janela = tk.Toplevel(self.root)
        janela.title("Exportar relatorios CSV (DB)")
        janela.configure(bg=CORES["branco"])
        janela.resizable(False, False)
        janela.transient(self.root)
        janela.grab_set()

        hoje_str = datetime.date.today().strftime("%Y-%m-%d")
        modo_var = tk.StringVar(value="dia")
        data_ini_var = tk.StringVar(value=hoje_str)
        data_fim_var = tk.StringVar(value=hoje_str)

        frame = tk.Frame(janela, bg=CORES["branco"], padx=14, pady=12)
        frame.pack(fill=tk.BOTH, expand=True)

        tk.Label(
            frame,
            text="Exportacao direta da DB para CSV",
            font=("Segoe UI", 11, "bold"),
            fg=CORES["cinza_escuro"],
            bg=CORES["branco"],
        ).grid(row=0, column=0, columnspan=3, sticky=tk.W, pady=(0, 8))

        tk.Radiobutton(
            frame,
            text="Um dia",
            variable=modo_var,
            value="dia",
            bg=CORES["branco"],
            fg=CORES["cinza_escuro"],
            activebackground=CORES["branco"],
            activeforeground=CORES["cinza_escuro"],
        ).grid(row=1, column=0, sticky=tk.W)
        tk.Radiobutton(
            frame,
            text="Intervalo de dias",
            variable=modo_var,
            value="intervalo",
            bg=CORES["branco"],
            fg=CORES["cinza_escuro"],
            activebackground=CORES["branco"],
            activeforeground=CORES["cinza_escuro"],
        ).grid(row=1, column=1, columnspan=2, sticky=tk.W, padx=(12, 0))

        tk.Label(
            frame,
            text="Data inicial (YYYY-MM-DD):",
            font=("Segoe UI", 9),
            fg=CORES["cinza_escuro"],
            bg=CORES["branco"],
        ).grid(row=2, column=0, sticky=tk.W, pady=(10, 2))
        entry_data_ini = tk.Entry(frame, textvariable=data_ini_var, width=16, font=("Segoe UI", 10), relief=tk.SOLID, bd=1)
        entry_data_ini.grid(row=2, column=1, sticky=tk.W, pady=(10, 2))

        tk.Label(
            frame,
            text="Data final (YYYY-MM-DD):",
            font=("Segoe UI", 9),
            fg=CORES["cinza_escuro"],
            bg=CORES["branco"],
        ).grid(row=3, column=0, sticky=tk.W, pady=(6, 2))
        entry_data_fim = tk.Entry(frame, textvariable=data_fim_var, width=16, font=("Segoe UI", 10), relief=tk.SOLID, bd=1)
        entry_data_fim.grid(row=3, column=1, sticky=tk.W, pady=(6, 2))

        def atualizar_estado_data_fim(*_):
            if modo_var.get() == "dia":
                data_fim_var.set(data_ini_var.get().strip())
                entry_data_fim.configure(state=tk.DISABLED)
            else:
                entry_data_fim.configure(state=tk.NORMAL)

        def exportar():
            try:
                data_ini = self._parse_data_relatorio(data_ini_var.get())
                if modo_var.get() == "dia":
                    data_fim = data_ini
                else:
                    data_fim = self._parse_data_relatorio(data_fim_var.get())
                    if data_fim < data_ini:
                        messagebox.showwarning(
                            "Intervalo invalido",
                            "A data final nao pode ser anterior a data inicial.",
                            parent=janela,
                        )
                        return

                detalhe_path, totais_path, registos = self._gerar_relatorio_csv_db(data_ini, data_fim)
            except ValueError:
                messagebox.showwarning(
                    "Data invalida",
                    "Use o formato YYYY-MM-DD (ex: 2026-03-01).",
                    parent=janela,
                )
                return
            except FileNotFoundError as err:
                messagebox.showerror("Relatorio CSV (DB)", str(err), parent=janela)
                return
            except sqlite3.Error as err:
                messagebox.showerror("Relatorio CSV (DB)", f"Erro ao ler a base de dados:\n\n{err}", parent=janela)
                return
            except OSError as err:
                messagebox.showerror("Relatorio CSV (DB)", f"Nao foi possivel gravar os ficheiros CSV:\n\n{err}", parent=janela)
                return

            msg = (
                "Relatorios CSV gerados com sucesso.\n\n"
                f"Detalhe: {os.path.abspath(detalhe_path)}\n"
                f"Totais: {os.path.abspath(totais_path)}\n\n"
                f"Registos exportados: {registos}"
            )
            if registos == 0:
                msg += "\n\nNao existem leituras no periodo selecionado."
            messagebox.showinfo("Relatorio CSV (DB)", msg, parent=janela)
            janela.destroy()

        botoes = tk.Frame(frame, bg=CORES["branco"])
        botoes.grid(row=4, column=0, columnspan=3, sticky=tk.W, pady=(12, 0))
        tk.Button(
            botoes,
            text="Exportar",
            font=("Segoe UI", 10, "bold"),
            fg=CORES["branco"],
            bg=CORES["azul"],
            activebackground=CORES["azul_claro"],
            activeforeground=CORES["branco"],
            relief=tk.FLAT,
            padx=14,
            pady=4,
            cursor="hand2",
            command=exportar,
        ).pack(side=tk.LEFT)
        tk.Button(
            botoes,
            text="Fechar",
            font=("Segoe UI", 10),
            fg=CORES["cinza_escuro"],
            bg=CORES["fundo"],
            activebackground=CORES["painel_titulo"],
            activeforeground=CORES["cinza_escuro"],
            relief=tk.FLAT,
            padx=12,
            pady=4,
            cursor="hand2",
            command=janela.destroy,
        ).pack(side=tk.LEFT, padx=(8, 0))

        modo_var.trace_add("write", atualizar_estado_data_fim)
        data_ini_var.trace_add("write", atualizar_estado_data_fim)
        atualizar_estado_data_fim()

        janela.bind("<Return>", lambda e: exportar())
        janela.bind("<Escape>", lambda e: janela.destroy())
        entry_data_ini.focus_set()
        entry_data_ini.select_range(0, tk.END)

    def _obter_leitura_selecionada(self, acao: str):
        sel = self.list_ultimas.curselection()
        if not sel:
            messagebox.showinfo("Nada selecionado", f"Selecione uma leitura na lista para {acao}.")
            return None, None, None
        idx = int(sel[0])
        items = list(self.ultimas_leituras)
        if idx >= len(items):
            return None, None, None
        item = items[idx]
        if not (isinstance(item, tuple) and len(item) >= 4 and isinstance(item[0], int)):
            messagebox.showwarning("Leitura invalida", f"Esta leitura nao pode ser {acao}.")
            return None, None, None
        return idx, item, items

    def _atualizar_detalhes_csv(self, timestamp, ref, qty, causa, defeito, destino, posto):
        if not self.logfile or not os.path.isfile(self.logfile):
            return

        self._garantir_cabecalho_csv_atual()
        with open(self.logfile, mode="r", newline="", encoding="utf-8") as f:
            rows = list(csv.reader(f, delimiter=";"))

        def linha_coincide(row):
            if len(row) < 7 or row[0] == "Data":
                return False
            return row[0] == timestamp and row[4].strip() == ref and str(row[6]).strip() == str(qty)

        matches = [i for i in range(len(rows)) if linha_coincide(rows[i])]
        if not matches:
            return

        idx = matches[-1]
        row = rows[idx]
        row_n = (row + [""] * 11)[:11]
        row_n[7] = causa
        row_n[8] = defeito
        row_n[9] = destino
        row_n[10] = posto
        rows[idx] = row_n

        with open(self.logfile, mode="w", newline="", encoding="utf-8") as f:
            w = csv.writer(f, delimiter=";")
            w.writerows(rows)

    def _dialogo_editar_detalhes(self, ref, qty, causa_atual="", defeito_atual="", destino_atual="", posto_atual=""):
        c = CORES
        top = tk.Toplevel(self.root)
        top.title("Editar leitura")
        top.transient(self.root)
        top.resizable(False, False)
        top.configure(bg=c["branco"])

        resultado = {"valor": None}
        frame = tk.Frame(top, bg=c["branco"], padx=12, pady=10)
        frame.pack(fill=tk.BOTH, expand=True)

        tk.Label(frame, text=f"Referencia: {ref}   Quantidade: {qty}",
                 font=("Segoe UI", 9, "bold"),
                 fg=c["cinza_escuro"], bg=c["branco"]).grid(row=0, column=0, columnspan=2, sticky=tk.W, pady=(0, 8))

        var_causa = tk.StringVar(value=str(causa_atual or ""))
        var_defeito = tk.StringVar(value=str(defeito_atual or ""))
        var_destino = tk.StringVar(value=str(destino_atual or ""))
        var_posto = tk.StringVar(value=str(posto_atual or ""))

        tk.Label(frame, text="Causa:", font=("Segoe UI", 9), fg=c["cinza_escuro"], bg=c["branco"]).grid(row=1, column=0, sticky=tk.W, pady=2)
        entry_causa = tk.Entry(frame, width=36, textvariable=var_causa, font=("Segoe UI", 10), relief=tk.SOLID, bd=1)
        entry_causa.grid(row=1, column=1, sticky=tk.W, pady=2, padx=(8, 0))

        tk.Label(frame, text="Defeito:", font=("Segoe UI", 9), fg=c["cinza_escuro"], bg=c["branco"]).grid(row=2, column=0, sticky=tk.W, pady=2)
        entry_defeito = tk.Entry(frame, width=36, textvariable=var_defeito, font=("Segoe UI", 10), relief=tk.SOLID, bd=1)
        entry_defeito.grid(row=2, column=1, sticky=tk.W, pady=2, padx=(8, 0))

        tk.Label(frame, text="Destino:", font=("Segoe UI", 9), fg=c["cinza_escuro"], bg=c["branco"]).grid(row=3, column=0, sticky=tk.W, pady=2)
        valores_destino = list(getattr(self, "destinos_list", []) or [])
        combo_destino = ttk.Combobox(frame, width=34, textvariable=var_destino, values=valores_destino,
                                     state="readonly" if valores_destino else "normal", font=("Segoe UI", 10))
        if not var_destino.get() and valores_destino:
            var_destino.set(valores_destino[0])
        combo_destino.grid(row=3, column=1, sticky=tk.W, pady=2, padx=(8, 0))

        tk.Label(frame, text="Posto:", font=("Segoe UI", 9), fg=c["cinza_escuro"], bg=c["branco"]).grid(row=4, column=0, sticky=tk.W, pady=2)
        valores_posto = list(getattr(self, "postos_list", []) or [])
        combo_posto = ttk.Combobox(frame, width=34, textvariable=var_posto, values=valores_posto,
                                   state="readonly" if valores_posto else "normal", font=("Segoe UI", 10))
        if not var_posto.get() and valores_posto:
            var_posto.set(valores_posto[0])
        combo_posto.grid(row=4, column=1, sticky=tk.W, pady=2, padx=(8, 0))

        btns = tk.Frame(frame, bg=c["branco"])
        btns.grid(row=5, column=0, columnspan=2, sticky=tk.E, pady=(10, 0))

        def cancelar(event=None):
            top.destroy()

        def confirmar(event=None):
            causa = var_causa.get().strip().replace("\n", " ").replace(";", ",")
            defeito = var_defeito.get().strip().replace("\n", " ").replace(";", ",")
            destino = var_destino.get().strip().replace("\n", " ").replace(";", ",")
            posto = var_posto.get().strip().replace("\n", " ").replace(";", ",")

            if not causa:
                messagebox.showwarning("Campo obrigatorio", "A Causa e obrigatoria.", parent=top)
                entry_causa.focus_set()
                return
            if not defeito:
                messagebox.showwarning("Campo obrigatorio", "O Defeito e obrigatorio.", parent=top)
                entry_defeito.focus_set()
                return
            if not destino:
                messagebox.showwarning("Campo obrigatorio", "O Destino e obrigatorio.", parent=top)
                combo_destino.focus_set()
                return
            if not posto:
                messagebox.showwarning("Campo obrigatorio", "O Posto e obrigatorio.", parent=top)
                combo_posto.focus_set()
                return

            resultado["valor"] = (causa, defeito, destino, posto)
            top.destroy()

        tk.Button(btns, text="Cancelar", font=("Segoe UI", 9),
                  fg=c["cinza_escuro"], bg=c["fundo"], relief=tk.FLAT,
                  padx=10, cursor="hand2", command=cancelar).pack(side=tk.RIGHT, padx=(8, 0))

        tk.Button(btns, text="Guardar", font=("Segoe UI", 9, "bold"),
                  fg=c["branco"], bg=c["azul"], activebackground=c["azul_claro"],
                  activeforeground=c["branco"], relief=tk.FLAT, padx=12,
                  cursor="hand2", command=confirmar).pack(side=tk.RIGHT)

        top.bind("<Escape>", cancelar)
        top.bind("<Return>", confirmar)
        top.protocol("WM_DELETE_WINDOW", cancelar)

        top.grab_set()
        entry_causa.focus_set()
        entry_causa.select_range(0, tk.END)
        self.root.wait_window(top)
        return resultado["valor"]

    def _editar_detalhes_leitura(self) -> None:
        if not self.sessao_iniciada:
            messagebox.showwarning("Sessao nao iniciada", "Inicie uma sessao primeiro.")
            return

        idx, item, items = self._obter_leitura_selecionada("editar")
        if item is None:
            return

        row_id, ref, qty, timestamp = item[:4]
        causa_atual = item[4] if len(item) >= 5 else ""
        defeito_atual = item[5] if len(item) >= 6 else ""
        destino_atual = item[6] if len(item) >= 7 else ""
        posto_atual = item[7] if len(item) >= 8 else ""
        descricao_item = item[8] if len(item) >= 9 else ""

        editados = self._dialogo_editar_detalhes(ref, qty, causa_atual, defeito_atual, destino_atual, posto_atual)
        if not editados:
            return
        nova_causa, novo_defeito, novo_destino, novo_posto = editados

        try:
            if self.db_con is None:
                raise sqlite3.Error("Ligacao DB nao inicializada.")
            self.db_con.execute(
                "UPDATE leituras SET causa = ?, defeito = ?, destino = ?, posto = ? WHERE id = ?",
                (nova_causa, novo_defeito, novo_destino, novo_posto, row_id),
            )
            self.db_con.commit()
        except sqlite3.Error as err:
            messagebox.showerror("Erro ao editar (DB)", str(err))
            return

        items[idx] = (row_id, ref, qty, timestamp, nova_causa, novo_defeito, novo_destino, novo_posto, descricao_item)
        self.ultimas_leituras = items

        try:
            self._atualizar_detalhes_csv(timestamp, ref, qty, nova_causa, novo_defeito, novo_destino, novo_posto)
        except OSError:
            pass

        self._atualizar_ultimas()
        self.list_ultimas.selection_clear(0, tk.END)
        self.list_ultimas.selection_set(idx)
        self.list_ultimas.see(idx)

    def _eliminar_leitura(self) -> None:
        if not self.sessao_iniciada:
            messagebox.showwarning("Sessao nao iniciada", "Inicie uma sessao primeiro.")
            return

        idx, item, items = self._obter_leitura_selecionada("eliminar")
        if item is None:
            return

        row_id, ref, qty, timestamp = item[:4]

        if not messagebox.askyesno("Eliminar leitura", f"Eliminar registo:\n  {ref}  {qty}\n\nConfirma?"):
            return

        try:
            if self.db_con is None:
                raise sqlite3.Error("Ligação DB não inicializada.")
            self.db_con.execute("DELETE FROM leituras WHERE id = ?", (row_id,))
            self.db_con.commit()
        except sqlite3.Error as err:
            messagebox.showerror("Erro ao eliminar (DB)", str(err))
            return

        self.consumos[ref] = self.consumos.get(ref, 0) - int(qty)
        if self.consumos[ref] <= 0:
            del self.consumos[ref]

        items.pop(idx)
        self.ultimas_leituras = items

        # best-effort no CSV
        try:
            self._garantir_cabecalho_csv_atual()
            with open(self.logfile, mode="r", newline="", encoding="utf-8") as f:
                rows = list(csv.reader(f, delimiter=";"))

            def linha_coincide(row):
                if len(row) < 7 or row[0] == "Data":
                    return False
                return row[0] == timestamp and row[4].strip() == ref and str(row[6]).strip() == str(qty)

            matches = [i for i in range(len(rows)) if linha_coincide(rows[i])]
            if matches:
                rows.pop(matches[-1])

            with open(self.logfile, mode="w", newline="", encoding="utf-8") as f:
                w = csv.writer(f, delimiter=";")
                w.writerows(rows)
        except OSError:
            pass

        self._atualizar_ultimas()
        self._atualizar_resumo()

    # -------------------- Terminar / Export CSV --------------------
    def _terminar_sessao(self) -> None:
        if self.sessao_iniciada:
            # Export CSV do dia a partir do SQLite
            if self.db_con is not None:
                self._exportar_csv_do_dia()

            self.sessao_iniciada = False

            if self._timer_duracao:
                self.root.after_cancel(self._timer_duracao)
                self._timer_duracao = None

            self.entry_operador.configure(state="normal")
            self.combo_projeto.configure(state="readonly")
            self.combo_turno.configure(state="readonly")
            self.btn_iniciar.configure(state=tk.NORMAL)

            self.label_sessao.configure(text="Sessao terminada. Pode iniciar uma nova sessao.", fg=CORES["cinza_claro"])
            self.label_inicio_sessao.configure(text="Sessao iniciada a\n--:--:--")

            try:
                if self.db_con is not None:
                    self.db_con.close()
            except Exception:
                pass
            self.db_con = None
            self.db_path = None
            self.sessao_id = None

            self._atualizar_resumo()
            messagebox.showinfo("Sessao terminada", "Sessao terminada.")

        self.entry_referencia.focus_set()

    def _exportar_csv_do_dia(self) -> None:
        """Gera os relatorios CSV do dia (detalhe + totais por referencia) a partir do SQLite."""
        if self.db_con is None:
            messagebox.showwarning("Relatorio CSV (DB)", "Base de dados nao disponivel. Nao foi possivel exportar.")
            return

        hoje = datetime.date.today()
        try:
            detalhe_path, totais_path, registos = self._gerar_relatorio_csv_db(hoje, hoje)
        except FileNotFoundError as err:
            messagebox.showerror("Relatorio CSV (DB)", str(err))
            return
        except sqlite3.Error as err:
            messagebox.showerror("Relatorio CSV (DB)", f"Erro ao ler a base de dados:\n\n{err}")
            return
        except OSError as err:
            messagebox.showerror("Relatorio CSV (DB)", f"Nao foi possivel gravar os ficheiros CSV:\n\n{err}")
            return

        msg = (
            "Relatorios CSV do dia gerados com sucesso.\n\n"
            f"Detalhe: {os.path.abspath(detalhe_path)}\n"
            f"Totais: {os.path.abspath(totais_path)}\n\n"
            f"Registos exportados: {registos}"
        )
        if registos == 0:
            msg += "\n\nNao existem leituras para o dia atual."
        messagebox.showinfo("Relatorio CSV (DB)", msg)

    # -------------------- Run --------------------
    def run(self) -> None:
        _startup_log("Mainloop start")
        self.root.mainloop()


if __name__ == "__main__":
    # limpa log de arranque em cada run (opcional)
    try:
        with open(_STARTUP_LOG, "w", encoding="utf-8") as f:
            f.write("Startup log - QualityControl (CSV version)\n")
    except Exception:
        pass

    app = QualityApp()
    app.run()
