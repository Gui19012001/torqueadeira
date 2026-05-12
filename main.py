# main.py
# Torque PF6000 - APK Kivy limpo
# Versão estável inicial: PF6000 via Ethernet + geração de ZPL SALVAR/IP.
# USB Zebra fica para segunda etapa, depois que o APK abrir e comunicar estável.

import os
import socket
import threading
import queue
import time
import hashlib
import csv
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional, List, Tuple

from kivy.app import App
from kivy.clock import Clock
from kivy.metrics import dp
from kivy.core.window import Window
from kivy.graphics import Color, RoundedRectangle
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.gridlayout import GridLayout
from kivy.uix.anchorlayout import AnchorLayout
from kivy.uix.label import Label
from kivy.uix.button import Button
from kivy.uix.textinput import TextInput
from kivy.uix.spinner import Spinner
from kivy.uix.checkbox import CheckBox
from kivy.uix.popup import Popup
from kivy.uix.scrollview import ScrollView


# =========================================================
# CONFIG PADRÃO
# =========================================================
DEFAULT_IP = "169.254.1.1"
DEFAULT_PORT = 4545
POSICOES = [f"P{i}" for i in range(1, 9)]

try:
    BASE_DIR = Path(os.environ.get("ANDROID_PRIVATE") or os.environ.get("ANDROID_ARGUMENT") or ".")
except Exception:
    BASE_DIR = Path(".")

LOG_DIR = BASE_DIR / "logs_torque_pf6000"
CSV_DIR = BASE_DIR / "registros_torque"


def safe_mkdirs():
    for p in (BASE_DIR, LOG_DIR, CSV_DIR):
        try:
            p.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass


# =========================================================
# FUNÇÕES GERAIS
# =========================================================
def now_br() -> str:
    return datetime.now().strftime("%d/%m/%Y %H:%M:%S")


def now_file() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def fmt_num(v: Optional[float]) -> str:
    if v is None:
        return ""
    try:
        return f"{float(v):.2f}".replace(".", ",")
    except Exception:
        return ""


def status_01(v: Any) -> str:
    s = str(v or "").strip().upper()
    if s in ("1", "01", "OK"):
        return "OK"
    if s in ("0", "00", "NOK"):
        return "NOK"
    return s


def scaled_number(raw: str, divisor: float) -> Optional[float]:
    s = str(raw or "").strip().replace(" ", "")
    if not s:
        return None
    if not s.lstrip("-").isdigit():
        return None
    try:
        return int(s) / float(divisor)
    except Exception:
        return None


# =========================================================
# OPEN PROTOCOL
# =========================================================
def montar_mid(mid: str, revision: str = "001", data: str = "") -> bytes:
    mid = str(mid).zfill(4)
    revision = str(revision).zfill(3)
    corpo_sem_tamanho = f"{mid}{revision}".ljust(16) + data
    tamanho = len(corpo_sem_tamanho) + 4
    return f"{tamanho:04d}{corpo_sem_tamanho}\x00".encode("ascii", errors="ignore")


def extrair_mid(frame: str) -> str:
    if len(frame) >= 8 and frame[:4].isdigit():
        return frame[4:8]
    return ""


def extrair_rev(frame: str) -> str:
    if len(frame) >= 11 and frame[8:11].isdigit():
        return frame[8:11]
    return "001"


def split_frames(buffer: bytes):
    parts = buffer.split(b"\x00")
    return parts[:-1], parts[-1]


# =========================================================
# PARSER MID 0061
# =========================================================
SPEC_REV1: List[Tuple[str, int, str]] = [
    ("01", 4, "cell_id"),
    ("02", 2, "channel_id"),
    ("03", 25, "controller_name"),
    ("04", 25, "vin"),
    ("05", 2, "job_id"),
    ("06", 3, "pset"),
    ("07", 4, "batch_size"),
    ("08", 4, "batch_counter"),
    ("09", 1, "tightening_status"),
    ("10", 1, "torque_status"),
    ("11", 1, "angle_status"),
    ("12", 6, "torque_min"),
    ("13", 6, "torque_max"),
    ("14", 6, "torque_target"),
    ("15", 6, "torque"),
    ("16", 5, "angle_min"),
    ("17", 5, "angle_max"),
    ("18", 5, "angle_target"),
    ("19", 5, "angle"),
    ("20", 19, "timestamp"),
    ("21", 19, "pset_last_change"),
    ("22", 1, "batch_status"),
    ("23", 10, "tightening_id"),
]

SPEC_REV2: List[Tuple[str, int, str]] = [
    ("01", 4, "cell_id"),
    ("02", 2, "channel_id"),
    ("03", 25, "controller_name"),
    ("04", 25, "vin"),
    ("05", 4, "job_id"),
    ("06", 3, "pset"),
    ("07", 2, "strategy"),
    ("08", 5, "strategy_options"),
    ("09", 4, "batch_size"),
    ("10", 4, "batch_counter"),
    ("11", 1, "tightening_status"),
    ("12", 1, "batch_status"),
    ("13", 1, "torque_status"),
    ("14", 1, "angle_status"),
    ("15", 1, "rundown_angle_status"),
    ("16", 1, "current_monitoring_status"),
    ("17", 1, "selftap_status"),
    ("18", 1, "prevail_torque_monitoring_status"),
    ("19", 1, "prevail_torque_comp_status"),
    ("20", 10, "tightening_error_status"),
    ("21", 6, "torque_min"),
    ("22", 6, "torque_max"),
    ("23", 6, "torque_target"),
    ("24", 6, "torque"),
    ("25", 5, "angle_min"),
    ("26", 5, "angle_max"),
    ("27", 5, "angle_target"),
    ("28", 5, "angle"),
    ("29", 5, "rundown_angle_min"),
    ("30", 5, "rundown_angle_max"),
    ("31", 5, "rundown_angle"),
    ("32", 3, "current_monitoring_min"),
    ("33", 3, "current_monitoring_max"),
    ("34", 3, "current_monitoring_value"),
    ("35", 6, "selftap_min"),
    ("36", 6, "selftap_max"),
    ("37", 6, "selftap_torque"),
    ("38", 6, "pvt_min"),
    ("39", 6, "pvt_max"),
    ("40", 6, "pvt_torque"),
    ("41", 10, "tightening_id"),
    ("42", 5, "job_sequence_number"),
    ("43", 5, "sync_tightening_id"),
    ("44", 14, "tool_serial"),
    ("45", 19, "timestamp"),
    ("46", 19, "pset_last_change"),
]


def parse_by_spec(frame: str, spec: List[Tuple[str, int, str]], start_pos: int = 20):
    fields_id: Dict[str, str] = {}
    fields_name: Dict[str, str] = {}
    pos = start_pos

    for fid, width, name in spec:
        if pos >= len(frame):
            break

        if frame[pos:pos + 2] == fid:
            id_pos = pos
        else:
            found = frame.find(fid, pos, min(len(frame), pos + 12))
            if found < 0:
                continue
            id_pos = found

        ini = id_pos + 2
        fim = ini + width
        value = frame[ini:fim]
        fields_id[fid] = value
        fields_name[name] = value
        pos = fim

    return fields_id, fields_name


def parse_mid0061(frame: str, torque_divisor: float = 100.0, angle_divisor: float = 1.0):
    revision = extrair_rev(frame)

    if revision == "001":
        spec = SPEC_REV1
        torque_field = "15"
        angle_field = "19"
        status_field = "09"
        torque_status_field = "10"
        angle_status_field = "11"
        tightening_id_field = "23"
    else:
        spec = SPEC_REV2
        torque_field = "24"
        angle_field = "28"
        status_field = "11"
        torque_status_field = "13"
        angle_status_field = "14"
        tightening_id_field = "41"

    fields_id, fields_name = parse_by_spec(frame, spec)

    raw_torque = (fields_id.get(torque_field, "") or "").strip()
    raw_angle = (fields_id.get(angle_field, "") or "").strip()

    tid = (fields_id.get(tightening_id_field, "") or fields_name.get("tightening_id", "") or "").strip()
    frame_hash = hashlib.sha1(frame.encode("utf-8", errors="ignore")).hexdigest()[:16]

    pset = (fields_name.get("pset", "") or "").strip()
    pset = pset.lstrip("0") or pset

    return {
        "frame": frame,
        "frame_hash": frame_hash,
        "revision": revision,
        "tightening_id": tid,
        "status_geral": status_01(fields_id.get(status_field, "")),
        "status_torque": status_01(fields_id.get(torque_status_field, "")),
        "status_angulo": status_01(fields_id.get(angle_status_field, "")),
        "torque": scaled_number(raw_torque, torque_divisor),
        "angulo": scaled_number(raw_angle, angle_divisor),
        "torque_raw": raw_torque,
        "angulo_raw": raw_angle,
        "pset": pset,
    }


# =========================================================
# CLIENTE PF6000
# =========================================================
class PF6000Client:
    def __init__(self, event_q: queue.Queue):
        self.q = event_q
        self.sock = None
        self.thread = None
        self.stop_flag = threading.Event()
        self.connected = False
        self.open_ok = False
        self.last_mid = "-"
        self.ip = DEFAULT_IP
        self.port = DEFAULT_PORT

    def emit(self, kind: str, data: Any = None):
        self.q.put((kind, data))

    def log(self, msg: str):
        self.emit("log", f"[{now_br()}] {msg}")
        try:
            safe_mkdirs()
            with (LOG_DIR / f"pf6000_{datetime.now().strftime('%Y%m%d')}.log").open("a", encoding="utf-8") as f:
                f.write(f"[{now_br()}] {msg}\n")
        except Exception:
            pass

    def snapshot(self):
        return {
            "connected": self.connected,
            "open_ok": self.open_ok,
            "last_mid": self.last_mid,
        }

    def connect(self, ip: str, port: int):
        self.disconnect()
        self.ip = ip
        self.port = int(port)
        self.stop_flag.clear()
        self.thread = threading.Thread(target=self._loop, daemon=True)
        self.thread.start()

    def disconnect(self):
        self.stop_flag.set()
        try:
            if self.sock:
                self.sock.close()
        except Exception:
            pass
        self.sock = None
        self.connected = False
        self.open_ok = False
        self.emit("status", self.snapshot())

    def send_mid(self, mid: str, rev: str = "001"):
        try:
            if self.sock:
                self.sock.sendall(montar_mid(mid, rev))
                self.log(f"ENVIADO MID {mid} REV {rev}")
        except Exception as e:
            self.log(f"Erro enviando MID {mid}: {e}")

    def _loop(self):
        while not self.stop_flag.is_set():
            try:
                self.log(f"Conectando {self.ip}:{self.port}")
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(4)
                s.connect((self.ip, self.port))
                s.settimeout(1)
                self.sock = s
                self.connected = True
                self.open_ok = False
                self.emit("status", self.snapshot())

                self.send_mid("0001", "006")
                self._reader(s)

            except Exception as e:
                self.connected = False
                self.open_ok = False
                self.emit("status", self.snapshot())
                self.log(f"Conexão falhou/caiu: {e}")
                time.sleep(2)

    def _reader(self, s):
        buffer = b""
        last_keepalive = time.time()

        while not self.stop_flag.is_set():
            try:
                if time.time() - last_keepalive >= 10:
                    self.send_mid("9999", "001")
                    last_keepalive = time.time()

                data = s.recv(4096)
                if not data:
                    raise RuntimeError("painel encerrou conexão")

                raw = buffer + data
                frames, buffer = split_frames(raw)

                for fb in frames:
                    frame = fb.decode("ascii", errors="ignore")
                    mid = extrair_mid(frame)
                    self.last_mid = mid
                    self.emit("status", self.snapshot())
                    self.log(f"RECEBIDO MID {mid}")

                    if mid == "0002":
                        self.open_ok = True
                        self.emit("status", self.snapshot())
                        self.send_mid("0060", "001")

                    elif mid == "0061":
                        self.send_mid("0062", "001")
                        self.emit("frame", frame)

                    elif mid == "9999":
                        self.send_mid("9999", "001")

            except socket.timeout:
                continue
            except Exception as e:
                self.log(f"Leitura encerrada: {e}")
                break

        self.connected = False
        self.open_ok = False
        self.emit("status", self.snapshot())


# =========================================================
# CARD VISUAL
# =========================================================
class PCard(BoxLayout):
    def __init__(self, ponto: str, callback, **kwargs):
        super().__init__(orientation="vertical", padding=dp(8), spacing=dp(4), **kwargs)
        self.ponto = ponto
        self.callback = callback
        self.status = "AGUARDANDO"
        self.bg_color = (0.08, 0.09, 0.10, 1)
        self.border_color = (0.25, 0.25, 0.25, 1)

        with self.canvas.before:
            self.c_bg = Color(*self.bg_color)
            self.rect = RoundedRectangle(pos=self.pos, size=self.size, radius=[dp(10)])
        self.bind(pos=self._upd, size=self._upd)

        self.lbl_p = Label(text=ponto, font_size="24sp", bold=True, color=(1, 1, 1, 1), size_hint_y=0.23)
        self.lbl_status = Label(text="AGUARDANDO", font_size="13sp", bold=True, color=(0.8, 0.8, 0.8, 1), size_hint_y=0.18)
        self.lbl_torque = Label(text="Torque:", font_size="13sp", color=(1, 1, 1, 1), size_hint_y=0.18)
        self.lbl_angulo = Label(text="Ângulo:", font_size="13sp", color=(1, 1, 1, 1), size_hint_y=0.18)
        self.lbl_info = Label(text="Tent: 0 | NOK: 0", font_size="11sp", color=(0.8, 0.8, 0.8, 1), size_hint_y=0.13)

        btn = Button(text=f"Selecionar {ponto}", font_size="12sp", size_hint_y=0.22)
        btn.bind(on_release=lambda *_: self.callback(self.ponto))

        self.add_widget(self.lbl_p)
        self.add_widget(self.lbl_status)
        self.add_widget(self.lbl_torque)
        self.add_widget(self.lbl_angulo)
        self.add_widget(self.lbl_info)
        self.add_widget(btn)

    def _upd(self, *_):
        self.rect.pos = self.pos
        self.rect.size = self.size

    def set_data(self, data: Dict[str, Any], current: bool):
        status = data.get("status", "AGUARDANDO")
        torque = fmt_num(data.get("torque"))
        angulo = fmt_num(data.get("angulo"))
        tent = data.get("tentativas", 0)
        nok = data.get("nok", 0)

        if current:
            self.bg_color = (0.02, 0.13, 0.24, 1)
            status_color = (0.0, 0.85, 1.0, 1)
        elif status == "OK":
            self.bg_color = (0.02, 0.18, 0.08, 1)
            status_color = (0.0, 0.95, 0.45, 1)
        elif status == "RETESTE":
            self.bg_color = (0.25, 0.18, 0.00, 1)
            status_color = (1.0, 0.86, 0.0, 1)
        else:
            self.bg_color = (0.08, 0.09, 0.10, 1)
            status_color = (0.8, 0.8, 0.8, 1)

        self.c_bg.rgba = self.bg_color
        self.lbl_status.text = status
        self.lbl_status.color = status_color
        self.lbl_torque.text = f"Torque: {torque}"
        self.lbl_angulo.text = f"Ângulo: {angulo}"
        self.lbl_info.text = f"Tent: {tent} | NOK: {nok}"


# =========================================================
# APP
# =========================================================
class TorqueApp(App):
    def build(self):
        safe_mkdirs()
        Window.clearcolor = (0.04, 0.05, 0.07, 1)

        self.q = queue.Queue()
        self.client = PF6000Client(self.q)

        self.current_idx = 0
        self.processed_ids = set()
        self.processed_hashes = set()
        self.cards: Dict[str, PCard] = {}
        self.data = {
            p: {"status": "AGUARDANDO", "torque": None, "angulo": None, "tentativas": 0, "nok": 0, "tid": ""}
            for p in POSICOES
        }

        self.serie = ""
        self.op = ""

        root = BoxLayout(orientation="vertical", padding=dp(8), spacing=dp(6))

        header = BoxLayout(orientation="horizontal", size_hint_y=None, height=dp(48), spacing=dp(6))
        title = Label(text="Torque Mola - PF6000", font_size="20sp", bold=True, color=(1, 1, 1, 1))
        btn_cfg = Button(text="CONFIG", size_hint_x=None, width=dp(120))
        btn_cfg.bind(on_release=self.open_config)
        header.add_widget(title)
        header.add_widget(btn_cfg)
        root.add_widget(header)

        inputs = BoxLayout(orientation="horizontal", size_hint_y=None, height=dp(44), spacing=dp(6))
        self.in_serie = TextInput(hint_text="Numero de serie", multiline=False, font_size="16sp")
        self.in_op = TextInput(hint_text="OP", multiline=False, font_size="16sp")
        inputs.add_widget(self.in_serie)
        inputs.add_widget(self.in_op)
        root.add_widget(inputs)

        statusbar = GridLayout(cols=5, size_hint_y=None, height=dp(56), spacing=dp(6))
        self.lbl_tcp = self.metric("TCP", "OFF")
        self.lbl_open = self.metric("OPEN", "OFF")
        self.lbl_mid = self.metric("MID", "-")
        self.lbl_pos = self.metric("POS", "P1")
        self.lbl_result = self.metric("GERAL", "PENDENTE")
        for w in (self.lbl_tcp, self.lbl_open, self.lbl_mid, self.lbl_pos, self.lbl_result):
            statusbar.add_widget(w)
        root.add_widget(statusbar)

        self.msg = Label(text="Aguardando.", size_hint_y=None, height=dp(34), color=(1, 0.9, 0.5, 1), font_size="13sp")
        root.add_widget(self.msg)

        grid = GridLayout(cols=4, spacing=dp(8))
        for p in POSICOES:
            card = PCard(p, self.select_pos)
            self.cards[p] = card
            grid.add_widget(card)
        root.add_widget(grid)

        buttons = BoxLayout(size_hint_y=None, height=dp(44), spacing=dp(6))
        b1 = Button(text="Conectar PF6000")
        b1.bind(on_release=lambda *_: self.connect_pf())
        b2 = Button(text="Desconectar")
        b2.bind(on_release=lambda *_: self.client.disconnect())
        b3 = Button(text="Reset ciclo")
        b3.bind(on_release=lambda *_: self.reset_cycle())
        b4 = Button(text="Salvar ZPL")
        b4.bind(on_release=lambda *_: self.save_zpl())
        for b in (b1, b2, b3, b4):
            buttons.add_widget(b)
        root.add_widget(buttons)

        self.cfg_ip = DEFAULT_IP
        self.cfg_port = str(DEFAULT_PORT)
        self.cfg_print_mode = "SALVAR"
        self.cfg_zebra_ip = "192.168.0.50"
        self.cfg_zebra_port = "9100"

        Clock.schedule_interval(self.poll_events, 0.1)
        self.refresh_cards()
        return root

    def metric(self, name: str, value: str):
        box = BoxLayout(orientation="vertical", padding=dp(4))
        box.add_widget(Label(text=name, font_size="10sp", color=(0.65, 0.7, 0.78, 1), size_hint_y=0.4))
        box.val = Label(text=value, font_size="17sp", bold=True, color=(1, 1, 1, 1), size_hint_y=0.6)
        box.add_widget(box.val)
        return box

    def set_metric(self, box, value: str):
        box.val.text = str(value)

    def open_config(self, *_):
        layout = BoxLayout(orientation="vertical", padding=dp(12), spacing=dp(8))

        ip = TextInput(text=self.cfg_ip, hint_text="IP PF6000", multiline=False)
        port = TextInput(text=self.cfg_port, hint_text="Porta PF6000", multiline=False)
        mode = Spinner(text=self.cfg_print_mode, values=["SALVAR", "IP"], size_hint_y=None, height=dp(44))
        zebra_ip = TextInput(text=self.cfg_zebra_ip, hint_text="IP Zebra", multiline=False)
        zebra_port = TextInput(text=self.cfg_zebra_port, hint_text="Porta Zebra", multiline=False)

        for lbl, widget in [
            ("IP PF6000", ip),
            ("Porta PF6000", port),
            ("Modo impressao", mode),
            ("IP Zebra", zebra_ip),
            ("Porta Zebra", zebra_port),
        ]:
            layout.add_widget(Label(text=lbl, size_hint_y=None, height=dp(22), color=(1, 1, 1, 1)))
            layout.add_widget(widget)

        actions = BoxLayout(size_hint_y=None, height=dp(48), spacing=dp(6))
        pop = Popup(title="Configuracoes", content=layout, size_hint=(0.9, 0.9))

        def salvar(*_):
            self.cfg_ip = ip.text.strip() or DEFAULT_IP
            self.cfg_port = port.text.strip() or str(DEFAULT_PORT)
            self.cfg_print_mode = mode.text
            self.cfg_zebra_ip = zebra_ip.text.strip()
            self.cfg_zebra_port = zebra_port.text.strip() or "9100"
            pop.dismiss()

        btn_ok = Button(text="Salvar")
        btn_ok.bind(on_release=salvar)
        btn_cancel = Button(text="Cancelar")
        btn_cancel.bind(on_release=lambda *_: pop.dismiss())
        actions.add_widget(btn_ok)
        actions.add_widget(btn_cancel)
        layout.add_widget(actions)
        pop.open()

    def connect_pf(self):
        try:
            port = int(self.cfg_port)
        except Exception:
            port = DEFAULT_PORT
        self.client.connect(self.cfg_ip, port)

    def select_pos(self, p: str):
        if p in POSICOES:
            self.current_idx = POSICOES.index(p)
            self.msg.text = f"Selecionado {p}"
            self.refresh_cards()

    def poll_events(self, dt):
        while True:
            try:
                kind, data = self.q.get_nowait()
            except queue.Empty:
                break

            if kind == "status":
                self.set_metric(self.lbl_tcp, "ON" if data.get("connected") else "OFF")
                self.set_metric(self.lbl_open, "OK" if data.get("open_ok") else "OFF")
                self.set_metric(self.lbl_mid, data.get("last_mid", "-"))

            elif kind == "log":
                # Mantém só última mensagem na tela para não pesar.
                self.msg.text = str(data)[-140:]

            elif kind == "frame":
                self.handle_frame(data)

    def is_duplicate(self, parsed: Dict[str, Any]):
        tid = parsed.get("tightening_id") or ""
        fh = parsed.get("frame_hash") or ""
        if tid and tid in self.processed_ids:
            return True
        if fh and fh in self.processed_hashes:
            return True
        if tid:
            self.processed_ids.add(tid)
        if fh:
            self.processed_hashes.add(fh)
        return False

    def handle_frame(self, frame: str):
        parsed = parse_mid0061(frame)

        if self.is_duplicate(parsed):
            self.msg.text = "Resultado duplicado ignorado."
            return

        p = POSICOES[self.current_idx]
        item = self.data[p]
        item["tentativas"] += 1
        item["torque"] = parsed.get("torque")
        item["angulo"] = parsed.get("angulo")
        item["tid"] = parsed.get("tightening_id") or ""

        status = parsed.get("status_geral") or "SEM LEITURA"

        if status == "OK":
            item["status"] = "OK"
            self.save_attempt(p, status, parsed)
            if self.current_idx < len(POSICOES) - 1:
                self.current_idx += 1
                self.msg.text = f"{p} OK. Avancou para {POSICOES[self.current_idx]}."
            else:
                self.msg.text = "P8 OK. Ciclo finalizado."
                self.save_cycle_csv()
                self.save_zpl()
                Clock.schedule_once(lambda *_: self.reset_cycle(), 3)

        elif status == "NOK":
            item["status"] = "RETESTE"
            item["nok"] += 1
            self.save_attempt(p, status, parsed)
            self.msg.text = f"{p} NOK. Torque salvo. Refaca o ponto."

        else:
            item["status"] = "RETESTE"
            self.save_attempt(p, status, parsed)
            self.msg.text = f"{p}: leitura incompleta. Refaca."

        self.refresh_cards()

    def refresh_cards(self):
        for i, p in enumerate(POSICOES):
            self.cards[p].set_data(self.data[p], i == self.current_idx)
        self.set_metric(self.lbl_pos, POSICOES[self.current_idx])
        geral = "OK" if all(self.data[p]["status"] == "OK" for p in POSICOES) else "PENDENTE"
        if any(self.data[p]["status"] == "RETESTE" for p in POSICOES):
            geral = "RETESTE"
        self.set_metric(self.lbl_result, geral)

    def reset_cycle(self):
        self.current_idx = 0
        self.data = {
            p: {"status": "AGUARDANDO", "torque": None, "angulo": None, "tentativas": 0, "nok": 0, "tid": ""}
            for p in POSICOES
        }
        self.msg.text = "Novo ciclo. Aguardando P1."
        self.refresh_cards()

    def save_attempt(self, p: str, status: str, parsed: Dict[str, Any]):
        try:
            safe_mkdirs()
            serie = (self.in_serie.text or "SEM_SERIE").replace("/", "_").replace("\\", "_")
            arq = CSV_DIR / f"tentativas_{serie}_{datetime.now().strftime('%Y%m%d')}.csv"
            exists = arq.exists()
            with arq.open("a", newline="", encoding="utf-8-sig") as f:
                w = csv.writer(f, delimiter=";")
                if not exists:
                    w.writerow(["data_hora", "serie", "op", "posicao", "status", "torque", "angulo", "pset", "tid", "rev"])
                w.writerow([
                    now_br(),
                    self.in_serie.text,
                    self.in_op.text,
                    p,
                    status,
                    parsed.get("torque"),
                    parsed.get("angulo"),
                    parsed.get("pset"),
                    parsed.get("tightening_id"),
                    parsed.get("revision"),
                ])
        except Exception as e:
            self.msg.text = f"Erro CSV: {e}"

    def save_cycle_csv(self):
        try:
            safe_mkdirs()
            serie = (self.in_serie.text or "SEM_SERIE").replace("/", "_").replace("\\", "_")
            arq = CSV_DIR / f"ciclo_{serie}_{now_file()}.csv"
            with arq.open("w", newline="", encoding="utf-8-sig") as f:
                w = csv.writer(f, delimiter=";")
                w.writerow(["serie", self.in_serie.text, "op", self.in_op.text, "data", now_br()])
                w.writerow(["posicao", "status", "torque", "angulo", "tentativas", "nok"])
                for p in POSICOES:
                    d = self.data[p]
                    w.writerow([p, d["status"], d["torque"], d["angulo"], d["tentativas"], d["nok"]])
        except Exception as e:
            self.msg.text = f"Erro ciclo CSV: {e}"

    def gerar_zpl(self) -> str:
        data_hora = now_br()
        serie = self.in_serie.text or ""
        linhas = []
        for p in POSICOES:
            d = self.data[p]
            st = "OK" if d["status"] == "OK" else ("NOK" if d["nok"] > 0 else d["status"])
            tq = fmt_num(d["torque"])
            linhas.append(f"{p}: {tq}-{st}")

        return f"""^XA
^CI28
^PW899
^LL399
^LH0,0
^CF0,18
^FO175,35^A0N,30,30^FB560,1,0,C,0^FDREGISTRO DE TORQUE^FS
^FO30,95^A0N,25,25^FDDATA/HORA: {data_hora}^FS
^FO30,130^A0N,25,25^FDSERIE: {serie}^FS
^FO10,170^GB850,2,2^FS
^FO35,200^A0N,28,28^FD{linhas[0]}^FS
^FO35,245^A0N,28,28^FD{linhas[1]}^FS
^FO35,290^A0N,28,28^FD{linhas[2]}^FS
^FO35,335^A0N,28,28^FD{linhas[3]}^FS
^FO460,200^A0N,28,28^FD{linhas[4]}^FS
^FO460,245^A0N,28,28^FD{linhas[5]}^FS
^FO460,290^A0N,28,28^FD{linhas[6]}^FS
^FO460,335^A0N,28,28^FD{linhas[7]}^FS
^PQ1,0,1,N
^XZ"""

    def save_zpl(self):
        zpl = self.gerar_zpl()
        try:
            safe_mkdirs()
            serie = (self.in_serie.text or "SEM_SERIE").replace("/", "_").replace("\\", "_")
            arq = CSV_DIR / f"etiqueta_{serie}_{now_file()}.zpl"
            arq.write_text(zpl, encoding="utf-8")
            self.msg.text = f"ZPL salvo: {arq.name}"
        except Exception as e:
            self.msg.text = f"Erro salvando ZPL: {e}"

        if self.cfg_print_mode == "IP":
            self.print_ip(zpl)

    def print_ip(self, zpl: str):
        try:
            port = int(self.cfg_zebra_port)
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(5)
            s.connect((self.cfg_zebra_ip, port))
            s.sendall(zpl.encode("utf-8"))
            s.close()
            self.msg.text = "Etiqueta enviada por IP."
        except Exception as e:
            self.msg.text = f"Falha impressao IP: {e}"


if __name__ == "__main__":
    TorqueApp().run()

    candidatos = []

    try:
        if app is not None and getattr(app, "user_data_dir", None):
            candidatos.append(Path(app.user_data_dir))
    except Exception:
        pass

    for env_name in ("ANDROID_PRIVATE", "ANDROID_ARGUMENT"):
        try:
            p = os.environ.get(env_name)
            if p:
                candidatos.append(Path(p))
        except Exception:
            pass

    candidatos.append(Path.cwd())
    candidatos.append(Path("."))

    for base in candidatos:
        try:
            base.mkdir(parents=True, exist_ok=True)
            log_dir = base / "logs_torque_pf6000"
            csv_dir = base / "registros_torque"
            log_dir.mkdir(parents=True, exist_ok=True)
            csv_dir.mkdir(parents=True, exist_ok=True)
            BASE_DIR = base
            LOG_DIR = log_dir
            CSV_DIR = csv_dir
            return True
        except Exception:
            continue

    BASE_DIR = Path(".")
    LOG_DIR = Path(".")
    CSV_DIR = Path(".")
    return False


def salvar_crash_log(exc: BaseException):
    try:
        import traceback
        inicializar_pastas(None)
        crash_file = BASE_DIR / "crash_torque_pf6000.txt"
        crash_file.write_text(traceback.format_exc(), encoding="utf-8")
    except Exception:
        pass


# =========================================================
# FUNCOES GERAIS
# =========================================================
def now_br() -> str:
    return datetime.now().strftime("%d/%m/%Y %H:%M:%S")


def now_file() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def to_float_text(v: Optional[float]) -> str:
    if v is None:
        return ""
    try:
        return f"{float(v):.2f}".replace(".", ",")
    except Exception:
        return ""


def only_ascii(texto: Any) -> str:
    return str(texto or "").encode("ascii", errors="ignore").decode("ascii", errors="ignore")


def status_01(valor: Any) -> str:
    v = str(valor or "").strip().upper()
    if v in ("1", "01", "OK"):
        return "OK"
    if v in ("0", "00", "NOK"):
        return "NOK"
    return v


def scaled_number(raw: str, divisor: float) -> Optional[float]:
    if raw is None:
        return None
    s = str(raw).strip().replace(" ", "")
    if not s:
        return None
    if not re.fullmatch(r"-?\d+", s):
        return None
    try:
        return int(s) / float(divisor)
    except Exception:
        return None


def safe_float(text: Any, default: float) -> float:
    try:
        return float(str(text).replace(",", "."))
    except Exception:
        return default


def safe_int(text: Any, default: int) -> int:
    try:
        return int(float(str(text).replace(",", ".")))
    except Exception:
        return default


# =========================================================
# OPEN PROTOCOL BASE
# =========================================================
def montar_mid(mid: str, revision: str = "001", data: str = "") -> bytes:
    mid = str(mid).zfill(4)
    revision = str(revision).zfill(3)
    header_sem_tamanho = f"{mid}{revision}"
    corpo_sem_tamanho = header_sem_tamanho.ljust(16) + data
    tamanho = len(corpo_sem_tamanho) + 4
    return f"{tamanho:04d}{corpo_sem_tamanho}\x00".encode("ascii", errors="ignore")


def extrair_mid(frame: str) -> str:
    if len(frame) >= 8 and frame[:4].isdigit():
        return frame[4:8]
    return ""


def extrair_rev(frame: str) -> str:
    if len(frame) >= 11 and frame[8:11].isdigit():
        return frame[8:11]
    return "001"


def split_frames(buffer: bytes) -> Tuple[List[bytes], bytes]:
    partes = buffer.split(b"\x00")
    return partes[:-1], partes[-1]


# =========================================================
# PARSER MID 0061
# =========================================================
# Rev.001:
#   campo 15 = torque real
#   campo 19 = angulo real
#   campo 23 = tightening_id
# Rev.002+:
#   campo 24 = torque real
#   campo 28 = angulo real
#   campo 41 = tightening_id

SPEC_REV1: List[Tuple[str, int, str]] = [
    ("01", 4, "cell_id"),
    ("02", 2, "channel_id"),
    ("03", 25, "controller_name"),
    ("04", 25, "vin"),
    ("05", 2, "job_id"),
    ("06", 3, "pset"),
    ("07", 4, "batch_size"),
    ("08", 4, "batch_counter"),
    ("09", 1, "tightening_status"),
    ("10", 1, "torque_status"),
    ("11", 1, "angle_status"),
    ("12", 6, "torque_min"),
    ("13", 6, "torque_max"),
    ("14", 6, "torque_target"),
    ("15", 6, "torque"),
    ("16", 5, "angle_min"),
    ("17", 5, "angle_max"),
    ("18", 5, "angle_target"),
    ("19", 5, "angle"),
    ("20", 19, "timestamp"),
    ("21", 19, "pset_last_change"),
    ("22", 1, "batch_status"),
    ("23", 10, "tightening_id"),
]

SPEC_REV2_BASE: List[Tuple[str, int, str]] = [
    ("01", 4, "cell_id"),
    ("02", 2, "channel_id"),
    ("03", 25, "controller_name"),
    ("04", 25, "vin"),
    ("05", 4, "job_id"),
    ("06", 3, "pset"),
    ("07", 2, "strategy"),
    ("08", 5, "strategy_options"),
    ("09", 4, "batch_size"),
    ("10", 4, "batch_counter"),
    ("11", 1, "tightening_status"),
    ("12", 1, "batch_status"),
    ("13", 1, "torque_status"),
    ("14", 1, "angle_status"),
    ("15", 1, "rundown_angle_status"),
    ("16", 1, "current_monitoring_status"),
    ("17", 1, "selftap_status"),
    ("18", 1, "prevail_torque_monitoring_status"),
    ("19", 1, "prevail_torque_comp_status"),
    ("20", 10, "tightening_error_status"),
    ("21", 6, "torque_min"),
    ("22", 6, "torque_max"),
    ("23", 6, "torque_target"),
    ("24", 6, "torque"),
    ("25", 5, "angle_min"),
    ("26", 5, "angle_max"),
    ("27", 5, "angle_target"),
    ("28", 5, "angle"),
    ("29", 5, "rundown_angle_min"),
    ("30", 5, "rundown_angle_max"),
    ("31", 5, "rundown_angle"),
    ("32", 3, "current_monitoring_min"),
    ("33", 3, "current_monitoring_max"),
    ("34", 3, "current_monitoring_value"),
    ("35", 6, "selftap_min"),
    ("36", 6, "selftap_max"),
    ("37", 6, "selftap_torque"),
    ("38", 6, "pvt_min"),
    ("39", 6, "pvt_max"),
    ("40", 6, "pvt_torque"),
    ("41", 10, "tightening_id"),
    ("42", 5, "job_sequence_number"),
    ("43", 5, "sync_tightening_id"),
    ("44", 14, "tool_serial"),
    ("45", 19, "timestamp"),
    ("46", 19, "pset_last_change"),
]


def parse_fields_by_spec(frame: str, spec: List[Tuple[str, int, str]], start_pos: int = 20) -> Tuple[Dict[str, str], Dict[str, str], List[str]]:
    fields_by_id: Dict[str, str] = {}
    fields_by_name: Dict[str, str] = {}
    warnings: List[str] = []
    pos = start_pos
    for fid, width, name in spec:
        if pos >= len(frame):
            break
        if frame[pos:pos + 2] == fid:
            id_pos = pos
        else:
            found = frame.find(fid, pos, min(len(frame), pos + 12))
            if found >= 0:
                warnings.append(f"ressync_{fid}:{pos}->{found}")
                id_pos = found
            else:
                warnings.append(f"missing_{fid}_at_{pos}")
                continue
        value_start = id_pos + 2
        value_end = value_start + width
        value = frame[value_start:value_end]
        fields_by_id[fid] = value
        fields_by_name[name] = value
        pos = value_end
    return fields_by_id, fields_by_name, warnings


def parse_mid0061(frame: str, torque_divisor: float = 100.0, angle_divisor: float = 1.0,
                  torque_field: str = "AUTO", angle_field: str = "AUTO") -> Dict[str, Any]:
    revision = extrair_rev(frame)
    if revision == "001":
        spec = SPEC_REV1
        auto_torque_field = "15"
        auto_angle_field = "19"
        status_field = "09"
        torque_status_field = "10"
        angle_status_field = "11"
        tightening_id_field = "23"
    else:
        spec = SPEC_REV2_BASE
        auto_torque_field = "24"
        auto_angle_field = "28"
        status_field = "11"
        torque_status_field = "13"
        angle_status_field = "14"
        tightening_id_field = "41"

    fields_by_id, fields_by_name, warnings = parse_fields_by_spec(frame, spec)
    torque_field_final = auto_torque_field if str(torque_field).upper() == "AUTO" else str(torque_field).zfill(2)
    angle_field_final = auto_angle_field if str(angle_field).upper() == "AUTO" else str(angle_field).zfill(2)
    raw_torque = (fields_by_id.get(torque_field_final, "") or "").strip()
    raw_angle = (fields_by_id.get(angle_field_final, "") or "").strip()
    torque = scaled_number(raw_torque, torque_divisor)
    angle = scaled_number(raw_angle, angle_divisor)
    tightening_id = (fields_by_id.get(tightening_id_field, "") or fields_by_name.get("tightening_id", "") or "").strip()
    pset = (fields_by_name.get("pset", "") or "").strip()
    pset = pset.lstrip("0") or pset
    frame_hash = hashlib.sha1(frame.encode("utf-8", errors="ignore")).hexdigest()[:16]

    fields_debug = dict(fields_by_id)
    fields_debug["_revision"] = revision
    fields_debug["_torque_field_usado"] = torque_field_final
    fields_debug["_angle_field_usado"] = angle_field_final
    fields_debug["_tightening_id"] = tightening_id
    fields_debug["_frame_hash"] = frame_hash

    return {
        "data_hora_pc": now_br(),
        "mid": "0061",
        "revision": revision,
        "status_geral": status_01(fields_by_id.get(status_field, "")),
        "status_torque": status_01(fields_by_id.get(torque_status_field, "")),
        "status_angulo": status_01(fields_by_id.get(angle_status_field, "")),
        "pset": pset,
        "tightening_id": tightening_id,
        "frame_hash": frame_hash,
        "torque": torque,
        "angulo": angle,
        "torque_raw": raw_torque,
        "angulo_raw": raw_angle,
        "parser_info": f"rev={revision};torque_field={torque_field_final};angle_field={angle_field_final};warnings={','.join(warnings)}",
        "fields": fields_debug,
        "frame": frame,
    }


# =========================================================
# IMPRESSAO ZEBRA
# =========================================================
def gerar_zpl_torque(serie: str, pontos: Dict[str, "PState"], copias: int = 1) -> str:
    copias = max(1, int(copias or 1))
    agora = datetime.now()
    data_hora = agora.strftime("%d/%m/%y / HORA: %H:%M:%S")
    serie = serie or "SEM SERIE"

    def ponto_txt(p: str) -> str:
        stt = pontos[p]
        status = stt.ultimo_status or stt.status
        torque = to_float_text(stt.torque)
        if not torque:
            torque = "--"
        return f"{p}: {torque}-{status}"

    # Layout baseado no ZPL enviado pelo usuario, ajustado para caber no ^LL399.
    return f"""
^XA
^CI28
^PW899
^LL399
^LH0,0
^CF0,18
^FO175,35^A0N,30,30^FB520,1,0,C,0^FDREGISTRO DE TORQUE^FS
^FO45,95^A0N,26,26^FD{{DATA: {data_hora}}}^FS
^FO45,135^A0N,26,26^FD{{SERIE: {only_ascii(serie)}}}^FS
^FO10,178^GB870,2,2^FS
^FO45,210^A0N,32,32^FD{{{only_ascii(ponto_txt('P1'))}}}^FS
^FO45,255^A0N,32,32^FD{{{only_ascii(ponto_txt('P2'))}}}^FS
^FO45,300^A0N,32,32^FD{{{only_ascii(ponto_txt('P3'))}}}^FS
^FO45,345^A0N,32,32^FD{{{only_ascii(ponto_txt('P4'))}}}^FS
^FO430,210^A0N,32,32^FD{{{only_ascii(ponto_txt('P5'))}}}^FS
^FO430,255^A0N,32,32^FD{{{only_ascii(ponto_txt('P6'))}}}^FS
^FO430,300^A0N,32,32^FD{{{only_ascii(ponto_txt('P7'))}}}^FS
^FO430,345^A0N,32,32^FD{{{only_ascii(ponto_txt('P8'))}}}^FS
^PQ{copias},0,1,N
^XZ
""".strip()


def imprimir_zebra_ip(ip: str, porta: int, zpl: str, timeout: float = 5.0):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(timeout)
        s.connect((ip, int(porta)))
        s.sendall(zpl.encode("utf-8", errors="ignore"))


def imprimir_zebra_usb_android(zpl: str) -> str:
    """Envia ZPL para primeira impressora USB encontrada no Android.

    Observacao: na primeira tentativa o Android pode pedir permissao USB.
    Depois de permitir, tente imprimir novamente.
    """
    if platform != "android":
        raise RuntimeError("USB Android disponível somente no APK instalado no tablet.")

    from jnius import autoclass, cast  # type: ignore

    PythonActivity = autoclass("org.kivy.android.PythonActivity")
    Context = autoclass("android.content.Context")
    UsbConstants = autoclass("android.hardware.usb.UsbConstants")
    PendingIntent = autoclass("android.app.PendingIntent")
    Intent = autoclass("android.content.Intent")
    Build = autoclass("android.os.Build")

    activity = PythonActivity.mActivity
    usb_manager = cast("android.hardware.usb.UsbManager", activity.getSystemService(Context.USB_SERVICE))
    device_list = usb_manager.getDeviceList()

    if device_list.isEmpty():
        raise RuntimeError("Nenhum dispositivo USB encontrado. Conecte a Zebra no OTG.")

    iterator = device_list.values().iterator()
    chosen = None
    while iterator.hasNext():
        dev = iterator.next()
        # Zebra costuma ser vendor 0x0A5F, mas deixamos aberto para testar qualquer USB printer.
        chosen = dev
        if int(dev.getVendorId()) == 0x0A5F:
            chosen = dev
            break

    if chosen is None:
        raise RuntimeError("Nenhuma impressora USB encontrada.")

    if not usb_manager.hasPermission(chosen):
        flags = 0
        try:
            if int(Build.VERSION.SDK_INT) >= 23:
                flags = PendingIntent.FLAG_IMMUTABLE
        except Exception:
            flags = 0
        permission_intent = PendingIntent.getBroadcast(activity, 0, Intent("br.com.ibero.USB_PERMISSION"), flags)
        usb_manager.requestPermission(chosen, permission_intent)
        raise RuntimeError("Permissão USB solicitada. Autorize no Android e clique em imprimir novamente.")

    connection = usb_manager.openDevice(chosen)
    if connection is None:
        raise RuntimeError("Não foi possível abrir conexão USB com a Zebra.")

    try:
        endpoint_out = None
        interface = None
        for i in range(chosen.getInterfaceCount()):
            itf = chosen.getInterface(i)
            for e in range(itf.getEndpointCount()):
                ep = itf.getEndpoint(e)
                if ep.getDirection() == UsbConstants.USB_DIR_OUT and ep.getType() == UsbConstants.USB_ENDPOINT_XFER_BULK:
                    interface = itf
                    endpoint_out = ep
                    break
            if endpoint_out is not None:
                break

        if interface is None or endpoint_out is None:
            raise RuntimeError("Endpoint USB OUT não encontrado na impressora.")

        if not connection.claimInterface(interface, True):
            raise RuntimeError("Não foi possível reservar interface USB da impressora.")

        data = zpl.encode("utf-8", errors="ignore")
        offset = 0
        chunk_size = 4096
        while offset < len(data):
            chunk = data[offset:offset + chunk_size]
            sent = connection.bulkTransfer(endpoint_out, chunk, len(chunk), 5000)
            if sent is None or int(sent) < 0:
                raise RuntimeError("Falha no bulkTransfer USB para a Zebra.")
            offset += len(chunk)

        try:
            connection.releaseInterface(interface)
        except Exception:
            pass

    finally:
        try:
            connection.close()
        except Exception:
            pass

    return f"ZPL enviado via USB para vendor={chosen.getVendorId()} product={chosen.getProductId()}"


# =========================================================
# ESTADO
# =========================================================
@dataclass
class PState:
    status: str = "AGUARDANDO"
    ultimo_status: str = ""
    torque: Optional[float] = None
    angulo: Optional[float] = None
    pset: str = ""
    data_hora: str = ""
    tentativas: int = 0
    nok_count: int = 0
    tightening_id: str = ""
    frame_hash: str = ""


# =========================================================
# CLIENTE OPEN PROTOCOL THREAD
# =========================================================
class OpenProtocolClient:
    def __init__(self, event_q: queue.Queue):
        self.event_q = event_q
        self.ip = DEFAULT_IP
        self.port = DEFAULT_PORT
        self.rev_0060 = "001"
        self.rev_0062 = "001"
        self.auto_reconnect = True
        self.sock: Optional[socket.socket] = None
        self.thread: Optional[threading.Thread] = None
        self.lock = threading.Lock()
        self.desired_connected = False
        self.connected_tcp = False
        self.connected_open = False
        self.reconnecting = False
        self.last_mid = "-"
        self.last_error = ""
        self.last_frame = ""

    def emit(self, kind: str, data: Any = None):
        self.event_q.put((kind, data))

    def log(self, msg: str):
        line = f"[{now_br()}] {msg}"
        self.emit("log", line)
        try:
            log_file = LOG_DIR / f"pf6000_{datetime.now().strftime('%Y%m%d')}.log"
            with log_file.open("a", encoding="utf-8") as f:
                f.write(line + "\n")
        except Exception:
            pass

    def status_snapshot(self) -> Dict[str, Any]:
        with self.lock:
            return {
                "connected_tcp": self.connected_tcp,
                "connected_open": self.connected_open,
                "reconnecting": self.reconnecting,
                "desired_connected": self.desired_connected,
                "last_mid": self.last_mid,
                "last_error": self.last_error,
            }

    def start(self, ip: str, port: int, rev_0060: str, rev_0062: str, auto_reconnect: bool):
        with self.lock:
            self.ip = ip.strip()
            self.port = int(port)
            self.rev_0060 = str(rev_0060).zfill(3)
            self.rev_0062 = str(rev_0062).zfill(3)
            self.auto_reconnect = bool(auto_reconnect)
            self.desired_connected = True
            self.last_error = ""

        if self.thread and self.thread.is_alive():
            self.log("Cliente já está em execução.")
            return

        self.thread = threading.Thread(target=self._manager_loop, daemon=True)
        self.thread.start()
        self.log("Gerenciador de conexão iniciado.")

    def stop(self):
        with self.lock:
            self.desired_connected = False
        self._close_socket()
        self.log("Desconectado pelo usuário.")

    def _close_socket(self):
        with self.lock:
            sock = self.sock
            self.sock = None
            self.connected_tcp = False
            self.connected_open = False
        if sock:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except Exception:
                pass
            try:
                sock.close()
            except Exception:
                pass
        self.emit("status", self.status_snapshot())

    def _send_mid(self, mid: str, rev: str = "001", data: str = "") -> bool:
        with self.lock:
            sock = self.sock
        if not sock:
            self.log(f"Não enviou MID {mid}: socket não conectado.")
            return False
        try:
            packet = montar_mid(mid, rev, data)
            sock.sendall(packet)
            self.log(f"ENVIADO MID {mid} REV {rev}")
            return True
        except Exception as e:
            with self.lock:
                self.last_error = str(e)
            self.log(f"ERRO enviando MID {mid}: {e}")
            self.emit("status", self.status_snapshot())
            return False

    def subscribe_result(self):
        with self.lock:
            rev = self.rev_0060
        self.log(f"Assinando resultado de aperto MID 0060 REV {rev}")
        self._send_mid("0060", rev)

    def _connect_once(self) -> bool:
        with self.lock:
            ip = self.ip
            port = self.port
            self.reconnecting = True
            self.last_error = ""
        self.emit("status", self.status_snapshot())
        try:
            self.log(f"Conectando no PF6000 {ip}:{port}...")
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(4)
            sock.connect((ip, int(port)))
            sock.settimeout(1)
            with self.lock:
                self.sock = sock
                self.connected_tcp = True
                self.connected_open = False
                self.reconnecting = False
            self.log("TCP CONECTADO.")
            self.emit("status", self.status_snapshot())
            self._send_mid("0001", "006")
            return True
        except Exception as e:
            with self.lock:
                self.connected_tcp = False
                self.connected_open = False
                self.reconnecting = False
                self.last_error = str(e)
            self.log(f"Falha ao conectar: {e}")
            self._close_socket()
            return False

    def _manager_loop(self):
        while True:
            with self.lock:
                desired = self.desired_connected
                auto = self.auto_reconnect
            if not desired:
                break
            ok = self._connect_once()
            if ok:
                self._reader_loop()
            self._close_socket()
            with self.lock:
                desired = self.desired_connected
                auto = self.auto_reconnect
            if not desired or not auto:
                break
            with self.lock:
                self.reconnecting = True
            self.emit("status", self.status_snapshot())
            self.log("Conexão caiu/foi encerrada. Tentando reconectar em 2 segundos...")
            time.sleep(2)
        with self.lock:
            self.connected_tcp = False
            self.connected_open = False
            self.reconnecting = False
        self.emit("status", self.status_snapshot())
        self.log("Gerenciador de conexão finalizado.")

    def _reader_loop(self):
        buffer = b""
        last_keepalive = time.time()
        while True:
            with self.lock:
                desired = self.desired_connected
                sock = self.sock
                rev_ack = self.rev_0062
            if not desired or not sock:
                break
            if time.time() - last_keepalive >= 10:
                self._send_mid("9999", "001")
                last_keepalive = time.time()
            try:
                data = sock.recv(4096)
                if not data:
                    self.log("Conexão encerrada pelo painel.")
                    break
                raw = buffer + data
                frames, buffer = split_frames(raw)
                for fb in frames:
                    frame = fb.decode("ascii", errors="ignore")
                    mid = extrair_mid(frame)
                    with self.lock:
                        self.last_mid = mid
                        self.last_frame = frame
                    self.emit("status", self.status_snapshot())
                    self.log(f"RECEBIDO MID {mid}")
                    if mid == "0002":
                        with self.lock:
                            self.connected_open = True
                        self.emit("status", self.status_snapshot())
                        self.log("Open Protocol iniciado.")
                        time.sleep(0.2)
                        self.subscribe_result()
                    elif mid == "0005":
                        self.log("Painel aceitou comando.")
                    elif mid == "0004":
                        self.log("Painel recusou algum MID. Teste outra REV do 0060.")
                    elif mid == "0061":
                        self.log("Resultado de aperto recebido.")
                        self._send_mid("0062", rev_ack)
                        self.emit("result_frame", frame)
                    elif mid == "9999":
                        self._send_mid("9999", "001")
            except socket.timeout:
                continue
            except OSError as e:
                self.log(f"Erro de rede: {e}")
                break
            except Exception as e:
                self.log(f"Erro inesperado na leitura: {e}")
                break


# =========================================================
# WIDGETS
# =========================================================
class PCard(BoxLayout):
    def __init__(self, ponto: str, on_select, **kwargs):
        super().__init__(orientation="vertical", padding=dp(10), spacing=dp(5), **kwargs)
        # NÃO usar self.pos para guardar "P1/P2".
        # self.pos é propriedade interna do Kivy para posição X/Y do widget.
        # Se colocar self.pos = "P1", o APK fecha ao abrir.
        self.ponto = ponto
        self.on_select = on_select
        self.size_hint_y = None
        self.height = dp(165)
        self.status = "AGUARDANDO"
        self._build()

    def _build(self):
        from kivy.graphics import Color, RoundedRectangle, Line
        self.bg_color = (0.06, 0.07, 0.08, 1)
        self.border_color = (0.20, 0.22, 0.25, 1)
        with self.canvas.before:
            self._color = Color(*self.bg_color)
            self._rect = RoundedRectangle(pos=self.pos, size=self.size, radius=[dp(12)])
            self._line_color = Color(*self.border_color)
            self._line = Line(rounded_rectangle=(self.x, self.y, self.width, self.height, dp(12)), width=1.3)
        self.bind(pos=self._update_canvas, size=self._update_canvas)

        self.lbl_pos = Label(text=self.ponto, font_size="24sp", bold=True, halign="left", valign="middle",
                             size_hint_y=None, height=dp(32), color=(1, 1, 1, 1))
        self.lbl_pos.bind(size=lambda w, s: setattr(w, "text_size", s))
        self.add_widget(self.lbl_pos)

        self.lbl_status = Label(text="AGUARDANDO", font_size="14sp", bold=True, halign="left",
                                size_hint_y=None, height=dp(26), color=(0.75, 0.78, 0.82, 1))
        self.lbl_status.bind(size=lambda w, s: setattr(w, "text_size", s))
        self.add_widget(self.lbl_status)

        self.lbl_torque = Label(text="Torque:", font_size="13sp", halign="left", size_hint_y=None, height=dp(24), color=(1, 1, 1, 1))
        self.lbl_torque.bind(size=lambda w, s: setattr(w, "text_size", s))
        self.add_widget(self.lbl_torque)

        self.lbl_angulo = Label(text="Ângulo:", font_size="13sp", halign="left", size_hint_y=None, height=dp(24), color=(1, 1, 1, 1))
        self.lbl_angulo.bind(size=lambda w, s: setattr(w, "text_size", s))
        self.add_widget(self.lbl_angulo)

        self.lbl_info = Label(text="Tentativas: 0 | NOK: 0", font_size="11sp", halign="left", size_hint_y=None, height=dp(20), color=(0.80, 0.84, 0.90, 1))
        self.lbl_info.bind(size=lambda w, s: setattr(w, "text_size", s))
        self.add_widget(self.lbl_info)

        btn = Button(text=f"Selecionar {self.ponto}", size_hint_y=None, height=dp(34), font_size="12sp")
        btn.bind(on_release=lambda *_: self.on_select(self.ponto))
        self.add_widget(btn)

    def _update_canvas(self, *args):
        self._rect.pos = self.pos
        self._rect.size = self.size
        self._line.rounded_rectangle = (self.x, self.y, self.width, self.height, dp(12))

    def set_colors(self, bg, border):
        self.bg_color = bg
        self.border_color = border
        self._color.rgba = bg
        self._line_color.rgba = border

    def update_data(self, state: PState, current: bool):
        if current:
            bg = (0.03, 0.14, 0.25, 1)
            border = (0.00, 0.82, 1.0, 1)
            status_color = (0.00, 0.82, 1.0, 1)
        elif state.status == "OK":
            bg = (0.02, 0.18, 0.08, 1)
            border = (0.00, 0.78, 0.32, 1)
            status_color = (0.00, 0.95, 0.46, 1)
        elif state.status == "AGUARDANDO RETESTE":
            bg = (0.23, 0.18, 0.00, 1)
            border = (1.00, 0.84, 0.00, 1)
            status_color = (1.00, 0.84, 0.00, 1)
        else:
            bg = (0.06, 0.07, 0.08, 1)
            border = (0.20, 0.22, 0.25, 1)
            status_color = (0.75, 0.78, 0.82, 1)
        self.set_colors(bg, border)
        self.lbl_status.text = state.status
        self.lbl_status.color = status_color
        self.lbl_torque.text = f"Torque: {to_float_text(state.torque)}"
        self.lbl_angulo.text = f"Ângulo: {to_float_text(state.angulo)}"
        self.lbl_info.text = f"Tentativas: {state.tentativas} | NOK: {state.nok_count}"


# =========================================================
# APP
# =========================================================
class TorquePF6000App(App):
    title = "Torque Mola - PF6000"

    def build(self):
        # Inicializa armazenamento somente aqui para evitar crash na abertura do APK.
        inicializar_pastas(self)

        self.event_q: queue.Queue = queue.Queue()
        self.client = OpenProtocolClient(self.event_q)
        self.posicoes: Dict[str, PState] = {p: PState() for p in POSICOES}
        self.current_idx = 0
        self.historico: List[Dict[str, Any]] = []
        self.processed_tightening_ids: set = set()
        self.processed_frame_hash_time: Dict[str, float] = {}
        self.ciclo_em_reset = False

        self.root_box = BoxLayout(orientation="vertical", padding=dp(8), spacing=dp(6))
        self._build_top()
        self._build_cards()
        self._build_bottom_log()

        Clock.schedule_interval(self._poll_events, 0.1)
        self._refresh_all()
        return self.root_box

    def _build_top(self):
        header = BoxLayout(orientation="horizontal", size_hint_y=None, height=dp(58), spacing=dp(8))
        title_box = BoxLayout(orientation="vertical")
        title_box.add_widget(Label(text="Controle de Torque Mola - PF6000", font_size="22sp", bold=True, halign="left", color=(1, 1, 1, 1)))
        self.lbl_msg = Label(text="Aguardando conexão.", font_size="13sp", halign="left", color=(1, 0.90, 0.30, 1))
        title_box.add_widget(self.lbl_msg)
        header.add_widget(title_box)
        btn_cfg = Button(text="CONFIG", size_hint_x=None, width=dp(130), font_size="14sp")
        btn_cfg.bind(on_release=lambda *_: self.open_config_popup())
        header.add_widget(btn_cfg)
        self.root_box.add_widget(header)

        data_row = BoxLayout(orientation="horizontal", size_hint_y=None, height=dp(54), spacing=dp(8))
        self.in_serie = TextInput(hint_text="Nº Série / Rastreio", multiline=False, font_size="16sp")
        self.in_op = TextInput(hint_text="OP", multiline=False, font_size="16sp")
        data_row.add_widget(self.in_serie)
        data_row.add_widget(self.in_op)
        self.root_box.add_widget(data_row)

        status_row = GridLayout(cols=5, size_hint_y=None, height=dp(58), spacing=dp(6))
        self.lbl_tcp = self._metric("TCP", "OFF")
        self.lbl_open = self._metric("Open", "OFF")
        self.lbl_mid = self._metric("MID", "-")
        self.lbl_pos = self._metric("Atual", "P1")
        self.lbl_geral = self._metric("Status", "PENDENTE")
        for w in [self.lbl_tcp, self.lbl_open, self.lbl_mid, self.lbl_pos, self.lbl_geral]:
            status_row.add_widget(w)
        self.root_box.add_widget(status_row)

    def _metric(self, title: str, value: str) -> BoxLayout:
        b = BoxLayout(orientation="vertical", padding=dp(6))
        b.add_widget(Label(text=title, font_size="11sp", color=(0.75, 0.80, 0.88, 1), size_hint_y=0.35))
        lab = Label(text=value, font_size="18sp", bold=True, color=(1, 1, 1, 1), size_hint_y=0.65)
        b.value_label = lab  # type: ignore
        b.add_widget(lab)
        return b

    def _build_cards(self):
        self.cards: Dict[str, PCard] = {}
        grid = GridLayout(cols=4, spacing=dp(8), size_hint_y=None)
        grid.bind(minimum_height=grid.setter("height"))
        for p in POSICOES:
            card = PCard(p, on_select=self.select_position)
            self.cards[p] = card
            grid.add_widget(card)
        # 2 linhas x 165 + espaçamento
        grid.height = dp(345)
        self.root_box.add_widget(grid)

    def _build_bottom_log(self):
        row = BoxLayout(orientation="horizontal", size_hint_y=None, height=dp(52), spacing=dp(8))
        self.btn_connect = Button(text="Conectar", font_size="14sp")
        self.btn_connect.bind(on_release=lambda *_: self.connect())
        row.add_widget(self.btn_connect)
        btn_stop = Button(text="Desconectar", font_size="14sp")
        btn_stop.bind(on_release=lambda *_: self.client.stop())
        row.add_widget(btn_stop)
        btn_reset = Button(text="Reset ciclo", font_size="14sp")
        btn_reset.bind(on_release=lambda *_: self.reset_cycle(clear_history=False))
        row.add_widget(btn_reset)
        btn_print = Button(text="Imprimir etiqueta", font_size="14sp")
        btn_print.bind(on_release=lambda *_: self.print_current_label())
        row.add_widget(btn_print)
        self.root_box.add_widget(row)

        self.log_label = Label(text="", font_size="11sp", color=(0.75, 0.80, 0.88, 1), halign="left", valign="top")
        self.log_label.bind(size=lambda w, s: setattr(w, "text_size", s))
        self.root_box.add_widget(self.log_label)
        self.logs: List[str] = []

    def open_config_popup(self):
        content = BoxLayout(orientation="vertical", padding=dp(10), spacing=dp(8))
        scroll = ScrollView()
        inner = GridLayout(cols=2, spacing=dp(8), size_hint_y=None)
        inner.bind(minimum_height=inner.setter("height"))

        self.cfg_ip = TextInput(text=getattr(self, "cfg_ip", DEFAULT_IP) if isinstance(getattr(self, "cfg_ip", None), str) else DEFAULT_IP, multiline=False)
        self.cfg_port = TextInput(text=getattr(self, "cfg_port", str(DEFAULT_PORT)) if isinstance(getattr(self, "cfg_port", None), str) else str(DEFAULT_PORT), multiline=False)
        self.cfg_rev0060 = Spinner(text=getattr(self, "cfg_rev0060", "001") if isinstance(getattr(self, "cfg_rev0060", None), str) else "001", values=["001", "002", "003", "004", "005", "006", "007", "008"])
        self.cfg_rev0062 = Spinner(text=getattr(self, "cfg_rev0062", "001") if isinstance(getattr(self, "cfg_rev0062", None), str) else "001", values=["001", "002", "003", "004", "005", "006", "007", "008"])
        self.cfg_modelo = TextInput(text=getattr(self, "cfg_modelo", "MOLA") if isinstance(getattr(self, "cfg_modelo", None), str) else "MOLA", multiline=False)
        self.cfg_tmin = TextInput(text=getattr(self, "cfg_tmin", "0") if isinstance(getattr(self, "cfg_tmin", None), str) else "0", multiline=False)
        self.cfg_tmax = TextInput(text=getattr(self, "cfg_tmax", "9999") if isinstance(getattr(self, "cfg_tmax", None), str) else "9999", multiline=False)
        self.cfg_tdiv = Spinner(text=getattr(self, "cfg_tdiv", "100") if isinstance(getattr(self, "cfg_tdiv", None), str) else "100", values=["1", "10", "100", "1000"])
        self.cfg_adiv = Spinner(text=getattr(self, "cfg_adiv", "1") if isinstance(getattr(self, "cfg_adiv", None), str) else "1", values=["1", "10", "100"])
        self.cfg_tfield = Spinner(text=getattr(self, "cfg_tfield", "AUTO") if isinstance(getattr(self, "cfg_tfield", None), str) else "AUTO", values=["AUTO", "15", "24", "12", "13", "14", "21", "22", "23"])
        self.cfg_afield = Spinner(text=getattr(self, "cfg_afield", "AUTO") if isinstance(getattr(self, "cfg_afield", None), str) else "AUTO", values=["AUTO", "19", "28", "16", "17", "18", "25", "26", "27"])
        self.cfg_print_mode = Spinner(text=getattr(self, "cfg_print_mode", "SALVAR") if isinstance(getattr(self, "cfg_print_mode", None), str) else "SALVAR", values=["SALVAR", "USB", "IP"])
        self.cfg_zebra_ip = TextInput(text=getattr(self, "cfg_zebra_ip", "192.168.0.50") if isinstance(getattr(self, "cfg_zebra_ip", None), str) else "192.168.0.50", multiline=False)
        self.cfg_zebra_port = TextInput(text=getattr(self, "cfg_zebra_port", "9100") if isinstance(getattr(self, "cfg_zebra_port", None), str) else "9100", multiline=False)
        self.cfg_copias = TextInput(text=getattr(self, "cfg_copias", "1") if isinstance(getattr(self, "cfg_copias", None), str) else "1", multiline=False)

        self.cfg_auto_reconnect = CheckBox(active=getattr(self, "cfg_auto_reconnect", True) if isinstance(getattr(self, "cfg_auto_reconnect", True), bool) else False)
        self.cfg_panel_status = CheckBox(active=getattr(self, "cfg_panel_status", True) if isinstance(getattr(self, "cfg_panel_status", True), bool) else False)
        self.cfg_auto_print = CheckBox(active=getattr(self, "cfg_auto_print", False) if isinstance(getattr(self, "cfg_auto_print", False), bool) else False)

        def add(label, widget):
            inner.add_widget(Label(text=label, font_size="14sp", color=(1, 1, 1, 1), size_hint_y=None, height=dp(44)))
            widget.size_hint_y = None
            widget.height = dp(44)
            inner.add_widget(widget)

        add("IP PF6000", self.cfg_ip)
        add("Porta PF6000", self.cfg_port)
        add("REV 0060", self.cfg_rev0060)
        add("REV 0062", self.cfg_rev0062)
        add("Auto-reconectar", self.cfg_auto_reconnect)
        add("Usar OK/NOK painel", self.cfg_panel_status)
        add("Modelo", self.cfg_modelo)
        add("Torque mínimo", self.cfg_tmin)
        add("Torque máximo", self.cfg_tmax)
        add("Campo torque", self.cfg_tfield)
        add("Campo ângulo", self.cfg_afield)
        add("Divisor torque", self.cfg_tdiv)
        add("Divisor ângulo", self.cfg_adiv)
        add("Modo impressão", self.cfg_print_mode)
        add("Auto imprimir P8 OK", self.cfg_auto_print)
        add("IP Zebra", self.cfg_zebra_ip)
        add("Porta Zebra", self.cfg_zebra_port)
        add("Cópias", self.cfg_copias)

        scroll.add_widget(inner)
        content.add_widget(scroll)
        row = BoxLayout(size_hint_y=None, height=dp(48), spacing=dp(8))
        btn_save = Button(text="Salvar")
        btn_test = Button(text="Teste Zebra")
        btn_close = Button(text="Fechar")
        row.add_widget(btn_save)
        row.add_widget(btn_test)
        row.add_widget(btn_close)
        content.add_widget(row)
        popup = Popup(title="Configurações", content=content, size_hint=(0.92, 0.92))
        btn_save.bind(on_release=lambda *_: (self.save_config_from_popup(), popup.dismiss()))
        btn_test.bind(on_release=lambda *_: self.test_print_from_popup())
        btn_close.bind(on_release=lambda *_: popup.dismiss())
        popup.open()

    def save_config_from_popup(self):
        self.cfg_ip = self.cfg_ip.text
        self.cfg_port = self.cfg_port.text
        self.cfg_rev0060 = self.cfg_rev0060.text
        self.cfg_rev0062 = self.cfg_rev0062.text
        self.cfg_auto_reconnect = self.cfg_auto_reconnect.active
        self.cfg_panel_status = self.cfg_panel_status.active
        self.cfg_auto_print = self.cfg_auto_print.active
        self.cfg_modelo = self.cfg_modelo.text
        self.cfg_tmin = self.cfg_tmin.text
        self.cfg_tmax = self.cfg_tmax.text
        self.cfg_tdiv = self.cfg_tdiv.text
        self.cfg_adiv = self.cfg_adiv.text
        self.cfg_tfield = self.cfg_tfield.text
        self.cfg_afield = self.cfg_afield.text
        self.cfg_print_mode = self.cfg_print_mode.text
        self.cfg_zebra_ip = self.cfg_zebra_ip.text
        self.cfg_zebra_port = self.cfg_zebra_port.text
        self.cfg_copias = self.cfg_copias.text
        self.set_msg("Configurações salvas.")

    def test_print_from_popup(self):
        self.save_config_from_popup()
        old = self.posicoes
        demo = {p: PState(status="OK", ultimo_status="OK", torque=465 + i, angulo=90, data_hora=now_br()) for i, p in enumerate(POSICOES)}
        self.posicoes = demo
        try:
            self.print_current_label()
        finally:
            self.posicoes = old

    # -----------------------------------------------------
    # EVENTOS
    # -----------------------------------------------------
    def _poll_events(self, _dt):
        try:
            while True:
                kind, data = self.event_q.get_nowait()
                if kind == "log":
                    self.add_log(data)
                elif kind == "status":
                    self.update_comm_status(data)
                elif kind == "result_frame":
                    self.handle_result_frame(data)
        except queue.Empty:
            pass

    def add_log(self, line: str):
        self.logs.insert(0, line)
        self.logs = self.logs[:6]
        self.log_label.text = "\n".join(self.logs)

    def update_comm_status(self, st: Dict[str, Any]):
        if st.get("connected_tcp"):
            self.lbl_tcp.value_label.text = "ON"  # type: ignore
        elif st.get("desired_connected") and st.get("reconnecting"):
            self.lbl_tcp.value_label.text = "RECONECTANDO"  # type: ignore
        elif st.get("desired_connected"):
            self.lbl_tcp.value_label.text = "AGUARDANDO"  # type: ignore
        else:
            self.lbl_tcp.value_label.text = "OFF"  # type: ignore

        self.lbl_open.value_label.text = "OK" if st.get("connected_open") else "OFF"  # type: ignore
        self.lbl_mid.value_label.text = st.get("last_mid") or "-"  # type: ignore
        if st.get("last_error"):
            self.set_msg(f"Erro: {st.get('last_error')}")

    def set_msg(self, text: str):
        self.lbl_msg.text = text

    # -----------------------------------------------------
    # COMANDOS
    # -----------------------------------------------------
    def connect(self):
        ip = getattr(self, "cfg_ip", DEFAULT_IP)
        port = safe_int(getattr(self, "cfg_port", DEFAULT_PORT), DEFAULT_PORT)
        rev0060 = getattr(self, "cfg_rev0060", "001")
        rev0062 = getattr(self, "cfg_rev0062", "001")
        auto = bool(getattr(self, "cfg_auto_reconnect", True))
        self.client.start(ip, port, rev0060, rev0062, auto)

    def select_position(self, p: str):
        if p in POSICOES:
            self.current_idx = POSICOES.index(p)
            self.set_msg(f"Posição atual selecionada: {p}")
            self._refresh_all()

    def reset_cycle(self, clear_history: bool = False):
        self.posicoes = {p: PState() for p in POSICOES}
        self.current_idx = 0
        self.ciclo_em_reset = False
        if clear_history:
            self.historico.clear()
        self.set_msg("Ciclo resetado. Aguardando P1.")
        self._refresh_all()

    # -----------------------------------------------------
    # PROCESSAMENTO
    # -----------------------------------------------------
    def handle_result_frame(self, frame: str):
        try:
            parsed = parse_mid0061(
                frame=frame,
                torque_divisor=safe_float(getattr(self, "cfg_tdiv", "100"), 100),
                angle_divisor=safe_float(getattr(self, "cfg_adiv", "1"), 1),
                torque_field=getattr(self, "cfg_tfield", "AUTO"),
                angle_field=getattr(self, "cfg_afield", "AUTO"),
            )
        except Exception as e:
            self.add_log(f"[{now_br()}] ERRO parser: {e}")
            return

        if self.is_duplicate(parsed):
            ident = parsed.get("tightening_id") or parsed.get("frame_hash")
            self.add_log(f"[{now_br()}] DUPLICADO IGNORADO: {ident}")
            self.set_msg(f"Duplicado ignorado: {ident}")
            return

        self.register_dedup(parsed)
        self.process_result(parsed)

    def is_duplicate(self, parsed: Dict[str, Any]) -> bool:
        tid = str(parsed.get("tightening_id") or "").strip()
        frame_hash = str(parsed.get("frame_hash") or "").strip()
        now = time.time()
        if tid and hasattr(self, "processed_tightening_ids") and tid in self.processed_tightening_ids:
            return True
        for h, ts in list(self.processed_frame_hash_time.items()):
            if now - ts > DUPLICATE_FRAME_WINDOW_SEC:
                self.processed_frame_hash_time.pop(h, None)
        if frame_hash and frame_hash in self.processed_frame_hash_time:
            return True
        return False

    def register_dedup(self, parsed: Dict[str, Any]):
        tid = str(parsed.get("tightening_id") or "").strip()
        frame_hash = str(parsed.get("frame_hash") or "").strip()
        if tid:
            self.processed_tightening_ids.add(tid)
        if frame_hash:
            self.processed_frame_hash_time[frame_hash] = time.time()

    def evaluate_status(self, parsed: Dict[str, Any]) -> str:
        status_panel = str(parsed.get("status_geral") or "").upper().strip()
        if bool(getattr(self, "cfg_panel_status", True)) and status_panel in ("OK", "NOK"):
            return status_panel
        torque = parsed.get("torque")
        if torque is None:
            return "SEM LEITURA"
        tmin = safe_float(getattr(self, "cfg_tmin", "0"), 0)
        tmax = safe_float(getattr(self, "cfg_tmax", "9999"), 9999)
        return "OK" if tmin <= torque <= tmax else "NOK"

    def process_result(self, parsed: Dict[str, Any]):
        p = POSICOES[self.current_idx]
        state = self.posicoes[p]
        state.tentativas += 1
        status_final = self.evaluate_status(parsed)
        state.torque = parsed.get("torque")
        state.angulo = parsed.get("angulo")
        state.pset = parsed.get("pset") or ""
        state.tightening_id = parsed.get("tightening_id") or ""
        state.frame_hash = parsed.get("frame_hash") or ""
        state.data_hora = now_br()

        registro = {
            "data_hora": now_br(),
            "serie": self.in_serie.text,
            "op": self.in_op.text,
            "posicao": p,
            "status": status_final,
            "torque": state.torque,
            "angulo": state.angulo,
            "pset": state.pset,
            "revision": parsed.get("revision"),
            "tightening_id": state.tightening_id,
            "frame_hash": state.frame_hash,
            "parser_info": parsed.get("parser_info"),
            "frame": parsed.get("frame"),
        }
        self.historico.insert(0, registro)
        self.save_attempt_csv(registro)

        if status_final == "OK":
            state.status = "OK"
            state.ultimo_status = "OK"
            if self.current_idx < len(POSICOES) - 1:
                self.current_idx += 1
                self.set_msg(f"{p} aprovado. Avançou para {POSICOES[self.current_idx]}.")
            else:
                self.set_msg("P8 aprovado. Ciclo finalizado. Imprimindo etiqueta e resetando...")
                self.save_cycle_csv()
                if bool(getattr(self, "cfg_auto_print", False)):
                    self.print_current_label()
                self.ciclo_em_reset = True
                Clock.schedule_once(lambda *_: self.auto_reset_after_p8(), AUTO_RESET_AFTER_SEC)
        elif status_final == "NOK":
            state.status = "AGUARDANDO RETESTE"
            state.ultimo_status = "NOK"
            state.nok_count += 1
            self.set_msg(f"{p} deu NOK. Torque: {to_float_text(state.torque)}. Refaça o mesmo ponto.")
        else:
            state.status = "AGUARDANDO RETESTE"
            state.ultimo_status = status_final
            self.set_msg(f"{p}: resultado sem leitura completa. Refaça o mesmo ponto.")
        self._refresh_all()

    def auto_reset_after_p8(self):
        if self.ciclo_em_reset:
            self.reset_cycle(clear_history=False)
            self.set_msg("Novo ciclo iniciado automaticamente. Aguardando P1.")

    # -----------------------------------------------------
    # SALVAR / IMPRIMIR
    # -----------------------------------------------------
    def save_attempt_csv(self, registro: Dict[str, Any]):
        serie = (registro.get("serie") or "SEM_SERIE").replace("/", "_").replace("\\", "_").strip()
        arquivo = CSV_DIR / f"tentativas_{serie}_{datetime.now().strftime('%Y%m%d')}.csv"
        fieldnames = ["data_hora", "serie", "op", "posicao", "status", "torque", "angulo", "pset", "revision", "tightening_id", "frame_hash", "parser_info", "frame"]
        write_header = not arquivo.exists()
        with arquivo.open("a", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
            if write_header:
                w.writeheader()
            w.writerow({k: registro.get(k, "") for k in fieldnames})

    def save_cycle_csv(self):
        serie = (self.in_serie.text or "SEM_SERIE").replace("/", "_").replace("\\", "_").strip()
        arquivo = CSV_DIR / f"ciclo_final_{serie}_{now_file()}.csv"
        fieldnames = ["data_hora", "serie", "op", "posicao", "status", "torque", "angulo", "pset", "tentativas", "nok_count", "tightening_id"]
        with arquivo.open("w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
            w.writeheader()
            for p in POSICOES:
                stt = self.posicoes[p]
                w.writerow({
                    "data_hora": stt.data_hora,
                    "serie": self.in_serie.text,
                    "op": self.in_op.text,
                    "posicao": p,
                    "status": stt.ultimo_status or stt.status,
                    "torque": stt.torque,
                    "angulo": stt.angulo,
                    "pset": stt.pset,
                    "tentativas": stt.tentativas,
                    "nok_count": stt.nok_count,
                    "tightening_id": stt.tightening_id,
                })
        self.add_log(f"Ciclo salvo: {arquivo}")

    def print_current_label(self):
        copias = safe_int(getattr(self, "cfg_copias", "1"), 1)
        zpl = gerar_zpl_torque(self.in_serie.text, self.posicoes, copias=copias)
        safe_serie = (self.in_serie.text or "SEM_SERIE").replace("/", "_").replace("\\", "_").strip()
        zpl_path = CSV_DIR / f"etiqueta_torque_{safe_serie}_{now_file()}.zpl"
        zpl_path.write_text(zpl, encoding="utf-8")

        mode = getattr(self, "cfg_print_mode", "SALVAR")
        try:
            if mode == "USB":
                msg = imprimir_zebra_usb_android(zpl)
                self.set_msg(msg)
            elif mode == "IP":
                ip = getattr(self, "cfg_zebra_ip", "192.168.0.50")
                port = safe_int(getattr(self, "cfg_zebra_port", "9100"), 9100)
                imprimir_zebra_ip(ip, port, zpl)
                self.set_msg(f"Etiqueta enviada para Zebra IP {ip}:{port}")
            else:
                self.set_msg(f"ZPL salvo: {zpl_path.name}")
        except Exception as e:
            self.set_msg(f"Falha na impressão. ZPL salvo. Erro: {e}")
            self.add_log(f"Erro impressão: {e}")

    # -----------------------------------------------------
    # REFRESH
    # -----------------------------------------------------
    def calc_status_geral(self) -> str:
        if all(self.posicoes[p].status == "OK" for p in POSICOES):
            return "OK"
        if any(self.posicoes[p].status == "AGUARDANDO RETESTE" for p in POSICOES):
            return "RETESTE"
        return "PENDENTE"

    def _refresh_all(self):
        for idx, p in enumerate(POSICOES):
            self.cards[p].update_data(self.posicoes[p], current=(idx == self.current_idx))
        self.lbl_pos.value_label.text = POSICOES[self.current_idx]  # type: ignore
        self.lbl_geral.value_label.text = self.calc_status_geral()  # type: ignore


if __name__ == "__main__":
    try:
        TorquePF6000App().run()
    except Exception as e:
        salvar_crash_log(e)
        raise
    global BASE_DIR, LOG_DIR, CSV_DIR

    candidatos = []

    try:
        if app is not None and getattr(app, "user_data_dir", None):
            candidatos.append(Path(app.user_data_dir))
    except Exception:
        pass

    for env_name in ("ANDROID_PRIVATE", "ANDROID_ARGUMENT"):
        try:
            p = os.environ.get(env_name)
            if p:
                candidatos.append(Path(p))
        except Exception:
            pass

    candidatos.append(Path.cwd())
    candidatos.append(Path("."))

    for base in candidatos:
        try:
            base.mkdir(parents=True, exist_ok=True)
            log_dir = base / "logs_torque_pf6000"
            csv_dir = base / "registros_torque"
            log_dir.mkdir(parents=True, exist_ok=True)
            csv_dir.mkdir(parents=True, exist_ok=True)
            BASE_DIR = base
            LOG_DIR = log_dir
            CSV_DIR = csv_dir
            return True
        except Exception:
            continue

    BASE_DIR = Path(".")
    LOG_DIR = Path(".")
    CSV_DIR = Path(".")
    return False


def salvar_crash_log(exc: BaseException):
    try:
        import traceback
        inicializar_pastas(None)
        crash_file = BASE_DIR / "crash_torque_pf6000.txt"
        crash_file.write_text(traceback.format_exc(), encoding="utf-8")
    except Exception:
        pass


# =========================================================
# FUNCOES GERAIS
# =========================================================
def now_br() -> str:
    return datetime.now().strftime("%d/%m/%Y %H:%M:%S")


def now_file() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def to_float_text(v: Optional[float]) -> str:
    if v is None:
        return ""
    try:
        return f"{float(v):.2f}".replace(".", ",")
    except Exception:
        return ""


def only_ascii(texto: Any) -> str:
    return str(texto or "").encode("ascii", errors="ignore").decode("ascii", errors="ignore")


def status_01(valor: Any) -> str:
    v = str(valor or "").strip().upper()
    if v in ("1", "01", "OK"):
        return "OK"
    if v in ("0", "00", "NOK"):
        return "NOK"
    return v


def scaled_number(raw: str, divisor: float) -> Optional[float]:
    if raw is None:
        return None
    s = str(raw).strip().replace(" ", "")
    if not s:
        return None
    if not re.fullmatch(r"-?\d+", s):
        return None
    try:
        return int(s) / float(divisor)
    except Exception:
        return None


def safe_float(text: Any, default: float) -> float:
    try:
        return float(str(text).replace(",", "."))
    except Exception:
        return default


def safe_int(text: Any, default: int) -> int:
    try:
        return int(float(str(text).replace(",", ".")))
    except Exception:
        return default


# =========================================================
# OPEN PROTOCOL BASE
# =========================================================
def montar_mid(mid: str, revision: str = "001", data: str = "") -> bytes:
    mid = str(mid).zfill(4)
    revision = str(revision).zfill(3)
    header_sem_tamanho = f"{mid}{revision}"
    corpo_sem_tamanho = header_sem_tamanho.ljust(16) + data
    tamanho = len(corpo_sem_tamanho) + 4
    return f"{tamanho:04d}{corpo_sem_tamanho}\x00".encode("ascii", errors="ignore")


def extrair_mid(frame: str) -> str:
    if len(frame) >= 8 and frame[:4].isdigit():
        return frame[4:8]
    return ""


def extrair_rev(frame: str) -> str:
    if len(frame) >= 11 and frame[8:11].isdigit():
        return frame[8:11]
    return "001"


def split_frames(buffer: bytes) -> Tuple[List[bytes], bytes]:
    partes = buffer.split(b"\x00")
    return partes[:-1], partes[-1]


# =========================================================
# PARSER MID 0061
# =========================================================
# Rev.001:
#   campo 15 = torque real
#   campo 19 = angulo real
#   campo 23 = tightening_id
# Rev.002+:
#   campo 24 = torque real
#   campo 28 = angulo real
#   campo 41 = tightening_id

SPEC_REV1: List[Tuple[str, int, str]] = [
    ("01", 4, "cell_id"),
    ("02", 2, "channel_id"),
    ("03", 25, "controller_name"),
    ("04", 25, "vin"),
    ("05", 2, "job_id"),
    ("06", 3, "pset"),
    ("07", 4, "batch_size"),
    ("08", 4, "batch_counter"),
    ("09", 1, "tightening_status"),
    ("10", 1, "torque_status"),
    ("11", 1, "angle_status"),
    ("12", 6, "torque_min"),
    ("13", 6, "torque_max"),
    ("14", 6, "torque_target"),
    ("15", 6, "torque"),
    ("16", 5, "angle_min"),
    ("17", 5, "angle_max"),
    ("18", 5, "angle_target"),
    ("19", 5, "angle"),
    ("20", 19, "timestamp"),
    ("21", 19, "pset_last_change"),
    ("22", 1, "batch_status"),
    ("23", 10, "tightening_id"),
]

SPEC_REV2_BASE: List[Tuple[str, int, str]] = [
    ("01", 4, "cell_id"),
    ("02", 2, "channel_id"),
    ("03", 25, "controller_name"),
    ("04", 25, "vin"),
    ("05", 4, "job_id"),
    ("06", 3, "pset"),
    ("07", 2, "strategy"),
    ("08", 5, "strategy_options"),
    ("09", 4, "batch_size"),
    ("10", 4, "batch_counter"),
    ("11", 1, "tightening_status"),
    ("12", 1, "batch_status"),
    ("13", 1, "torque_status"),
    ("14", 1, "angle_status"),
    ("15", 1, "rundown_angle_status"),
    ("16", 1, "current_monitoring_status"),
    ("17", 1, "selftap_status"),
    ("18", 1, "prevail_torque_monitoring_status"),
    ("19", 1, "prevail_torque_comp_status"),
    ("20", 10, "tightening_error_status"),
    ("21", 6, "torque_min"),
    ("22", 6, "torque_max"),
    ("23", 6, "torque_target"),
    ("24", 6, "torque"),
    ("25", 5, "angle_min"),
    ("26", 5, "angle_max"),
    ("27", 5, "angle_target"),
    ("28", 5, "angle"),
    ("29", 5, "rundown_angle_min"),
    ("30", 5, "rundown_angle_max"),
    ("31", 5, "rundown_angle"),
    ("32", 3, "current_monitoring_min"),
    ("33", 3, "current_monitoring_max"),
    ("34", 3, "current_monitoring_value"),
    ("35", 6, "selftap_min"),
    ("36", 6, "selftap_max"),
    ("37", 6, "selftap_torque"),
    ("38", 6, "pvt_min"),
    ("39", 6, "pvt_max"),
    ("40", 6, "pvt_torque"),
    ("41", 10, "tightening_id"),
    ("42", 5, "job_sequence_number"),
    ("43", 5, "sync_tightening_id"),
    ("44", 14, "tool_serial"),
    ("45", 19, "timestamp"),
    ("46", 19, "pset_last_change"),
]


def parse_fields_by_spec(frame: str, spec: List[Tuple[str, int, str]], start_pos: int = 20) -> Tuple[Dict[str, str], Dict[str, str], List[str]]:
    fields_by_id: Dict[str, str] = {}
    fields_by_name: Dict[str, str] = {}
    warnings: List[str] = []
    pos = start_pos
    for fid, width, name in spec:
        if pos >= len(frame):
            break
        if frame[pos:pos + 2] == fid:
            id_pos = pos
        else:
            found = frame.find(fid, pos, min(len(frame), pos + 12))
            if found >= 0:
                warnings.append(f"ressync_{fid}:{pos}->{found}")
                id_pos = found
            else:
                warnings.append(f"missing_{fid}_at_{pos}")
                continue
        value_start = id_pos + 2
        value_end = value_start + width
        value = frame[value_start:value_end]
        fields_by_id[fid] = value
        fields_by_name[name] = value
        pos = value_end
    return fields_by_id, fields_by_name, warnings


def parse_mid0061(frame: str, torque_divisor: float = 100.0, angle_divisor: float = 1.0,
                  torque_field: str = "AUTO", angle_field: str = "AUTO") -> Dict[str, Any]:
    revision = extrair_rev(frame)
    if revision == "001":
        spec = SPEC_REV1
        auto_torque_field = "15"
        auto_angle_field = "19"
        status_field = "09"
        torque_status_field = "10"
        angle_status_field = "11"
        tightening_id_field = "23"
    else:
        spec = SPEC_REV2_BASE
        auto_torque_field = "24"
        auto_angle_field = "28"
        status_field = "11"
        torque_status_field = "13"
        angle_status_field = "14"
        tightening_id_field = "41"

    fields_by_id, fields_by_name, warnings = parse_fields_by_spec(frame, spec)
    torque_field_final = auto_torque_field if str(torque_field).upper() == "AUTO" else str(torque_field).zfill(2)
    angle_field_final = auto_angle_field if str(angle_field).upper() == "AUTO" else str(angle_field).zfill(2)
    raw_torque = (fields_by_id.get(torque_field_final, "") or "").strip()
    raw_angle = (fields_by_id.get(angle_field_final, "") or "").strip()
    torque = scaled_number(raw_torque, torque_divisor)
    angle = scaled_number(raw_angle, angle_divisor)
    tightening_id = (fields_by_id.get(tightening_id_field, "") or fields_by_name.get("tightening_id", "") or "").strip()
    pset = (fields_by_name.get("pset", "") or "").strip()
    pset = pset.lstrip("0") or pset
    frame_hash = hashlib.sha1(frame.encode("utf-8", errors="ignore")).hexdigest()[:16]

    fields_debug = dict(fields_by_id)
    fields_debug["_revision"] = revision
    fields_debug["_torque_field_usado"] = torque_field_final
    fields_debug["_angle_field_usado"] = angle_field_final
    fields_debug["_tightening_id"] = tightening_id
    fields_debug["_frame_hash"] = frame_hash

    return {
        "data_hora_pc": now_br(),
        "mid": "0061",
        "revision": revision,
        "status_geral": status_01(fields_by_id.get(status_field, "")),
        "status_torque": status_01(fields_by_id.get(torque_status_field, "")),
        "status_angulo": status_01(fields_by_id.get(angle_status_field, "")),
        "pset": pset,
        "tightening_id": tightening_id,
        "frame_hash": frame_hash,
        "torque": torque,
        "angulo": angle,
        "torque_raw": raw_torque,
        "angulo_raw": raw_angle,
        "parser_info": f"rev={revision};torque_field={torque_field_final};angle_field={angle_field_final};warnings={','.join(warnings)}",
        "fields": fields_debug,
        "frame": frame,
    }


# =========================================================
# IMPRESSAO ZEBRA
# =========================================================
def gerar_zpl_torque(serie: str, pontos: Dict[str, "PState"], copias: int = 1) -> str:
    copias = max(1, int(copias or 1))
    agora = datetime.now()
    data_hora = agora.strftime("%d/%m/%y / HORA: %H:%M:%S")
    serie = serie or "SEM SERIE"

    def ponto_txt(p: str) -> str:
        stt = pontos[p]
        status = stt.ultimo_status or stt.status
        torque = to_float_text(stt.torque)
        if not torque:
            torque = "--"
        return f"{p}: {torque}-{status}"

    # Layout baseado no ZPL enviado pelo usuario, ajustado para caber no ^LL399.
    return f"""
^XA
^CI28
^PW899
^LL399
^LH0,0
^CF0,18
^FO175,35^A0N,30,30^FB520,1,0,C,0^FDREGISTRO DE TORQUE^FS
^FO45,95^A0N,26,26^FD{{DATA: {data_hora}}}^FS
^FO45,135^A0N,26,26^FD{{SERIE: {only_ascii(serie)}}}^FS
^FO10,178^GB870,2,2^FS
^FO45,210^A0N,32,32^FD{{{only_ascii(ponto_txt('P1'))}}}^FS
^FO45,255^A0N,32,32^FD{{{only_ascii(ponto_txt('P2'))}}}^FS
^FO45,300^A0N,32,32^FD{{{only_ascii(ponto_txt('P3'))}}}^FS
^FO45,345^A0N,32,32^FD{{{only_ascii(ponto_txt('P4'))}}}^FS
^FO430,210^A0N,32,32^FD{{{only_ascii(ponto_txt('P5'))}}}^FS
^FO430,255^A0N,32,32^FD{{{only_ascii(ponto_txt('P6'))}}}^FS
^FO430,300^A0N,32,32^FD{{{only_ascii(ponto_txt('P7'))}}}^FS
^FO430,345^A0N,32,32^FD{{{only_ascii(ponto_txt('P8'))}}}^FS
^PQ{copias},0,1,N
^XZ
""".strip()


def imprimir_zebra_ip(ip: str, porta: int, zpl: str, timeout: float = 5.0):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(timeout)
        s.connect((ip, int(porta)))
        s.sendall(zpl.encode("utf-8", errors="ignore"))


def imprimir_zebra_usb_android(zpl: str) -> str:
    """Envia ZPL para primeira impressora USB encontrada no Android.

    Observacao: na primeira tentativa o Android pode pedir permissao USB.
    Depois de permitir, tente imprimir novamente.
    """
    if platform != "android":
        raise RuntimeError("USB Android disponível somente no APK instalado no tablet.")

    from jnius import autoclass, cast  # type: ignore

    PythonActivity = autoclass("org.kivy.android.PythonActivity")
    Context = autoclass("android.content.Context")
    UsbConstants = autoclass("android.hardware.usb.UsbConstants")
    PendingIntent = autoclass("android.app.PendingIntent")
    Intent = autoclass("android.content.Intent")
    Build = autoclass("android.os.Build")

    activity = PythonActivity.mActivity
    usb_manager = cast("android.hardware.usb.UsbManager", activity.getSystemService(Context.USB_SERVICE))
    device_list = usb_manager.getDeviceList()

    if device_list.isEmpty():
        raise RuntimeError("Nenhum dispositivo USB encontrado. Conecte a Zebra no OTG.")

    iterator = device_list.values().iterator()
    chosen = None
    while iterator.hasNext():
        dev = iterator.next()
        # Zebra costuma ser vendor 0x0A5F, mas deixamos aberto para testar qualquer USB printer.
        chosen = dev
        if int(dev.getVendorId()) == 0x0A5F:
            chosen = dev
            break

    if chosen is None:
        raise RuntimeError("Nenhuma impressora USB encontrada.")

    if not usb_manager.hasPermission(chosen):
        flags = 0
        try:
            if int(Build.VERSION.SDK_INT) >= 23:
                flags = PendingIntent.FLAG_IMMUTABLE
        except Exception:
            flags = 0
        permission_intent = PendingIntent.getBroadcast(activity, 0, Intent("br.com.ibero.USB_PERMISSION"), flags)
        usb_manager.requestPermission(chosen, permission_intent)
        raise RuntimeError("Permissão USB solicitada. Autorize no Android e clique em imprimir novamente.")

    connection = usb_manager.openDevice(chosen)
    if connection is None:
        raise RuntimeError("Não foi possível abrir conexão USB com a Zebra.")

    try:
        endpoint_out = None
        interface = None
        for i in range(chosen.getInterfaceCount()):
            itf = chosen.getInterface(i)
            for e in range(itf.getEndpointCount()):
                ep = itf.getEndpoint(e)
                if ep.getDirection() == UsbConstants.USB_DIR_OUT and ep.getType() == UsbConstants.USB_ENDPOINT_XFER_BULK:
                    interface = itf
                    endpoint_out = ep
                    break
            if endpoint_out is not None:
                break

        if interface is None or endpoint_out is None:
            raise RuntimeError("Endpoint USB OUT não encontrado na impressora.")

        if not connection.claimInterface(interface, True):
            raise RuntimeError("Não foi possível reservar interface USB da impressora.")

        data = zpl.encode("utf-8", errors="ignore")
        offset = 0
        chunk_size = 4096
        while offset < len(data):
            chunk = data[offset:offset + chunk_size]
            sent = connection.bulkTransfer(endpoint_out, chunk, len(chunk), 5000)
            if sent is None or int(sent) < 0:
                raise RuntimeError("Falha no bulkTransfer USB para a Zebra.")
            offset += len(chunk)

        try:
            connection.releaseInterface(interface)
        except Exception:
            pass

    finally:
        try:
            connection.close()
        except Exception:
            pass

    return f"ZPL enviado via USB para vendor={chosen.getVendorId()} product={chosen.getProductId()}"


# =========================================================
# ESTADO
# =========================================================
@dataclass
class PState:
    status: str = "AGUARDANDO"
    ultimo_status: str = ""
    torque: Optional[float] = None
    angulo: Optional[float] = None
    pset: str = ""
    data_hora: str = ""
    tentativas: int = 0
    nok_count: int = 0
    tightening_id: str = ""
    frame_hash: str = ""


# =========================================================
# CLIENTE OPEN PROTOCOL THREAD
# =========================================================
class OpenProtocolClient:
    def __init__(self, event_q: queue.Queue):
        self.event_q = event_q
        self.ip = DEFAULT_IP
        self.port = DEFAULT_PORT
        self.rev_0060 = "001"
        self.rev_0062 = "001"
        self.auto_reconnect = True
        self.sock: Optional[socket.socket] = None
        self.thread: Optional[threading.Thread] = None
        self.lock = threading.Lock()
        self.desired_connected = False
        self.connected_tcp = False
        self.connected_open = False
        self.reconnecting = False
        self.last_mid = "-"
        self.last_error = ""
        self.last_frame = ""

    def emit(self, kind: str, data: Any = None):
        self.event_q.put((kind, data))

    def log(self, msg: str):
        line = f"[{now_br()}] {msg}"
        self.emit("log", line)
        try:
            log_file = LOG_DIR / f"pf6000_{datetime.now().strftime('%Y%m%d')}.log"
            with log_file.open("a", encoding="utf-8") as f:
                f.write(line + "\n")
        except Exception:
            pass

    def status_snapshot(self) -> Dict[str, Any]:
        with self.lock:
            return {
                "connected_tcp": self.connected_tcp,
                "connected_open": self.connected_open,
                "reconnecting": self.reconnecting,
                "desired_connected": self.desired_connected,
                "last_mid": self.last_mid,
                "last_error": self.last_error,
            }

    def start(self, ip: str, port: int, rev_0060: str, rev_0062: str, auto_reconnect: bool):
        with self.lock:
            self.ip = ip.strip()
            self.port = int(port)
            self.rev_0060 = str(rev_0060).zfill(3)
            self.rev_0062 = str(rev_0062).zfill(3)
            self.auto_reconnect = bool(auto_reconnect)
            self.desired_connected = True
            self.last_error = ""

        if self.thread and self.thread.is_alive():
            self.log("Cliente já está em execução.")
            return

        self.thread = threading.Thread(target=self._manager_loop, daemon=True)
        self.thread.start()
        self.log("Gerenciador de conexão iniciado.")

    def stop(self):
        with self.lock:
            self.desired_connected = False
        self._close_socket()
        self.log("Desconectado pelo usuário.")

    def _close_socket(self):
        with self.lock:
            sock = self.sock
            self.sock = None
            self.connected_tcp = False
            self.connected_open = False
        if sock:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except Exception:
                pass
            try:
                sock.close()
            except Exception:
                pass
        self.emit("status", self.status_snapshot())

    def _send_mid(self, mid: str, rev: str = "001", data: str = "") -> bool:
        with self.lock:
            sock = self.sock
        if not sock:
            self.log(f"Não enviou MID {mid}: socket não conectado.")
            return False
        try:
            packet = montar_mid(mid, rev, data)
            sock.sendall(packet)
            self.log(f"ENVIADO MID {mid} REV {rev}")
            return True
        except Exception as e:
            with self.lock:
                self.last_error = str(e)
            self.log(f"ERRO enviando MID {mid}: {e}")
            self.emit("status", self.status_snapshot())
            return False

    def subscribe_result(self):
        with self.lock:
            rev = self.rev_0060
        self.log(f"Assinando resultado de aperto MID 0060 REV {rev}")
        self._send_mid("0060", rev)

    def _connect_once(self) -> bool:
        with self.lock:
            ip = self.ip
            port = self.port
            self.reconnecting = True
            self.last_error = ""
        self.emit("status", self.status_snapshot())
        try:
            self.log(f"Conectando no PF6000 {ip}:{port}...")
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(4)
            sock.connect((ip, int(port)))
            sock.settimeout(1)
            with self.lock:
                self.sock = sock
                self.connected_tcp = True
                self.connected_open = False
                self.reconnecting = False
            self.log("TCP CONECTADO.")
            self.emit("status", self.status_snapshot())
            self._send_mid("0001", "006")
            return True
        except Exception as e:
            with self.lock:
                self.connected_tcp = False
                self.connected_open = False
                self.reconnecting = False
                self.last_error = str(e)
            self.log(f"Falha ao conectar: {e}")
            self._close_socket()
            return False

    def _manager_loop(self):
        while True:
            with self.lock:
                desired = self.desired_connected
                auto = self.auto_reconnect
            if not desired:
                break
            ok = self._connect_once()
            if ok:
                self._reader_loop()
            self._close_socket()
            with self.lock:
                desired = self.desired_connected
                auto = self.auto_reconnect
            if not desired or not auto:
                break
            with self.lock:
                self.reconnecting = True
            self.emit("status", self.status_snapshot())
            self.log("Conexão caiu/foi encerrada. Tentando reconectar em 2 segundos...")
            time.sleep(2)
        with self.lock:
            self.connected_tcp = False
            self.connected_open = False
            self.reconnecting = False
        self.emit("status", self.status_snapshot())
        self.log("Gerenciador de conexão finalizado.")

    def _reader_loop(self):
        buffer = b""
        last_keepalive = time.time()
        while True:
            with self.lock:
                desired = self.desired_connected
                sock = self.sock
                rev_ack = self.rev_0062
            if not desired or not sock:
                break
            if time.time() - last_keepalive >= 10:
                self._send_mid("9999", "001")
                last_keepalive = time.time()
            try:
                data = sock.recv(4096)
                if not data:
                    self.log("Conexão encerrada pelo painel.")
                    break
                raw = buffer + data
                frames, buffer = split_frames(raw)
                for fb in frames:
                    frame = fb.decode("ascii", errors="ignore")
                    mid = extrair_mid(frame)
                    with self.lock:
                        self.last_mid = mid
                        self.last_frame = frame
                    self.emit("status", self.status_snapshot())
                    self.log(f"RECEBIDO MID {mid}")
                    if mid == "0002":
                        with self.lock:
                            self.connected_open = True
                        self.emit("status", self.status_snapshot())
                        self.log("Open Protocol iniciado.")
                        time.sleep(0.2)
                        self.subscribe_result()
                    elif mid == "0005":
                        self.log("Painel aceitou comando.")
                    elif mid == "0004":
                        self.log("Painel recusou algum MID. Teste outra REV do 0060.")
                    elif mid == "0061":
                        self.log("Resultado de aperto recebido.")
                        self._send_mid("0062", rev_ack)
                        self.emit("result_frame", frame)
                    elif mid == "9999":
                        self._send_mid("9999", "001")
            except socket.timeout:
                continue
            except OSError as e:
                self.log(f"Erro de rede: {e}")
                break
            except Exception as e:
                self.log(f"Erro inesperado na leitura: {e}")
                break


# =========================================================
# WIDGETS
# =========================================================
class PCard(BoxLayout):
    def __init__(self, pos: str, on_select, **kwargs):
        super().__init__(orientation="vertical", padding=dp(10), spacing=dp(5), **kwargs)
        self.pos = pos
        self.on_select = on_select
        self.size_hint_y = None
        self.height = dp(165)
        self.status = "AGUARDANDO"
        self._build()

    def _build(self):
        from kivy.graphics import Color, RoundedRectangle, Line
        self.bg_color = (0.06, 0.07, 0.08, 1)
        self.border_color = (0.20, 0.22, 0.25, 1)
        with self.canvas.before:
            self._color = Color(*self.bg_color)
            self._rect = RoundedRectangle(pos=self.pos, size=self.size, radius=[dp(12)])
            self._line_color = Color(*self.border_color)
            self._line = Line(rounded_rectangle=(self.x, self.y, self.width, self.height, dp(12)), width=1.3)
        self.bind(pos=self._update_canvas, size=self._update_canvas)

        self.lbl_pos = Label(text=self.pos, font_size="24sp", bold=True, halign="left", valign="middle",
                             size_hint_y=None, height=dp(32), color=(1, 1, 1, 1))
        self.lbl_pos.bind(size=lambda w, s: setattr(w, "text_size", s))
        self.add_widget(self.lbl_pos)

        self.lbl_status = Label(text="AGUARDANDO", font_size="14sp", bold=True, halign="left",
                                size_hint_y=None, height=dp(26), color=(0.75, 0.78, 0.82, 1))
        self.lbl_status.bind(size=lambda w, s: setattr(w, "text_size", s))
        self.add_widget(self.lbl_status)

        self.lbl_torque = Label(text="Torque:", font_size="13sp", halign="left", size_hint_y=None, height=dp(24), color=(1, 1, 1, 1))
        self.lbl_torque.bind(size=lambda w, s: setattr(w, "text_size", s))
        self.add_widget(self.lbl_torque)

        self.lbl_angulo = Label(text="Ângulo:", font_size="13sp", halign="left", size_hint_y=None, height=dp(24), color=(1, 1, 1, 1))
        self.lbl_angulo.bind(size=lambda w, s: setattr(w, "text_size", s))
        self.add_widget(self.lbl_angulo)

        self.lbl_info = Label(text="Tentativas: 0 | NOK: 0", font_size="11sp", halign="left", size_hint_y=None, height=dp(20), color=(0.80, 0.84, 0.90, 1))
        self.lbl_info.bind(size=lambda w, s: setattr(w, "text_size", s))
        self.add_widget(self.lbl_info)

        btn = Button(text=f"Selecionar {self.pos}", size_hint_y=None, height=dp(34), font_size="12sp")
        btn.bind(on_release=lambda *_: self.on_select(self.pos))
        self.add_widget(btn)

    def _update_canvas(self, *args):
        self._rect.pos = self.pos
        self._rect.size = self.size
        self._line.rounded_rectangle = (self.x, self.y, self.width, self.height, dp(12))

    def set_colors(self, bg, border):
        self.bg_color = bg
        self.border_color = border
        self._color.rgba = bg
        self._line_color.rgba = border

    def update_data(self, state: PState, current: bool):
        if current:
            bg = (0.03, 0.14, 0.25, 1)
            border = (0.00, 0.82, 1.0, 1)
            status_color = (0.00, 0.82, 1.0, 1)
        elif state.status == "OK":
            bg = (0.02, 0.18, 0.08, 1)
            border = (0.00, 0.78, 0.32, 1)
            status_color = (0.00, 0.95, 0.46, 1)
        elif state.status == "AGUARDANDO RETESTE":
            bg = (0.23, 0.18, 0.00, 1)
            border = (1.00, 0.84, 0.00, 1)
            status_color = (1.00, 0.84, 0.00, 1)
        else:
            bg = (0.06, 0.07, 0.08, 1)
            border = (0.20, 0.22, 0.25, 1)
            status_color = (0.75, 0.78, 0.82, 1)
        self.set_colors(bg, border)
        self.lbl_status.text = state.status
        self.lbl_status.color = status_color
        self.lbl_torque.text = f"Torque: {to_float_text(state.torque)}"
        self.lbl_angulo.text = f"Ângulo: {to_float_text(state.angulo)}"
        self.lbl_info.text = f"Tentativas: {state.tentativas} | NOK: {state.nok_count}"


# =========================================================
# APP
# =========================================================
class TorquePF6000App(App):
    title = "Torque Mola - PF6000"

    def build(self):
        # Inicializa armazenamento somente aqui para evitar crash na abertura do APK.
        inicializar_pastas(self)

        self.event_q: queue.Queue = queue.Queue()
        self.client = OpenProtocolClient(self.event_q)
        self.posicoes: Dict[str, PState] = {p: PState() for p in POSICOES}
        self.current_idx = 0
        self.historico: List[Dict[str, Any]] = []
        self.processed_tightening_ids: set = set()
        self.processed_frame_hash_time: Dict[str, float] = {}
        self.ciclo_em_reset = False

        self.root_box = BoxLayout(orientation="vertical", padding=dp(8), spacing=dp(6))
        self._build_top()
        self._build_cards()
        self._build_bottom_log()

        Clock.schedule_interval(self._poll_events, 0.1)
        self._refresh_all()
        return self.root_box

    def _build_top(self):
        header = BoxLayout(orientation="horizontal", size_hint_y=None, height=dp(58), spacing=dp(8))
        title_box = BoxLayout(orientation="vertical")
        title_box.add_widget(Label(text="Controle de Torque Mola - PF6000", font_size="22sp", bold=True, halign="left", color=(1, 1, 1, 1)))
        self.lbl_msg = Label(text="Aguardando conexão.", font_size="13sp", halign="left", color=(1, 0.90, 0.30, 1))
        title_box.add_widget(self.lbl_msg)
        header.add_widget(title_box)
        btn_cfg = Button(text="CONFIG", size_hint_x=None, width=dp(130), font_size="14sp")
        btn_cfg.bind(on_release=lambda *_: self.open_config_popup())
        header.add_widget(btn_cfg)
        self.root_box.add_widget(header)

        data_row = BoxLayout(orientation="horizontal", size_hint_y=None, height=dp(54), spacing=dp(8))
        self.in_serie = TextInput(hint_text="Nº Série / Rastreio", multiline=False, font_size="16sp")
        self.in_op = TextInput(hint_text="OP", multiline=False, font_size="16sp")
        data_row.add_widget(self.in_serie)
        data_row.add_widget(self.in_op)
        self.root_box.add_widget(data_row)

        status_row = GridLayout(cols=5, size_hint_y=None, height=dp(58), spacing=dp(6))
        self.lbl_tcp = self._metric("TCP", "OFF")
        self.lbl_open = self._metric("Open", "OFF")
        self.lbl_mid = self._metric("MID", "-")
        self.lbl_pos = self._metric("Atual", "P1")
        self.lbl_geral = self._metric("Status", "PENDENTE")
        for w in [self.lbl_tcp, self.lbl_open, self.lbl_mid, self.lbl_pos, self.lbl_geral]:
            status_row.add_widget(w)
        self.root_box.add_widget(status_row)

    def _metric(self, title: str, value: str) -> BoxLayout:
        b = BoxLayout(orientation="vertical", padding=dp(6))
        b.add_widget(Label(text=title, font_size="11sp", color=(0.75, 0.80, 0.88, 1), size_hint_y=0.35))
        lab = Label(text=value, font_size="18sp", bold=True, color=(1, 1, 1, 1), size_hint_y=0.65)
        b.value_label = lab  # type: ignore
        b.add_widget(lab)
        return b

    def _build_cards(self):
        self.cards: Dict[str, PCard] = {}
        grid = GridLayout(cols=4, spacing=dp(8), size_hint_y=None)
        grid.bind(minimum_height=grid.setter("height"))
        for p in POSICOES:
            card = PCard(p, on_select=self.select_position)
            self.cards[p] = card
            grid.add_widget(card)
        # 2 linhas x 165 + espaçamento
        grid.height = dp(345)
        self.root_box.add_widget(grid)

    def _build_bottom_log(self):
        row = BoxLayout(orientation="horizontal", size_hint_y=None, height=dp(52), spacing=dp(8))
        self.btn_connect = Button(text="Conectar", font_size="14sp")
        self.btn_connect.bind(on_release=lambda *_: self.connect())
        row.add_widget(self.btn_connect)
        btn_stop = Button(text="Desconectar", font_size="14sp")
        btn_stop.bind(on_release=lambda *_: self.client.stop())
        row.add_widget(btn_stop)
        btn_reset = Button(text="Reset ciclo", font_size="14sp")
        btn_reset.bind(on_release=lambda *_: self.reset_cycle(clear_history=False))
        row.add_widget(btn_reset)
        btn_print = Button(text="Imprimir etiqueta", font_size="14sp")
        btn_print.bind(on_release=lambda *_: self.print_current_label())
        row.add_widget(btn_print)
        self.root_box.add_widget(row)

        self.log_label = Label(text="", font_size="11sp", color=(0.75, 0.80, 0.88, 1), halign="left", valign="top")
        self.log_label.bind(size=lambda w, s: setattr(w, "text_size", s))
        self.root_box.add_widget(self.log_label)
        self.logs: List[str] = []

    def open_config_popup(self):
        content = BoxLayout(orientation="vertical", padding=dp(10), spacing=dp(8))
        scroll = ScrollView()
        inner = GridLayout(cols=2, spacing=dp(8), size_hint_y=None)
        inner.bind(minimum_height=inner.setter("height"))

        self.cfg_ip = TextInput(text=getattr(self, "cfg_ip", DEFAULT_IP) if isinstance(getattr(self, "cfg_ip", None), str) else DEFAULT_IP, multiline=False)
        self.cfg_port = TextInput(text=getattr(self, "cfg_port", str(DEFAULT_PORT)) if isinstance(getattr(self, "cfg_port", None), str) else str(DEFAULT_PORT), multiline=False)
        self.cfg_rev0060 = Spinner(text=getattr(self, "cfg_rev0060", "001") if isinstance(getattr(self, "cfg_rev0060", None), str) else "001", values=["001", "002", "003", "004", "005", "006", "007", "008"])
        self.cfg_rev0062 = Spinner(text=getattr(self, "cfg_rev0062", "001") if isinstance(getattr(self, "cfg_rev0062", None), str) else "001", values=["001", "002", "003", "004", "005", "006", "007", "008"])
        self.cfg_modelo = TextInput(text=getattr(self, "cfg_modelo", "MOLA") if isinstance(getattr(self, "cfg_modelo", None), str) else "MOLA", multiline=False)
        self.cfg_tmin = TextInput(text=getattr(self, "cfg_tmin", "0") if isinstance(getattr(self, "cfg_tmin", None), str) else "0", multiline=False)
        self.cfg_tmax = TextInput(text=getattr(self, "cfg_tmax", "9999") if isinstance(getattr(self, "cfg_tmax", None), str) else "9999", multiline=False)
        self.cfg_tdiv = Spinner(text=getattr(self, "cfg_tdiv", "100") if isinstance(getattr(self, "cfg_tdiv", None), str) else "100", values=["1", "10", "100", "1000"])
        self.cfg_adiv = Spinner(text=getattr(self, "cfg_adiv", "1") if isinstance(getattr(self, "cfg_adiv", None), str) else "1", values=["1", "10", "100"])
        self.cfg_tfield = Spinner(text=getattr(self, "cfg_tfield", "AUTO") if isinstance(getattr(self, "cfg_tfield", None), str) else "AUTO", values=["AUTO", "15", "24", "12", "13", "14", "21", "22", "23"])
        self.cfg_afield = Spinner(text=getattr(self, "cfg_afield", "AUTO") if isinstance(getattr(self, "cfg_afield", None), str) else "AUTO", values=["AUTO", "19", "28", "16", "17", "18", "25", "26", "27"])
        self.cfg_print_mode = Spinner(text=getattr(self, "cfg_print_mode", "SALVAR") if isinstance(getattr(self, "cfg_print_mode", None), str) else "SALVAR", values=["SALVAR", "USB", "IP"])
        self.cfg_zebra_ip = TextInput(text=getattr(self, "cfg_zebra_ip", "192.168.0.50") if isinstance(getattr(self, "cfg_zebra_ip", None), str) else "192.168.0.50", multiline=False)
        self.cfg_zebra_port = TextInput(text=getattr(self, "cfg_zebra_port", "9100") if isinstance(getattr(self, "cfg_zebra_port", None), str) else "9100", multiline=False)
        self.cfg_copias = TextInput(text=getattr(self, "cfg_copias", "1") if isinstance(getattr(self, "cfg_copias", None), str) else "1", multiline=False)

        self.cfg_auto_reconnect = CheckBox(active=getattr(self, "cfg_auto_reconnect", True) if isinstance(getattr(self, "cfg_auto_reconnect", True), bool) else True)
        self.cfg_panel_status = CheckBox(active=getattr(self, "cfg_panel_status", True) if isinstance(getattr(self, "cfg_panel_status", True), bool) else True)
        self.cfg_auto_print = CheckBox(active=getattr(self, "cfg_auto_print", False) if isinstance(getattr(self, "cfg_auto_print", False), bool) else False)

        def add(label, widget):
            inner.add_widget(Label(text=label, font_size="14sp", color=(1, 1, 1, 1), size_hint_y=None, height=dp(44)))
            widget.size_hint_y = None
            widget.height = dp(44)
            inner.add_widget(widget)

        add("IP PF6000", self.cfg_ip)
        add("Porta PF6000", self.cfg_port)
        add("REV 0060", self.cfg_rev0060)
        add("REV 0062", self.cfg_rev0062)
        add("Auto-reconectar", self.cfg_auto_reconnect)
        add("Usar OK/NOK painel", self.cfg_panel_status)
        add("Modelo", self.cfg_modelo)
        add("Torque mínimo", self.cfg_tmin)
        add("Torque máximo", self.cfg_tmax)
        add("Campo torque", self.cfg_tfield)
        add("Campo ângulo", self.cfg_afield)
        add("Divisor torque", self.cfg_tdiv)
        add("Divisor ângulo", self.cfg_adiv)
        add("Modo impressão", self.cfg_print_mode)
        add("Auto imprimir P8 OK", self.cfg_auto_print)
        add("IP Zebra", self.cfg_zebra_ip)
        add("Porta Zebra", self.cfg_zebra_port)
        add("Cópias", self.cfg_copias)

        scroll.add_widget(inner)
        content.add_widget(scroll)
        row = BoxLayout(size_hint_y=None, height=dp(48), spacing=dp(8))
        btn_save = Button(text="Salvar")
        btn_test = Button(text="Teste Zebra")
        btn_close = Button(text="Fechar")
        row.add_widget(btn_save)
        row.add_widget(btn_test)
        row.add_widget(btn_close)
        content.add_widget(row)
        popup = Popup(title="Configurações", content=content, size_hint=(0.92, 0.92))
        btn_save.bind(on_release=lambda *_: (self.save_config_from_popup(), popup.dismiss()))
        btn_test.bind(on_release=lambda *_: self.test_print_from_popup())
        btn_close.bind(on_release=lambda *_: popup.dismiss())
        popup.open()

    def save_config_from_popup(self):
        self.cfg_ip = self.cfg_ip.text
        self.cfg_port = self.cfg_port.text
        self.cfg_rev0060 = self.cfg_rev0060.text
        self.cfg_rev0062 = self.cfg_rev0062.text
        self.cfg_auto_reconnect = self.cfg_auto_reconnect.active
        self.cfg_panel_status = self.cfg_panel_status.active
        self.cfg_auto_print = self.cfg_auto_print.active
        self.cfg_modelo = self.cfg_modelo.text
        self.cfg_tmin = self.cfg_tmin.text
        self.cfg_tmax = self.cfg_tmax.text
        self.cfg_tdiv = self.cfg_tdiv.text
        self.cfg_adiv = self.cfg_adiv.text
        self.cfg_tfield = self.cfg_tfield.text
        self.cfg_afield = self.cfg_afield.text
        self.cfg_print_mode = self.cfg_print_mode.text
        self.cfg_zebra_ip = self.cfg_zebra_ip.text
        self.cfg_zebra_port = self.cfg_zebra_port.text
        self.cfg_copias = self.cfg_copias.text
        self.set_msg("Configurações salvas.")

    def test_print_from_popup(self):
        self.save_config_from_popup()
        old = self.posicoes
        demo = {p: PState(status="OK", ultimo_status="OK", torque=465 + i, angulo=90, data_hora=now_br()) for i, p in enumerate(POSICOES)}
        self.posicoes = demo
        try:
            self.print_current_label()
        finally:
            self.posicoes = old

    # -----------------------------------------------------
    # EVENTOS
    # -----------------------------------------------------
    def _poll_events(self, _dt):
        try:
            while True:
                kind, data = self.event_q.get_nowait()
                if kind == "log":
                    self.add_log(data)
                elif kind == "status":
                    self.update_comm_status(data)
                elif kind == "result_frame":
                    self.handle_result_frame(data)
        except queue.Empty:
            pass

    def add_log(self, line: str):
        self.logs.insert(0, line)
        self.logs = self.logs[:6]
        self.log_label.text = "\n".join(self.logs)

    def update_comm_status(self, st: Dict[str, Any]):
        if st.get("connected_tcp"):
            self.lbl_tcp.value_label.text = "ON"  # type: ignore
        elif st.get("desired_connected") and st.get("reconnecting"):
            self.lbl_tcp.value_label.text = "RECONECTANDO"  # type: ignore
        elif st.get("desired_connected"):
            self.lbl_tcp.value_label.text = "AGUARDANDO"  # type: ignore
        else:
            self.lbl_tcp.value_label.text = "OFF"  # type: ignore

        self.lbl_open.value_label.text = "OK" if st.get("connected_open") else "OFF"  # type: ignore
        self.lbl_mid.value_label.text = st.get("last_mid") or "-"  # type: ignore
        if st.get("last_error"):
            self.set_msg(f"Erro: {st.get('last_error')}")

    def set_msg(self, text: str):
        self.lbl_msg.text = text

    # -----------------------------------------------------
    # COMANDOS
    # -----------------------------------------------------
    def connect(self):
        ip = getattr(self, "cfg_ip", DEFAULT_IP)
        port = safe_int(getattr(self, "cfg_port", DEFAULT_PORT), DEFAULT_PORT)
        rev0060 = getattr(self, "cfg_rev0060", "001")
        rev0062 = getattr(self, "cfg_rev0062", "001")
        auto = bool(getattr(self, "cfg_auto_reconnect", True))
        self.client.start(ip, port, rev0060, rev0062, auto)

    def select_position(self, p: str):
        if p in POSICOES:
            self.current_idx = POSICOES.index(p)
            self.set_msg(f"Posição atual selecionada: {p}")
            self._refresh_all()

    def reset_cycle(self, clear_history: bool = False):
        self.posicoes = {p: PState() for p in POSICOES}
        self.current_idx = 0
        self.ciclo_em_reset = False
        if clear_history:
            self.historico.clear()
        self.set_msg("Ciclo resetado. Aguardando P1.")
        self._refresh_all()

    # -----------------------------------------------------
    # PROCESSAMENTO
    # -----------------------------------------------------
    def handle_result_frame(self, frame: str):
        try:
            parsed = parse_mid0061(
                frame=frame,
                torque_divisor=safe_float(getattr(self, "cfg_tdiv", "100"), 100),
                angle_divisor=safe_float(getattr(self, "cfg_adiv", "1"), 1),
                torque_field=getattr(self, "cfg_tfield", "AUTO"),
                angle_field=getattr(self, "cfg_afield", "AUTO"),
            )
        except Exception as e:
            self.add_log(f"[{now_br()}] ERRO parser: {e}")
            return

        if self.is_duplicate(parsed):
            ident = parsed.get("tightening_id") or parsed.get("frame_hash")
            self.add_log(f"[{now_br()}] DUPLICADO IGNORADO: {ident}")
            self.set_msg(f"Duplicado ignorado: {ident}")
            return

        self.register_dedup(parsed)
        self.process_result(parsed)

    def is_duplicate(self, parsed: Dict[str, Any]) -> bool:
        tid = str(parsed.get("tightening_id") or "").strip()
        frame_hash = str(parsed.get("frame_hash") or "").strip()
        now = time.time()
        if tid and hasattr(self, "processed_tightening_ids") and tid in self.processed_tightening_ids:
            return True
        for h, ts in list(self.processed_frame_hash_time.items()):
            if now - ts > DUPLICATE_FRAME_WINDOW_SEC:
                self.processed_frame_hash_time.pop(h, None)
        if frame_hash and frame_hash in self.processed_frame_hash_time:
            return True
        return False

    def register_dedup(self, parsed: Dict[str, Any]):
        tid = str(parsed.get("tightening_id") or "").strip()
        frame_hash = str(parsed.get("frame_hash") or "").strip()
        if tid:
            self.processed_tightening_ids.add(tid)
        if frame_hash:
            self.processed_frame_hash_time[frame_hash] = time.time()

    def evaluate_status(self, parsed: Dict[str, Any]) -> str:
        status_panel = str(parsed.get("status_geral") or "").upper().strip()
        if bool(getattr(self, "cfg_panel_status", True)) and status_panel in ("OK", "NOK"):
            return status_panel
        torque = parsed.get("torque")
        if torque is None:
            return "SEM LEITURA"
        tmin = safe_float(getattr(self, "cfg_tmin", "0"), 0)
        tmax = safe_float(getattr(self, "cfg_tmax", "9999"), 9999)
        return "OK" if tmin <= torque <= tmax else "NOK"

    def process_result(self, parsed: Dict[str, Any]):
        p = POSICOES[self.current_idx]
        state = self.posicoes[p]
        state.tentativas += 1
        status_final = self.evaluate_status(parsed)
        state.torque = parsed.get("torque")
        state.angulo = parsed.get("angulo")
        state.pset = parsed.get("pset") or ""
        state.tightening_id = parsed.get("tightening_id") or ""
        state.frame_hash = parsed.get("frame_hash") or ""
        state.data_hora = now_br()

        registro = {
            "data_hora": now_br(),
            "serie": self.in_serie.text,
            "op": self.in_op.text,
            "posicao": p,
            "status": status_final,
            "torque": state.torque,
            "angulo": state.angulo,
            "pset": state.pset,
            "revision": parsed.get("revision"),
            "tightening_id": state.tightening_id,
            "frame_hash": state.frame_hash,
            "parser_info": parsed.get("parser_info"),
            "frame": parsed.get("frame"),
        }
        self.historico.insert(0, registro)
        self.save_attempt_csv(registro)

        if status_final == "OK":
            state.status = "OK"
            state.ultimo_status = "OK"
            if self.current_idx < len(POSICOES) - 1:
                self.current_idx += 1
                self.set_msg(f"{p} aprovado. Avançou para {POSICOES[self.current_idx]}.")
            else:
                self.set_msg("P8 aprovado. Ciclo finalizado. Imprimindo etiqueta e resetando...")
                self.save_cycle_csv()
                if bool(getattr(self, "cfg_auto_print", True)):
                    self.print_current_label()
                self.ciclo_em_reset = True
                Clock.schedule_once(lambda *_: self.auto_reset_after_p8(), AUTO_RESET_AFTER_SEC)
        elif status_final == "NOK":
            state.status = "AGUARDANDO RETESTE"
            state.ultimo_status = "NOK"
            state.nok_count += 1
            self.set_msg(f"{p} deu NOK. Torque: {to_float_text(state.torque)}. Refaça o mesmo ponto.")
        else:
            state.status = "AGUARDANDO RETESTE"
            state.ultimo_status = status_final
            self.set_msg(f"{p}: resultado sem leitura completa. Refaça o mesmo ponto.")
        self._refresh_all()

    def auto_reset_after_p8(self):
        if self.ciclo_em_reset:
            self.reset_cycle(clear_history=False)
            self.set_msg("Novo ciclo iniciado automaticamente. Aguardando P1.")

    # -----------------------------------------------------
    # SALVAR / IMPRIMIR
    # -----------------------------------------------------
    def save_attempt_csv(self, registro: Dict[str, Any]):
        serie = (registro.get("serie") or "SEM_SERIE").replace("/", "_").replace("\\", "_").strip()
        arquivo = CSV_DIR / f"tentativas_{serie}_{datetime.now().strftime('%Y%m%d')}.csv"
        fieldnames = ["data_hora", "serie", "op", "posicao", "status", "torque", "angulo", "pset", "revision", "tightening_id", "frame_hash", "parser_info", "frame"]
        write_header = not arquivo.exists()
        with arquivo.open("a", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
            if write_header:
                w.writeheader()
            w.writerow({k: registro.get(k, "") for k in fieldnames})

    def save_cycle_csv(self):
        serie = (self.in_serie.text or "SEM_SERIE").replace("/", "_").replace("\\", "_").strip()
        arquivo = CSV_DIR / f"ciclo_final_{serie}_{now_file()}.csv"
        fieldnames = ["data_hora", "serie", "op", "posicao", "status", "torque", "angulo", "pset", "tentativas", "nok_count", "tightening_id"]
        with arquivo.open("w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
            w.writeheader()
            for p in POSICOES:
                stt = self.posicoes[p]
                w.writerow({
                    "data_hora": stt.data_hora,
                    "serie": self.in_serie.text,
                    "op": self.in_op.text,
                    "posicao": p,
                    "status": stt.ultimo_status or stt.status,
                    "torque": stt.torque,
                    "angulo": stt.angulo,
                    "pset": stt.pset,
                    "tentativas": stt.tentativas,
                    "nok_count": stt.nok_count,
                    "tightening_id": stt.tightening_id,
                })
        self.add_log(f"Ciclo salvo: {arquivo}")

    def print_current_label(self):
        copias = safe_int(getattr(self, "cfg_copias", "1"), 1)
        zpl = gerar_zpl_torque(self.in_serie.text, self.posicoes, copias=copias)
        safe_serie = (self.in_serie.text or "SEM_SERIE").replace("/", "_").replace("\\", "_").strip()
        zpl_path = CSV_DIR / f"etiqueta_torque_{safe_serie}_{now_file()}.zpl"
        zpl_path.write_text(zpl, encoding="utf-8")

        mode = getattr(self, "cfg_print_mode", "SALVAR")
        try:
            if mode == "USB":
                msg = imprimir_zebra_usb_android(zpl)
                self.set_msg(msg)
            elif mode == "IP":
                ip = getattr(self, "cfg_zebra_ip", "192.168.0.50")
                port = safe_int(getattr(self, "cfg_zebra_port", "9100"), 9100)
                imprimir_zebra_ip(ip, port, zpl)
                self.set_msg(f"Etiqueta enviada para Zebra IP {ip}:{port}")
            else:
                self.set_msg(f"ZPL salvo: {zpl_path.name}")
        except Exception as e:
            self.set_msg(f"Falha na impressão. ZPL salvo. Erro: {e}")
            self.add_log(f"Erro impressão: {e}")

    # -----------------------------------------------------
    # REFRESH
    # -----------------------------------------------------
    def calc_status_geral(self) -> str:
        if all(self.posicoes[p].status == "OK" for p in POSICOES):
            return "OK"
        if any(self.posicoes[p].status == "AGUARDANDO RETESTE" for p in POSICOES):
            return "RETESTE"
        return "PENDENTE"

    def _refresh_all(self):
        for idx, p in enumerate(POSICOES):
            self.cards[p].update_data(self.posicoes[p], current=(idx == self.current_idx))
        self.lbl_pos.value_label.text = POSICOES[self.current_idx]  # type: ignore
        self.lbl_geral.value_label.text = self.calc_status_geral()  # type: ignore


if __name__ == "__main__":
    try:
        TorquePF6000App().run()
    except Exception as e:
        salvar_crash_log(e)
        raise
