# main.py
# APK Kivy - Controle de Torque PF6000 + Impressao Zebra USB
# Projeto leve para tablet industrial.
#
# Fluxo:
#   Ethernet tablet -> PF6000 169.254.1.1:4545
#   APK captura P1..P8 via Open Protocol
#   USB/OTG tablet -> Zebra USB com ZPL direto
#
# Buildozer:
#   requirements = python3,kivy,pyjnius

import csv
import hashlib
import os
import queue
import re
import socket
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from kivy.app import App
from kivy.clock import Clock, mainthread
from kivy.core.window import Window
from kivy.core.text import Label as CoreLabel
from kivy.graphics import Color, RoundedRectangle, Line, Rectangle
from kivy.graphics.texture import Texture
from kivy.metrics import dp
from kivy.properties import StringProperty, BooleanProperty, NumericProperty
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.button import Button
from kivy.uix.checkbox import CheckBox
from kivy.uix.gridlayout import GridLayout
from kivy.uix.label import Label
from kivy.uix.popup import Popup
from kivy.uix.scrollview import ScrollView
from kivy.uix.spinner import Spinner
from kivy.uix.textinput import TextInput
from kivy.utils import platform

# Não importo android.storage na abertura.
# Em alguns builds isso fecha o APK antes da tela abrir se a recipe "android" não estiver embutida.
app_storage_path = None


# =========================================================
# CONFIG
# =========================================================
DEFAULT_IP = "169.254.1.1"
DEFAULT_PORT = 4545
POSICOES = [f"P{i}" for i in range(1, 9)]
DUPLICATE_FRAME_WINDOW_SEC = 20
AUTO_RESET_AFTER_SEC = 3.0

# Tamanho visual pensado para tablet em landscape.
# Paleta baseada nos últimos APKs: azul, branco e preto.
BG_APP = (1, 1, 1, 1)
CARD_BG = (1, 1, 1, 1)
CARD_BORDER = (0.83, 0.88, 0.95, 1)
HEADER_TOP = (0.03, 0.14, 0.34, 1)
HEADER_BOTTOM = (0.10, 0.31, 0.61, 1)
FIELD_TOP = (0.05, 0.20, 0.44, 1)
FIELD_BOTTOM = (0.09, 0.30, 0.58, 1)
FIELD_BORDER = (0.18, 0.41, 0.74, 1)
BUTTON_TOP = (0.03, 0.14, 0.34, 1)
BUTTON_BOTTOM = (0.10, 0.31, 0.61, 1)
BUTTON_SECONDARY_TOP = (0.29, 0.35, 0.48, 1)
BUTTON_SECONDARY_BOTTOM = (0.21, 0.26, 0.38, 1)
TEXT_DARK = (0.09, 0.14, 0.22, 1)
TEXT_MUTED = (0.40, 0.47, 0.58, 1)
TEXT_LIGHT = (1, 1, 1, 1)
SUCCESS = (0.20, 0.64, 0.33, 1)
WARNING = (0.88, 0.58, 0.00, 1)
ERROR = (0.82, 0.22, 0.22, 1)
CURRENT_BLUE = (0.03, 0.14, 0.34, 1)
CURRENT_BORDER = (0.00, 0.62, 0.95, 1)
Window.clearcolor = BG_APP


def _rgba255(rgba):
    """Converte tupla RGBA 0-1 para bytes 0-255."""
    return bytes(max(0, min(255, int(round(c * 255)))) for c in rgba)


def make_vertical_gradient_texture(top_rgba, bottom_rgba):
    """
    Cria textura de gradiente vertical para os cards/botões.
    Essa função estava faltando e causava o erro:
    NameError: make_vertical_gradient_texture is not defined
    """
    texture = Texture.create(size=(1, 2), colorfmt="rgba")
    buf = _rgba255(bottom_rgba) + _rgba255(top_rgba)
    texture.blit_buffer(buf, colorfmt="rgba", bufferfmt="ubyte")
    texture.wrap = "clamp_to_edge"
    texture.mag_filter = "linear"
    texture.min_filter = "linear"
    return texture



# =========================================================
# PATHS
# =========================================================
# IMPORTANTE:
# Não crie pasta em /sdcard ou em Path.cwd() na importação do app.
# No Android isso pode fechar o APK antes da primeira tela.
BASE_DIR = Path(".")
LOG_DIR = Path(".")
CSV_DIR = Path(".")


def inicializar_pastas(app=None):
    """Inicializa pastas somente depois que o Kivy App já abriu."""
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


def _usb_device_basic_info(dev) -> str:
    try:
        return (
            f"vendor=0x{int(dev.getVendorId()):04X} "
            f"product=0x{int(dev.getProductId()):04X} "
            f"class={int(dev.getDeviceClass())} "
            f"interfaces={int(dev.getInterfaceCount())}"
        )
    except Exception:
        return "USB device"


def _find_usb_printer_android(request_permission: bool = True):
    """
    Detecta automaticamente uma impressora USB no Android.

    Critérios:
    1) Preferência para Zebra vendorId 0x0A5F.
    2) Depois, qualquer dispositivo/interface classe PRINTER = 7.
    3) Depois, qualquer USB com endpoint BULK OUT.

    Retorna:
      usb_manager, device, interface, endpoint_out, info
    """
    if platform != "android":
        raise RuntimeError("USB disponível somente no APK instalado no tablet Android.")

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

    if device_list is None or device_list.isEmpty():
        raise RuntimeError("Nenhum USB detectado. Conecte a Zebra no OTG/USB do tablet.")

    candidates = []
    dispositivos_vistos = []

    iterator = device_list.values().iterator()
    while iterator.hasNext():
        dev = iterator.next()
        info_dev = _usb_device_basic_info(dev)
        dispositivos_vistos.append(info_dev)

        vendor = int(dev.getVendorId())
        product = int(dev.getProductId())
        device_class = int(dev.getDeviceClass())

        for i in range(dev.getInterfaceCount()):
            itf = dev.getInterface(i)
            interface_class = int(itf.getInterfaceClass())

            for e in range(itf.getEndpointCount()):
                ep = itf.getEndpoint(e)
                is_bulk_out = (
                    ep.getDirection() == UsbConstants.USB_DIR_OUT
                    and ep.getType() == UsbConstants.USB_ENDPOINT_XFER_BULK
                )

                if not is_bulk_out:
                    continue

                score = 0
                if vendor == 0x0A5F:  # Zebra Technologies
                    score += 100
                if device_class == 7 or interface_class == 7:  # USB printer class
                    score += 50

                # Ainda aceita bulk OUT como fallback, pois algumas Zebras aparecem como vendor-specific.
                score += 10

                candidates.append((score, dev, itf, ep, vendor, product, device_class, interface_class))

    if not candidates:
        raise RuntimeError(
            "Nenhuma impressora USB com endpoint BULK OUT encontrada. "
            f"USBs vistos: {' | '.join(dispositivos_vistos)}"
        )

    candidates.sort(key=lambda x: x[0], reverse=True)
    _score, chosen, interface, endpoint_out, vendor, product, device_class, interface_class = candidates[0]

    info = (
        f"USB detectado: vendor=0x{vendor:04X} product=0x{product:04X} "
        f"device_class={device_class} interface_class={interface_class}"
    )

    if not usb_manager.hasPermission(chosen):
        if request_permission:
            flags = 0
            try:
                if int(Build.VERSION.SDK_INT) >= 23:
                    flags = PendingIntent.FLAG_IMMUTABLE
            except Exception:
                flags = 0

            permission_intent = PendingIntent.getBroadcast(
                activity,
                0,
                Intent("br.com.ibero.USB_PERMISSION"),
                flags,
            )
            usb_manager.requestPermission(chosen, permission_intent)

        raise RuntimeError(
            f"{info}. Permissão USB solicitada. Autorize no Android e clique em imprimir novamente."
        )

    return usb_manager, chosen, interface, endpoint_out, info


def detectar_zebra_usb_android() -> str:
    """Apenas detecta a impressora USB e solicita permissão se necessário."""
    _usb_manager, _chosen, _interface, _endpoint_out, info = _find_usb_printer_android(request_permission=True)
    return f"{info}. Permissão OK. Pronta para imprimir."


def imprimir_zebra_usb_android(zpl: str) -> str:
    """Detecta automaticamente a Zebra USB e envia o ZPL por bulkTransfer."""
    usb_manager, chosen, interface, endpoint_out, info = _find_usb_printer_android(request_permission=True)

    connection = usb_manager.openDevice(chosen)
    if connection is None:
        raise RuntimeError("USB detectado, mas não foi possível abrir conexão com a impressora.")

    claimed = False
    try:
        claimed = bool(connection.claimInterface(interface, True))
        if not claimed:
            raise RuntimeError("Não foi possível reservar a interface USB da impressora.")

        data = zpl.encode("utf-8", errors="ignore")
        offset = 0
        chunk_size = 4096

        while offset < len(data):
            chunk = data[offset:offset + chunk_size]
            buffer = bytearray(chunk)

            sent = connection.bulkTransfer(endpoint_out, buffer, len(buffer), 5000)
            if sent is None or int(sent) < 0:
                raise RuntimeError("Falha no bulkTransfer USB para a Zebra.")

            offset += int(sent) if int(sent) > 0 else len(chunk)

        return f"ZPL enviado via USB. {info}"

    finally:
        try:
            if claimed:
                connection.releaseInterface(interface)
        except Exception:
            pass

        try:
            connection.close()
        except Exception:
            pass



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
class RoundedPanel(BoxLayout):
    def __init__(self, bg=CARD_BG, border=CARD_BORDER, radius=18, **kwargs):
        super().__init__(**kwargs)
        self._bg = bg
        self._border = border
        self._radius = dp(radius) if isinstance(radius, (int, float)) else radius
        with self.canvas.before:
            self._bg_color = Color(*self._bg)
            self._bg_rect = RoundedRectangle(pos=self.pos, size=self.size, radius=[self._radius] * 4)
            self._border_color = Color(*self._border)
            self._border_line = Line(rounded_rectangle=(self.x, self.y, self.width, self.height, self._radius), width=1.15)
        self.bind(pos=self._update_panel, size=self._update_panel)

    def set_style(self, bg=None, border=None, line_width=1.15):
        if bg is not None:
            self._bg = bg
            self._bg_color.rgba = bg
        if border is not None:
            self._border = border
            self._border_color.rgba = border
        self._border_line.width = line_width

    def _update_panel(self, *_):
        self._bg_rect.pos = self.pos
        self._bg_rect.size = self.size
        self._border_line.rounded_rectangle = (self.x, self.y, self.width, self.height, self._radius)


class GradientPanel(BoxLayout):
    def __init__(self, top_color=HEADER_TOP, bottom_color=HEADER_BOTTOM, border=FIELD_BORDER, radius=18, **kwargs):
        super().__init__(**kwargs)
        self._radius = dp(radius) if isinstance(radius, (int, float)) else radius
        self._texture = make_vertical_gradient_texture(top_color, bottom_color)
        with self.canvas.before:
            self._color = Color(1, 1, 1, 1)
            self._rect = RoundedRectangle(pos=self.pos, size=self.size, radius=[self._radius] * 4, texture=self._texture)
            self._border_color = Color(*border)
            self._line = Line(rounded_rectangle=(self.x, self.y, self.width, self.height, self._radius), width=1.10)
        self.bind(pos=self._update, size=self._update)

    def _update(self, *_):
        self._rect.pos = self.pos
        self._rect.size = self.size
        self._line.rounded_rectangle = (self.x, self.y, self.width, self.height, self._radius)


class StyledButton(Button):
    def __init__(self, text='', primary=True, **kwargs):
        kwargs.setdefault('size_hint_y', None)
        kwargs.setdefault('height', dp(44))
        super().__init__(text=text, background_normal='', background_down='', background_color=(0, 0, 0, 0), color=TEXT_LIGHT, bold=True, **kwargs)
        self._radius = dp(14)
        top, bottom, border = (BUTTON_TOP, BUTTON_BOTTOM, FIELD_BORDER) if primary else (BUTTON_SECONDARY_TOP, BUTTON_SECONDARY_BOTTOM, (0.38, 0.45, 0.56, 1))
        self._texture = make_vertical_gradient_texture(top, bottom)
        with self.canvas.before:
            self._c = Color(1, 1, 1, 1)
            self._rect = RoundedRectangle(pos=self.pos, size=self.size, radius=[self._radius] * 4, texture=self._texture)
            self._bc = Color(*border)
            self._line = Line(rounded_rectangle=(self.x, self.y, self.width, self.height, self._radius), width=1.1)
        self.bind(pos=self._update_btn, size=self._update_btn)

    def _update_btn(self, *_):
        self._rect.pos = self.pos
        self._rect.size = self.size
        self._line.rounded_rectangle = (self.x, self.y, self.width, self.height, self._radius)


class StyledInput(TextInput):
    def __init__(self, hint='', navy=False, **kwargs):
        kwargs.setdefault('multiline', False)
        kwargs.setdefault('font_size', '16sp')
        kwargs.setdefault('size_hint_y', None)
        kwargs.setdefault('height', dp(48))
        kwargs.setdefault('padding', [dp(14), dp(13), dp(14), dp(10)])
        self._navy = navy
        self._radius = dp(14)
        self._texture = make_vertical_gradient_texture(FIELD_TOP, FIELD_BOTTOM)

        # Para campo branco, escondo o texto nativo e desenho manualmente em preto.
        # Em alguns Android/Kivy o foreground_color ficava cinza claro mesmo setando preto.
        native_text_color = TEXT_LIGHT if navy else (0, 0, 0, 0)
        native_hint_color = (0.92, 0.96, 1, 0.88) if navy else (0, 0, 0, 0)

        super().__init__(
            hint_text=hint,
            foreground_color=native_text_color,
            disabled_foreground_color=native_text_color,
            hint_text_color=native_hint_color,
            cursor_color=(1, 1, 1, 1) if navy else (0, 0, 0, 1),
            selection_color=(1, 1, 1, 0.25) if navy else (0.20, 0.40, 0.80, 0.35),
            background_color=(0, 0, 0, 0),
            background_normal='',
            background_active='',
            write_tab=False,
            **kwargs,
        )

        with self.canvas.before:
            self._fill_color = Color(1, 1, 1, 1)
            if navy:
                self._fill_rect = RoundedRectangle(pos=self.pos, size=self.size, radius=[self._radius] * 4, texture=self._texture)
                self._border_color = Color(*FIELD_BORDER)
            else:
                self._fill_rect = RoundedRectangle(pos=self.pos, size=self.size, radius=[self._radius] * 4)
                self._border_color = Color(*CARD_BORDER)
            self._line = Line(rounded_rectangle=(self.x, self.y, self.width, self.height, self._radius), width=1.05)

        # Camada manual do texto preto para campos brancos.
        self._manual_text_rect = None
        self._manual_text_color = None
        if not navy:
            with self.canvas.after:
                self._manual_text_color = Color(0, 0, 0, 1)
                self._manual_text_rect = Rectangle(pos=(0, 0), size=(0, 0))

        self.bind(pos=self._update_input, size=self._update_input)
        self.bind(text=self._update_manual_text, hint_text=self._update_manual_text, focus=self._update_manual_text)

        Clock.schedule_once(lambda dt: self._update_manual_text(), 0)

    def _update_input(self, *_):
        self._fill_rect.pos = self.pos
        self._fill_rect.size = self.size
        self._line.rounded_rectangle = (self.x, self.y, self.width, self.height, self._radius)
        self._update_manual_text()

    def _make_text_texture(self, txt, color):
        # Mostra o final do texto se ficar maior que o campo.
        max_w = max(20, self.width - dp(28))
        draw_txt = txt or ''
        while len(draw_txt) > 1:
            label = CoreLabel(text=draw_txt, font_size=self.font_size, color=color)
            label.refresh()
            if label.texture.width <= max_w:
                return label.texture
            draw_txt = draw_txt[1:]

        label = CoreLabel(text=draw_txt, font_size=self.font_size, color=color)
        label.refresh()
        return label.texture

    def _update_manual_text(self, *_):
        if self._navy or self._manual_text_rect is None:
            return

        if self.text:
            txt = self.text
            color = (0, 0, 0, 1)  # PRETO FORTE
        else:
            txt = self.hint_text or ''
            color = (0.18, 0.22, 0.30, 1)  # placeholder mais escuro

        if self._manual_text_color:
            self._manual_text_color.rgba = color

        if not txt:
            self._manual_text_rect.texture = None
            self._manual_text_rect.size = (0, 0)
            return

        tex = self._make_text_texture(txt, color)
        self._manual_text_rect.texture = tex
        self._manual_text_rect.size = tex.size
        self._manual_text_rect.pos = (
            self.x + dp(14),
            self.center_y - tex.height / 2
        )

class PCard(RoundedPanel):
    CARD_H = dp(194)

    def __init__(self, ponto: str, on_select, **kwargs):
        super().__init__(
            orientation='vertical',
            padding=[dp(12), dp(10), dp(12), dp(10)],
            spacing=dp(3),
            bg=CARD_BG,
            border=CARD_BORDER,
            radius=18,
            **kwargs
        )
        # NÃO use self.pos para guardar P1/P2. self.pos é propriedade interna do Kivy.
        self.ponto = ponto
        self.on_select = on_select
        self.size_hint_y = None
        self.height = self.CARD_H
        self.status = 'AGUARDANDO'
        self._build()

    def _build(self):
        self.lbl_pos = Label(
            text=self.ponto,
            font_size='24sp',
            bold=True,
            halign='left',
            valign='middle',
            size_hint_y=None,
            height=dp(30),
            color=TEXT_DARK,
        )
        self.lbl_pos.bind(size=lambda w, s: setattr(w, 'text_size', s))
        self.add_widget(self.lbl_pos)

        self.lbl_status = Label(
            text='AGUARDANDO',
            font_size='12sp',
            bold=True,
            halign='left',
            valign='middle',
            size_hint_y=None,
            height=dp(22),
            color=TEXT_MUTED,
        )
        self.lbl_status.bind(size=lambda w, s: setattr(w, 'text_size', s))
        self.add_widget(self.lbl_status)

        self.lbl_torque = Label(text='Torque:', font_size='12sp', halign='left', valign='middle', size_hint_y=None, height=dp(22), color=TEXT_DARK)
        self.lbl_torque.bind(size=lambda w, s: setattr(w, 'text_size', s))
        self.add_widget(self.lbl_torque)

        self.lbl_angulo = Label(text='Ângulo:', font_size='12sp', halign='left', valign='middle', size_hint_y=None, height=dp(22), color=TEXT_DARK)
        self.lbl_angulo.bind(size=lambda w, s: setattr(w, 'text_size', s))
        self.add_widget(self.lbl_angulo)

        self.lbl_info = Label(text='Tentativas: 0 | NOK: 0', font_size='9sp', halign='left', valign='middle', size_hint_y=None, height=dp(17), color=TEXT_MUTED)
        self.lbl_info.bind(size=lambda w, s: setattr(w, 'text_size', s))
        self.add_widget(self.lbl_info)

        self.lbl_time = Label(text='', font_size='8sp', halign='left', valign='middle', size_hint_y=None, height=dp(14), color=TEXT_MUTED)
        self.lbl_time.bind(size=lambda w, s: setattr(w, 'text_size', s))
        self.add_widget(self.lbl_time)

        self.btn = StyledButton(text=f'Selecionar {self.ponto}', primary=False, size_hint_y=None, height=dp(30), font_size='10sp')
        self.btn.bind(on_release=lambda *_: self.on_select(self.ponto))
        self.add_widget(self.btn)

    def update_data(self, state: PState, current: bool):
        # OK tem prioridade sobre CURRENT para o P8 ficar verde antes do reset automático.
        if state.status == 'OK':
            bg = (0.06, 0.38, 0.18, 1)
            border = (0.10, 0.85, 0.36, 1)
            status_color = (1, 1, 1, 1)
            main_text = (1, 1, 1, 1)
            muted = (0.88, 1, 0.92, 1)
        elif state.status == 'AGUARDANDO RETESTE':
            bg = (1.00, 0.96, 0.78, 1)
            border = WARNING
            status_color = (0.52, 0.34, 0.00, 1)
            main_text = TEXT_DARK
            muted = (0.52, 0.34, 0.00, 1)
        elif current:
            bg = CURRENT_BLUE
            border = CURRENT_BORDER
            status_color = (0.35, 0.88, 1.0, 1)
            main_text = TEXT_LIGHT
            muted = (0.86, 0.92, 0.98, 1)
        else:
            bg = CARD_BG
            border = CARD_BORDER
            status_color = TEXT_MUTED
            main_text = TEXT_DARK
            muted = TEXT_MUTED

        self.set_style(bg, border, line_width=1.9 if current or state.status in ('OK', 'AGUARDANDO RETESTE') else 1.15)
        self.lbl_pos.color = main_text
        self.lbl_status.text = state.status
        self.lbl_status.color = status_color
        self.lbl_torque.color = main_text
        self.lbl_angulo.color = main_text
        self.lbl_info.color = muted
        self.lbl_time.color = muted
        self.lbl_torque.text = f'Torque: {to_float_text(state.torque)}'
        self.lbl_angulo.text = f'Ângulo: {to_float_text(state.angulo)}'
        self.lbl_info.text = f'Tentativas: {state.tentativas} | NOK: {state.nok_count}'
        self.lbl_time.text = state.data_hora or ''


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

        # Scroll principal: em tablet menor dá para rolar a tela sem deformar layout.
        self.main_scroll = ScrollView(do_scroll_x=False, do_scroll_y=True, bar_width=dp(8))
        self.root_box = BoxLayout(orientation="vertical", padding=[dp(12), dp(10), dp(12), dp(14)], spacing=dp(8), size_hint_y=None)
        self.root_box.bind(minimum_height=self.root_box.setter("height"))
        self.main_scroll.add_widget(self.root_box)

        self._build_top()
        self._build_cards()
        self._build_bottom_log()

        Clock.schedule_interval(self._poll_events, 0.1)
        self._refresh_all()
        return self.main_scroll

    def _build_top(self):
        header = GradientPanel(orientation="horizontal", size_hint_y=None, height=dp(74), padding=dp(14), spacing=dp(10), radius=20)
        title_box = BoxLayout(orientation="vertical", spacing=dp(2))
        lbl_title = Label(text="Controle de Torque Mola - PF6000", font_size="22sp", bold=True, halign="left", valign="middle", color=TEXT_LIGHT)
        lbl_title.bind(size=lambda w, s: setattr(w, "text_size", s))
        title_box.add_widget(lbl_title)
        self.lbl_msg = Label(text="Aguardando conexão.", font_size="12sp", halign="left", valign="middle", color=(0.92, 0.96, 1, 1))
        self.lbl_msg.bind(size=lambda w, s: setattr(w, "text_size", s))
        title_box.add_widget(self.lbl_msg)
        header.add_widget(title_box)
        btn_cfg = StyledButton(text="CONFIG", primary=False, size_hint_x=None, width=dp(130), height=dp(44), font_size="13sp")
        btn_cfg.bind(on_release=lambda *_: self.open_config_popup())
        header.add_widget(btn_cfg)
        self.root_box.add_widget(header)

        # Somente número de série/rastreio na tela principal. OP fica em branco internamente.
        data_card = RoundedPanel(orientation="vertical", bg=CARD_BG, border=CARD_BORDER, radius=18, size_hint_y=None, height=dp(82), padding=dp(10), spacing=dp(5))
        lbl_serie = Label(text="Nº Série / Rastreio", font_size="11sp", color=TEXT_MUTED, halign="left", size_hint_y=None, height=dp(18))
        lbl_serie.bind(size=lambda w, s: setattr(w, "text_size", s))
        data_card.add_widget(lbl_serie)
        self.in_serie = StyledInput("Bipe ou digite a série", navy=False, input_type="text")
        # Texto do número de série é desenhado manualmente em preto pela classe StyledInput.
        data_card.add_widget(self.in_serie)

        # Compatibilidade com as rotinas de CSV antigas que ainda leem self.in_op.text.
        self.in_op = TextInput(text="", multiline=False, opacity=0, size_hint=(None, None), size=(0, 0))
        self.root_box.add_widget(data_card)

        status_row = GridLayout(cols=5, size_hint_y=None, height=dp(66), spacing=dp(8))
        self.lbl_tcp = self._metric("TCP", "OFF")
        self.lbl_open = self._metric("Open Protocol", "OFF")
        self.lbl_mid = self._metric("MID", "-")
        self.lbl_pos = self._metric("Atual", "P1")
        self.lbl_geral = self._metric("Status", "PENDENTE")
        for w in [self.lbl_tcp, self.lbl_open, self.lbl_mid, self.lbl_pos, self.lbl_geral]:
            status_row.add_widget(w)
        self.root_box.add_widget(status_row)

    def _metric(self, title: str, value: str) -> BoxLayout:
        b = RoundedPanel(orientation="vertical", padding=dp(8), bg=CARD_BG, border=CARD_BORDER, radius=16)
        t = Label(text=title, font_size="10sp", color=TEXT_MUTED, size_hint_y=0.35, halign="left", valign="middle")
        t.bind(size=lambda w, s: setattr(w, "text_size", s))
        b.add_widget(t)
        lab = Label(text=value, font_size="17sp", bold=True, color=TEXT_DARK, size_hint_y=0.65, halign="left", valign="middle")
        lab.bind(size=lambda w, s: setattr(w, "text_size", s))
        b.value_label = lab  # type: ignore
        b.add_widget(lab)
        return b

    def _build_cards(self):
        lbl = Label(text="Pontos de aperto", font_size="19sp", bold=True, color=TEXT_DARK, halign="left", valign="middle", size_hint_y=None, height=dp(30))
        lbl.bind(size=lambda w, s: setattr(w, "text_size", s))
        self.root_box.add_widget(lbl)

        self.cards: Dict[str, PCard] = {}
        cards_wrap = BoxLayout(orientation="vertical", spacing=dp(10), size_hint_y=None, height=(PCard.CARD_H * 2 + dp(10)))

        for row_points in (POSICOES[:4], POSICOES[4:]):
            row = BoxLayout(orientation="horizontal", spacing=dp(10), size_hint_y=None, height=PCard.CARD_H)
            for p in row_points:
                card = PCard(p, on_select=self.select_position)
                card.size_hint_x = 1
                self.cards[p] = card
                row.add_widget(card)
            cards_wrap.add_widget(row)

        self.root_box.add_widget(cards_wrap)

    def _build_bottom_log(self):
        row = GridLayout(cols=4, size_hint_y=None, height=dp(46), spacing=dp(8))
        self.btn_connect = StyledButton(text="Conectar", primary=True, font_size="13sp")
        self.btn_connect.bind(on_release=lambda *_: self.connect())
        row.add_widget(self.btn_connect)
        btn_stop = StyledButton(text="Desconectar", primary=False, font_size="13sp")
        btn_stop.bind(on_release=lambda *_: self.client.stop())
        row.add_widget(btn_stop)
        btn_reset = StyledButton(text="Reset ciclo", primary=False, font_size="13sp")
        btn_reset.bind(on_release=lambda *_: self.reset_cycle(clear_history=False))
        row.add_widget(btn_reset)
        btn_print = StyledButton(text="Imprimir etiqueta", primary=True, font_size="13sp")
        btn_print.bind(on_release=lambda *_: self.print_current_label())
        row.add_widget(btn_print)
        self.root_box.add_widget(row)

        log_panel = RoundedPanel(orientation="vertical", bg=(0.98, 0.99, 1, 1), border=CARD_BORDER, radius=16, size_hint_y=None, height=dp(76), padding=dp(8))
        self.log_label = Label(text="", font_size="10sp", color=TEXT_MUTED, halign="left", valign="top")
        self.log_label.bind(size=lambda w, s: setattr(w, "text_size", s))
        log_panel.add_widget(self.log_label)
        self.root_box.add_widget(log_panel)
        self.logs: List[str] = []

    def open_config_popup(self):
        content = BoxLayout(orientation="vertical", padding=dp(10), spacing=dp(8))
        scroll = ScrollView()
        inner = GridLayout(cols=2, spacing=dp(8), size_hint_y=None)
        inner.bind(minimum_height=inner.setter("height"))

        # Importante:
        # Widgets do popup usam prefixo w_cfg_.
        # Valores persistidos ficam em cfg_.
        # Isso evita o erro: AttributeError: 'str' object has no attribute 'text'
        self.w_cfg_ip = TextInput(text=str(getattr(self, "cfg_ip", DEFAULT_IP)), multiline=False)
        self.w_cfg_port = TextInput(text=str(getattr(self, "cfg_port", str(DEFAULT_PORT))), multiline=False)
        self.w_cfg_rev0060 = Spinner(text=str(getattr(self, "cfg_rev0060", "001")), values=["001", "002", "003", "004", "005", "006", "007", "008"])
        self.w_cfg_rev0062 = Spinner(text=str(getattr(self, "cfg_rev0062", "001")), values=["001", "002", "003", "004", "005", "006", "007", "008"])
        self.w_cfg_modelo = TextInput(text=str(getattr(self, "cfg_modelo", "MOLA")), multiline=False)
        self.w_cfg_tmin = TextInput(text=str(getattr(self, "cfg_tmin", "0")), multiline=False)
        self.w_cfg_tmax = TextInput(text=str(getattr(self, "cfg_tmax", "9999")), multiline=False)
        self.w_cfg_tdiv = Spinner(text=str(getattr(self, "cfg_tdiv", "100")), values=["1", "10", "100", "1000"])
        self.w_cfg_adiv = Spinner(text=str(getattr(self, "cfg_adiv", "1")), values=["1", "10", "100"])
        self.w_cfg_tfield = Spinner(text=str(getattr(self, "cfg_tfield", "AUTO")), values=["AUTO", "15", "24", "12", "13", "14", "21", "22", "23"])
        self.w_cfg_afield = Spinner(text=str(getattr(self, "cfg_afield", "AUTO")), values=["AUTO", "19", "28", "16", "17", "18", "25", "26", "27"])
        self.w_cfg_print_mode = Spinner(text=str(getattr(self, "cfg_print_mode", "USB")), values=["USB", "SALVAR"])
        self.w_cfg_copias = TextInput(text=str(getattr(self, "cfg_copias", "1")), multiline=False)

        self.w_cfg_auto_reconnect = CheckBox(active=bool(getattr(self, "cfg_auto_reconnect", True)))
        self.w_cfg_panel_status = CheckBox(active=bool(getattr(self, "cfg_panel_status", True)))
        self.w_cfg_auto_print = CheckBox(active=bool(getattr(self, "cfg_auto_print", True)))

        def add(label, widget):
            inner.add_widget(Label(
                text=label,
                font_size="14sp",
                color=TEXT_DARK,
                halign="left",
                valign="middle",
                size_hint_y=None,
                height=dp(44),
            ))
            widget.size_hint_y = None
            widget.height = dp(44)
            inner.add_widget(widget)

        add("IP PF6000", self.w_cfg_ip)
        add("Porta PF6000", self.w_cfg_port)
        add("REV 0060", self.w_cfg_rev0060)
        add("REV 0062", self.w_cfg_rev0062)
        add("Auto-reconectar", self.w_cfg_auto_reconnect)
        add("Usar OK/NOK painel", self.w_cfg_panel_status)
        add("Modelo", self.w_cfg_modelo)
        add("Torque mínimo", self.w_cfg_tmin)
        add("Torque máximo", self.w_cfg_tmax)
        add("Campo torque", self.w_cfg_tfield)
        add("Campo ângulo", self.w_cfg_afield)
        add("Divisor torque", self.w_cfg_tdiv)
        add("Divisor ângulo", self.w_cfg_adiv)
        add("Modo impressão", self.w_cfg_print_mode)
        add("Auto imprimir P8 OK", self.w_cfg_auto_print)
        add("Cópias", self.w_cfg_copias)

        scroll.add_widget(inner)
        content.add_widget(scroll)

        row = BoxLayout(size_hint_y=None, height=dp(48), spacing=dp(8))
        btn_save = StyledButton(text="Salvar", primary=True)
        btn_detect = StyledButton(text="Detectar USB", primary=False)
        btn_test = StyledButton(text="Teste Zebra", primary=False)
        btn_close = StyledButton(text="Fechar", primary=False)

        row.add_widget(btn_save)
        row.add_widget(btn_detect)
        row.add_widget(btn_test)
        row.add_widget(btn_close)
        content.add_widget(row)

        popup = Popup(title="Configurações", content=content, size_hint=(0.94, 0.94))

        btn_save.bind(on_release=lambda *_: (self.save_config_from_popup(), popup.dismiss()))
        btn_detect.bind(on_release=lambda *_: self.detect_usb_from_popup())
        btn_test.bind(on_release=lambda *_: self.test_print_from_popup())
        btn_close.bind(on_release=lambda *_: popup.dismiss())

        popup.open()

    def _popup_text(self, widget_attr: str, stored_attr: str, default: str = "") -> str:
        obj = getattr(self, widget_attr, None)
        if hasattr(obj, "text"):
            return str(obj.text)
        return str(getattr(self, stored_attr, default))

    def _popup_active(self, widget_attr: str, stored_attr: str, default: bool = False) -> bool:
        obj = getattr(self, widget_attr, None)
        if hasattr(obj, "active"):
            return bool(obj.active)
        return bool(getattr(self, stored_attr, default))

    def save_config_from_popup(self):
        self.cfg_ip = self._popup_text("w_cfg_ip", "cfg_ip", DEFAULT_IP)
        self.cfg_port = self._popup_text("w_cfg_port", "cfg_port", str(DEFAULT_PORT))
        self.cfg_rev0060 = self._popup_text("w_cfg_rev0060", "cfg_rev0060", "001")
        self.cfg_rev0062 = self._popup_text("w_cfg_rev0062", "cfg_rev0062", "001")
        self.cfg_auto_reconnect = self._popup_active("w_cfg_auto_reconnect", "cfg_auto_reconnect", True)
        self.cfg_panel_status = self._popup_active("w_cfg_panel_status", "cfg_panel_status", True)
        self.cfg_auto_print = self._popup_active("w_cfg_auto_print", "cfg_auto_print", True)
        self.cfg_modelo = self._popup_text("w_cfg_modelo", "cfg_modelo", "MOLA")
        self.cfg_tmin = self._popup_text("w_cfg_tmin", "cfg_tmin", "0")
        self.cfg_tmax = self._popup_text("w_cfg_tmax", "cfg_tmax", "9999")
        self.cfg_tdiv = self._popup_text("w_cfg_tdiv", "cfg_tdiv", "100")
        self.cfg_adiv = self._popup_text("w_cfg_adiv", "cfg_adiv", "1")
        self.cfg_tfield = self._popup_text("w_cfg_tfield", "cfg_tfield", "AUTO")
        self.cfg_afield = self._popup_text("w_cfg_afield", "cfg_afield", "AUTO")
        self.cfg_print_mode = self._popup_text("w_cfg_print_mode", "cfg_print_mode", "USB")
        self.cfg_copias = self._popup_text("w_cfg_copias", "cfg_copias", "1")
        self.set_msg("Configurações salvas.")

    def detect_usb_from_popup(self):
        self.save_config_from_popup()
        try:
            msg = detectar_zebra_usb_android()
            self.set_msg(msg)
            self.add_log(msg)
        except Exception as e:
            msg = f"USB: {e}"
            self.set_msg(msg)
            self.add_log(msg)

    def test_print_from_popup(self):
        self.save_config_from_popup()
        old = self.posicoes
        demo = {
            p: PState(
                status="OK",
                ultimo_status="OK",
                torque=465 + i,
                angulo=90,
                data_hora=now_br(),
            )
            for i, p in enumerate(POSICOES)
        }
        self.posicoes = demo
        try:
            self.print_current_label()
        finally:
            self.posicoes = old
            self._refresh_all()


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
                self.save_cycle_summary_csv()
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
        fieldnames = ["data_hora", "serie", "posicao", "status", "torque", "angulo", "pset", "revision", "tightening_id", "frame_hash", "parser_info", "frame"]
        write_header = not arquivo.exists()
        with arquivo.open("a", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
            if write_header:
                w.writeheader()
            w.writerow({k: registro.get(k, "") for k in fieldnames})

    def save_cycle_csv(self):
        serie = (self.in_serie.text or "SEM_SERIE").replace("/", "_").replace("\\", "_").strip()
        arquivo = CSV_DIR / f"ciclo_final_{serie}_{now_file()}.csv"
        fieldnames = ["data_hora", "serie", "posicao", "status", "torque", "angulo", "pset", "tentativas", "nok_count", "tightening_id"]
        with arquivo.open("w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
            w.writeheader()
            for p in POSICOES:
                stt = self.posicoes[p]
                w.writerow({
                    "data_hora": stt.data_hora,
                    "serie": self.in_serie.text,
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

    def save_cycle_summary_csv(self):
        """
        CSV único acumulativo.
        A cada P8 OK, adiciona uma nova linha com o resumo dos 8 pontos.
        """
        arquivo = CSV_DIR / "ciclos_torque_resumo.csv"

        fieldnames = [
            "finalizado_em",
            "serie",
            "status_geral",
        ]

        for p in POSICOES:
            fieldnames.extend([
                f"{p}_torque",
                f"{p}_status",
                f"{p}_angulo",
                f"{p}_tentativas",
                f"{p}_nok",
                f"{p}_tightening_id",
            ])

        row = {
            "finalizado_em": now_br(),
            "serie": self.in_serie.text or "SEM_SERIE",
            "status_geral": "OK",
        }

        for p in POSICOES:
            stt = self.posicoes[p]
            row[f"{p}_torque"] = to_float_text(stt.torque)
            row[f"{p}_status"] = stt.ultimo_status or stt.status
            row[f"{p}_angulo"] = to_float_text(stt.angulo)
            row[f"{p}_tentativas"] = stt.tentativas
            row[f"{p}_nok"] = stt.nok_count
            row[f"{p}_tightening_id"] = stt.tightening_id

        write_header = not arquivo.exists()
        with arquivo.open("a", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
            if write_header:
                w.writeheader()
            w.writerow(row)

        self.add_log(f"Resumo acumulado atualizado: {arquivo}")

    def print_current_label(self):
        copias = safe_int(getattr(self, "cfg_copias", "1"), 1)
        zpl = gerar_zpl_torque(self.in_serie.text, self.posicoes, copias=copias)

        safe_serie = (self.in_serie.text or "SEM_SERIE").replace("/", "_").replace("\\", "_").strip()
        zpl_path = CSV_DIR / f"etiqueta_torque_{safe_serie}_{now_file()}.zpl"
        zpl_path.write_text(zpl, encoding="utf-8")

        mode = getattr(self, "cfg_print_mode", "USB")
        try:
            if mode == "USB":
                msg = imprimir_zebra_usb_android(zpl)
                self.set_msg(msg)
                self.add_log(msg)
            else:
                self.set_msg(f"ZPL salvo: {zpl_path.name}")
                self.add_log(f"ZPL salvo: {zpl_path}")
        except Exception as e:
            # Nunca perde a etiqueta: se o USB falhar, o ZPL fica salvo.
            self.set_msg(f"Falha USB. ZPL salvo. Erro: {e}")
            self.add_log(f"Erro impressão USB: {e}. ZPL salvo em {zpl_path}")

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
