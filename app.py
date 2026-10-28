"""
Recordatorio de medicamentos por WhatsApp (Meta WhatsApp Cloud API).

Flujo:
  1. A cada hora del horario, envía una plantilla al paciente con botón "Ya la tomé".
  2. Si no confirma en `repeat_after_min`, repite el recordatorio.
  3. Si no confirma en `escalate_after_min`, avisa a los familiares.
  4. Los familiares pueden escribir "resumen" para ver el estado del día.
"""
import hashlib
import hmac
import json
import logging
import os
import sqlite3
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from itertools import groupby
from zoneinfo import ZoneInfo

import httpx
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED, EVENT_JOB_MISSED
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import HTMLResponse

# ---------- Configuración ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("medibot")
WA_TOKEN = os.environ["WA_TOKEN"]
WA_PHONE_ID = os.environ["WA_PHONE_ID"]
VERIFY_TOKEN = os.environ["VERIFY_TOKEN"]
APP_SECRET = os.environ["APP_SECRET"]
PATIENT = os.environ["PATIENT_PHONE"]  # ej. 573001234567 (sin +)
FAMILY = [p.strip() for p in os.environ["FAMILY_PHONES"].split(",") if p.strip()]
DB_PATH = os.environ.get("DB_PATH", os.path.join(os.path.dirname(__file__), "medibot.db"))
GRAPH_URL = f"https://graph.facebook.com/v21.0/{WA_PHONE_ID}/messages"

with open(os.path.join(os.path.dirname(__file__), "schedule.json"), encoding="utf-8") as f:
    CFG = json.load(f)
TZ = ZoneInfo(CFG["timezone"])
REPEAT_MIN = CFG["repeat_after_min"]
ESCALATE_MIN = CFG["escalate_after_min"]

# Agrupa medicamentos que comparten hora
GRUPOS = {
    hora: list(items)
    for hora, items in groupby(
        sorted(CFG["medicamentos"], key=lambda m: m["hora"]), key=lambda m: m["hora"]
    )
}


def lista_texto(meds):
    # Los parámetros de plantilla no admiten saltos de línea: se separa con " | "
    return " | ".join(
        f'{m["nombre"]} ({m["nota"]})' if m.get("nota") else m["nombre"] for m in meds
    )


def hora_bonita(hhmm):
    h, m = map(int, hhmm.split(":"))
    return datetime(2000, 1, 1, h, m).strftime("%I:%M %p").lstrip("0").lower()


# ---------- Base de datos ----------
def db():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con


def init_db():
    db_dir = os.path.dirname(os.path.abspath(DB_PATH))
    os.makedirs(db_dir, exist_ok=True)
    log.info("Inicializando base de datos en: %s", os.path.abspath(DB_PATH))
    with db() as con:
        con.execute(
            """CREATE TABLE IF NOT EXISTS dosis (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fecha TEXT, hora TEXT, meds TEXT,
                enviado TEXT, confirmado TEXT,
                repetido INTEGER DEFAULT 0, escalado INTEGER DEFAULT 0)"""
        )


def ahora():
    return datetime.now(TZ)


# ---------- WhatsApp ----------
async def wa_post(payload):
    async with httpx.AsyncClient(timeout=20) as c:
        r = await c.post(
            GRAPH_URL, json=payload, headers={"Authorization": f"Bearer {WA_TOKEN}"}
        )
        if r.status_code >= 400:
            log.error("WhatsApp rechazó el envío: HTTP %s; respuesta=%s", r.status_code, r.text)
        else:
            try:
                body = r.json()
                msg_id = (body.get("messages") or [{}])[0].get("id", "sin id de mensaje")
            except Exception:
                msg_id = "respuesta sin JSON"
            log.info("WhatsApp aceptó la solicitud: HTTP %s; mensaje=%s", r.status_code, msg_id)
        return r


async def enviar_texto(to, texto):
    # Solo funciona dentro de las 24 h posteriores a un mensaje de esa persona
    return await wa_post(
        {"messaging_product": "whatsapp", "to": to, "type": "text", "text": {"body": texto}}
    )


async def enviar_recordatorio(dosis_id, hora, meds_txt):
    return await wa_post(
        {
            "messaging_product": "whatsapp",
            "to": PATIENT,
            "type": "template",
            "template": {
                "name": "recordatorio_medicamentos",
                "language": {"code": "es"},
                "components": [
                    {
                        "type": "body",
                        "parameters": [
                            {"type": "text", "text": hora_bonita(hora)},
                            {"type": "text", "text": meds_txt},
                        ],
                    },
                    {
                        "type": "button",
                        "sub_type": "quick_reply",
                        "index": "0",
                        "parameters": [{"type": "payload", "payload": f"OK:{dosis_id}"}],
                    },
                ],
            },
        }
    )


async def alertar_familia(hora, meds_txt):
    resultados = []
    for to in FAMILY:
        respuesta = await wa_post(
            {
                "messaging_product": "whatsapp",
                "to": to,
                "type": "template",
                "template": {
                    "name": "alerta_dosis_omitida",
                    "language": {"code": "es"},
                    "components": [
                        {
                            "type": "body",
                            "parameters": [
                                {"type": "text", "text": hora_bonita(hora)},
                                {"type": "text", "text": meds_txt},
                            ],
                        }
                    ],
                },
            }
        )
        resultados.append(respuesta is not None and respuesta.is_success)
    return bool(resultados) and all(resultados)


# ---------- Tareas programadas ----------
async def disparar(hora):
    log.info("TAREA EJECUTADA: recordatorio programado para %s; hora Colombia actual=%s", hora, ahora().isoformat())
    meds_txt = lista_texto(GRUPOS[hora])
    n = ahora()
    with db() as con:
        cur = con.execute(
            "INSERT INTO dosis (fecha, hora, meds, enviado) VALUES (?,?,?,?)",
            (n.date().isoformat(), hora, meds_txt, n.isoformat()),
        )
        dosis_id = cur.lastrowid
    log.info("Dosis registrada en base de datos: id=%s hora=%s", dosis_id, hora)
    try:
        respuesta = await enviar_recordatorio(dosis_id, hora, meds_txt)
        if respuesta is not None and respuesta.is_success:
            log.info("Recordatorio enviado/aceptado por Meta: dosis_id=%s hora=%s", dosis_id, hora)
        else:
            log.error("Recordatorio NO aceptado por Meta: dosis_id=%s hora=%s", dosis_id, hora)
    except Exception:
        log.exception("Excepción enviando recordatorio: dosis_id=%s hora=%s", dosis_id, hora)


async def revisar_pendientes():
    n = ahora()
    with db() as con:
        pend = con.execute(
            "SELECT * FROM dosis WHERE fecha=? AND confirmado IS NULL",
            (n.date().isoformat(),),
        ).fetchall()
        for d in pend:
            t = n - datetime.fromisoformat(d["enviado"])
            if t >= timedelta(minutes=ESCALATE_MIN) and not d["escalado"]:
                try:
                    ok = await alertar_familia(d["hora"], d["meds"])
                    if ok:
                        con.execute("UPDATE dosis SET escalado=1 WHERE id=?", (d["id"],))
                        log.info("Alerta familiar aceptada por Meta: dosis_id=%s", d["id"])
                    else:
                        log.error("No se pudo confirmar el envío a todos los familiares: dosis_id=%s", d["id"])
                except Exception:
                    log.exception("Excepción al alertar a familiares: dosis_id=%s", d["id"])
            elif t >= timedelta(minutes=REPEAT_MIN) and not d["repetido"]:
                try:
                    respuesta = await enviar_recordatorio(d["id"], d["hora"], d["meds"])
                    if respuesta is not None and respuesta.is_success:
                        con.execute("UPDATE dosis SET repetido=1 WHERE id=?", (d["id"],))
                        log.info("Recordatorio repetido aceptado por Meta: dosis_id=%s", d["id"])
                    else:
                        log.error("Meta no aceptó el recordatorio repetido: dosis_id=%s", d["id"])
                except Exception:
                    log.exception("Excepción al repetir recordatorio: dosis_id=%s", d["id"])


def resumen_hoy():
    with db() as con:
        rows = con.execute(
            "SELECT * FROM dosis WHERE fecha=? ORDER BY hora", (ahora().date().isoformat(),)
        ).fetchall()
    if not rows:
        return "Hoy todavía no se ha enviado ningún recordatorio."
    lineas = []
    for r in rows:
        estado = "✅ tomada" if r["confirmado"] else "⏳ sin confirmar"
        lineas.append(f'{hora_bonita(r["hora"])}: {estado}')
    return "Resumen de hoy:\n" + "\n".join(lineas)


# ---------- App ----------
scheduler = AsyncIOScheduler(timezone=TZ)

def registrar_evento_scheduler(event):
    if event.code == EVENT_JOB_MISSED:
        log.error("TAREA OMITIDA por retraso: job_id=%s hora_programada=%s", event.job_id, event.scheduled_run_time)
    elif event.code == EVENT_JOB_ERROR:
        log.error("ERROR en tarea programada: job_id=%s excepción=%s", event.job_id, event.exception)
    elif event.code == EVENT_JOB_EXECUTED:
        log.info("Tarea finalizada: job_id=%s", event.job_id)


@asynccontextmanager
async def lifespan(app):
    init_db()
    scheduler.add_listener(
        registrar_evento_scheduler,
        EVENT_JOB_MISSED | EVENT_JOB_ERROR | EVENT_JOB_EXECUTED,
    )
    for hora in GRUPOS:
        h, m = map(int, hora.split(":"))
        scheduler.add_job(
            disparar, "cron", hour=h, minute=m, args=[hora],
            id=f"recordatorio_{hora.replace(':', '')}",
            misfire_grace_time=300, coalesce=True, max_instances=1, replace_existing=True,
        )
    scheduler.add_job(
        revisar_pendientes, "interval", minutes=1, id="revisar_pendientes",
        coalesce=True, max_instances=1, replace_existing=True,
    )
    scheduler.start()
    log.info("MEDIBOT INICIADO. Zona horaria=%s; hora actual=%s", TZ, ahora().isoformat())
    for job in scheduler.get_jobs():
        log.info("TAREA PROGRAMADA: id=%s próxima_ejecución=%s", job.id, job.next_run_time)
    yield
    log.info("Cerrando programador de MediBot")
    scheduler.shutdown()


app = FastAPI(lifespan=lifespan)


CONTACTO = os.environ.get("CONTACT_EMAIL", "el responsable de este servicio")


def pagina(titulo, cuerpo):
    return HTMLResponse(
        f"<!doctype html><html lang='es'><head><meta charset='utf-8'>"
        f"<meta name='viewport' content='width=device-width, initial-scale=1'>"
        f"<title>{titulo}</title></head>"
        f"<body style='font-family:sans-serif;max-width:640px;margin:2rem auto;padding:0 1rem;line-height:1.5'>"
        f"<h1>{titulo}</h1>{cuerpo}</body></html>"
    )


@app.get("/privacidad")
async def privacidad():
    return pagina(
        "Política de privacidad",
        "<p>Este servicio es de uso familiar y privado. Envía recordatorios de horarios por WhatsApp "
        "a un número y avisa a unos pocos familiares autorizados.</p>"
        "<p>Datos que guarda: los números de teléfono configurados, la lista de horarios y el registro de "
        "las confirmaciones (fecha, hora y si se confirmó). No vende ni comparte estos datos con terceros; "
        "solo se procesan a través de WhatsApp (Meta) para enviar y recibir los mensajes.</p>"
        f"<p>Para consultas o para pedir la eliminación de los datos, escribe a {CONTACTO}.</p>",
    )


@app.get("/terminos")
async def terminos():
    return pagina(
        "Condiciones del servicio",
        "<p>Servicio de recordatorios de uso familiar y privado. Es solo una ayuda para recordar horarios: "
        "no reemplaza el consejo, la indicación ni el seguimiento de un profesional de la salud. "
        "No se garantiza que cada mensaje llegue a tiempo.</p>",
    )


@app.get("/eliminar-datos")
async def eliminar_datos():
    return pagina(
        "Eliminación de datos",
        "<p>Para eliminar los datos asociados a tu número (teléfono y registro de confirmaciones), "
        f"escribe a {CONTACTO} indicando tu número. Los datos se borran del servicio.</p>",
    )


@app.get("/")
async def salud():
    return {"status": "vivo"}


@app.get("/webhook")
async def verificar(request: Request):
    q = request.query_params
    if q.get("hub.mode") == "subscribe" and q.get("hub.verify_token") == VERIFY_TOKEN:
        return Response(content=q.get("hub.challenge"), media_type="text/plain")
    raise HTTPException(403)


def confirmar(dosis_id=None):
    """Confirma una dosis concreta o, si no se indica, la pendiente más reciente de hoy."""
    n = ahora().isoformat()
    with db() as con:
        if dosis_id is None:
            row = con.execute(
                "SELECT id FROM dosis WHERE fecha=? AND confirmado IS NULL ORDER BY id DESC LIMIT 1",
                (ahora().date().isoformat(),),
            ).fetchone()
            if not row:
                return False
            dosis_id = row["id"]
        cur = con.execute(
            "UPDATE dosis SET confirmado=? WHERE id=? AND confirmado IS NULL", (n, dosis_id)
        )
        return cur.rowcount > 0


@app.post("/webhook")
async def recibir(request: Request):
    cuerpo = await request.body()
    firma = request.headers.get("X-Hub-Signature-256", "")
    esperada = "sha256=" + hmac.new(APP_SECRET.encode(), cuerpo, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(firma, esperada):
        raise HTTPException(403)

    data = json.loads(cuerpo)
    for entry in data.get("entry", []):
        for change in entry.get("changes", []):
            for m in change.get("value", {}).get("messages", []):
                origen = m["from"]
                tipo = m.get("type")

                # Botón "Ya la tomé"
                if tipo == "button" and m["button"].get("payload", "").startswith("OK:"):
                    ok = confirmar(int(m["button"]["payload"].split(":")[1]))
                    await enviar_texto(origen, "✅ Anotado, gracias." if ok else "Ya estaba anotada 👍")

                # Texto libre
                elif tipo == "text":
                    txt = m["text"]["body"].strip().lower()
                    if origen in FAMILY and "resumen" in txt:
                        await enviar_texto(origen, resumen_hoy())
                    elif origen == PATIENT and txt in {"ya", "listo", "tomé", "tome", "ok", "ya la tomé", "ya las tomé"}:
                        ok = confirmar()
                        await enviar_texto(origen, "✅ Anotado, gracias." if ok else "No tengo nada pendiente 👍")
    return {"status": "ok"}