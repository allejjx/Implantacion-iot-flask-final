"""recoge posiciones de opensky y las publica en mqtt."""
import json
import logging
import os
import random
import time
from collections import defaultdict

import paho.mqtt.client as mqtt
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("collector")
api_url = "https://opensky-network.org/api/states/all"
auth_url = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
client_id = os.getenv("OPENSKY_CLIENT_ID", "").strip()
client_secret = os.getenv("OPENSKY_CLIENT_SECRET", "").strip()
authenticated = bool(client_id and client_secret)
max_aviones = max(100, int(os.getenv("OPENSKY_MAX_AVIONES", "1500")))
# la consulta global cuesta 4 creditos; 1200 s equivalen a 288 creditos/dia.
intervalo = max(1200 if not authenticated else 60, int(os.getenv("OPENSKY_INTERVAL_SECONDS", "1200" if not authenticated else "300")))
reserva_creditos = max(0, int(os.getenv("OPENSKY_MIN_CREDITS", "24")))

session = requests.Session()
session.headers.update({"User-Agent": "ASIR-OpenSky-Dashboard/2.0", "Accept": "application/json"})
token = None
expira = 0


def autorizacion():
    # usa oauth2 cuando el usuario configure sus credenciales.
    global token, expira
    if not authenticated:
        return {}
    if not token or time.time() > expira - 120:
        respuesta = session.post(auth_url, data={
            "grant_type": "client_credentials",
            "client_id": client_id, "client_secret": client_secret,
        }, timeout=(10, 20))
        respuesta.raise_for_status()
        datos = respuesta.json()
        token = datos["access_token"]
        expira = time.time() + int(datos.get("expires_in", 300))
    return {"Authorization": f"Bearer {token}"}


def pausa_api(respuesta, defecto):
    # respeta el tiempo de espera indicado por opensky al devolver 429.
    for cabecera in ("X-Rate-Limit-Retry-After-Seconds", "Retry-After"):
        valor = respuesta.headers.get(cabecera)
        try:
            if valor is not None:
                return max(60, int(float(valor)) + 10)
        except (ValueError, TypeError):
            pass
    return defecto


def seleccionar_aviones(estados):
    # se reparte el cupo por cuadrantes para que no aparezcan solo aviones europeos.
    grupos = defaultdict(list)
    vistos = set()
    for a in estados:
        if not a or len(a) < 11 or not a[0] or a[5] is None or a[6] is None:
            continue
        try:
            lat, lon = float(a[6]), float(a[5])
            if not (-90 <= lat <= 90 and -180 <= lon <= 180):
                continue
            icao = str(a[0]).strip()
            if icao in vistos:
                continue
            vistos.add(icao)
            avion = {
                "icao24": icao,
                "pais": a[2] or "Desconocido",
                "latitud": lat,
                "longitud": lon,
                "altitud": float(a[7]) if a[7] is not None else None,
                "velocidad": float(a[9]) if a[9] is not None else None,
                "direccion": float(a[10]) if a[10] is not None else None,
                "en_tierra": bool(a[8]),
            }
        except (ValueError, TypeError):
            continue
        celda = (int((lat + 90) // 15), int((lon + 180) // 20))
        grupos[celda].append(avion)

    # mezclar de forma estable evita priorizar las matrículas alfabéticas.
    for grupo in grupos.values():
        grupo.sort(key=lambda a: a["icao24"])
    celdas = sorted(grupos)
    seleccion = []
    while celdas and len(seleccion) < max_aviones:
        disponibles = []
        for celda in celdas:
            seleccion.append(grupos[celda].pop(0))
            if grupos[celda]:
                disponibles.append(celda)
            if len(seleccion) >= max_aviones:
                break
        celdas = disponibles
    return seleccion


def conectar_mqtt():
    # se reintenta la conexión local sin consumir peticiones de opensky.
    cliente = mqtt.Client(client_id="opensky-collector", clean_session=True)
    cliente.reconnect_delay_set(min_delay=2, max_delay=60)
    while True:
        try:
            cliente.connect("mosquitto", 1883, keepalive=60)
            cliente.loop_start()
            return cliente
        except OSError as error:
            log.warning("mqtt no disponible: %s", error)
            time.sleep(10)


def main():
    global token, expira
    cliente = conectar_mqtt()
    fallos = 0
    log.info("cobertura mundial, intervalo=%ss, max_aviones=%s, oauth=%s", intervalo, max_aviones, authenticated)
    try:
        while True:
            inicio = time.monotonic()
            espera = intervalo
            try:
                respuesta = session.get(api_url, headers=autorizacion(), timeout=(10, 90))
                if respuesta.status_code == 429:
                    espera = pausa_api(respuesta, 7200)
                    log.warning("limite de opensky alcanzado; pausa de %s segundos", espera)
                    fallos = 0
                elif respuesta.status_code == 401 and authenticated:
                    token, expira = None, 0
                    raise requests.HTTPError("oauth2 no autorizado; comprueba client_id y client_secret")
                else:
                    respuesta.raise_for_status()
                    estados = respuesta.json().get("states") or []
                    aviones = seleccionar_aviones(estados)
                    for avion in aviones:
                        # solo se publican posiciones reales, nunca coordenadas simuladas.
                        cliente.publish("aviones/opensky", json.dumps(avion), qos=0)
                    restantes = respuesta.headers.get("X-Rate-Limit-Remaining")
                    log.info("recibidos=%d publicados=%d creditos_restantes=%s", len(estados), len(aviones), restantes)
                    fallos = 0
                    if restantes is not None and int(restantes) <= reserva_creditos:
                        # cuando quedan pocos creditos no se insiste hasta el siguiente ciclo de cuota.
                        espera = pausa_api(respuesta, 24 * 3600)
                        log.warning("reserva de creditos alcanzada; pausa %ss", espera)
            except (requests.RequestException, ValueError, KeyError, TypeError) as error:
                fallos += 1
                espera = min(7200, max(intervalo, 120 * 2 ** min(fallos, 5)))
                log.warning("consulta fallida (%s): %s; nueva prueba en %ss", fallos, error, espera)
            time.sleep(max(1, espera - (time.monotonic() - inicio)) + random.uniform(0, 4))
    finally:
        cliente.loop_stop()
        cliente.disconnect()
        session.close()


if __name__ == "__main__":
    main()
