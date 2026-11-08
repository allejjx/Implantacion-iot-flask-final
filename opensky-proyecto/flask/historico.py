import logging
import os
import time
from collections import defaultdict

import psycopg
from influxdb_client import InfluxDBClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
INTERVALO_SEGUNDOS = max(300, int(os.getenv("HISTORICO_INTERVAL_SECONDS", "1200")))
RETENCION_DIAS = 7


def conectar():
    return psycopg.connect(
        host=os.environ["DB_HOST"], port=os.environ["DB_PORT"],
        dbname=os.environ["DB_NAME"], user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
    )


def obtener_estados_recientes():
    cliente = InfluxDBClient(
        url=os.environ["INFLUX_URL"], token=os.environ["INFLUX_TOKEN"],
        org=os.environ["INFLUX_ORG"], timeout=30000,
    )
    consulta = '''
        from(bucket: "aviones")
            |> range(start: -45m)
            |> filter(fn: (r) => r._field == "velocidad" or r._field == "altitud")
            |> group(columns: ["icao24", "pais", "_field"])
            |> last()
    '''
    try:
        tablas = cliente.query_api().query(consulta)
        aeronaves = {}
        for tabla in tablas:
            for registro in tabla.records:
                icao = registro.values.get("icao24")
                pais = registro.values.get("pais") or "Desconocido"
                campo = registro.get_field()
                valor = registro.get_value()
                if not icao or valor is None:
                    continue
                aeronaves.setdefault((pais, icao), {})[campo] = float(valor)
        return aeronaves
    finally:
        cliente.close()


def guardar_captura():
    # guardar una captura al ritmo de la api, no cada cinco minutos.
    aeronaves = obtener_estados_recientes()
    if not aeronaves:
        logging.warning("No hay datos recientes, no se guardan capturas antiguas")
        return
    agrupados = defaultdict(lambda: {"total": 0, "velocidades": [], "altitudes": []})
    for (pais, _icao), datos in aeronaves.items():
        agrupados[pais]["total"] += 1
        if "velocidad" in datos:
            agrupados[pais]["velocidades"].append(datos["velocidad"])
        if "altitud" in datos:
            agrupados[pais]["altitudes"].append(datos["altitud"])
    velocidades_todas = [d["velocidad"] for d in aeronaves.values() if "velocidad" in d]
    velocidad_total = (sum(velocidades_todas) / len(velocidades_todas)) if velocidades_todas else None
    with conectar() as conexion:
        with conexion.cursor() as cursor:
            # actualizar las estadísticas y guardar los datos de la última captura.
            for pais, datos in agrupados.items():
                velocidades, altitudes = datos["velocidades"], datos["altitudes"]
                media_v = sum(velocidades) / len(velocidades) if velocidades else None
                media_a = sum(altitudes) / len(altitudes) if altitudes else None
                cursor.execute("INSERT INTO paises (nombre) VALUES (%s) ON CONFLICT (nombre) DO NOTHING", (pais,))
                cursor.execute("""
                    INSERT INTO estadisticas_pais
                        (pais_id, total_aviones, velocidad_media, altitud_media, fecha_actualizacion)
                    SELECT id, %s, %s, %s, CURRENT_TIMESTAMP FROM paises WHERE nombre = %s
                    ON CONFLICT (pais_id) DO UPDATE SET
                        total_aviones = EXCLUDED.total_aviones,
                        velocidad_media = EXCLUDED.velocidad_media,
                        altitud_media = EXCLUDED.altitud_media,
                        fecha_actualizacion = CURRENT_TIMESTAMP
                """, (datos["total"], media_v, media_a, pais))
                cursor.execute("""
                    INSERT INTO historico_estadisticas
                        (pais_id, total_aviones, velocidad_media, altitud_media, fecha_captura)
                    SELECT id, %s, %s, %s, CURRENT_TIMESTAMP FROM paises WHERE nombre = %s
                """, (datos["total"], media_v, media_a, pais))
            if velocidad_total is not None:
                cursor.execute("INSERT INTO historico_velocidad (velocidad_media, fecha_captura) VALUES (%s, CURRENT_TIMESTAMP)", (velocidad_total,))
            cursor.execute("DELETE FROM historico_estadisticas WHERE fecha_captura < CURRENT_TIMESTAMP - INTERVAL '7 days'")
            cursor.execute("DELETE FROM historico_velocidad WHERE fecha_captura < CURRENT_TIMESTAMP - INTERVAL '7 days'")
    logging.info("Captura de %s paises y %s aeronaves; velocidad media: %s", len(agrupados), len(aeronaves), velocidad_total)


if __name__ == "__main__":
    while True:
        try:
            guardar_captura()
        except Exception as error:
            logging.exception("Error guardando el historico: %s", error)
        time.sleep(INTERVALO_SEGUNDOS)
