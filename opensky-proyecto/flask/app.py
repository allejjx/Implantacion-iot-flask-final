"""dashboard histórico: consulta postgresql y prepara los gráficos svg."""
import os

import psycopg
from flask import Flask, render_template

app = Flask(__name__)
ANCHO, ALTO, MARGEN_X, MARGEN_Y = 800, 300, 55, 35


def conectar():
    # todas las consultas usan las credenciales actuales del compose.
    return psycopg.connect(
        host=os.environ["DB_HOST"], port=os.getenv("DB_PORT", "5432"),
        dbname=os.environ["DB_NAME"], user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"], connect_timeout=5,
    )


def obtener_datos():
    # una sola conexión para las dos gráficas y las dos tablas.
    with conectar() as conexion, conexion.cursor() as cursor:
        cursor.execute("""
            SELECT fecha_captura, SUM(total_aviones)::integer
            FROM historico_estadisticas
            WHERE fecha_captura >= NOW() - INTERVAL '7 days'
            GROUP BY fecha_captura ORDER BY fecha_captura
        """)
        cantidades = cursor.fetchall()
        cursor.execute("""
            SELECT fecha_captura, velocidad_media FROM historico_velocidad
            WHERE fecha_captura >= NOW() - INTERVAL '7 days'
            ORDER BY fecha_captura
        """)
        velocidades = cursor.fetchall()
        cursor.execute("""
            SELECT p.nombre, h.total_aviones,
                   ROUND(100.0 * h.total_aviones / NULLIF(SUM(h.total_aviones) OVER (), 0), 1),
                   h.fecha_captura
            FROM historico_estadisticas h JOIN paises p ON p.id = h.pais_id
            WHERE h.fecha_captura = (
                SELECT MAX(fecha_captura) FROM historico_estadisticas
                WHERE fecha_captura >= NOW() - INTERVAL '7 days'
            )
            ORDER BY h.total_aviones DESC, p.nombre LIMIT 20
        """)
        actual = cursor.fetchall()
        cursor.execute("""
            SELECT p.nombre, COUNT(*)::integer,
                   ROUND(AVG(h.total_aviones)::numeric, 1),
                   ROUND(AVG(h.velocidad_media)::numeric, 1),
                   ROUND(AVG(h.altitud_media)::numeric, 1)
            FROM historico_estadisticas h JOIN paises p ON p.id = h.pais_id
            WHERE h.fecha_captura >= NOW() - INTERVAL '7 days'
            GROUP BY p.nombre ORDER BY 3 DESC, p.nombre LIMIT 20
        """)
        medias = cursor.fetchall()
    return cantidades, velocidades, actual, medias


def preparar_grafica(registros, entero=False):
    # convierte los registros en coordenadas svg y conserva las fechas originales.
    if not registros:
        return {"puntos": [], "linea": "", "minimo": 0, "maximo": 0, "capturas": 0}
    valores = [float(valor) for _, valor in registros]
    minimo, maximo = min(valores), max(valores)
    margen = max((maximo - minimo) * 0.25, 1)
    base = minimo - margen
    amplitud = maximo - minimo + 2 * margen
    puntos = []
    for indice, (fecha, valor) in enumerate(registros):
        x = MARGEN_X + indice * (ANCHO - 2 * MARGEN_X) / max(1, len(registros) - 1)
        y = ALTO - MARGEN_Y - (float(valor) - base) / amplitud * (ALTO - 2 * MARGEN_Y)
        puntos.append({"x": round(x, 2), "y": round(y, 2),
                       "fecha": fecha.strftime("%d/%m/%Y %H:%M"),
                       "valor": int(valor) if entero else round(float(valor), 2)})
    return {"puntos": puntos, "linea": " ".join(f"{p['x']},{p['y']}" for p in puntos),
            "minimo": int(minimo) if entero else round(minimo, 2),
            "maximo": int(maximo) if entero else round(maximo, 2), "capturas": len(puntos)}


@app.route("/")
def inicio():
    # no se llama a opensky desde flask; las visitas no consumen creditos.
    try:
        cantidades, velocidades, actual, medias = obtener_datos()
        error = None
    except psycopg.Error:
        app.logger.exception("error leyendo datos historicos")
        cantidades, velocidades, actual, medias = [], [], [], []
        error = "No se puede consultar PostgreSQL. Revisa las credenciales y los registros del contenedor."
    return render_template("index.html", aviones=preparar_grafica(cantidades, entero=True),
                           velocidades=preparar_grafica(velocidades), tabla_actual=actual,
                           tabla_medias=medias, error=error)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
