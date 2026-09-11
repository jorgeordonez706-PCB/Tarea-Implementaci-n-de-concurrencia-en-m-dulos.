import threading
import time
import random

BUFFER_CAPACITY = 5
buffer_compartido = []

espacios_vacios = threading.Semaphore(BUFFER_CAPACITY)
datos_disponibles = threading.Semaphore(0)
mutex = threading.Semaphore(1)


def generar_dato_trafico(id_sensor):
    return f"[Sensor {id_sensor} | Vehículos: {random.randint(10, 50)}/min]"


def insertar_en_buffer(dato):
    espacios_vacios.acquire()
    mutex.acquire()
    buffer_compartido.append(dato)
    print(f"🔼 PRODUCCIÓN: {dato} añadido. | Ocupación búfer: {len(buffer_compartido)}/{BUFFER_CAPACITY}")
    mutex.release()
    datos_disponibles.release()


def extraer_de_buffer(id_modulo):
    datos_disponibles.acquire()
    mutex.acquire()
    dato = buffer_compartido.pop(0)
    print(f"🔽 CONSUMO: Módulo {id_modulo} procesando {dato}. | Ocupación búfer: {len(buffer_compartido)}/{BUFFER_CAPACITY}")
    mutex.release()
    espacios_vacios.release()
    return dato


def sensor_trafico(id_sensor):
    for _ in range(4):
        time.sleep(random.uniform(0.5, 1.5))
        dato = generar_dato_trafico(id_sensor)
        insertar_en_buffer(dato)


def modulo_analisis(id_modulo):
    while True:
        extraer_de_buffer(id_modulo)
        time.sleep(random.uniform(1.0, 2.5))


def buffer_esta_vacio():
    mutex.acquire()
    vacio = len(buffer_compartido) == 0
    mutex.release()
    return vacio


def esperar_a_que_termine_el_procesamiento():
    while True:
        if buffer_esta_vacio():
            time.sleep(0.5)
            if buffer_esta_vacio():
                return
        time.sleep(0.2)


def iniciar_modulos_analisis(cantidad):
    for i in range(cantidad):
        hilo = threading.Thread(target=modulo_analisis, args=(i + 1,))
        hilo.daemon = True
        hilo.start()


def iniciar_sensores(cantidad):
    hilos = []
    for i in range(cantidad):
        hilo = threading.Thread(target=sensor_trafico, args=(i + 1,))
        hilo.start()
        hilos.append(hilo)
    return hilos


def esperar_sensores(hilos):
    for hilo in hilos:
        hilo.join()


if __name__ == "__main__":
    print("Iniciando Sistema de Gestión de Tráfico (SIGET)...")
    print("-" * 50)

    iniciar_modulos_analisis(2)
    hilos_sensores = iniciar_sensores(2)
    esperar_sensores(hilos_sensores)
    esperar_a_que_termine_el_procesamiento()

    print("-" * 50)
    print("Restantes en búfer:", len(buffer_compartido))
    print("Simulación finalizada: Todos los datos han sido procesados sin corrupción.")
