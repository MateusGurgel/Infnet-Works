import threading
import random
import uuid
from datetime import datetime
from kafka import KafkaProducer
from json import dumps
from time import sleep

TOPICO = "stream_apostas"
KAFKA_BROKERS = ["localhost:9092", "localhost:9092"]

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKERS,
    value_serializer=lambda v: dumps(v).encode("utf-8")
)

apostadores = [f"u{i}" for i in range(1, 101)]
jogos = [f"jogo{i}" for i in range(1, 101)]

def gerar_aposta():
    valor_base = random.uniform(10, 5000)
    odd_base = random.uniform(1.2, 10.0)

    # Gerar apostas suspeitas em 3% dos casos
    if random.random() < 0.03:
        valor_base = random.uniform(12000, 20000)
        odd_base = random.uniform(15.0, 25.0)

    return {
        "aposta_id": str(uuid.uuid4())[:8],
        "apostador_id": random.choice(apostadores),
        "jogo_id": random.choice(jogos),
        "valor": round(valor_base, 2),
        "odd": round(odd_base, 2),
        "timestamp": datetime.utcnow().isoformat()
    }


def enviar_apostas(thread_id):
    while True:
        for _ in range(random.randint(5, 10)):
            aposta = gerar_aposta()
            producer.send(TOPICO, aposta)
            print(f"[Thread-{thread_id}] Enviada: {aposta}")
        sleep(random.uniform(1, 3))

if __name__ == "__main__":
    print("Iniciando envio de apostas com múltiplas threads...")
    threads = []
    for i in range(4):
        t = threading.Thread(target=enviar_apostas, args=(i,))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()#
