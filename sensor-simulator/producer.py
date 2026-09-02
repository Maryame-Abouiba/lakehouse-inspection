import json
import random
import time
from datetime import datetime, timezone
from kafka import KafkaProducer

# Liste des zones existantes dans ton use case (à adapter selon ton HBIM)
ZONES = ["ZONE01", "ZONE02"]

# Bornes physiques plausibles (utilisées aussi pour générer volontairement
# des valeurs aberrantes de temps en temps, pour tester tes transformations Silver)
TEMP_MIN, TEMP_MAX = 15.0, 35.0
HUMIDITY_MIN, HUMIDITY_MAX = 30.0, 90.0

def generate_reading():
    """Génère une mesure de capteur simulée, avec parfois une anomalie volontaire."""
    zone = random.choice(ZONES)
    sensor_id = f"S{ZONES.index(zone)+1:03d}"
    
    # 5% de chance de générer une valeur manquante
    if random.random() < 0.05:
        temperature = None
    # 5% de chance de générer une valeur aberrante
    elif random.random() < 0.05:
        temperature = round(random.uniform(80, 150), 1)  # valeur impossible
    else:
        temperature = round(random.uniform(TEMP_MIN, TEMP_MAX), 1)
    
    if random.random() < 0.05:
        humidity = None
    else:
        humidity = round(random.uniform(HUMIDITY_MIN, HUMIDITY_MAX), 1)
    
    return {
        "sensor_id": sensor_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "zone_id": zone,
        "temperature": temperature,
        "humidity": humidity,
    }

def main():
    # Connexion au broker Kafka. "kafka:9092" fonctionne si ce script tourne
    # lui-même dans un conteneur Docker sur le même réseau.
    # Si tu le lances depuis ta machine (hors Docker), utilise "localhost:9092".
    producer = KafkaProducer(
        bootstrap_servers="kafka:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print("Simulateur de capteur démarré. Envoi vers le topic 'sensor-readings'...")

    while True:
        reading = generate_reading()
        producer.send("sensor-readings", value=reading)
        producer.flush()  # force l'envoi immédiat (utile pour bien voir les messages arriver)
        print(f"Message envoyé : {reading}")
        time.sleep(10)  # attend 10 secondes avant la prochaine mesure

if __name__ == "__main__":
    main()