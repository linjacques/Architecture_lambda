# Spark Streaming + Kafka + Docker

Ce projet montre comment connecter **Apache Spark** (Structured Streaming) à **Apache Kafka** via **Docker**, pour effectuer des agrégations en temps réel et exporter les résultats vers le file system.


## Lancer le projet

### 1. Lancer les containers Confluent Kafka

```bash
docker-compose up -d
```

### 2. Construire et connecter le container Spark à Kafka

> ⚠️ Spark doit être sur le même **Docker network** que Kafka pour accéder au broker !

**Construire le container Pyspark** :

```bash
docker compose up --build -d
```

**Puis connecte le container Spark au réseau de Kafka :**

```bash
docker network connect cp-all-in-one_default pyspark
```

Où :

* `cp-all-in-one_default` est le nom du **réseau Docker** utilisé par Confluent Kafka
* `pyspark` est le nom du **container Spark**

> ⚠️ après avoir connecté le container pyspark au container confluent kafka, il faut impérativement changer le port de bootstrap_servers par :

```python
bootstrap_servers = "broker:9092"  # broker correspond au nom du container de confluent kafka
```
Dans le `main.py` 


### 3. Démarrer l'application

à la racine du projet, où se trouve le fichier docker-compose.yml :

```bash
docker compose up -d
```

