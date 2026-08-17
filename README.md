# Simulateur du Bundle Protocol (DTN)

Simulation à événements discrets d'un réseau **DTN (Delay/Disruption Tolerant
Networking)** implémentant le **Bundle Protocol**, un protocole conçu pour les
réseaux où les connexions sont instables ou intermittentes (communications
spatiales, réseaux satellites, zones sans infrastructure réseau fiable).

## Ce que fait le projet

Le simulateur modélise un réseau de nœuds qui s'échangent des "bundles"
(paquets de données) à travers un réseau instable, avec :

- **Routage dynamique** : calcul des routes par BFS (parcours en largeur),
  avec recalcul automatique en cas de route invalide
- **Custody transfer** : mécanisme où un nœud intermédiaire prend la
  responsabilité de la fiabilité d'un bundle, le libérant du nœud précédent
- **Accusés de réception (ACK)** avec gestion de timeout et retransmission
  automatique (jusqu'à 3 tentatives)
- **Expiration des bundles** (TTL) : les paquets trop anciens sont abandonnés
- **Coupures de liaison simulées** : 10% de chance d'échec de transmission à
  chaque saut, avec durée de coupure aléatoire
- **Plusieurs topologies réseau** configurables : liaison unique, liaison en
  chaîne, ou maillage avec liens aléatoires additionnels

À la fin d'une simulation, le programme affiche des statistiques complètes :
taux de livraison, nombre moyen de sauts par bundle, temps de livraison
moyen, nombre de transferts de custody, nombre de retransmissions, etc.

## Stack technique

- **Python 3**
- **[SimPy](https://simpy.readthedocs.io/)** — framework de simulation à
  événements discrets, utilisé pour modéliser le temps, la concurrence entre
  nœuds et les délais réseau
- Structures de données custom (table de routage, files de stockage borné,
  gestion d'état par bundle)

## Résultat d'exemple

Sur une simulation de 20 nœuds en topologie maillée sur 1000 unités de temps :

```
Bundles créés: 460
Bundles livrés avec succès: 458
Taux de livraison : 99.6%
Transferts de custody: 471
Nombre de nœuds moyen parcouru par bundle : 3.24
Temps de livraison moyen: 0.34
Nombre de liaisons: 24
```

## Lancer le projet

```bash
pip install simpy
python3 bundle_protocol.py
```

Le fichier contient 3 scénarios en commentaire en bas du fichier (liaison
unique, liaison multiple simple, liaison multiple avec maillage) — décommenter
celui à tester.

## Ce que ce projet démontre

- Modélisation d'un système distribué et concurrent avec gestion d'état
- Compréhension de protocoles réseau réels (inspiré du RFC 5050 / Bundle
  Protocol du DTN Research Group, utilisé par la NASA pour les communications
  spatiales)
- Simulation à événements discrets avec SimPy
- Algorithmes de graphes (BFS pour le routage)
- Gestion de la fiabilité dans des environnements réseau dégradés (ACK,
  retransmission, custody transfer, TTL)

---

