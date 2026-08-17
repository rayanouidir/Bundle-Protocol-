import simpy
import random
from collections import defaultdict
from collections import deque


class Node:
    def __init__(self, env, node_id, storage_capacity, bundle_protocol):
        self.env = env
        self.node_id = node_id
        self.storage_capacity = storage_capacity
        self.storage = simpy.Store(env, capacity=storage_capacity)
        self.neighbors = []
        self.routing_table = {}
        self.bundle_protocol = bundle_protocol # Permet d'utiliser la classe BundleProtocol
        # Statistiques
        self.delivered = 0
        self.dropped_bundles = 0
        self.forwarded_bundles = 0
        self.received_bundles = 0
        self.delivery_times = []
        self.custody_bundles = defaultdict(int)
        self.custody_transfers = 0
        self.bundle_ttl = 2.0 
        self.pending_acks = {}  
        self.ack_timeout = 0.5  
        self.nbr_tentativepernode = 0


    def update_routing(self, destination, next_hop):
        self.routing_table[destination] = next_hop # Ajoute le dernier noeud à la fin du routage 
        
    def add_neighbor(self, neighbor): 
        if neighbor not in self.neighbors: # On vérifie si le voisin n'est pas déjà dans la liste
            self.neighbors.append(neighbor) # On l'ajoute dans la liste 
            self.update_routing(neighbor.node_id, neighbor) # On met à jour la table de routage avec le voisin

    def find_alternative_route(self, destination): # On cherche une route alternative
        current_next_hop = self.routing_table.get(destination) # On récupère le prochain noeud
        for neighbor in self.neighbors: # On parcourt les voisins
            if neighbor != current_next_hop: # On vérifie si le voisin n'est pas le prochain noeud
                return neighbor # On retourne le voisin
        return None

    def bundle_expired(self, bundle): # On vérifie si le bundle a expiré
        current_time = self.env.now # On récupère le temps actuel
        return (current_time - bundle['Durée_envoie']) > self.bundle_ttl # On vérifie si le temps écoulé est supérieur à la durée de vie du bundle


    def send_ack(self, bundle_id, destination_node,custody):
        """Envoie un ACK pour confirmer la réception d'un bundle"""
    
        ack_bundle = { # On définit le bundle ack 
            'id': f"ACK_{bundle_id}",
            'source': self.node_id,
            'destination': destination_node,
            'Durée_envoie': self.env.now,
            'custodian': None,
            'coupures': [],
            'coupures total': 0,
            'is_ack': True,
            'bundle_id': bundle_id,
            'Custody': custody, # Boolean définit pour savoir si c'est un ack custody ou non
            'Tentative': 0,
            'noeud_parcouru' : 0,
            "Livraison reussie" : 0
        }
        
        print(f"Temps {self.env.now:.2f}: Node {self.node_id} envoie ACK pour bundle {bundle_id} à Node {destination_node}")
        self.env.process(self.forward_bundle(ack_bundle)) # On l'envoie et on l'ajoute dans les bundles forwardés 


    def handle_ack(self, ack_bundle):
        original_bundle_id = ack_bundle['bundle_id'] # On récupère l'id du bundle d'origine
        if original_bundle_id in self.pending_acks: # On vérifie si le bundle d'origine est dans les ACK en attente
            print(f"Temps {self.env.now:.2f}: Node {self.node_id} reçoit ACK pour bundle {original_bundle_id}")
            del self.pending_acks[original_bundle_id] # On supprime le bundle d'origine des ACK en attente
            
            return True
        return False

    def check_ack_timeout(self, bundle_id):
        yield self.env.timeout(self.ack_timeout) # On attend le timeout
        if bundle_id in self.pending_acks: # On vérifie si le bundle d'origine est dans les ACK en attente
            print(f"Temps {self.env.now:.2f}: Timeout ACK pour bundle {bundle_id}") 
            self.pending_acks[bundle_id]['retries'] += 1 # On incrémente le nombre de tentatives
            if self.pending_acks[bundle_id]['retries'] < 3:  # Nombre max de tentatives
                print(f"Temps {self.env.now:.2f}: Retransmission bundle {bundle_id} (tentative {self.pending_acks[bundle_id]['retries']})")
                
            else: # On abandonne le bundle ack après 3 tentatives
                print(f"Temps {self.env.now:.2f}: Abandon bundle {bundle_id} après 3 tentatives")
                val = self.bundle_protocol.get_node_by_id(self.pending_acks[bundle_id])# On extrait le bundle d'origine
                del self.pending_acks[bundle_id] # On supprime le bundle d'origine des ACK en attente
                self.env.process(self.forward_bundle(val))# On l'envoie 
            


    def forward_bundle(self, bundle):
        self.nbr_tentativepernode +=1
        
  
        while True:
            # Vérifier l'expiration du bundle
            if self.bundle_expired(bundle):
                
                print(f"Temps {self.env.now:.2f}: Bundle {bundle['id']}  expiré après {self.bundle_ttl}s")
                bundle['Durée_envoie'] = self.bundle_ttl
                self.dropped_bundles += 1 # On incrémente le nombre de bundles dropés
                if bundle['id'] in self.pending_acks: # On vérifie si le bundle d'origine est dans les ACK en attente
                    del self.pending_acks[bundle['id']] # On supprime le bundle d'origine des ACK en attente
                return
                
            # Temps de transmission
            yield self.env.timeout(0.05) 
            
            next_hop = self.routing_table.get(bundle['destination']) # On récupère le prochain noeud
         
          
            if next_hop and next_hop in self.neighbors : # On vérifie si le prochain noeud est un voisin
                if random.random() > 0.1:  # 90% de chance de succès
                    print(f"Temps {self.env.now:.2f}: Node {self.node_id} forwarde bundle {bundle['id']} vers Node {next_hop.node_id}")
                    
                    # Si ce n'est pas un ACK et que le bundle vient d'être créé (premier envoi)
                    if  not bundle.get('is_ack', False) and bundle.get('source') == self.node_id: 
                        self.pending_acks[bundle['id']] = { # On ajoute le bundle d'origine dans les ACK en attente
                            'bundle': bundle,
                            'retries': 0,
                            'next_hop': next_hop.node_id
                        }
                        
                        self.env.process(self.check_ack_timeout(bundle['id'])) # On vérifie le timeout
                    self.forwarded_bundles += 1 
                    self.env.process(next_hop.receive_bundle(bundle)) # On appel la fonction concernant le reçu du bundle 
                    return
                
                else: # Si on n'a pas réussi à transmettre le bundle
                    print(f"Temps {self.env.now:.2f}: Échec transmission bundle {bundle['id']}") 
                    coupure = random.uniform(0,1)

                    bundle['coupures'].append(coupure)
                    bundle["coupures total"]+=coupure
                    yield self.env.timeout(coupure)  #duree de la coupure 

                    
            else: # Si pas de route valide
                print(f"Temps {self.env.now:.2f}: Pas de route valide pour bundle {bundle['id']}")
                
                alternative = self.find_alternative_route(bundle['destination'])
                if alternative:
                    self.update_routing(bundle['destination'], alternative)
                yield self.env.timeout(1.0)  # Attendre avant de réessayer

    def receive_bundle(self, bundle):
        if self.bundle_expired(bundle): # On vérifie si le bundle a expiré
            print(f"Temps {self.env.now:.2f}: Bundle {bundle['id']}  expiré après {self.bundle_ttl}s")
            bundle['Durée_envoie'] = self.bundle_ttl
            self.dropped_bundles += 1 # On incrémente le nombre de bundles dropés
            return

        if bundle.get('is_ack', False): # On vérifie si c'est un ACK
            self.handle_ack(bundle) # On traite l'ACK
            return

        if bundle['destination'] == self.node_id: # Si le bundle est arrivé à destination
            if bundle.get('custodian') == self.node_id: # On vérifie si le noeud est le custodian
                    self.custody_bundles.pop(bundle['id'], None) # On supprime le bundle des bundles qui sont sous la responsabilité d'un noeud 
            self.delivered += 1 # On incrémente le nombre de bundles livrés
            bundle["Livraison reussie"] = 1
            bundle["noeud_parcouru"]+=1
            bundle['Durée_envoie'] = self.env.now - bundle['Durée_envoie']
            delivery_time = self.env.now - bundle['Durée_envoie'] # On calcule le temps de livraison
            self.delivery_times.append(delivery_time) 
            print(f"Bundle {bundle['id']} livré après {delivery_time:.2f}s")
            self.send_ack(bundle['id'], bundle.get('last_hop', bundle['source']), False) # On envoie un ACK au noeud précédent
            return
            
        
        print(f"Temps {self.env.now:.1f}: Node {self.node_id} reçoit bundle {bundle['id']}") 
        self.received_bundles += 1 # On incrémente le nombre de bundles reçus
        if bundle.get('source') != self.node_id:  # Ne pas envoyer d'ACK pour les bundles qu'on a créés
                self.send_ack(bundle['id'], bundle.get('last_hop', bundle['source']), False) # On envoie un ack au noeud précédent
            

        # Gestion custody transfer
        if  bundle['destination'] != self.node_id: # Si le bundle n'est pas arrivé à destination
            if bundle.get('custodian') is None : # Si le bundle n'est pas sous la responsabilité d'un noeud
                if random.random() < 0.6  :  # Probabilité d'accepter la custody
                    if len(self.storage.items) < self.storage.capacity : # On vérifie si le stockage est plein
                        yield self.storage.put(bundle) # On ajoute le bundle dans le stockage
                        bundle['custodian'] = self.node_id # Le noeud devient le custodian
                        self.custody_bundles[bundle['id']] += 1
                        self.custody_transfers += 1
                        print(f"Temps {self.env.now:.2f}: Node {self.node_id} prend custody du bundle {bundle['id']}")
    
                    else:
                        # Si stockage plein, réessayer plus tard
                        print(f"Temps {self.env.now:.2f}: Stockage plein sur node {self.node_id}, réessai plus tard")
                        yield self.env.timeout(2.0)
                        self.env.process(self.receive_bundle(bundle)) # On réessaie de recevoir le bundle
            else : 
                if random.random() < 0.3  :  # Probabilité d'accepter la custody
                    if len(self.storage.items) < self.storage.capacity : # On vérifie si le stockage est plein
                        previous_node = bundle.get('custodian') # On récupère l'id de l'ancien noeud responsable 
                        val = self.bundle_protocol.get_node_by_id(previous_node) # A partir de l'id, on récupère le noeud
                        for i, item in enumerate(val.storage.items): # On parcourt les items du stockage
                            if item['id'] == bundle['id']: # On vérifie si l'id du bundle est dans le stockage
                                val.storage.items.pop(i) # On le supprime du stockage
                                break
                        yield self.storage.put(bundle) # On ajoute le bundle dans le stockage du nouveau noeud responsable 
                        bundle['custodian'] = self.node_id # Le noeud devient le custodian
                        self.custody_bundles[bundle['id']] += 1 
                        self.custody_transfers += 1
                        print(f"Temps {self.env.now:.2f}: Node {self.node_id} prend custody du bundle {bundle['id']}") 
                        # Envoyer un ACK au précédent saut
                        if bundle.get('source') != self.node_id:  # Ne pas envoyer d'ACK pour les bundles qu'on a créés
                            self.send_ack(bundle['id'], previous_node, True) # On envoie le ack custody au noeud responsable précédemment 
                    else :
                        # Si stockage plein, réessayer plus tard
                        print(f"Temps {self.env.now:.2f}: Stockage plein sur node {self.node_id}, réessai plus tard")
                        yield self.env.timeout(2.0)
                        self.env.process(self.receive_bundle(bundle)) # On réessaie de recevoir le bundle
                    

       
        bundle["noeud_parcouru"]+=1
        bundle['last_hop'] = self.node_id 
        self.env.process(self.forward_bundle(bundle)) # On envoie le bundle au prochain noeud
                
                
    
        

class BundleProtocol:
    def __init__(self, env, nbr_noeud, simulation_time):
        self.env = env
        self.nbr_noeud = nbr_noeud
        self.simulation_time = simulation_time
        self.nodes = []
        self.nbr_bundle = 0  
        self.nbr_lien = 0
        self.bundle = []

    def Bundle_Generator(self, UniqueLiaison=False):
        while True:
            yield self.env.timeout(random.expovariate(0.5)) # On génère un bundle toutes les 0.5 unités de temps
            if self.env.now > self.simulation_time: # On vérifie si le temps de simulation est dépassé
                break

            if UniqueLiaison == True:  # Si on a une liaison unique
                source = self.nodes[0] if random.random() < 0.5 else self.nodes[1] # Soit le noeud 0 soit le noeud 1 en source 
                destination = self.nodes[1] if source == self.nodes[0] else self.nodes[0] # L'autre noeud est la destination
            else: # Si on a plusieurs liaisons
                source = random.choice(self.nodes) # On choisit un noeud au hasard
                destination = random.choice([n for n in self.nodes if n != source]) # On choisit un autre noeud au hasard qui n'est pas la source
        
            
            self.nbr_bundle += 1 # On incrémente le nombre de bundles
            Bundle = { # On définit le bundle
                'id': self.nbr_bundle,
                'source': source.node_id,
                'destination': destination.node_id,
                'Durée_envoie': self.env.now,
                'custodian': None,
                'coupures': [], # Contient les durées de toutes les coupures si il y'en a 
                'coupures total': 0,
                'Tentative': 1,
                'noeud_parcouru' : 0,
                "Livraison reussie" : 0
            }
            self.bundle.append(Bundle)

            print(f"Temps {self.env.now:.2f}: Bundle {Bundle['id']} créé")
            self.env.process(source.forward_bundle(Bundle)) # On envoie le bundle au premier noeud 

    def get_node_by_id(self, node_id): # On crée une fonction pour trouver le noeud à partir de l'id pour l'utiliser lors du custody 
        for node in self.nodes:
            if node.node_id == node_id:
                return node
        return None

    def bfs_next_hop(self, start_node, end_node_id): 
        visited = set() # On définit les noeuds déjà visité 
        queue = deque([(start_node, None, 0)])   # On définit la queue avec le noeud de départ, le premier saut et le poids

        while queue: # Tant qu'il reste des noeuds à explorer
            current_node, first_hop, weight = queue.popleft() # On prend le premier élément de la file 
            if current_node.node_id == end_node_id: # Si on a atteint la destination
                return first_hop if first_hop else current_node # On retourne le premier noeud ou le noeud courant

            for neighbor in current_node.neighbors: # On parcourt les voisins du noeud courant
                if neighbor.node_id not in visited: # Si le voisin n'a pas été visité
                    visited.add(neighbor.node_id) # On l'ajoute dans la liste des noeuds visités
                    queue.append((neighbor, neighbor if not first_hop else first_hop, weight )) # On l'ajoute dans la queue avec le premier noeud et le poids
        return None # Si pas de chemin trouvé

    def network(self, type_liaison="Multiple_liaison_identiques"):
        for i in range(self.nbr_noeud): # On crée tout les noeuds 
            stockage = random.randint(int(0.5*self.simulation_time), int(1.0*self.simulation_time)) # On définit leur capacité de stockage
            self.nodes.append(Node(self.env, i, stockage, self)) # On les ajoute dans la liste des noeuds tout en les créeant 

        if type_liaison == "Unique_liaison":  # Si on a une liaison unique
            if self.nbr_noeud >= 2: # On vérifie qu'il y a au moins 2 noeuds
                self.nodes[0].add_neighbor(self.nodes[1]) # On ajoute le noeud 1 comme voisin du noeud 0
                self.nodes[1].add_neighbor(self.nodes[0]) # On ajoute le noeud 0 comme voisin du noeud 1
                self.nbr_lien +=1
                
        elif type_liaison == "Multiple_liaison": # Si on a plusieurs liaisons
            for i in range(self.nbr_noeud - 1): # On parcourt les noeuds
                self.nodes[i].add_neighbor(self.nodes[i+1]) # On ajoute le noeud i+1 comme voisin du noeud i
                self.nodes[i+1].add_neighbor(self.nodes[i]) # On ajoute le noeud i comme voisin du noeud i+1
                self.nbr_lien +=1
        elif type_liaison == "Multiple_liaison_identiques": # Si on a plusieurs liaisons identiques
            for i in range(self.nbr_noeud): # On parcourt les noeuds
                self.nodes[i].add_neighbor(self.nodes[(i+1) % self.nbr_noeud]) # On ajoute le noeud i+1 comme voisin du noeud i
                self.nodes[i].add_neighbor(self.nodes[(i-1) % self.nbr_noeud]) # On ajoute le noeud i-1 comme voisin du noeud i
                self.nbr_lien +=1
                if random.random() < 0.2: # Et on a 20% de chance d'ajouter un voisin aléatoire
                    j = random.choice([n for n in range(self.nbr_noeud) if n != i]) # On choisit un noeud au hasard qui n'est pas le noeud i
                    self.nodes[i].add_neighbor(self.nodes[j]) # On ajoute le noeud j comme voisin du noeud i
                    self.nodes[j].add_neighbor(self.nodes[i]) # On ajoute le noeud i comme voisin du noeud j
                    self.nbr_lien +=1

        for node in self.nodes: # On parcourt les noeuds
            for dest_node in self.nodes: 
                if dest_node.node_id != node.node_id: # On vérifie que ce n'est pas le même noeud
                    next_hop = self.bfs_next_hop(node, dest_node.node_id) # On définit un chemin qui permet d'atteindre la destination
                    if next_hop: # On vérifie que le chemin existe
                        node.update_routing(dest_node.node_id, next_hop) # On met à jour la table de routage
    
    def run_simulation(self, Unique_Liaison=False):
        if Unique_Liaison == False: # Si on a pas à faire à une liaison unique
            self.env.process(self.Bundle_Generator()) # On lance la génération de bundle pour une liaison multiple
        else: 
            self.env.process(self.Bundle_Generator(True))# Sinon on lance la génération de bundle pour une liaison unique

        self.env.run(until=self.simulation_time) # On lance la simulation jusqu'à la fin du temps de simulation
        
        additional_time = max(3, self.nbr_bundle * 0.5)  # On définit un temps additionnel pour la simulation pour laisser du temps au bundle restant d'être envoyé 
        print(f"\nOn continue la simulation pour {additional_time:.2f} unités de temps additionel.")
        self.env.run(until=self.simulation_time + additional_time) # On continue la simulation pour laisser le temps aux derniers bundles d'être envoyés
        self.print_statistics() # On affiche les statistiques de la simulation



    def print_statistics(self):
        # On calcule les statistiques de la simulation qu'on affiche à la fin 
        print("\nStatistiques finales de la simulation")
        bundle_livree = []
        for Bundle in self.bundle:
            if Bundle["Durée_envoie"] != 2.0:
                bundle_livree.append(Bundle)
                
        delivered = sum(node.delivered for node in self.nodes)
        custody_transfers = sum(node.custody_transfers for node in self.nodes)
        tentative_per_bundle = sum(Bundle["Tentative"] for Bundle in bundle_livree)/len(bundle_livree)
        noeud_parcouru_moyen = sum(Bundle["noeud_parcouru"]for Bundle in bundle_livree)/len(bundle_livree)
        nbr_coupure_par_bundle = sum(len(Bundle["coupures"])for Bundle in bundle_livree)/len(bundle_livree)
        
        coupures_non_nulles = [Bundle["coupures total"] for Bundle in bundle_livree if Bundle["coupures total"] > 0]

        duree_coupure_par_bundle = sum(coupures_non_nulles) / len(coupures_non_nulles)

        duree_bundle_livree = []
        for Bundle in self.bundle:
            if Bundle["Durée_envoie"] != 2.0:
                duree_bundle_livree.append(Bundle["Durée_envoie"])
        
        duree_bundlelivree = sum(duree_bundle_livree)/len(duree_bundle_livree)
        

        temps_moyen_livraison = sum(Bundle["Durée_envoie"]for Bundle in self.bundle)/self.nbr_bundle
        total_dropped = sum(node.dropped_bundles for node in self.nodes)

        
        print(f"Temps total de simulation: {self.env.now:.3f}")
        print(f"Bundles créés: {self.nbr_bundle:.3f}")
        print(f"Bundles livrés avec succès: {delivered:.3f}")
        print(f"Bundles dropés: {total_dropped:.3f}")

        print(f"Taux de livraison : {delivered / self.nbr_bundle * 100:.3f}%")
        print(f"Transferts de custody: {custody_transfers:.3f}")
                
        print(f"Nombre de noeud moyen parcouru par bundle : {noeud_parcouru_moyen:.3f}")
        print(f"Temps de livraison moyen: {temps_moyen_livraison:.3f}")
        print(f"Temps de livraison moyen des bundles livrée: {duree_bundlelivree:.3f}")


        total_pending_acks = sum(len(node.pending_acks) for node in self.nodes)
        print(f"ACKs en attente à la fin: {total_pending_acks:.3f}")
        print(f"Durée de toutes les coupures subies moyenne par bundle (qui ont subies des coupures) : {duree_coupure_par_bundle:.3f} ")
        print(f'Nombre moyen de coupure par bundle : {nbr_coupure_par_bundle:.3f} ')
        print(f"Nombre de tentatives de retransmission moyen par bundle : {tentative_per_bundle:.3f}")
        print(f"Nombre de tentatives de retransmission moyen dans chaque liaison :{sum(node.nbr_tentativepernode for node in self.nodes) / self.nbr_noeud :.3f}") # On affiche le nombre de tentatives de retransmission
        print(f"Nombre de liaisons: {self.nbr_lien}")
        
        

       
# Cas pour une liaison unique

"""
env = simpy.Environment()
sim = BundleProtocol(env, 20, simulation_time=1000)
sim.network("Unique_liaison")
sim.run_simulation(True)
"""


# Cas pour une liaison multiple
"""
env = simpy.Environment()
sim = BundleProtocol(env, 20, simulation_time=100)
sim.network("Multiple_liaison")
sim.run_simulation(False)
"""
# Cas pour une liaison multiple identique

env = simpy.Environment()
sim = BundleProtocol(env, 20, simulation_time=1000)
sim.network("Multiple_liaison_identiques")
sim.run_simulation(False)



