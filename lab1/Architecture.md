# Document d'Architecture Logicielle

## RR

### Architecture

Une instance `RR` est initialisée grâce à un `NetWrapper` composé de 2 channels : une vers le réseau et l'autre depuis le réseau.
Grâce à ces deux channels, `RR` est indépendant et notre implémentation devient modulable.

Notre implémentation utilise les 2 channels mis à disposition afin d'envoyer et recevoir des requêtes du réseau.
Pour ce faire, nous disposons des fonctions suivantes :

Lorsqu'une nouvelle instance de `RR` est créée via `NewRR`, plusieurs événements se produisent :

1. Un objet `RR` est créé, ce dernier sera retourné par `NewRR()` et est, entre autres, composé de :
    1. le `NetWrapper` passé en paramètre
    2. une channel `receivedResponseChan` créée à la volée pour recevoir les réponses venant du remote en lien avec ce RR
    3. une channel `receivedRequestChan` créée à la volée pour recevoir les requêtes venant du remote
    4. une channel `activeRequestChan` créée à la volée, servant à communiquer entre goroutines quelle est la requête active
    5. un générateur d'identifiants `uidGenerator` permettant d'identifier les requêtes
    6. un `timeout` pour savoir après combien de temps renvoyer les requêtes restées sans réponse
    7. une channel `newRequestHandler` pour mettre à jour le gestionnaire de requêtes
    8. une channel `newSlowDown` pour configurer les délais artificiels (utile pour les tests)
2. Une goroutine avec la méthode `handleState()` de RR est lancée après création de notre instance

#### Goroutine `handleState()` - point d'entrée

Cette goroutine crée deux goroutines : `handleSendingRequest()` et `handleReceivingRequest()`.
Elle s'occupe ensuite de rediriger les messages vers la bonne channel (`receivedResponseChan`/`receivedRequestChan`) selon qu'ils soient des requêtes ou des réponses.
Dans le cas d'un message `done`, on ferme la routine.

##### Goroutine `handleSendingRequest()`

Cette routine s'occupe tout d'abord de récupérer la requête active via l'attribut `activeRequestChan` de `rr`.
`activeRequestChan` contient un payload et un channel de réponse.
Un numéro de séquence attribué à la requête est ensuite créé grâce au uidGenerator de `rr` et à l'`ID` de l'instance.
Le message (composé du `payload`, de son `seqnum` et de son `type`) est ensuite encodé et envoyé sur le réseau via `netWrapper.IntoNet`.

Une boucle d'attente de réponse se lance après l'envoi du message.
Celle-ci prend en charge 3 cas :
1. Le timeout est dépassé sans recevoir de réponse du remote : la requête est renvoyée indéfiniment pour garantir qu'aucune requête ne soit perdue.
2. Une réponse est reçue via `receivedResponseChan` :
    1. Si le numéro de séquence de la réponse est inférieur à celui de la requête actuelle, elle est ignorée (réponse ancienne)
    2. Si le numéro de séquence correspond ou est plus récent, la réponse est transmise à l'envoyeur via le channel de réponse
3. Si le RR est fermé (`done`), un message d'erreur est transmis à l'envoyeur et on ferme la routine.

##### Goroutine `handleReceivingRequest()`

Cette routine gère le traitement des requêtes entrantes. Elle maintient un état local comprenant :
- Le gestionnaire de requêtes actuel `requestHandler`
- Le délai artificiel configuré (pour les tests) `slowDown`
- Le dernier numéro de séquence traité `seqnum`
- La dernière payload de réponse générée `previousPayload`

Pour chaque requête reçue :
1. Un délai artificiel est appliqué si configuré
2. Les requêtes avec des numéros de séquence obsolètes sont ignorées
3. Les requêtes dupliquées (même numéro de séquence) renvoient la réponse précédente
4. Les nouvelles requêtes sont traitées par le gestionnaire configuré `newRequestHandler`
5. La réponse est encodée et envoyée sur le réseau

La routine écoute également les mises à jour du gestionnaire de requêtes et des paramètres de délai.

#### Méthode `SendRequest()` - interface publique

La méthode `SendRequest()` est l'interface principale pour envoyer des requêtes au remote associé. Son fonctionnement est le suivant :

1. **Création du canal de réponse** : Un canal `SendResponse` est créé pour retourner le résultat de manière asynchrone
2. **Soumission de la requête** : La requête est encapsulée dans une structure `ActiveRequest` et envoyée sur `activeRequestChan`
3. **Gestion des erreurs** : Si le RR est fermé pendant l'envoi, un message d'erreur est immédiatement retourné
4. **Retour non-bloquant** : La méthode retourne immédiatement une promesse de réponse via une channel `responseChan` , permettant à l'appelant d'attendre la réponse de manière asynchrone

Le traitement effectif de l'envoi et de la réception de la réponse est délégué à la goroutine `handleSendingRequest()`, ce qui permet :
- Une gestion centralisée des timeouts et des retransmissions
- Un ordonnancement séquentiel des requêtes sortantes
- Une isolation de la complexité de gestion des numéros de séquence

#### Gestion de la concurrence et de l'état

L'architecture utilise plusieurs channels pour coordonner les différentes goroutines :
- `activeRequestChan` : sert de mécanisme de verrouillage pour les requêtes sortantes
- `receivedResponseChan` et `receivedRequestChan` : permettent le routage des messages entre goroutines
- `newRequestHandler` et `newSlowDown` : permettent la mise à jour dynamique des paramètres
- `done` : coordonne l'arrêt propre de toutes les goroutines

La séquence des messages sera toujours respectée même en cas de renvoi puisqu'un numéro de séquence est attribué à chaque échange avec un remote donné.

## UDP

Le module UDP utilise le module RR présenté ci-dessus. Pour leur permettre de communiquer, un RRWrapper est ajouté dans le module UDP.
Dans la structure de l'objet UDP les éléments suivants sont ajoutés pour intégrer RR:
- `rrMap` : un map contenant les instances RR indexées par leur addresse
- `rrWrapperMap` : un map contenant les RRWrapper indexés par leur adresse.
- `networkMessages` : un bufferChan qui reçoit les messages provenant du réseau et les transmets à la bonne instance RR.

Ces deux map distinctes permettent d'accéder au intoNet et fromNet d'une instance RR donnée.
Ce point pourrait être amélioré en ayant une structure contenant une instance de RR ainsi que son netWrapper.
De plus, au lieu d'utiliser un attribut, il faudrait idéalement qu'uniquement la goroutine principale soit détentrice de la map.
Avec l'implémentation actuelle, il y a un risque de race condition.

Lors de l'instanciation d'UDP avec `NewUDP()`, les 2 maps décritent ci-dessus sont créées à la volée.

### goroutine `handleState()` - gestionnaire d'état principal

Lors de l'appel à NewUDP(), la goroutine `handleState()` est lancée.
Les modifications apportées à cette routine pour intégrer RR sont les suivantes:
- dans le cas d'une requête voulant être envoyée, la goroutine `handleSend()` est lancée.
  - cette modification a été implémentée pour permettre au testSpam de passer. Cependant, cela pourrait être amélioré en modifiant la structure `sendRequest` pour y ajouter une promesse de retour de channel afin que la goroutine principale handleState() ne soit pas bloquante.
- dans le cas d'un message reçu du network:
  - on cherche le RRWrapper correspondant à la source du message reçu, s'il n'exite pas on instancie un RR et son wrapper
  - la map rrWrapperMap est utilisée pour trouver la channel incoming `incomingChan` correpondant à ce RR
  - le payload du message reçu via le résau est transmis à la channel `incomingChan` dans une goroutine anonyme afin que l'action ne soit pas bloquante. 
- dans le cas d'un message reçu de la part d'une instance de RR
  - il est broadcast aux machines abonnées.


### goroutine `handleSend()`
La méthode handleSend() est lancée en tant que goroutine dans l'adaptation d'UDP pour utiliser le module RR.
Cette dernière a également été modifiée de la manière suivante afin d'intégrer RR:
- elle ne prend qu'un seul argument: une struct `sendRequest`
- si aucune instance de RR n'est associée à l'adresse de destination de la requête, une nouvelle instance RR et un RRWrapper sont instanciés avec l'adresse de destination
- la requête est envoyée via l'instance RR associée
- une boucle d'attente avec timeout pour la réponse est lancée et consite aux éléments suivants:
  - si une réponse se trouve dans la channel responseChan:
    - les potentielles erreurs reçues sont stockées dans une variable `err`
    - dans le cas d'un message reçu correctement, on log le remote qui l'a reçu
  - si nous dépassons le timeout sans réponse un warning est loggé, et rien d'autre n'est fait. Ici, il y a potentiellement un risque de rester infiniement dans cette boucle.
  - dans le cas d'une fermeture des channels, une erreur est stockée dans `err`
- la routine se ferme en retournant les potentielles erreurs

### méthode `initRRInstance()`
Cette méthode a été ajoutée pour l'intégration du module RR dans UDP. 
Elle permet de créer une instance de RR ainsi que le wrapper associé en utilisant l'adresse de destination du remote.
Elle contient les étapes suivantes:
- contrôler qu'aucune instance de RR ne soit déjà associée à l'adresse de destination
- créer le `RRWrapper` et les channels qui le composent à la volée et l'ajouter à la map `rrWrapperMap`
- appeler la méthode `startHandlingSends()` qui va lancer une goroutine anonyme pour gérer les envois de cette nouvelle instance de RR avec le channel `outgoing` de son `RRWrapper`
  - une amélioration serait de lancer `startHandlingSends()` en tant que routine et supprimer la goroutine anonyme écrite dedans, reliquat d'essai d'implémentation qui n'ont pas donné fruits.
- l'instance de `RR` associée à l'adresse de destination est ensuite instanciée et ajoutée dans la map `rrMap`
- la méthode SetRequestHandler() de l'instance de RR est ensuite appelée pour s'occuper des requêtes entrantes
- un message contenant la source et le payload est crée
- une go routine anynonyme non-bloquante est lancée pour la gestion de la réception de la requête
  - si le message est reçu avec succès il est déliveré à l'application en utilisant un bufferedChan d'UDP `receivedMessages.Inlet()`
  - si la channel closeChan est fermée, on envoie un warning
- un OK est retourné - que le message ait été déliveré avec succès ou non pour que cela ne soit pas bloquant.

### la méthode `startHandlingSends()`

Cette méthode est appelée par `initRRInstance()` comme décrit ci-dessus.
Elle est composée d'une goroutine anonyme qui procède aux actions suivantes pour le remote passé en argument (seulement les actions ajoutées pour l'intégration de RR sont décrites):
- la partie de gestion connexion au remote a été déplacée dans la goroutine anonyme
- la channel `outgoing` remplace la channel `sendRequests` comme deuxième argument de la méthode
- lors de la routine anonyme les actions suivantes sont en place:
  - si la channel closeChan est utilisée, la routine est quittée avec un warning
  - si un payload de la channel outgoing est reçu:
    - il est stocké dans rrPayload 
    - la variable ok est utilisée pour voir si la channel est encore utilisée par le remote, si ok est false, cela signifie que la channel a été fermée par le remote avec un `close()`
    - un check pour savoir si un shutdown a été demandé est exécuté:
      - si c'est le cas la routine est quittée
      - sinon un transport.Message avec le payload et l'adresse de la source est crée puis il est envoyé avec writeToConn comme dans l'implémentation de base

### goroutine listenIncomingMessages()

Cette routine s'occupe de gérer les messages reçus. Pour intégrer le module RR, les modifications se trouvent dans le dernier select qui s'occupe de stocker le message reçu.
Il n'est plus envoyé dans la channel receivedMessages mais dans la bufferedChannel `networkMessages` dont les messages seront traité dans la routine principale `handleState()` comme décrit précédemment.


### méthode Close()

La méthode close permet de fermer une connection UDP.
Elle a dû être modifiée afin d'également fermer les ajouts liés à RR, à savoir, les instances de RR ouvertes contenues dans rrMap ainsi que les 2 channels des `RRWrappers` associés contenus dans `rrWrapperMap`.
