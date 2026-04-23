# Trabajo Práctico - Coordinación

En este trabajo se busca familiarizar a los estudiantes con los desafíos de la coordinación del trabajo y el control de la complejidad en sistemas distribuidos. Para tal fin se provee un esqueleto de un sistema de control de stock de una verdulería y un conjunto de escenarios de creciente grado de complejidad y distribución que demandarán mayor sofisticación en la comunicación de las partes involucradas.

## Ejecución

`make up` : Inicia los contenedores del sistema y comienza a seguir los logs de todos ellos en un solo flujo de salida.

`make down`:   Detiene los contenedores y libera los recursos asociados.

`make logs`: Sigue los logs de todos los contenedores en un solo flujo de salida.

`make test`: Inicia los contenedores del sistema, espera a que los clientes finalicen, compara los resultados con una ejecución serial y detiene los contenederes.

`make switch`: Permite alternar rápidamente entre los archivos de docker compose de los distintos escenarios provistos.

## Elementos del sistema objetivo

![ ](./imgs/diagrama_de_robustez.jpg  "Diagrama de Robustez")
*Fig. 1: Diagrama de Robustez*

### Client

Lee un archivo de entrada y envía por TCP/IP pares (fruta, cantidad) al sistema.
Cuando finaliza el envío de datos, aguarda un top de pares (fruta, cantidad) y vuelca el resultado en un archivo de salida csv.
El criterio y tamaño del top dependen de la configuración del sistema. Por defecto se trata de un top 3 de frutas de acuerdo a la cantidad total almacenada.

### Gateway

Es el punto de entrada y salida del sistema. Intercambia mensajes con los clientes y las colas internas utilizando distintos protocolos.

### Sum
 
Recibe pares  (fruta, cantidad) y aplica la función Suma de la clase `FruitItem`. Por defecto esa suma es la canónica para los números enteros, ej:

`("manzana", 5) + ("manzana", 8) = ("manzana", 13)`

Pero su implementación podría modificarse.
Cuando se detecta el final de la ingesta de datos envía los pares (fruta, cantidad) totales a los Aggregators.

### Aggregator

Consolida los datos de las distintas instancias de Sum.
Cuando se detecta el final de la ingesta, se calcula un top parcial y se envía esa información al Joiner.

### Joiner

Recibe tops parciales de las instancias del Aggregator.
Cuando se detecta el final de la ingesta, se envía el top final hacia el gateway para ser entregado al cliente.

## Informe del trabajo
A lo largo del trabajo se fueron tomando decisiones de diseño para que el sistema escale correctamente, que serán explicadas a continuación.

### Utilización de librería threading
En este trabajo se decidió utilizar threads en Python, y conociendo las limitaciones de los hilos en este lenguaje, es necesario justificar su uso.
En las instancias hay principalmente operaciones de E/S bloqueante (consumo de RabbitMQ, sockets) y trabajo ligero por mensaje (sumas y pequeños ordenamientos).
Las operaciones de E/S bloqueante liberan el GIL, por lo que otros hilos pueden ejecutarse mientras uno espera I/O. En consecuencia, threads permiten concurrencia efectiva para este tipo de carga.

En sintesis, en este Trabajo las operaciones CPU-intensivas son puntuales y pequeñas (ordenar top de frutas), por lo que el impacto es mínimo. Si se volviera CPU‑limitante, usar multiprocessing es la alternativa correcta.

### Sum
#### Lectura de datos
Cada instancia Sum recibe datos desde dos origenes:
- Recibe pares (fruta, cantidad) desde una RabbitMQ Queue, que funciona como una Working Queue ya que cada instancia de Sum puede tomar datos de cualquier cliente que envie datos por esta queue.
- Recibe los EOFs de control desde un RabbitMQ Exchange asociado a la routing key "EOFs".
Para esto, se preciso de un hilo separado ejecutando _start_consuming_ sobre el Exchange de control de EOFs, ya que el middleware tiene un tipo de conexion BlockingConnection, que no permite escuchar de dos origenes distintos concurrentemente.

#### Acumulacion de pares
Los clientes envian sus datos al gateway adjuntando su client_id, por lo que cuando estos datos llegan a cada instancia de Sum, estos los iran acumulando segun client_id, para luego reenviarlo a los agregadores.

#### Coordinacion de EOFs
Los clientes solo envian 1 EOF cuando terminan de enviar sus datos, que es enviado por la working queue mencionada anteriormente. Esto genera que solo una instancia de Sum detecte que el cliente termino de enviar sus datos.

Ante esto, el Sum que detecta el EOF del cliente, utilizara el exchange de control de EOFs (mencionado anteriormente) para avisarle al resto de instancias Sum que llego el EOF del cliente. 

El resto de instancias Sum, al recibir por el exchange el EOF del cliente, antes de realizar el reenvio de datos deben analizar si tienen datos pendientes por procesar para ese cliente:
- Si hay mensajes por procesar para ese cliente, almacena el EOF como pendiente para realizar el flush mas tarde.
- Si no hay mensajes por procesar para ese cliente, ejecuta una revalidacion corta para asegurarse que en un periodo corto de tiempo no lleguen mas mensajes de ese cliente. Si esta revalidacion pasa, procede a procesar el EOF del cliente y reenviar la acumulacion de pares al Aggregator.

#### Criterio para envio de valores acumulados al Aggregator
Si las instancias Sum envian el valor acumulado de un cliente a todos los Aggregators, estos estarian procesando datos demas, realizando trabajo innecesario. 

Para eso, se establecio el siguiente criterio para enviar los datos acumulados de un cliente a un Aggregator especifico:
- Se routea cada par (client_id, fruit) a una instancia de Aggregator especifica segun la funcion de hash (client_id:fruit). Con el hash calculado (h), se calcula a qué Aggregator enviar los datos a partir de realizar h % AGGREGATOR_AMOUNT = id agregador. De esta manera, cada par (client_id, fruit) va a ser enviado siempre a la misma instancia Aggregator.
- Además, luego de enviar los pares a los Aggregator, se envia un mensaje EOF junto con el client_id a todos las instancias de Aggregator para que detecten cuando la instancia de Sum no le enviará mas datos de ese cliente.

#### Sender
Todas las publicaciones AMQP salen por un sender corriendo en un thread aparte. Al tener dos hilos escuchando desde dos origenes distintos, Pika puede generar errores al publicar mensajes desde dos hilos distintos ya que este tipo de conexion no es thread safe. 
El sender tiene una cola de tareas y un hilo que procesa estas tareas en loop. A su vez, mantiene un cache de publishers para enviar los payloads por la salida correcta, y abstrae logica de reintentos/backoff.

#### Limitaciones
- Pika no es thread safe, por lo que cuando las instancias de Sum intentan cerrar sus conexiones, puede generar errores del estilo "IndexError: pop from an empty deque".
- La revalidación corta al llegar un EOF del cliente no es el mejor mecanismo de sincronización, ya que deja de ser determinístico para ser probabilístico. Puede suceder que por más que pase el tiempo de revalidación, un mensaje con datos del mismo cliente llegue luego de hacer el flush. Por cuestiones de tiempo no se pudo desarrollar una forma mas robusta de sincronizar las instancias de Sum cuando llega un EOF del cliente.

### Aggregator
#### Lectura de datos
Cada instancia de Aggregator recibe datos desde un RabbitMQ Exchange asociado a la Queue con routing key AGGREGATION_PREFIX_{ID}. De esta cola recibe datos (client_id, fruit, amount) y EOFs (client_id).

#### Acumulación de pares
Mantiene un diccionario donde suma las cantidades recibidas por fruta para cada cliente.
El cálculo es local al Aggregator: suma incremental por fruta hasta que llegue la señal de cierre para ese cliente.

#### Coordinación de EOFs
Para detectar que llegaron todos los datos para un cliente proveniente de las instancias de Sum, mantiene un contador por client_id. Cuando la cantidad de EOFs recibidos para un cliente es igual a la cantidad total de instancias Sum, detecta que ya llegaron todos los datos para ese cliente.

#### Criterio para envío de parciales
Cuando todas las instancias de Sum anunciaron el EOF, el Aggregator construye el top de frutas parcial del cliente y lo envia por la Output Queue para que la reciba el Join.
Además del top parcial, adjunta en el mensaje el client_id y su propio ID.

### Joiner
#### Lectura de datos
El Joiner consume los tops parciales desde una RabbitMQ Queue que recibe mensaje de los Aggregators.

#### Almacenamiento y deduplicación
Mantiene un diccionario por cliente y aggregator, de esta forma acumula los parciales recibidos. Al recibir un parcial, tambien se almacena el par (ack, nack) para utilizarlos solo cuando esta seguro que el Gateway recibio bien el top final. El motivo de almacenar las funciones ack y nack es porque el top final solo lo puede enviar cuando todos las instancias de Aggregator le enviaron sus parciales, por lo que al momento de recibir un parcial de un Aggregator no puede enviar el ack sino el Broker se desentiende.

#### Criterio para emitir resultado final
Espera a recibir AGGREGATION_AMOUNT parciales distintos (uno por cada agregador) para el mismo client_id.
Cuando tiene todos:
- fusiona las listas (suma montos por fruta),
- calcula el top
- envía el final_top al Gateway.
Sólo después de que envío al Gateway retorna sin excepción se ackean los mensajes originales y se marca client_id como enviado (sent_clients).

#### Manejo de fallos
Si el send al Gateway falla:
- restaura los parciales a las estructuras activas
- llama a nack() sobre cada callback original para forzar redelivery por el broker.

#### Limitaciones
- Mensajes retenidos sin ack: almacenar callbacks y no ackear hasta el final mantiene mensajes sin ack en RabbitMQ → requiere prefetch adecuado; sin basic_qos(prefetch) pueden acumularse muchos mensajes y bloquear otros consumidores/partes del flujo.

### Escalado del Sistema
El sistema está diseñado para ser escalable de la siguiente manera:
- **Sum**: réplicas múltiples (N): trabajan en paralelo consumiendo la working queue de datos. Un solo Sum detecta el EOF del cliente y lo publica al Exchange de control para que las N réplicas coordinen el flush. Escala la ingestión de eventos.
- **Aggregator**: particionamiento por hash: hay AGGREGATION_AMOUNT particiones. Cada (client_id,fruit) siempre va a la misma instancia (h % AGGREGATION_AMOUNT). Escalar = aumentar AGGREGATION_AMOUNT y balancear instancias por cola. Esto garantiza que todos los eventos de una key (client_id,fruit) vayan siempre al mismo Aggregator -> permite sumar localmente sin coordinación global.
- **Clientes**: Al recibir una mayor cantidad de clientes, el sistema deberia aumentar las replicas de Sum y Aggregators para aumentar la capacidad de ingesta y procesamiento de datos.
