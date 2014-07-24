Este es el repositorio principal de los programas que se han creado y
ejecutado para el PFM.

Ejecución
---------

Cargar el proyecto `proyectofinal` como proyecto de Maven en Eclipse, y ejecutar.

> *NOTA:* Es posible que sea necesario cambiar algunos de los datos, puesto que está pensado
para ejecutarse en un cluster de EC2 con Cloudera Manager y no en local o en otras
rutas. Ver la información más abajo.


Ingesta
-------

La ingesta de los datos se realizó de forma inicial bajo una base de datos MongoDB
(como se explica más adelante), pero posteriormente es sobre HBase.

Bajo la carpeta `proyectofinal` se tiene todo lo necesario para hacer funcionar el
proyecto Eclipse, con unas credenciales en el fichero `twitter4j.properties` que habría
que rellenar para que funcionara correctamente.

### Streaming desde Twitter (Spout)

En el fichero `proyectofinal/src/com/willy/pfm/stream/StreamTweetsSpouts.java`
están el código necesario que realiza la escucha del API de streaming de Twitter4j
y que emite a través de Storm (siendo un spout), que es usado por los dos bolts siguientes.

Trackea lo siguiente:

```java
private String[] KEYWORDS = { "Justin Bieber", "Bieber", "@justinbieber",
            "believers", "believer", "#beliebers" };
```

### Analizar twits

Este bolt, que está en `proyectofinal/src/com/willy/pfm/stream/AnalyzeTwitBolt.java`
se encarga de recoger los twits que envía el Spout de antes y emite sólo los datos que
necesitamos (para filtrar información), como: `TWEET_TEXT`, `TWEET_USER_ID`,
`TWEET_USER_NAME`, `TWEET_IS_RETWEET`.

### Analizar usuarios

Disponible en `proyectofinal/src/com/willy/pfm/stream/AnalyzeUserBolt.java`.

Realiza más o menos lo mismo que el bolt superior, pero sólo emite el `TWEET_USER_NAME` y
el `TWEET_USER_ID`, que se usaba para guardar en otra tabla de la base de datos.

### Guardar twits

En la ruta `proyectofinal/src/com/willy/pfm/stream/SaveTweetBolt.java` tenemos el código
que escucha del bolt _Analizar twits_ y se encarga de guardarlos en HBase. Para hacerlo
funcionar, hay que cambiar la ruta del HBase para que apunte tanto al Zookeeper en el que
estamos ejecutando el cluster y el maestro de HBase.

```java
final String TWEET_TABLE_NAME = "Tweet";
final String[] TWEET_TABLE_COLUMNS = { "text", "userId", "userName",
    "isRetweet" };
```

### Guardar usuarios

Disponible en: `proyectofinal/src/com/willy/pfm/stream/SaveUserBolt.java`. Realiza lo
mismo que el bolt anterior, pero guardando en otra tabla de HBase.

Los datos de la tabla de HBase son:

```java
final String USER_TABLE_NAME = "User";
final String[] USER_TABLE_COLUMNS = { "username" };
```

### Topología

`proyectofinal/src/com/willy/pfm/stream/TwitterStreamTopology.java`

La topología es como sigue:

```java
builder.setSpout("streamTweet", new StreamTweetsSpout(), 1);
// We early-analyze things here: stream users and then tweets
builder.setBolt("analyzeUser", new AnalyzeUserBolt(), 10)
        .shuffleGrouping("streamTweet");
builder.setBolt("analyzeTweet", new AnalyzeTwitBolt(), 10)
        .shuffleGrouping("streamTweet");

// Al usar .fieldsGrouping, todo lo igual irá al mismo SaveUserBolt,
// por lo que pasamos de distribuir de una forma uniforme pero aleatoria
// los datos a que el sistema nos distribuya un mismo usuario a un mismo
// bolt
// haciendo que no enviemos usuarios duplicados.
builder.setBolt("saveUser", new SaveUserBolt(), 10).fieldsGrouping(
        "analyzeUser",
        new Fields(com.willy.pfm.stream.Fields.TWEET_USER_ID));

builder.setBolt("saveTweet", new SaveTweetBolt(), 10).shuffleGrouping(
        "analyzeTweet");
```

Hive
----

### Creación de tablas

Las consultas para la creación de tablas está dentro del fichero
`createTablesFromHbase.hql`, que deben ejecutarse en el CLI de Hive o bien
dentro de Hue.

### Consultas

Dentro de hive están los diferentes ficheros `.hql` que se necesitan para ejecutar
las consultas que se han realizado para el proyecto.

Pueden copiar y pegarse, o bien ejecutarse como `hive -f fichero.hql`


MongoDB a CSV
-------------

Durante la iteración del proyecto, también se guardaron los tuits directamente
en MongoDB, debido a mi mayor conocmiento sobre esta NoSQL y la posibilidad de
también realizar MapReduces de una forma simple.

Como hubo un tiempo en el que sólo se guardaban en MongoDB, se creó un exportador
de los datos desde MongoDB en Node.JS, bajo la carpeta `MongoDBToCSV` que es
posible de ejecutar como `node main.js` después de haber instalado un par de paquetes
necesarios con `npm install fast-csv mongodb`

CVS a HBase
-----------

Bajo el proyecto `bulkToHBase` se encuentra el código Java que recoge el CSV
exportado por MongoDB y lo importa a HBase. Los datos configurados están puestos
"a pelo" directamente para usar las máquinas de Amazon que contraté durante el
desarrollo del proyecto.