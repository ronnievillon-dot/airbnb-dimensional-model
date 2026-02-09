# Diseño del Data Warehouse – Airbnb Listings Analytics

## Selección del grain de la tabla de hechos

El grain de la tabla de hechos se define como:

**Una fila por cada listing capturado en una fecha específica (snapshot_date).**

Este enfoque fue elegido porque el dataset disponible representa el estado actual de las propiedades y no eventos transaccionales como reservas o pagos. Por ello, una tabla de hechos tipo snapshot permite agregar métricas como precio, disponibilidad y número de reseñas sin riesgo de duplicidad.

Definir un grano claro y atómico garantiza consistencia en los reportes y evita ambigüedad en los cálculos.

Este nivel de detalle permite responder preguntas de negocio como:

- ¿Cómo varían los precios según la ubicación?
- ¿Qué hosts tienen mejor desempeño?
- ¿Dónde existen oportunidades de mercado?


---

## Decisiones de diseño de dimensiones

El modelo sigue los principios del esquema en estrella, separando los atributos descriptivos en tablas de dimensiones y almacenando las métricas en la tabla de hechos.

### Dimensión Host
La tabla **dim_host** contiene los atributos relacionados con los anfitriones y permite analizar su desempeño. Separar esta información evita la duplicación de datos y mejora la eficiencia de las consultas.

### Dimensión Ubicación
La tabla **dim_location** centraliza la información geográfica como barrio y coordenadas. Esto facilita análisis espaciales, por ejemplo, comparar precios por zona o identificar áreas con baja oferta.

### Dimensión Propiedad
La tabla **dim_property** almacena características relativamente estables del listing, como el tipo de habitación o el mínimo de noches. Mantener estos atributos fuera de la tabla de hechos simplifica los filtros analíticos y mejora la mantenibilidad del modelo.

### Dimensión Fecha
Aunque la fuente no incluye una fecha de snapshot explícita, se incorporó una **dim_date** para habilitar análisis temporales y soportar futuras cargas incrementales. Las dimensiones de tiempo son una práctica estándar en modelado dimensional.


---

## Manejo de Slowly Changing Dimensions (SCD)

Se implementó una estrategia **Slowly Changing Dimension Tipo 2** para la dimensión Host.

Los atributos de un host pueden cambiar con el tiempo (por ejemplo, el nombre o la cantidad de propiedades administradas). Mantener el historial es clave para realizar análisis de desempeño precisos.

La implementación incluye los campos:

- `effective_date`
- `end_date`
- `is_current`

Esto permite conservar versiones históricas de cada host sin perder trazabilidad.


---

## Estrategia de indexación y particionamiento

Se crearon índices para optimizar los patrones de consulta más comunes en entornos analíticos.

- Índices en las llaves foráneas mejoran el rendimiento de los joins entre la tabla de hechos y las dimensiones.
- Índices únicos sobre las llaves de negocio ayudan a preservar la integridad de los datos.

Para escenarios de mayor volumen, la tabla de hechos puede particionarse por `snapshot_date`. Esto mejora el rendimiento en consultas por rango de fechas y permite cargas incrementales más eficientes.

Además, se utilizaron claves sustitutas (surrogate keys) para mejorar el rendimiento de los joins y desacoplar el modelo analítico de los identificadores operacionales.


---

## Tradeoffs y alternativas consideradas

### Esquema en estrella vs Snowflake
Se optó por un esquema en estrella para reducir la complejidad de los joins y mejorar el rendimiento de las consultas analíticas.

### Tabla de hechos Snapshot vs Transaccional
Dado que el dataset no contiene transacciones a nivel de reserva, una tabla snapshot fue la alternativa más adecuada para representar el estado de cada propiedad en un momento determinado.

### Surrogate Keys vs Business Keys
El uso de claves sustitutas permite gestionar correctamente el historial (SCD) y evita dependencias directas con las llaves del sistema fuente.

### Alcance del SCD
El manejo histórico se aplicó únicamente a la dimensión Host, ya que las demás dimensiones contienen atributos más estables o cuyo historial no es crítico para los análisis planteados.


---

## Alineación con la metodología Kimball

El modelo propuesto sigue las buenas prácticas del modelado dimensional:

- Definición clara del grain 
- Separación entre hechos y dimensiones  
- Uso de claves sustitutas  
- Soporte para historial mediante SCD  
- Optimización para consultas analíticas  

El resultado es un modelo escalable y preparado para soportar análisis de estrategia de precios, desempeño de hosts e inteligencia de mercado.
