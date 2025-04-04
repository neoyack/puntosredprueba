# 🧠 Prueba Técnica - Ingeniero de Datos | Puntored

Este repositorio contiene la solución completa a la **Prueba Técnica para el cargo de Ingeniero de Datos** asignada por Puntored. El desarrollo incluye respuestas teóricas, consultas SQL optimizadas, scripts en Python y diseños de pipelines en AWS para procesamiento batch y en tiempo real.

---
Tener en cuenta que para la seccion 3 al modelo compartido en la seccion 2 se realizo y se modifico el modelo original de la siguiente manera:

proveedores
id (INT, PRIMARY KEY)
nombre (VARCHAR)
contacto (VARCHAR)

clientes 
id (INT, PRIMARY KEY) nombre (VARCHAR) apellido (VARCHAR)

productos
id (INT, PRIMARY KEY)
nombre (VARCHAR)

productos_proveedor
proveedor_id (INT, FOREIGN KEY a proveedores.id)
producto_id (INT, FOREIGN KEY a productos.id)
PRIMARY KEY (proveedor_id, producto_id)

ventas
id (INT, PRIMARY KEY)
cliente_id (INT, FOREIGN KEY a clientes.id)
producto_id (INT, FOREIGN KEY a productos.id)
fecha (DATE)
monto (DECIMAL)

Todo esto para cumplir los requerimientos propuestos en la seccion 3.

Saludos
## 📁 Estructura del repositorio
```bash
puntosredprueba/
├── Seccion 1&2/
│   ├── # Archivos o subcarpetas relacionados con la sección 1 y 2
│   └── ...
├── Seccion 3/
│   ├── # Archivos o subcarpetas relacionados con la sección 3
│   └── ...
└── README.md

Contacto
Autor: Yamid Paez Perez
Correo: yamidp@gmail.com
LinkedIn: https://www.linkedin.com/in/efrainyamidpaezperez
