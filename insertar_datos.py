from neo4j import GraphDatabase
from random import randint

class GrafosBD:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def ejecutar(self):
        with self.driver.session() as session:
            session.write_transaction(self.insertar)

    @staticmethod
    def insertar(tx):
        # Proveedores
        for i in range(5000):
            nombre = "Proveedor " + str(i)
            saldo = randint(25000, 75000)
            cmd = "CREATE(proveedor" + str(i) + ":Proveedor { id:" + str(i) + ", nombre:'" + nombre + "', saldo:" + str(saldo) + " })"
            tx.run(cmd)
            print("Agregado Proveedor " + str(i))
        # Productos
        for i in range(19000):
            nombre = "Producto " + str(i)
            peso = randint(1, 50)
            cmd = "CREATE(producto" + str(i) + ":Producto { id:" + str(i) + ", nombre:'" + nombre + "', peso:" + str(peso) + " })"
            tx.run(cmd)
            print("Agregado Producto " + str(i))
        # Pedidos
        for i in range(5778):
            anio = randint(1999, 2020)
            cmd = "CREATE(pedido" + str(i) + ":Pedido { id:" + str(i) + ", anio:" + str(anio) + " })"
            tx.run(cmd)
            print("Agregado Pedido " + str(i))  
        # Proveedor Provee Producto
        for i in range(19000):
            nodo1 = i % 5000
            nodo2 = i
            tx.run("MATCH (proveedor:Proveedor),(producto:Producto) WHERE proveedor.id = $nodo1 AND producto.id = $nodo2 CREATE (proveedor)-[r:PROVEE]->(producto)", nodo1=nodo1, nodo2=nodo2)
            print("Agregada Relacion Provee " + str(i))
        # Proveedor Atiende Pedido
        for i in range(5778):
            nodo1 = randint(0, 4999)
            nodo2 = i
            tx.run("MATCH (proveedor:Proveedor),(pedido:Pedido) WHERE proveedor.id = $nodo1 AND pedido.id = $nodo2 CREATE (proveedor)-[r:ATIENDE]->(pedido)", nodo1=nodo1, nodo2=nodo2)
            print("Agregada Relacion Atiende " + str(i))
        # Pedido Tiene Producto
        tx.run("MATCH (proveedor:Proveedor)-[:ATIENDE]->(pedido:Pedido), (proveedor:Proveedor)-[:PROVEE]->(producto:Producto) CREATE (pedido)-[r:TIENE]->(producto)")
        print("Agregadas Relaciones Tiene")

if __name__ == "__main__":
    conn = GrafosBD("bolt://localhost:7687", "neo4j", "1234")
    conn.ejecutar()
    conn.close()
