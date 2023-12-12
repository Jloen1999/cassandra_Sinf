package unex;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.*;

import static org.fusesource.jansi.Ansi.Color.*;
import static org.fusesource.jansi.Ansi.ansi;

/**
 * @author Jose Luis Obiang Ela Nanguang
 * @version 1.0
 */
public class QueryExecutor {

    private final CqlSession session;
    private ResultSet rs = null;
    private final CassandraConfig cassandraConfig;

    public QueryExecutor(CqlSession session) {
        this.session = session;
        this.cassandraConfig = new CassandraConfig();
    }

    public void getAllDestinos(String table) {
        rs = session.execute("SELECT * FROM  " + cassandraConfig.getKeyspace() + "." + table);
        // Configurar Jansi para colores en la consola
        System.out.println(ansi().fg(YELLOW).a("\t\t\t\t\t+--------------------------------------+").reset());
        System.out.println(ansi().fg(YELLOW).a("\t\t\t\t\t|             Registros Destinos       |").reset());
        System.out.println(ansi().fg(YELLOW).a("\t\t\t\t\t+--------------------------------------+").reset());

        System.out.println(ansi().fg(CYAN).a("|             destino_id            |     nombre     |   pais   |                          descripcion                          |     clima     |").reset());
        System.out.println(ansi().fg(YELLOW).a("+-----------------------------------+----------------+----------+---------------------------------------------------------------+---------------+").reset());
        for (Row row : rs) {
            System.out.println(
                    ansi().fg(GREEN).a("| " + row.getUuid("destino_id")).reset() + " | " +
                            ansi().fg(GREEN).a(row.getString("nombre")).reset() + " | " +
                            ansi().fg(GREEN).a(row.getString("pais")).reset() + " | " +
                            ansi().fg(GREEN).a(row.getString("descripcion")).reset() + " | " +
                            ansi().fg(GREEN).a(row.getString("clima")).reset() + " |"
            );
        }

        System.out.println(ansi().fg(YELLOW).a("+---------------------------------+----------------+----------+---------------------------------------------------------------+---------------+").reset());

    }

    public void getAllClientes(String table) {
        rs = session.execute("SELECT * FROM " + cassandraConfig.getKeyspace() + "." + table);

        // Configurar Jansi para colores en la consola
        System.out.println(ansi().fg(YELLOW).a("\t\t\t\t\t+--------------------------------------+").reset());
        System.out.println(ansi().fg(YELLOW).a("\t\t\t\t\t|           Registros Clientes         |").reset());
        System.out.println(ansi().fg(YELLOW).a("\t\t\t\t\t+--------------------------------------+").reset());

        System.out.println(ansi().fg(CYAN).a("|             cliente_id             |      nombre      |       correo_electronico      |   telefono   |").reset());
        System.out.println(ansi().fg(YELLOW).a("+------------------------------------+------------------+-------------------------------+--------------+").reset());

        for (Row row : rs) {
            System.out.println(
                    ansi().fg(GREEN).a("| " + row.getUuid("cliente_id")).reset() + " | " +
                            ansi().fg(GREEN).a(row.getString("nombre")).reset() + " | " +
                            ansi().fg(GREEN).a(row.getString("correo_electronico")).reset() + " | " +
                            ansi().fg(GREEN).a(row.getString("telefono")).reset() + " |"
            );
        }

        System.out.println(ansi().fg(YELLOW).a("+------------------------------------+------------------+-------------------------------+--------------+").reset());
    }

    public void getAllPaquetes() {
        rs = session.execute("SELECT * FROM " + cassandraConfig.getKeyspace() + ".paquetes");

        // Configurar Jansi para colores en la consola
        System.out.println(ansi().fg(YELLOW).a("\t\t\t\t\t+--------------------------------------+").reset());
        System.out.println(ansi().fg(YELLOW).a("\t\t\t\t\t|           Registros Paquetes         |").reset());
        System.out.println(ansi().fg(YELLOW).a("\t\t\t\t\t+--------------------------------------+").reset());

        System.out.println(ansi().fg(CYAN).a("|             paquete_id             |       nombre       |             destino_id             | duracion |  precio  |").reset());
        System.out.println(ansi().fg(YELLOW).a("+------------------------------------+--------------------+------------------------------------+----------+----------+").reset());

        for (Row row : rs) {
            System.out.println(
                    ansi().fg(GREEN).a("| " + row.getUuid("paquete_id")).reset() + " | " +
                            ansi().fg(GREEN).a(row.getString("nombre")).reset() + " | " +
                            ansi().fg(GREEN).a(row.getUuid("destino_id")).reset() + " | " +
                            ansi().fg(GREEN).a(row.getInt("duracion")).reset() + " | " +
                            ansi().fg(GREEN).a(row.getBigDecimal("precio")).reset() + " |"
            );
        }

        System.out.println(ansi().fg(YELLOW).a("+------------------------------------+--------------------+------------------------------------+----------+----------+").reset());
    }

    public void getAllReservas() {
        rs = session.execute("SELECT * FROM " + cassandraConfig.getKeyspace() + ".reservas");

        // Configurar Jansi para colores en la consola
        System.out.println(ansi().fg(YELLOW).a("\t\t\t\t\t+--------------------------------------+").reset());
        System.out.println(ansi().fg(YELLOW).a("\t\t\t\t\t|           Registros Reservas         |").reset());
        System.out.println(ansi().fg(YELLOW).a("\t\t\t\t\t+--------------------------------------+").reset());

        System.out.println(ansi().fg(CYAN).a("|             reserva_id             |             paquete_id             |             cliente_id             | fecha_inicio | fecha_fin | pagado |").reset());
        System.out.println(ansi().fg(YELLOW).a("+------------------------------------+------------------------------------+------------------------------------+--------------+-----------+--------+").reset());

        for (Row row : rs) {
            System.out.println(
                    ansi().fg(GREEN).a("| " + row.getUuid("reserva_id")).reset() + " | " +
                            ansi().fg(GREEN).a(row.getUuid("paquete_id")).reset() + " | " +
                            ansi().fg(GREEN).a(row.getUuid("cliente_id")).reset() + " | " +
                            ansi().fg(GREEN).a(row.getLocalDate("fecha_inicio")).reset() + " | " +
                            ansi().fg(GREEN).a(row.getLocalDate("fecha_fin")).reset() + " | " +
                            ansi().fg(GREEN).a(row.getBool("pagado")).reset() + " |"
            );
        }

        System.out.println(ansi().fg(YELLOW).a("+------------------------------------+------------------------------------+------------------------------------+--------------+-----------+--------+").reset());
    }


    public void getPaqueteDetails(UUID paqueteId) {
        rs = session.execute("SELECT * FROM  " + cassandraConfig.getKeyspace() + ".paquetes WHERE paquete_id = ? ALLOW FILTERING", paqueteId);

        System.out.println(ansi().fg(YELLOW).a("\t\t\t\t\t+--------------------------------------+").reset());
        System.out.println(ansi().fg(YELLOW).a("\t\t\t\t\t|          Detalles del Paquete        |").reset());
        System.out.println(ansi().fg(YELLOW).a("\t\t\t\t\t+--------------------------------------+").reset());

        System.out.println(ansi().fg(CYAN).a("|             paquete_id             |     nombre     |             destino_id             | duracion | precio |").reset());
        System.out.println(ansi().fg(YELLOW).a("+------------------------------------+----------------+------------------------------------+----------+--------+").reset());

        for (Row row : rs) {
            System.out.println(
                    ansi().fg(GREEN).a("| " + row.getUuid("paquete_id")).reset() + " | " +
                            ansi().fg(GREEN).a(row.getString("nombre")).reset() + " | " +
                            ansi().fg(GREEN).a(row.getUuid("destino_id")).reset() + " | " +
                            ansi().fg(GREEN).a(row.getInt("duracion")).reset() + " | " +
                            ansi().fg(GREEN).a(row.getBigDecimal("precio")).reset() + " |"
            );
        }

        System.out.println(ansi().fg(YELLOW).a("+------------------------------------+----------------+------------------------------------+----------+--------+").reset());
    }

    public void getClientsForId(UUID clienteID) {
        rs = session.execute("SELECT * FROM  " + cassandraConfig.getKeyspace() + ".clientes WHERE cliente_id = ? ALLOW FILTERING", clienteID);
        for (Row row : rs) {
            System.out.println(
                    ansi().fg(GREEN).a("| " + row.getUuid("cliente_id")).reset() + " | " +
                            ansi().fg(GREEN).a(row.getString("nombre")).reset() + " | " +
                            ansi().fg(GREEN).a(row.getString("correo_electronico")).reset() + " | " +
                            ansi().fg(GREEN).a(row.getString("telefono")).reset() + " |"
            );
        }

    }

    public void getReservasByCliente(UUID clienteId) {
        rs = session.execute("SELECT * FROM  " + cassandraConfig.getKeyspace() + ".reservas WHERE cliente_id = ? ALLOW FILTERING", clienteId);

        System.out.println(ansi().fg(YELLOW).a("\t\t\t\t\t+--------------------------------------+").reset());
        System.out.println(ansi().fg(YELLOW).a("\t\t\t\t\t|         Reservas del Cliente              |").reset());
        System.out.println(ansi().fg(YELLOW).a("\t\t\t\t\t+--------------------------------------+").reset());

        System.out.println(ansi().fg(CYAN).a("| reserva_id           | paquete_id               | fecha_inicio               | fecha_fin                  | pagado          |").reset());
        System.out.println(ansi().fg(YELLOW).a("+----------------------+--------------------------+-----------------------------+----------------------------+-----------------+").reset());

        for (Row row : rs) {
            System.out.println(
                    ansi().fg(GREEN).a("| " + row.getUuid("reserva_id")).reset() + " | " +
                            ansi().fg(GREEN).a(row.getUuid("paquete_id")).reset() + " | " +
                            ansi().fg(GREEN).a(row.getLocalDate("fecha_inicio")).reset() + " | " +
                            ansi().fg(GREEN).a(row.getLocalDate("fecha_fin")).reset() + " | " +
                            ansi().fg(GREEN).a(row.getBoolean("pagado")).reset() + " |"
            );
        }

        System.out.println(ansi().fg(YELLOW).a("+----------------------+--------------------------+-----------------------------+----------------------------+-----------------+").reset());
    }

//    public void createIndexForReservasByClienteId() {
//        session.execute("CREATE CUSTOM INDEX IF NOT EXISTS ON " + cassandraConfig.getKeyspace() + ".reservas (cliente_id) " +
//                "USING 'org.apache.cassandra.index.sasi.SASIIndex';");
//    }


    public void getPaquetesByDestino(UUID destinoId) {
        rs = session.execute("SELECT * FROM  " + cassandraConfig.getKeyspace() + ".paquetes WHERE destino_id = ? ALLOW FILTERING", destinoId);

        System.out.println(ansi().fg(YELLOW).a("\t\t\t\t\t+--------------------------------------+").reset());
        System.out.println(ansi().fg(YELLOW).a("\t\t\t\t\t|           Detalles del Paquete            |").reset());
        System.out.println(ansi().fg(YELLOW).a("\t\t\t\t\t+--------------------------------------+").reset());

        System.out.println(ansi().fg(CYAN).a("| paquete_id           | nombre                    | destino_id                | duracion         | precio          |").reset());
        System.out.println(ansi().fg(YELLOW).a("+----------------------+---------------------------+---------------------------+------------------+-----------------+").reset());

        for (Row row : rs) {
            System.out.println(
                    ansi().fg(GREEN).a("| " + row.getUuid("paquete_id")).reset() + " | " +
                            ansi().fg(GREEN).a(row.getString("nombre")).reset() + " | " +
                            ansi().fg(GREEN).a(row.getUuid("destino_id")).reset() + " | " +
                            ansi().fg(GREEN).a(row.getInt("duracion")).reset() + " | " +
                            ansi().fg(GREEN).a(row.getBigDecimal("precio")).reset() + " |"
            );
        }

        System.out.println(ansi().fg(YELLOW).a("+----------------------+---------------------------+---------------------------+------------------+-----------------+").reset());
    }

    public void createIndexForPaquetesByDestinoId() {
        session.execute("CREATE INDEX IF NOT EXISTS ON " + cassandraConfig.getKeyspace() + ".paquetes (destino_id) " +
                "USING 'org.apache.cassandra.index.sasi.SASIIndex';");
    }

    public void getClientesByReservationDateRange(LocalDate startDate, LocalDate endDate) {
        // Primero, consulta para obtener los cliente_id de reservas en el rango de fechas
        rs = session.execute("SELECT cliente_id FROM " + cassandraConfig.getKeyspace() + ".reservas " +
                "WHERE fecha_inicio >= ? AND fecha_fin <= ? ALLOW FILTERING", startDate, endDate);

        System.out.println(ansi().fg(YELLOW).a("\t\t\t\t\t+---------------------------------------------------------------------------------------------+").reset());
        System.out.println(ansi().fg(YELLOW).a("\t\t\t\t\t| Clientes con reservas en el rango de fechas: " + startDate + "-" + endDate + " |").reset());
        System.out.println(ansi().fg(YELLOW).a("\t\t\t\t\t+---------------------------------------------------------------------------------------------+").reset());
        System.out.println(ansi().fg(CYAN).a("|             cliente_id             |      nombre      |       correo_electronico      |   telefono   |").reset());
        System.out.println(ansi().fg(YELLOW).a("+------------------------------------+------------------+-------------------------------+--------------+").reset());

        for (Row row : rs) {
            UUID clienteId = row.getUuid("cliente_id");
            getClientsForId(clienteId);
        }

        System.out.println(ansi().fg(YELLOW).a("+------------------------------------+------------------+-------------------------------+--------------+").reset());
    }


    public void createResumenReservasPorPaqueteTable() {
        session.execute("CREATE TABLE IF NOT EXISTS " + cassandraConfig.getKeyspace() + ".resumen_reservas_por_paquete ("
                + "paquete_id UUID,"
                + "total_reservas INT,"
                + "PRIMARY KEY(paquete_id))");
    }

    public void updateResumenReservasPorPaquete() {
        // Obtener todas las reservas para un paquete
        rs = session.execute("SELECT * FROM " + cassandraConfig.getKeyspace() + ".reservas ALLOW FILTERING");

        // Actualizar la tabla resumen_reservas_por_paquete con los resultados de la consulta
        for (Row row : rs) {
            UUID paqueteId = row.getUuid("paquete_id");
            rs = session.execute("SELECT COUNT(reserva_id) as total_reservas FROM " + cassandraConfig.getKeyspace() + ".reservas WHERE paquete_id = ? ALLOW FILTERING", paqueteId);
            Row rw = rs.one();
            if (row != null) {
                int totalReservas = (int) rw.getLong("total_reservas");
                session.execute("INSERT INTO " + cassandraConfig.getKeyspace() + ".resumen_reservas_por_paquete (paquete_id, total_reservas) VALUES (?, ?) IF NOT EXISTS;",
                        paqueteId, totalReservas);
            }
        }
        System.out.println(ansi().fg(BLUE).a("+-----------------TABLA resumen_reservas_por_paquete actualizada con registros---------------------+").reset());
        System.out.println(ansi().fg(YELLOW).a("+--------------------------------------------------------------------------------------------------+").reset());

    }

    public void getResumenReservasPorPaqueteTable() {
        rs = session.execute("SELECT * FROM " + cassandraConfig.getKeyspace() + ".resumen_reservas_por_paquete");
        System.out.println(ansi().fg(YELLOW).a("\t\t\t\t\t+--------------------------------------+").reset());
        System.out.println(ansi().fg(YELLOW).a("\t\t\t\t\t| Resumen Total de Reservas por paquete |").reset());
        System.out.println(ansi().fg(YELLOW).a("\t\t\t\t\t+--------------------------------------+").reset());

        System.out.println(ansi().fg(CYAN).a("|             paquete_id             | total_reservas |").reset());
        System.out.println(ansi().fg(YELLOW).a("+------------------------------------+----------------+").reset());

        for (Row row : rs) {
            UUID paqueteId = row.getUuid("paquete_id");
            int totalReservas = row.getInt("total_reservas");
            System.out.println(ansi().fg(GREEN).a("| " + paqueteId + "|\t\t" + totalReservas).reset() + "\t\t|");
        }
        System.out.println(ansi().fg(YELLOW).a("+------------------------------------+----------------+").reset());
    }

    public String obtenerDestinoDeCliente(UUID clienteId) {
        String clima = "";
        // Paso 1: Obtener el paquete_id del cliente desde la tabla de reservas
        ResultSet reservasResultSet = session.execute(
                "SELECT paquete_id FROM " + cassandraConfig.getKeyspace() + ".reservas WHERE cliente_id = ? ALLOW FILTERING",
                clienteId
        );

        Row reservaRow = reservasResultSet.one();
        if (reservaRow != null) {
            UUID paqueteId = reservaRow.getUuid("paquete_id");

            // Paso 2: Obtener el destino_id del paquete desde la tabla de paquetes
            ResultSet paquetesResultSet = session.execute(
                    "SELECT destino_id FROM " + cassandraConfig.getKeyspace() + ".paquetes WHERE paquete_id = ? ALLOW FILTERING",
                    paqueteId
            );

            Row paqueteRow = paquetesResultSet.one();
            if (paqueteRow != null) {
                UUID destinoId = paqueteRow.getUuid("destino_id");

                // Paso 3: Obtener los detalles del destino desde la tabla de destinos
                ResultSet destinosResultSet = session.execute(
                        "SELECT * FROM " + cassandraConfig.getKeyspace() + ".destinos WHERE destino_id = ? ALLOW FILTERING",
                        destinoId
                );

                Row destinoRow = destinosResultSet.one();
                if (destinoRow != null) {
                    // Obtener Clima
                    clima = destinoRow.getString("clima");

                } else {
                    System.out.println("No se encontró información del destino para el cliente " + clienteId);
                }
            } else {
                System.out.println("No se encontró información del paquete para el cliente " + clienteId);
            }
        } else {
            System.out.println("No se encontró información de reservas para el cliente " + clienteId);
        }
        return clima;
    }

    public void getAllClientesByDestinoClima(String clima) {
        // Crear indice para acelerar la busqueda por clima
        session.execute("CREATE INDEX IF NOT EXISTS ON " + cassandraConfig.getKeyspace() + ".destinos (clima) " +
                "USING 'org.apache.cassandra.index.sasi.SASIIndex';");

        System.out.println(ansi().fg(YELLOW).a("\t\t\t\t\t+--------------------------------------+").reset());
        System.out.println(ansi().fg(YELLOW).a("\t\t\t\t\t|         Clientes por Clima           |").reset());
        System.out.println(ansi().fg(YELLOW).a("\t\t\t\t\t+--------------------------------------+").reset());
        System.out.println(ansi().fg(CYAN).a("|             cliente_id             |      nombre      | correo_electronico |  telefono  |").reset());
        System.out.println(ansi().fg(YELLOW).a("+------------------------------------+------------------+--------------------+------------+").reset());
        // Obtener todos los clientes que tienen reserva
        rs = session.execute("SELECT * FROM " + cassandraConfig.getKeyspace() + ".reservas ALLOW FILTERING");

        for (Row row : rs) {
            UUID clienteId = row.getUuid("cliente_id"); // Obtener id del cliente
            String climaCliente = obtenerDestinoDeCliente(clienteId); // Obtener clima del cliente

            // Obtener datos de cliente por id
            ResultSet rs1 = session.execute("SELECT * FROM " + cassandraConfig.getKeyspace() + ".clientes WHERE cliente_id = ? ALLOW FILTERING", clienteId);
            for (Row row1 : rs1) {
                if (climaCliente.equalsIgnoreCase(clima)) {

                    System.out.println(
                            ansi().fg(GREEN).a("| " + clienteId).reset() + " | " +
                                    ansi().fg(GREEN).a(row1.getString("nombre")).reset() + " | " +
                                    ansi().fg(GREEN).a(row1.getString("correo_electronico")).reset() + " | " +
                                    ansi().fg(GREEN).a(row1.getString("telefono")).reset() + " |"
                    );


                }
                System.out.println(ansi().fg(YELLOW).a("+------------------------------------+------------------+--------------------+------------+").reset());

            }

        }

    }

    public void createIndexForReservasClienteFecha() {
        // Crear un índice compuesto para optimizar la búsqueda de reservas por cliente y fecha
        session.execute("CREATE INDEX IF NOT EXISTS ON " + cassandraConfig.getKeyspace() + ".reservas (fecha_inicio);");
        session.execute("CREATE INDEX IF NOT EXISTS ON " + cassandraConfig.getKeyspace() + ".reservas (fecha_fin);");
    }

    public void getAllReservasByClienteAndDateRange(UUID clienteId, LocalDate startDate, LocalDate endDate) {
        ResultSet rs = session.execute("SELECT * FROM  " + cassandraConfig.getKeyspace() + ".reservas " +
                "WHERE cliente_id = ? AND fecha_inicio >= ? AND fecha_inicio <= ? ALLOW FILTERING", clienteId, startDate, endDate);

        System.out.println(ansi().fg(YELLOW).a("\t\t\t\t\t+--------------------------------------+").reset());
        System.out.println(ansi().fg(YELLOW).a("\t\t\t\t\t| Reservas por Cliente y Rango de Fechas |").reset());
        System.out.println(ansi().fg(YELLOW).a("\t\t\t\t\t+--------------------------------------+").reset());

        System.out.println(ansi().fg(CYAN).a("|             reserva_id             |             paquete_id             | fecha_inicio | fecha_fin | pagado |").reset());
        System.out.println(ansi().fg(YELLOW).a("+------------------------------------+------------------------------------+--------------+-----------+--------+").reset());

        for (Row row : rs) {
            System.out.println(
                    ansi().fg(GREEN).a("| " + row.getUuid("reserva_id")).reset() + " | " +
                            ansi().fg(GREEN).a(row.getUuid("paquete_id")).reset() + " | " +
                            ansi().fg(GREEN).a(row.getLocalDate("fecha_inicio")).reset() + " | " +
                            ansi().fg(GREEN).a(row.getLocalDate("fecha_fin")).reset() + " | " +
                            ansi().fg(GREEN).a(row.getBoolean("pagado")).reset() + " |"
            );
        }

        System.out.println(ansi().fg(YELLOW).a("+------------------------------------+------------------------------------+--------------+-----------+--------+").reset());
    }

    public void createIndiceDestinosPorPaisTable() {
        session.execute("CREATE TABLE IF NOT EXISTS " + cassandraConfig.getKeyspace() + ".destinos_por_pais ("
                + "destino_id UUID, "
                + "nombre TEXT, "
                + "pais TEXT, "
                + "descripcion TEXT, "
                + "clima TEXT, "
                + "PRIMARY KEY((destino_id, nombre), pais, clima));");
        session.execute("CREATE INDEX IF NOT EXISTS ON " + cassandraConfig.getKeyspace() + ".destinos_por_pais (pais);");
    }

    public void insertDestinosPorPais(String pais) {
        // Obtener los destinos para el pais
        rs = session.execute("SELECT * FROM " + cassandraConfig.getKeyspace() + ".destinos WHERE pais = ? ALLOW FILTERING", pais);

        // Guardar datos en la tabla indice_destinos_por_pais
        for (Row row : rs) {
            UUID destino_id = row.getUuid("destino_id");
            String nombre = row.getString("nombre");
            String descripcion = row.getString("descripcion");
            String clima = row.getString("clima");
            session.execute("INSERT INTO " + cassandraConfig.getKeyspace() + ".destinos_por_pais (destino_id, nombre, pais, descripcion, clima) VALUES (?,?,?,?,?) IF NOT EXISTS;", destino_id, nombre, pais, descripcion, clima);
        }

    }

    public void createTableDestinosPopulares() {
        // Crear tabla de resumen de destinos populares
        session.execute("CREATE TABLE IF NOT EXISTS  " + cassandraConfig.getKeyspace() + ".destinos_populares ("
                + "destino_id UUID,"
                + "nombreDestino TEXT,"
                + "totalReservado INT,"
                + "PRIMARY KEY(destino_id, totalReservado)"
                + ") WITH CLUSTERING ORDER BY (totalReservado DESC);");

        // obtener todas las reservas
        rs = session.execute("SELECT * FROM " + cassandraConfig.getKeyspace() + ".reservas");
        for (Row reserva : rs) {
            // Obtener id del paquete desde la reserva
            UUID idPaquete = reserva.getUuid("paquete_id");
            rs = session.execute("SELECT * FROM " + cassandraConfig.getKeyspace() + ".paquetes WHERE paquete_id =? ALLOW FILTERING", idPaquete);

            for (Row paquete : rs) {
                // Obtener id del destino desde el paquete
                UUID idDestino = paquete.getUuid("destino_id");
                String nombreDestino = paquete.getString("nombre");
                // Obtener cantidad de apariciones de cada destino en paquetes

                rs = session.execute("SELECT COUNT(destino_id) as total_destino_reservado FROM " + cassandraConfig.getKeyspace() + ".paquetes WHERE destino_id =? ALLOW FILTERING", idDestino);
                Row row = rs.one();
                if (row != null) {
                    int totalDestinoReservado = (int) row.getLong("total_destino_reservado");
                    // Insertar datos en la tabla destinos_populares
                    session.execute("INSERT INTO " + cassandraConfig.getKeyspace() + ".destinos_populares (destino_id, nombreDestino, totalReservado) VALUES (?,?,?) IF NOT EXISTS;", idDestino, nombreDestino, totalDestinoReservado);
                }
            }


        }
    }

    public void showDestinosPopulares() {
        rs = session.execute("SELECT * FROM " + cassandraConfig.getKeyspace() + ".destinos_populares");

        // Usar una lista para almacenar todos los registros
        List<RegistroDestino> destinosList = new ArrayList<>();

        // Iterar sobre los registros y llenar la lista
        for (Row row : rs) {
            UUID destinoId = row.getUuid("destino_id");
            String nombreDestino = row.getString("nombreDestino");
            int totalReservado = row.getInt("totalReservado");

            // Crear un objeto RegistroDestino y agregarlo a la lista
            destinosList.add(new RegistroDestino(destinoId, nombreDestino, totalReservado));
        }

        // Ordenar la lista por totalReservado en orden descendente
        destinosList.sort(Comparator.comparingInt(RegistroDestino::getTotalReservado).reversed());

        // Eliminar los registros de la tabla
        // rs = session.execute("TRUNCATE " + cassandraConfig.getKeyspace() + ".destinos_populares");

        // Imprimir los registros
        System.out.println(ansi().fg(YELLOW).a("\t\t\t\t\t+--------------------------------------+").reset());
        System.out.println(ansi().fg(YELLOW).a("\t\t\t\t\t|         Destinos Populares           |").reset());
        System.out.println(ansi().fg(YELLOW).a("\t\t\t\t\t+--------------------------------------+").reset());

        System.out.println(ansi().fg(CYAN).a("|             destino_id             |   nombreDestino   | totalReservado |").reset());
        System.out.println(ansi().fg(YELLOW).a("+------------------------------------+-------------------+----------------+").reset());

        // Iterar sobre la lista y mostrar los registros
        for (RegistroDestino registro : destinosList) {
            UUID destinoId = registro.getDestinoId();
            String nombreDestino = registro.getNombreDestino();
            int totalReservado = registro.getTotalReservado();

            // Insertar registros ordenados a la tabla
            // rs = session.execute("INSERT INTO "+cassandraConfig.getKeyspace()+".destinos_populares (destino_id, nombreDestino, totalReservado) VALUES(?,?,?) IF NOT EXISTS;", destinoId, nombreDestino, totalReservado);

            // Imprimir los datos
            System.out.println(
                    ansi().fg(GREEN).a("| " + destinoId).reset() + " | " +
                            ansi().fg(GREEN).a(nombreDestino).reset() + " | " +
                            ansi().fg(GREEN).a(totalReservado).reset() + " |"
            );
        }

        System.out.println(ansi().fg(YELLOW).a("+------------------------------------+-------------------+----------------+").reset());
    }


    public void createIndiceClientesPorCorreoTable() {
        session.execute("CREATE TABLE IF NOT EXISTS " + cassandraConfig.getKeyspace() + ".clientes_por_correo ("
                + "cliente_id UUID, "
                + "nombre TEXT, "
                + "correo_electronico TEXT, "
                + "telefono TEXT, "
                + "PRIMARY KEY((cliente_id, nombre), correo_electronico));");

        // Crear índice en correo_electronico para acelerar la búsqueda
        session.execute("CREATE INDEX IF NOT EXISTS ON " + cassandraConfig.getKeyspace() + ".clientes_por_correo (correo_electronico);");
    }

    public void insertClientesPorCorreo(String correo) {
        rs = session.execute("SELECT * FROM " + cassandraConfig.getKeyspace() + ".clientes WHERE correo_electronico = ? ALLOW FILTERING", correo);
        for (Row row : rs) {
            UUID idCliente = row.getUuid("cliente_id");
            String nombreCliente = row.getString("nombre");
            String telefono = row.getString("telefono");
            session.execute("INSERT INTO " + cassandraConfig.getKeyspace() + ".clientes_por_correo (cliente_id, nombre, correo_electronico, telefono) VALUES (?, ?, ?, ?) IF NOT EXISTS;", idCliente, nombreCliente, correo, telefono);
        }

    }


    public void getPaquetesForName(String nombre) {

        rs = session.execute("SELECT * FROM " + cassandraConfig.getKeyspace() + ".paquetes WHERE nombre = ?", nombre);

        System.out.println(ansi().fg(YELLOW).a("\t\t\t\t\t+--------------------------------------+").reset());
        System.out.println(ansi().fg(YELLOW).a("\t\t\t\t\t|          Detalles del Paquete        |").reset());
        System.out.println(ansi().fg(YELLOW).a("\t\t\t\t\t+--------------------------------------+").reset());

        System.out.println(ansi().fg(CYAN).a("|             paquete_id             |     nombre    |             destino_id             | duracion | precio |").reset());
        System.out.println(ansi().fg(YELLOW).a("+------------------------------------+---------------+------------------------------------+----------+--------+").reset());

        for (Row row : rs) {
            System.out.println(
                    ansi().fg(GREEN).a("| " + row.getUuid("paquete_id")).reset() + " | " +
                            ansi().fg(GREEN).a(row.getString("nombre")).reset() + " | " +
                            ansi().fg(GREEN).a(row.getUuid("destino_id")).reset() + " | " +
                            ansi().fg(GREEN).a(row.getInt("duracion")).reset() + " | " +
                            ansi().fg(GREEN).a(row.getBigDecimal("precio")).reset() + " |"
            );
        }

        System.out.println(ansi().fg(YELLOW).a("+------------------------------------+---------------+------------------------------------+----------+--------+").reset());
    }

    public void createIndexForPaquetesByName() {
        session.execute("CREATE INDEX IF NOT EXISTS paquetes_nombre_idx ON  " + cassandraConfig.getKeyspace() + ".paquetes (nombre)");
    }

    public void createIndiceDisponibilidadPaquetesPorDestinoYFecha() {
        session.execute("CREATE TABLE IF NOT EXISTS " + cassandraConfig.getKeyspace() + ".disponibilidad_paquetes ("
                + "destino_id UUID,"
                + "fecha DATE,"
                + "paquete_id UUID,"
                + "nombre TEXT,"
                + "duracion INT,"
                + "precio DECIMAL,"
                + "PRIMARY KEY((paquete_id), fecha, destino_id, duracion));");
        session.execute("CREATE INDEX IF NOT EXISTS indice_disponibilidad_paquetes_destino_idx ON " + cassandraConfig.getKeyspace() + ".disponibilidad_paquetes (destino_id);");
        session.execute("CREATE INDEX IF NOT EXISTS indice_disponibilidad_paquetes_fecha_idx ON " + cassandraConfig.getKeyspace() + ".disponibilidad_paquetes (fecha);");
    }

    public void getPaquetesDisponibles() {
        // Obtener paquetes reservados
        ResultSet reservas = session.execute("SELECT * FROM " + cassandraConfig.getKeyspace() + ".reservas");
        List<RegistroPaquete> listReservas = new ArrayList<>();
        listReservas = getListPaquete(reservas, listReservas, 2); // Obtener lista de paquetes reservados

        // Obtener id existentes de la tabla destinos
        rs = session.execute("SELECT * FROM " + cassandraConfig.getKeyspace() + ".destinos");
        List<UUID> idDestinos = new ArrayList<>();
        for (Row row : rs) {
            idDestinos.add(row.getUuid("destino_id"));
        }

        // Insertar nuevos registros en la tabla paquetes por si acaso
        int duracion = (int) (Math.random() * 10) + 1;
        BigDecimal precio = BigDecimal.valueOf(Math.round((Math.random() * 1000) * 100.0) / 100.0);

        rs = session.execute("INSERT INTO " + cassandraConfig.getKeyspace() + ".paquetes(paquete_id, nombre, destino_id, duracion, precio) VALUES(?,?,?,?,?);", UUID.randomUUID(), "Retiro de Bienestar", idDestinos.get(1), 7, precio);
        rs = session.execute("INSERT INTO " + cassandraConfig.getKeyspace() + ".paquetes(paquete_id, nombre, destino_id, duracion, precio) VALUES(?,?,?,?,?);", UUID.randomUUID(), "Viaje de Montaña", idDestinos.get(new Random().nextInt(0, idDestinos.size() - 1)), duracion, precio);
        rs = session.execute("INSERT INTO " + cassandraConfig.getKeyspace() + ".paquetes(paquete_id, nombre, destino_id, duracion, precio) VALUES(?,?,?,?,?);", UUID.randomUUID(), "Descanso en Resorts de Lujo", idDestinos.get(new Random().nextInt(0, idDestinos.size() - 1)), duracion, precio);

        // Obtener todos los paquetes
        ResultSet paquetes = session.execute("SELECT * FROM " + cassandraConfig.getKeyspace() + ".paquetes");
        List<RegistroPaquete> listPaquetes = new ArrayList<>();
        listPaquetes = getListPaquete(paquetes, listPaquetes, 1); // Obtener lista de todos los paquetes

        List<RegistroPaquete> listPaquetesDisponibles = new ArrayList<>();

        // Comprobar existencia de paquetes en reservas
        for (RegistroPaquete paquete : listPaquetes) {
            if (!listReservas.stream().anyMatch(paq -> paq.getPaqueteId().equals(paquete.getPaqueteId()))) { // Si no existe paquete en reserva
                // Generar fecha aleatoria entre hoy y 30 días después
                Random random = new Random();
                int diasAleatorios = random.nextInt(30);
                LocalDate fecha = LocalDate.now().plusDays(diasAleatorios);
                listPaquetesDisponibles.add(new RegistroPaquete(paquete.getPaqueteId(), paquete.getNombre(), paquete.getDestinoId(), fecha, duracion, paquete.getPrecio())); // Añadir paquete
            }
        }

        // Insertar paquetes disponibles a la tabla
        for (RegistroPaquete paquete : listPaquetesDisponibles) {
            rs = session.execute("INSERT INTO " + cassandraConfig.getKeyspace() + ".disponibilidad_paquetes (paquete_id, nombre, destino_id, fecha, duracion, precio) VALUES(?,?,?,?,?,?);", paquete.getPaqueteId(), paquete.getNombre(), paquete.getDestinoId(), paquete.getFecha(), paquete.getDuracion(), paquete.getPrecio());
        }

    }

    public void mostrarTablaDisponibilidadPaquetes() {
        ResultSet rs = session.execute("SELECT * FROM " + cassandraConfig.getKeyspace() + ".disponibilidad_paquetes");

        // Imprimir los registros
        System.out.println(ansi().fg(GREEN).a("\t\t\t\t\t\t+--------------------------------------+"));
        System.out.println("\t\t\t\t\t\t|      Tabla Disponibilidad Paquetes   |");
        System.out.println("\t\t\t\t\t\t+--------------------------------------+");

        System.out.println("\t|             paquete_id              |         nombre         |             destino_id            |   fecha   | duracion | precio |");
        System.out.println("\t+-------------------------------------+------------------------+-----------------------------------+-----------+----------+--------+");

        // Iterar sobre los registros y mostrarlos
        for (Row row : rs) {
            UUID paqueteId = row.getUuid("paquete_id");
            LocalDate fecha = row.getLocalDate("fecha");
            UUID destinoId = row.getUuid("destino_id");
            String nombre = row.getString("nombre");
            int duracion = row.getInt("duracion");
            BigDecimal precio = row.getBigDecimal("precio");

            // Imprimir los datos
            System.out.println("\t| " + paqueteId + " | " + nombre + " | " + destinoId + " | " + fecha + " | " + duracion + " | " + precio + " |");
        }

        System.out.println("\t+-------------------------------------+------------------------+-----------------------------------+-----------+----------+--------+");
    }


    public List<RegistroPaquete> getListPaquete(ResultSet rs, List<RegistroPaquete> listPaquetes, int tipo) {
        for (Row row : rs) {
            // Obtener id de paquete
            UUID idPaquete = row.getUuid("paquete_id");
            if (tipo == 1) {
                String nombre = row.getString("nombre");
                UUID idDestino = row.getUuid("destino_id");
                BigDecimal precio = row.getBigDecimal("precio");
                int duracion = row.getInt("duracion");
                listPaquetes.add(new RegistroPaquete(idPaquete, nombre, idDestino, duracion, precio));
            } else if (tipo == 2) {
                listPaquetes.add(new RegistroPaquete(idPaquete));
            }

        }
        return listPaquetes;
    }

    public void getPaquetesPorDestinoYDuracion(UUID destinoId, int duracion) {
        rs = session.execute("SELECT * " +
                "FROM  " + cassandraConfig.getKeyspace() + ".disponibilidad_paquetes " +
                "WHERE destino_id = ? AND duracion = ? ALLOW FILTERING", destinoId, duracion);

        System.out.println(ansi().fg(YELLOW).a("\t\t\t\t\t+--------------------------------------+").reset());
        System.out.println(ansi().fg(YELLOW).a("\t\t\t\t\t|      Paquetes por Destino y Duración       |").reset());
        System.out.println(ansi().fg(YELLOW).a("\t\t\t\t\t+--------------------------------------+").reset());

        System.out.println(ansi().fg(CYAN).a("|             paquete_id             |      nombre      |             destino_id             | duracion | precio |").reset());
        System.out.println(ansi().fg(YELLOW).a("+------------------------------------+------------------+------------------------------------+----------+--------+").reset());

        for (Row row : rs) {
            System.out.println(
                    ansi().fg(GREEN).a("| " + row.getUuid("paquete_id")).reset() + " | " +
                            ansi().fg(GREEN).a(row.getString("nombre")).reset() + " | " +
                            ansi().fg(GREEN).a(row.getUuid("destino_id")).reset() + " | " +
                            ansi().fg(GREEN).a(row.getInt("duracion")).reset() + " | " +
                            ansi().fg(GREEN).a(row.getBigDecimal("precio")).reset() + " |"
            );
        }

        System.out.println(ansi().fg(YELLOW).a("+------------------------------------+------------------+------------------------------------+----------+--------+").reset());
    }

    public void createIndexForReservasPorClienteDestinoYEstadoPago() {
        session.execute("CREATE INDEX IF NOT EXISTS reservas_cliente_destino_pagado_idx " +
                "ON  " + cassandraConfig.getKeyspace() + ".reservas (cliente_id, paquete_id, pagado)");
    }

    public void getReservasPorClienteDestinoYEstadoPago(UUID clienteId, UUID destinoId, boolean pagado) {
        boolean existClient = false;
        boolean existDestin = false;
        // Obtener reservas dado el cliente y el estado de pago
        rs = session.execute("SELECT * FROM " + cassandraConfig.getKeyspace() + ".reservas WHERE cliente_id = ? AND pagado = ? ALLOW FILTERING", clienteId, pagado);
        for (Row row : rs) {
            // Comprobar existencia del cliente
            if (clienteId.equals(row.getUuid("cliente_id"))) {
                // Obtener id paquete
                UUID idPaquete = row.getUuid("paquete_id");
                // Obtener paquetes
                ResultSet rs1 = session.execute("SELECT * FROM " + cassandraConfig.getKeyspace() + ".paquetes WHERE destino_id = ? ALLOW FILTERING", destinoId);
                for (Row row1 : rs1) {
                    // Obtener destino de paquete
                    UUID idDestino = row1.getUuid("destino_id");

                    if (idDestino.equals(destinoId)) {
                        // Mostrar reservas
                        System.out.println(ansi().fg(YELLOW).a("\t\t\t\t\t+--------------------------------------+").reset());
                        System.out.println(ansi().fg(YELLOW).a("\t\t\t\t\t| Reservas por Cliente, Destino y Estado de Pago |").reset());
                        System.out.println(ansi().fg(YELLOW).a("\t\t\t\t\t+--------------------------------------+").reset());

                        System.out.println(ansi().fg(CYAN).a("|             reserva_id             |             paquete_id             |             cliente_id             |  fecha_inicio  |  fecha_fin  | pagado |").reset());
                        System.out.println(ansi().fg(YELLOW).a("+------------------------------------+------------------------------------+------------------------------------+----------------+-------------+--------+").reset());

                        System.out.println(
                                ansi().fg(GREEN).a("| " + row.getUuid("reserva_id")).reset() + " | " +
                                        ansi().fg(GREEN).a(row.getUuid("paquete_id")).reset() + " | " +
                                        ansi().fg(GREEN).a(row.getUuid("cliente_id")).reset() + " | " +
                                        ansi().fg(GREEN).a(row.getLocalDate("fecha_inicio")).reset() + " | " +
                                        ansi().fg(GREEN).a(row.getLocalDate("fecha_fin")).reset() + " | " +
                                        ansi().fg(GREEN).a(row.getBoolean("pagado")).reset() + " |"
                        );
                        existDestin = true;
                    }

                    System.out.println(ansi().fg(YELLOW).a("+------------------------------------+------------------------------------+------------------------------------+----------------+-------------+--------+").reset());
                }
                existClient = true;
            }
        }

        if (!existClient) {
            System.out.println(ansi().fg(RED).a("==>No existe el cliente").reset());
        }

        if (!existDestin) {
            System.out.println(ansi().fg(RED).a("==>No existe el destino").reset());
        }

    }

    // Eliminar base de datos si existe
    public void limpiarDatos() {
        String keyspace = cassandraConfig.getKeyspace();
        if(existKeyspace(keyspace)) {
            truncateTable("destinos");
            truncateTable("clientes");
            truncateTable("paquetes");
            truncateTable("reservas");
            truncateTable("destinos_populares");
            truncateTable("destinos_por_pais");
            truncateTable("disponibilidad_paquetes");
            truncateTable("resumen_reservas_por_paquete");
            truncateTable("clientes_por_correo");
        } else {
            System.out.println(ansi().fg(RED).a("==>El Keyspace " + keyspace + " no existe.").reset());
        }

    }

    public void truncateTable(String tableName) {
        if(existeTabla(tableName)){
            String truncateQuery = String.format("TRUNCATE %s.%s;", cassandraConfig.getKeyspace(), tableName);
            // Ejecutar la consulta de truncado
            session.execute(truncateQuery);
            System.out.println(ansi().fg(GREEN).a("==>Datos de la tabla " + tableName + " en el Keyspace " + cassandraConfig.getKeyspace() + " eliminados.").reset());
        }
    }

    // Verificar si existe el keyspace
    public boolean existKeyspace(String keyspace) {
        List<Row> result = session.execute("SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = ?", keyspace).all();
        return !result.isEmpty();
    }

    public void cargarDatos() throws IOException {
        // Insertar datos en la base de datos
        DataInitializer dataInitializer = new DataInitializer();
        dataInitializer.initializeData(session, 50, 100, 500, 200);
    }

    // Eliminar registro por ID en Cassandra
    public void eliminarRegistroPorId(String nameTable, UUID id) {
        if (existeTabla(nameTable)) {
            String deleteQuery = "";
            PreparedStatement preparedStatement = null;
            ResultSet rs = null;
                switch (nameTable) {
                    case "clientes":
                        // Preparar la sentencia DELETE según la colección
                        deleteQuery = "DELETE FROM " + cassandraConfig.getKeyspace() + "." + nameTable + " WHERE cliente_id = ?";
                        preparedStatement = session.prepare(deleteQuery);

                        // Ejecutar la sentencia DELETE con el ID
                         rs = session.execute(preparedStatement.bind(id));
                        if(rs.one() == null){
                            System.out.println(ansi().fg(GREEN).a("\t\t\t\t\t| Cliente " + id + " eliminado correctamente").reset());
                        }else{
                            System.out.println(ansi().fg(RED).a("\t\t\t\t\t| No existe un cliente con el id: " + id).reset());
                        }

                        break;

                    case "destinos":
                        // Preparar la sentencia DELETE según la colección
                        deleteQuery = "DELETE FROM " + cassandraConfig.getKeyspace() + "." + nameTable + " WHERE destino_id = ?";
                        preparedStatement = session.prepare(deleteQuery);

                        // Ejecutar la sentencia DELETE con el ID
                        rs = session.execute(preparedStatement.bind(id));
                        if(rs.one() != null){
                            System.out.println(ansi().fg(GREEN).a("\t\t\t\t\t| Destino " + id + " eliminado correctamente").reset());
                        }else{
                            System.out.println(ansi().fg(RED).a("\t\t\t\t\t| No existe un destino con el id: " + id).reset());
                        }
                        break;

                    case "paquetes":
                        // Preparar la sentencia DELETE según la colección
                        deleteQuery = "DELETE FROM " + cassandraConfig.getKeyspace() + "." + nameTable + " WHERE paquete_id = ?";
                        preparedStatement = session.prepare(deleteQuery);

                        rs = session.execute(preparedStatement.bind(id));
                        if(rs.one() != null){
                            System.out.println(ansi().fg(GREEN).a("\t\t\t\t\t| Paquete " + id + " eliminado correctamente").reset());
                        }else{
                            System.out.println(ansi().fg(RED).a("\t\t\t\t\t| No existe un paquete con el id: " + id).reset());
                        }
                        break;

                    case "reservas":
                        // Preparar la sentencia DELETE según la colección
                        deleteQuery = "DELETE FROM " + cassandraConfig.getKeyspace() + "." + nameTable + " WHERE reserva_id = ?";
                        preparedStatement = session.prepare(deleteQuery);

                        // Ejecutar la sentencia DELETE con el ID
                        rs = session.execute(preparedStatement.bind(id));
                        if(rs.one() != null){
                            System.out.println(ansi().fg(GREEN).a("\t\t\t\t\t| Reserva " + id + " eliminado correctamente").reset());
                        }else{
                            System.out.println(ansi().fg(RED).a("\t\t\t\t\t| No existe una reserva con el id: " + id).reset());
                        }
                        break;

                    default:
                        System.err.println(ansi().fg(RED).a("==>Error, NO existe una tabla: " + nameTable + " con el id " + id).reset());
                        break;
                }


        } else {
            System.err.println(ansi().fg(RED).a("Error, NO existe la tabla: " + nameTable).reset());
        }
    }


    public void dropTablasSecundarias() {
        dropTableIfExists("destinos_populares");
        dropTableIfExists("destinos_por_pais");
        dropTableIfExists("disponibilidad_paquetes");
        dropTableIfExists("resumen_reservas_por_paquete");
        dropTableIfExists("clientes_por_correo");
    }

    // Eliminar tabla si existe
    public void dropTableIfExists(String nameTable) {

        if (existeTabla(nameTable)) {
            // Ejecutar la instrucción CQL para eliminar la tabla
            try {
                session.execute(SimpleStatement.newInstance("DROP TABLE IF EXISTS " + nameTable));
                System.out.println(ansi().fg(GREEN).a("==>Tabla '" + nameTable + "' eliminada exitosamente.").reset());
            } catch (DriverTimeoutException e) {
                System.err.println(ansi().fg(GREEN).a("==>Tabla '" + nameTable + "' eliminada exitosamente.").reset());
            }
        } else {
            System.out.println(ansi().fg(RED).a("La tabla '" + nameTable + "' no existe.").reset());
        }

    }

    // Verificar si existe la tabla
    public boolean existeTabla(String nameTable) {
        List<Row> result = session.execute("SELECT table_name FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?", cassandraConfig.getKeyspace(), nameTable).all();
        return !result.isEmpty();
    }


}
