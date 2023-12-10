package unex;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.github.javafaker.Faker;

import java.io.*;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.LocalDate;
import java.util.*;

import static org.fusesource.jansi.Ansi.Color.*;
import static org.fusesource.jansi.Ansi.ansi;

/**
 * @author Jose Luis Obiang Ela Nanguang
 * @version 1.0
 */
public class DataInitializer {

    private String query;
    private final CassandraConfig cassandraConfig;

    public DataInitializer() {
        this.cassandraConfig = new CassandraConfig();
    }

    public void initializeData(CqlSession session, int numDestinos, int numPaquetes, int numReservas, int numClientes) throws IOException {
        ResultSet rs = null;
        int min = 0, max = 9, range = max - min + 1;
        int index = 0;
        boolean exist = false;
        String nombre = "";
        Faker faker = new Faker(); // Para la generación de datos aleatorios

        File fileData = new File("src/main/resources/data/bd.txt");
        if (fileData.exists()) {
            FileReader fr = null;
            BufferedReader br = null;

            List<String> clientes = new ArrayList<>();
            List<String> paquetes = new ArrayList<>();
            List<String> emailDomains = new ArrayList<>();
            List<String> destinos = new ArrayList<>();
            List<String> descripcionDestinos = new ArrayList<>();
            List<String> climas = new ArrayList<>();
            List<String> paises = new ArrayList<>();

            try {
                fr = new FileReader(fileData);
                br = new BufferedReader(fr);
                String linea, newLinea;
                while ((linea = br.readLine()) != null) {
                    if (!linea.isEmpty()) {
                        newLinea = linea.substring(linea.indexOf(':') + 1).trim();
                        switch (linea.substring(0, linea.indexOf(':'))) {
                            case "clientes":
                                clientes = List.of(newLinea.split(",")); // Nombres de clientes
                                break;
                            case "paquetes":
                                paquetes = List.of(newLinea.split(",")); // Nombres de paquetes
                                break;
                            case "climas":
                                climas = List.of(newLinea.split(",")); // Nombres de climas
                                break;
                            case "destinos":
                                destinos = List.of(newLinea.split(",")); // Nombres de destinos
                                break;
                            case "descripcionDestinos":
                                descripcionDestinos = List.of(newLinea.split(",")); // Descripcion de cada uno de los destinos
                                break;
                            case "paises":
                                paises = List.of(newLinea.split(",")); // Nombre de los paises
                                break;
                            case "emailDomains":
                                emailDomains = List.of(newLinea.split(",")); // Nombre de dominios de correo electronico
                                break;
                            default:
                                break;
                        }
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                if (br != null) {
                    try {
                        br.close();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
                if (fr != null) {
                    try {
                        fr.close();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }

            System.out.println(ansi().fg(YELLOW).a(("\n\n\t\t\t\t================Insertando registros================")).reset());
            // Insertar destinos
            System.out.println(ansi().fg(BLUE).a(("\n\n\t\t\t\t==>TABLA DESTINOS:")).reset());
            for (int i = 0; i < numDestinos; i++) {
                UUID destinoId = UUID.randomUUID();
                nombre = destinos.get(i);
                String pais = paises.get(i);
                String descripcion = descripcionDestinos.get(i);
                String clima = climas.get((int) (Math.random() * climas.size()));

                // Instrucción INSERT
                query = String.format("INSERT INTO " + cassandraConfig.getKeyspace() + ".destinos " +
                        "(destino_id, nombre, pais, descripcion, clima) " +
                        "VALUES (?, ?, ?, ?, ?) IF NOT EXISTS");

                // Ejecutar la instrucción INSERT
                if(session.execute(query, destinoId, nombre, pais, descripcion, clima) != null){
                    System.out.println(ansi().fg(BLUE).a(("===>Destino insertado: " + destinoId)).reset());
                }


            }

            rs = session.execute("SELECT destino_id FROM  " + cassandraConfig.getKeyspace() + ".destinos"); // Obtener id de todos los destinos
            List<UUID> destinoIdsExistente = new ArrayList<>();

            for (Row row : rs) {
                destinoIdsExistente.add(row.getUuid("destino_id"));
            }

            // Insertar clientes
            System.out.println(ansi().fg(BLUE).a(("\n\n\t\t\t\t==>TABLA CLIENTES:")).reset());
            for (int i = 0; i < numClientes; i++) {
                UUID clienteId = UUID.randomUUID();
                //nombre = clientes.get(i);
                nombre = faker.name().fullName();
                //String correoElectronico = nombre + emailDomains.get((int) Math.random() * emailDomains.size());
                String correoElectronico = faker.internet().emailAddress(nombre).replace(" ", "");
                String telefono = "+34-" + String.valueOf((int) (Math.random() * range) + min) + String.valueOf((int) (Math.random() * range) + min) + String.valueOf((int) (Math.random() * range) + min) + "-" + String.valueOf((int) (Math.random() * range) + min) + String.valueOf((int) (Math.random() * range) + min) + "-" + String.valueOf((int) (Math.random() * range) + min) + String.valueOf((int) (Math.random() * range) + min) + "-" + String.valueOf((int) (Math.random() * range) + min) + String.valueOf((int) (Math.random() * range) + min); // Ajusta según sea necesario

                // Instrucción INSERT
                query = String.format("INSERT INTO " + cassandraConfig.getKeyspace() + ".clientes (cliente_ID, nombre, correo_electronico, telefono) " +
                        "VALUES (?,?,?,?) IF NOT EXISTS;", cassandraConfig.getKeyspace());

                // Ejecutar la instrucción
                session.execute(query, clienteId, nombre, correoElectronico, telefono);

                System.out.println(ansi().fg(BLUE).a(("===>Cliente insertado: " + clienteId)).reset());

            }

            rs = session.execute("SELECT cliente_id FROM " + cassandraConfig.getKeyspace() + ".clientes"); // Obtener id de todos los clientes
            List<UUID> clienteIdsExistente = new ArrayList<>();
            for (Row row : rs) {
                clienteIdsExistente.add(row.getUuid("cliente_id"));
            }

            // Insertar paquetes
            System.out.println(ansi().fg(BLUE).a(("\n\n\t\t\t\t==>TABLA PAQUETES:")).reset());
            for (int i = 0; i < numPaquetes; i++) {

                UUID paqueteId = UUID.randomUUID();
                nombre = paquetes.get(i);
                UUID destinoId = destinoIdsExistente.get((int) (Math.random() * destinoIdsExistente.size())); // Obtener id de destino existente en la tabla destinos aleatoriamente
                int duracion = (int) (Math.random() * 10) + 1; // Duración aleatoria entre 1 y 10 días
                BigDecimal precio = BigDecimal.valueOf(Math.round((Math.random() * 1000) * 100.0) / 100.0); // Precio aleatorio;

                // Instrucción INSERT
                query = String.format("INSERT INTO " + cassandraConfig.getKeyspace() + ".paquetes (paquete_id, nombre, destino_id, duracion, precio) " +
                        "VALUES (?, ?, ?, ?, ?) IF NOT EXISTS;", cassandraConfig.getKeyspace());

                // Ejecutar la instrucción INSERT
                session.execute(query, paqueteId, nombre, destinoId, duracion, precio);
                System.out.println(ansi().fg(BLUE).a(("===>Paquete insertado: " + destinoId)).reset());
            }

            rs = session.execute("SELECT paquete_id FROM " + cassandraConfig.getKeyspace() + ".paquetes"); // Obtener id de todos los paquetes
            List<UUID> paqueteIdsExistente = new ArrayList<>();
            for (Row row : rs) {
                paqueteIdsExistente.add(row.getUuid("paquete_id"));
            }

            // Insertar reservas
            System.out.println(ansi().fg(BLUE).a(("\n\n\t\t\t\t==>TABLA RESERVAS:")).reset());
            for (int i = 0; i < numReservas; i++) {
                UUID reservaId = UUID.randomUUID();
                UUID paqueteId = paqueteIdsExistente.get((int) (Math.random() * paqueteIdsExistente.size())); // Obtener id de paquete existente en la tabla paquetes aleatoriamente
                UUID clienteId = clienteIdsExistente.get((int) (Math.random() * clienteIdsExistente.size())); // Obtener id de cliente existente en la tabla clientes aleatoriamente
                LocalDate fechaInicio = LocalDate.now().plusDays(i); // Fecha de inicio incremental
                LocalDate fechaFin = fechaInicio.plusDays((int) (Math.random() * 10) + 1); // Duración aleatoria entre 1 y 10 días
                boolean pagado = Math.random() < 0.5; // Pagado aleatorio

                // Instrucción INSERT
                query = String.format("INSERT INTO " + cassandraConfig.getKeyspace() + ".reservas (reserva_id, paquete_id, cliente_id, fecha_inicio, fecha_fin, pagado) " +
                        "VALUES (?, ?, ?, ?, ?, ?) IF NOT EXISTS;", cassandraConfig.getKeyspace());


                // Ejecutar la instrucción
                session.execute(query, reservaId, paqueteId, clienteId, fechaInicio, fechaFin, pagado);

                System.out.println(ansi().fg(BLUE).a(("===>Reserva insertada: " + reservaId)).reset());
            }


            System.out.println(ansi().fg(YELLOW).a(("================Datos inicializados================")).reset());

        } else {
            System.out.println(ansi().fg(RED).a("--->NO existe el archivo 'bd.txt'").reset());
        }

    }

}
