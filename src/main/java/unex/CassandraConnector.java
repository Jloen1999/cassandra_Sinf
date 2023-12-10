package unex;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;

import java.net.InetSocketAddress;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static org.fusesource.jansi.Ansi.Color.*;
import static org.fusesource.jansi.Ansi.ansi;

/**
 * @author Jose Luis Obiang Ela Nanguang
 * @version 1.0
 */
public class CassandraConnector {

    private static final String KEYSPACE_NAME = "cassandraDB";
    private static final Scanner in = new Scanner(System.in);
    private static UUID id;
    private static String input = "";
    private static boolean opcionValida = false;
    int opcion = 0;

    // Crear un objeto de sesión
    private final CqlSession session;

    // Crear un objeto de configuración
    private final CassandraConfig cassandraConfig;
    private static List<String> opcionesList;
    private QueryExecutor queryExecutor;

    public CassandraConnector() {
        this.cassandraConfig = new CassandraConfig();
        this.session = createSession();
        this.queryExecutor = new QueryExecutor(session);
        // Crea el keyspace y las tablas
        createKeyspace();
        createTables();
    }

    private CqlSession createSession() {
        // Inicializa la sesión aquí
        // Crear un objeto de constructor de sesión
        // Cargar la configuración del controlador
        // Especificar el punto de contacto del cluster de Cassandra
        // Especificar el centro de datos local
        // Crear un objeto de configuración del controlador
        DriverConfigLoader loader = DriverConfigLoader.fromClasspath("application.conf");
        return CqlSession.builder()
                .withConfigLoader(loader) // Cargar la configuración del controlador
                .addContactPoint(new InetSocketAddress(cassandraConfig.getHost(), cassandraConfig.getPort())) // Especificar el punto de contacto del cluster de Cassandra
                .withLocalDatacenter(cassandraConfig.getDataCenter())
                .withAuthCredentials(cassandraConfig.getUsername(), cassandraConfig.getPassword())
                .build();

    }

    public CqlSession getSession() {
        return session;
    }

    public void createKeyspace() {
        session.execute(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};", KEYSPACE_NAME));
    }

    public void createTables() {
        // Crear tabla Destinos
        session.execute("CREATE TABLE IF NOT EXISTS " + cassandraConfig.getKeyspace() + ".destinos ("
                + "destino_id UUID, "
                + "nombre TEXT, "
                + "pais TEXT, "
                + "descripcion TEXT, "
                + "clima TEXT, "
                + "PRIMARY KEY((destino_id, nombre), pais, clima));");

        // Crear tabla Paquetes
        session.execute("CREATE TABLE IF NOT EXISTS " + cassandraConfig.getKeyspace() + ".paquetes ("
                + "paquete_id UUID, "
                + "nombre TEXT, "
                + "destino_id UUID, "
                + "duracion INT, "
                + "precio DECIMAL, "
                + "PRIMARY KEY((paquete_id, nombre), destino_id));");

        // Crear tabla Clientes
        session.execute("CREATE TABLE IF NOT EXISTS " + cassandraConfig.getKeyspace() + ".clientes ("
                + "cliente_id UUID, "
                + "nombre TEXT, "
                + "correo_electronico TEXT, "
                + "telefono TEXT, "
                + "PRIMARY KEY((cliente_id, nombre, correo_electronico)));");

        // Crear tabla Reservas
        session.execute("CREATE TABLE IF NOT EXISTS " + cassandraConfig.getKeyspace() + ".reservas ("
                + "reserva_id UUID, "
                + "paquete_id UUID, "
                + "cliente_id UUID, "
                + "fecha_inicio DATE, "
                + "fecha_fin DATE, "
                + "pagado BOOLEAN, "
                + "PRIMARY KEY((reserva_id, cliente_id, fecha_inicio, fecha_fin), paquete_id, pagado));");

    }

    public void executeQueries() {

        opcionesList = getTiposConsultas();

        while (true) {
            // Imprimir las opciones
            System.out.println(ansi().fg(YELLOW).a("╔═══════════════════════════════════════════════════════════════════════════════════════════════════════════════╗\n" +
                    ansi().fg(BLUE).a("\t\t\t\t\t\t\t\t\tMenú de Opciones").reset() + "\n" +
                    ansi().fg(YELLOW).a("╠═══════════════════════════════════════════════════════════════════════════════════════════════════════════════╣\n").reset()));

            showOptions(opcionesList);

            opcionValida = false;

            while (!opcionValida) {
                System.out.print(ansi().fg(YELLOW).a("==>Seleccione una opción: ").reset());
                // Verificar si la entrada es un número
                if (in.hasNextInt()) {
                    opcion = in.nextInt();
                    opcionValida = true;
                } else {
                    // La entrada no es un número
                    System.out.println(ansi().fg(RED).a("La opcion introducida no es un número.\n").reset());
                    in.next();
                }
            }
            in.nextLine(); // Consumir la nueva línea después de leer un entero

            switch (opcion) {
                case 1:
                    menuConsultasBasicas();
                    break;
                case 2:
                    menuConsultasAvanzadas();
                    break;
                case 3:
                    menuConsultasComplejas();
                    break;
                case 4:
                    salir();
                    break;
                default:
                    System.out.println(ansi().fg(RED).a("Opción no válida. Por favor, seleccione una opción válida.").reset());
            }
        }


    }

    private void menuConsultasBasicas() {
        opcionesList = getConsultasBasicas();

        while (true) {
            // Imprimir las opciones del Set
            System.out.println(ansi().fg(YELLOW).a("╔═══════════════════════════════════════════════════════════════════════════════════════════════════════════════╗\n" +
                    ansi().fg(BLUE).a("\t\t\t\t\t\t\t\t\tCONSULTAS BÁSICAS").reset() + "\n" +
                    ansi().fg(YELLOW).a("╠═══════════════════════════════════════════════════════════════════════════════════════════════════════════════╣\n").reset()));

            showOptions(opcionesList);

            opcionValida = false;
            while (!opcionValida) {
                System.out.print(ansi().fg(YELLOW).a("==>Seleccione una opción: ").reset());
                // Verificar si la entrada es un número
                if (in.hasNextInt()) {
                    opcion = in.nextInt();
                    opcionValida = true;
                } else {
                    // La entrada no es un número
                    System.out.println(ansi().fg(RED).a("La opcion introducida no es un número.\n").reset());
                    in.next();
                }
            }
            in.nextLine(); // Consumir la nueva línea después de leer un entero

            switch (opcion) {
                case 1:
                    queryExecutor.getAllDestinos("destinos");
                    break;
                case 2:
                    queryExecutor.getAllClientes("clientes");
                    break;
                case 3:
                    queryExecutor.getAllPaquetes();
                    break;
                case 4:
                    queryExecutor.getAllReservas();
                    break;
                case 5:
                    System.out.print("==>Introduce el Id de un paquete: ");
                    input = in.nextLine();
                    try {
                        id = UUID.fromString(input);
                        queryExecutor.getPaqueteDetails(id);
                    } catch (IllegalArgumentException e) {
                        System.out.print((ansi().fg(RED).a("Introduce un Id de un paquete existente\n").reset()));
                    }
                    break;
                case 6:
                    System.out.print("==>Introduce el Id de un cliente: ");
                    input = in.nextLine();
                    try {
                        id = UUID.fromString(input);
                        queryExecutor.getReservasByCliente(id);
                    } catch (IllegalArgumentException e) {
                        System.out.print((ansi().fg(RED).a("Introduce un Id de un cliente existente\n").reset()));
                    }
                    break;
                case 7:
                    System.out.print("==>Introduce el Id de un destino: ");
                    input = in.nextLine();
                    try {
                        id = UUID.fromString(input);
                        queryExecutor.getPaquetesByDestino(id);
                    } catch (IllegalArgumentException e) {
                        System.out.print((ansi().fg(RED).a("Introduce el Id de un destino existente\n").reset()));
                    }
                    break;
                case 8:
                    // Solicitar la fecha de inicio
                    LocalDate startDate = solicitarFecha("==>Introduce la fecha de inicio (AAAA-MM-DD): ");

                    // Solicitar la fecha de fin
                    LocalDate endDate = solicitarFecha("==>Introduce la fecha de fin (AAAA-MM-DD): ");

                    // Mostrar las fechas ingresadas
                    System.out.print(
                            "Fecha de inicio: " + startDate + "\n" +
                                    "Fecha de fin: " + endDate + "\n" +
                                    "Fechas Ingresadas");

                    queryExecutor.getClientesByReservationDateRange(startDate, endDate);
                    break;
                case 9:
                    executeQueries(); // Volver al menu principal
                    break;
                case 10:
                    salir();
                    break;
                default:
                    System.out.println(ansi().fg(RED).a("Opción no válida. Por favor, seleccione una opción válida.").reset());
            }
        }
    }

    private void menuConsultasAvanzadas(){
        opcionesList = getConsultasAvanzadas();

        while (true) {
            // Imprimir las opciones del Set
            System.out.println(ansi().fg(YELLOW).a("╔═══════════════════════════════════════════════════════════════════════════════════════════════════════════════╗\n" +
                    ansi().fg(BLUE).a("\t\t\t\t\t\t\t\t\tCONSULTAS AVANZADAS CON INDEXACIÓN").reset() + "\n" +
                    ansi().fg(YELLOW).a("╠═══════════════════════════════════════════════════════════════════════════════════════════════════════════════╣\n").reset()));

            showOptions(opcionesList);

            opcionValida = false;
            while (!opcionValida) {
                System.out.print(ansi().fg(YELLOW).a("==>Seleccione una opción: ").reset());
                // Verificar si la entrada es un número
                if (in.hasNextInt()) {
                    opcion = in.nextInt();
                    opcionValida = true;
                } else {
                    // La entrada no es un número
                    System.out.println(ansi().fg(RED).a("La opcion introducida no es un número.\n").reset());
                    in.next();
                }
            }
            in.nextLine(); // Consumir la nueva línea después de leer un entero

            switch (opcion) {
                case 1:
                    queryExecutor.createIndexForPaquetesByName();
                    System.out.print("==>Introduce un nombre de paquete: ");
                    input = in.nextLine();
                    queryExecutor.getPaquetesForName(input);
                    break;
                case 2:
                    queryExecutor.createResumenReservasPorPaqueteTable();
                    queryExecutor.updateResumenReservasPorPaquete();
                    queryExecutor.getResumenReservasPorPaqueteTable();
                    break;
                case 3:
                    System.out.print("==>Introduce el clima de un destino: ");
                    input = in.nextLine();
                    queryExecutor.getAllClientesByDestinoClima(input);
                    break;
                case 4:
                    queryExecutor.createIndexForReservasClienteFecha();
                    System.out.print("==>Introduce el Id de un cliente: ");
                    try {
                        input = in.nextLine();
                        id = UUID.fromString(input);
                        // Solicitar la fecha de inicio
                        LocalDate startDate = solicitarFecha("Introduce la fecha de inicio (AAAA-MM-DD): ");

                        // Solicitar la fecha de fin
                        LocalDate endDate = solicitarFecha("Introduce la fecha de fin (AAAA-MM-DD): ");

                        // Mostrar las fechas ingresadas
                        System.out.print(
                                "Fecha de inicio: " + startDate + "\n" +
                                        "Fecha de fin: " + endDate + "\n" +
                                        "Fechas Ingresadas");


                        queryExecutor.getAllReservasByClienteAndDateRange(id, startDate, endDate);

                    } catch (IllegalArgumentException e) {
                        System.out.print((ansi().fg(RED).a("Introduce un Id de un cliente existente\n").reset()));
                    }

                    break;
                case 5:
                    queryExecutor.createIndiceDestinosPorPaisTable();
                    System.out.print("==>Introduce el pais: ");
                    try {
                        input = in.nextLine();
                        queryExecutor.insertDestinosPorPais(input);
                        queryExecutor.getAllDestinos("destinos_por_pais");
                    } catch (IllegalArgumentException e) {
                        System.out.println((ansi().fg(RED).a("Introduce un pais existente\n").reset()));
                    }
                    break;
                case 6: executeQueries();
                    break;
                case 7:
                    salir();
                    break;
                default:
                    System.out.println(ansi().fg(RED).a("Opción no válida. Por favor, seleccione una opción válida.").reset());
            }
        }
    }

    private void menuConsultasComplejas() {
        opcionesList = getConsultasComplejas();

        while (true) {
            // Imprimir las opciones del Set
            System.out.println(ansi().fg(YELLOW).a("╔═══════════════════════════════════════════════════════════════════════════════════════════════════════════════╗\n" +
                    ansi().fg(BLUE).a("\t\t\t\t\t\t\t\t\tCONSULTAS COMPLEJAS CON INDEXACIÓN").reset() + "\n" +
                    ansi().fg(YELLOW).a("╠═══════════════════════════════════════════════════════════════════════════════════════════════════════════════╣\n").reset()));

            showOptions(opcionesList);

            boolean opcionValida = false;

            while (!opcionValida) {
                System.out.print(ansi().fg(YELLOW).a("==>Seleccione una opción: ").reset());
                // Verificar si la entrada es un número
                if (in.hasNextInt()) {
                    opcion = in.nextInt();
                    opcionValida = true;
                } else {
                    // La entrada no es un número
                    System.out.println(ansi().fg(RED).a("La opcion introducida no es un número.\n").reset());
                    in.next();
                }
            }
            in.nextLine(); // Consumir la nueva línea después de leer un entero

            switch (opcion) {
                case 1:
                    queryExecutor.createTableDestinosPopulares();
                    queryExecutor.showDestinosPopulares();
                    break;
                case 2:
                    queryExecutor.createIndiceClientesPorCorreoTable();

                    System.out.print("==>Introduce el correo: ");
                    try {
                        input = in.nextLine();
                        queryExecutor.insertClientesPorCorreo(input);
                        queryExecutor.getAllClientes("clientes_por_correo");
                    } catch (IllegalArgumentException e) {
                        System.out.println((ansi().fg(RED).a("Introduce un correo existente\n").reset()));
                    }
                    break;
                case 3:
                    queryExecutor.createIndiceDisponibilidadPaquetesPorDestinoYFecha();
                    queryExecutor.getPaquetesDisponibles();
                    queryExecutor.mostrarTablaDisponibilidadPaquetes();
                    break;
                case 4:
                    System.out.print("==>Introduce el Id de un destino: ");
                    input = in.nextLine();
                    System.out.print("==>Introduce la duración del paquete: ");
                    int duracion = in.nextInt();
                    in.nextLine(); // Consumir la nueva línea después de leer un entero
                    try {
                        id = UUID.fromString(input);
                        queryExecutor.getPaquetesPorDestinoYDuracion(id, duracion);
                    } catch (IllegalArgumentException e) {
                        System.out.print((ansi().fg(RED).a("Introduce un Id de un destino existente\n").reset()));
                    }

                    break;
                case 5:
                    System.out.print("==>Introduce el id de un cliente: ");
                    input = in.nextLine();
                    System.out.print("==>Introduce el Id de un destino: ");
                    String idD = in.nextLine();
                    System.out.print("==>Introduce el estado de pago del destino (1 o 0):\n0->NO Pagado\n1->Pagado");
                    int pagado = in.nextInt();
                    in.nextLine(); // Consumir la nueva línea después de leer un entero
                    try {
                        queryExecutor.getReservasPorClienteDestinoYEstadoPago(UUID.fromString(input), UUID.fromString(idD), (pagado != 0));
                    } catch (IllegalArgumentException e) {
                        System.out.print(ansi().fg(RED).a("Introduce un Id de un cliente y de un destino existentes\n").reset());
                    }

                    break;
                case 6:
                    executeQueries();
                    break;
                case 7:
                    salir();
                    break;
                default:
                    System.out.println(ansi().fg(RED).a("Opción no válida. Por favor, seleccione una opción válida.").reset());
            }
        }
    }

    private static List<String> getTiposConsultas() {

        opcionesList = new ArrayList<>();
        opcionesList.add("CONSULTAS BÁSICAS.");
        opcionesList.add("CONSULTAS AVANZADAS CON INDEXACIÓN.");
        opcionesList.add("CONSULTAS COMPLEJAS CON INDEXACIÓN");
        opcionesList.add("Salir");

        return opcionesList;
    }

    public static List<String> getConsultasBasicas() {

        opcionesList = new ArrayList<>();
        opcionesList.add("Obtener todos los destinos disponibles en la agencia de viajes.");
        opcionesList.add("Obtener todos los clientes.");
        opcionesList.add("Obtener todos los paquetes turísticos");
        opcionesList.add("Obtener todas las reservas.");
        opcionesList.add("Obtener los detalles de un paquete turístico específico a través de su ID.");
        opcionesList.add("Obtener todas las reservas realizadas por un cliente específico.");
        opcionesList.add("Obtener todos los paquetes turísticos disponibles para un destino en particular.");
        opcionesList.add("Obtener todos los clientes que han realizado reservas en un rango de fechas específico.");
        opcionesList.add("Volver");
        opcionesList.add("Salir");

        return opcionesList;

    }

    public static List<String> getConsultasAvanzadas() {
        opcionesList = new ArrayList<>();
        opcionesList.add("Generar un índice para acelerar la búsqueda de paquetes por nombre.");
        opcionesList.add("Crear una tabla de resumen para almacenar el número total de reservas realizadas por cada paquete.");
        opcionesList.add("Obtener todos los clientes que han realizado reservas en destinos con un clima específico.");
        opcionesList.add("Generar un índice compuesto para optimizar la búsqueda de reservas de un cliente en un rango de fechas determinado.");
        opcionesList.add("Crear una tabla de índice para acelerar la búsqueda de destinos por país.");
        opcionesList.add("Volver");
        opcionesList.add("Salir");

        return opcionesList;
    }

    public static List<String> getConsultasComplejas() {
        opcionesList = new ArrayList<>();
        opcionesList.add("Obtener todos los destinos populares (los más reservados) en orden descendente según el número de reservas.");
        opcionesList.add("Generar un índice para acelerar la búsqueda de clientes por correo electrónico.");
        opcionesList.add("Crear una tabla de índice para almacenar información sobre la disponibilidad de paquetes por destino y fecha.");
        opcionesList.add("Obtener todos los paquetes turísticos disponibles para un destino específico y con una duración determinada.");
        opcionesList.add("Generar un índice compuesto para optimizar la búsqueda de reservas por cliente, destino y estado de pago.");
        opcionesList.add("Volver");
        opcionesList.add("Salir");

        return opcionesList;
    }


    // Función para solicitar una fecha usando JOptionPane
    private static LocalDate solicitarFecha(String mensaje) {
        do {
            System.out.print(mensaje);
            String fechaString = in.nextLine();
            try {
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
                LocalDate fecha = LocalDate.parse(fechaString, formatter);

                // Si llegamos aquí, la fecha es válida
                System.out.println("Fecha ingresada: " + fecha);
                return fecha;

            } catch (Exception e) {
                System.out.println("Formato de fecha incorrecto. Por favor, inténtelo de nuevo.");
            }

        } while (true); // El bucle se ejecutará siempre hasta que se Introduce una fecha válida
    }

    public void showOptions(List<String> opciones) {
        for (int i = 0; i < opciones.size(); i++) {
            System.out.println(ansi().fg(YELLOW).a("==>" + (i + 1) + ". " + ansi().fg(BLUE).a(opcionesList.get(i)).reset()).reset());
        }
        System.out.println(ansi().fg(YELLOW).a("╚═══════════════════════════════════════════════════════════════════════════════════════════════════════════════╝\n").reset());
    }


    public void salir(){
        System.out.println(ansi().fg(BLUE).a("  ___ ___ _  _   ___  ___   _      _     ___ ___    __ ___ _____ ___ ___   _   \n" +
                " | __|_ _| \\| | |   \\| __| | |    /_\\   | _ \\ _ \\  /_// __|_   _|_ _/ __| /_\\  \n" +
                " | _| | || .` | | |) | _|  | |__ / _ \\  |  _/   / /--\\ (__  | |  | | (__ / _ \\ \n" +
                " |_| |___|_|\\_| |___/|___| |____/_/ \\_\\ |_| |_|_\\/_/\\_\\___| |_| |___\\___/_/ \\_\\\n" +
                "                                                                               ").reset());
        System.exit(0);
    }
}
