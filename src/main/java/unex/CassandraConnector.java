package unex;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;

import java.io.IOException;
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
    private static int opcion = 0;
    private static boolean isAdmin = false;

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
                + "PRIMARY KEY((destino_id), nombre, pais, clima));");

        // Crear tabla Paquetes
        session.execute("CREATE TABLE IF NOT EXISTS " + cassandraConfig.getKeyspace() + ".paquetes ("
                + "paquete_id UUID, "
                + "nombre TEXT, "
                + "destino_id UUID, "
                + "duracion INT, "
                + "precio DECIMAL, "
                + "PRIMARY KEY((paquete_id), nombre, destino_id));");

        // Crear tabla Clientes
        session.execute("CREATE TABLE IF NOT EXISTS " + cassandraConfig.getKeyspace() + ".clientes ("
                + "cliente_id UUID, "
                + "nombre TEXT, "
                + "correo_electronico TEXT, "
                + "telefono TEXT, "
                + "PRIMARY KEY((cliente_id), nombre, correo_electronico));");

        // Crear tabla Reservas
        session.execute("CREATE TABLE IF NOT EXISTS " + cassandraConfig.getKeyspace() + ".reservas ("
                + "reserva_id UUID, "
                + "paquete_id UUID, "
                + "cliente_id UUID, "
                + "fecha_inicio DATE, "
                + "fecha_fin DATE, "
                + "pagado BOOLEAN, "
                + "PRIMARY KEY((reserva_id), cliente_id, fecha_inicio, fecha_fin, paquete_id, pagado));");

    }

    public void menuPrincipal() throws IOException {

        opcionesList = getMenuPrincipal();

        while (true) {
            System.out.println(ansi().fg(YELLOW).a("╔═══════════════════════════════════════════════════════════════════════════════════════════════════════════════╗\n" +
                    ansi().fg(BLUE).a("\t\t\t\t\t\t\t\t\tMENÚ").reset() + "\n" +
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
                    isAdmin = true;
                    menuAdmin();
                    break;
                case 2:
                    isAdmin = false;
                    executeQueries();
                    break;
                case 3:
                    salir();
                    break;
                default:
                    System.out.println(ansi().fg(RED).a("Opción no válida. Por favor, seleccione una opción válida.").reset());
            }
        }
    }

    private void menuAdmin() throws IOException {
        isAdmin = true;

        opcionesList = getMenuAdmin();

        while (true) {
            // Imprimir las opciones del Set
            System.out.println(ansi().fg(YELLOW).a("╔═══════════════════════════════════════════════════════════════════════════════════════════════════════════════╗\n" +
                    ansi().fg(BLUE).a("\t\t\t\t\t\t\t\t\tMENÚ ADMIN").reset() + "\n" +
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
                    queryExecutor.cargarDatos();
                    break;
                case 2:
                    queryExecutor.limpiarDatos();
                    break;
                case 3:
                    executeQueries();
                    break;
                case 4:
                    System.out.print(ansi().fg(BLUE).a("==>Introduce el Id de un cliente: ").reset());
                    input = in.nextLine();
                    try {
                        id = UUID.fromString(input);
                        queryExecutor.eliminarRegistroPorId("clientes",id);
                    } catch (IllegalArgumentException e) {
                        System.out.print((ansi().fg(RED).a("Introduce un Id de un cliente existente\n").reset()));
                    }
                    break;
                case 5:
                    System.out.print(ansi().fg(BLUE).a("==>Introduce el Id de un destino: ").reset());
                    input = in.nextLine();
                    try {
                        id = UUID.fromString(input);
                        queryExecutor.eliminarRegistroPorId("destinos",id);
                    } catch (IllegalArgumentException e) {
                        System.out.print((ansi().fg(RED).a("Introduce un Id de un destino existente\n").reset()));
                    }
                    break;
                case 6:
                    System.out.print(ansi().fg(BLUE).a("==>Introduce el Id de un paquete: ").reset());
                    input = in.nextLine();
                    try {
                        id = UUID.fromString(input);
                        queryExecutor.eliminarRegistroPorId("paquetes",id);
                    } catch (IllegalArgumentException e) {
                        System.out.print((ansi().fg(RED).a("Introduce un Id de un destino existente\n").reset()));
                    }
                    break;
                case 7:
                    System.out.print(ansi().fg(BLUE).a("==>Introduce el Id de una reserva: ").reset());
                    input = in.nextLine();
                    try {
                        id = UUID.fromString(input);
                        queryExecutor.eliminarRegistroPorId("reservas",id);
                    } catch (IllegalArgumentException e) {
                        System.out.print((ansi().fg(RED).a("Introduce un Id de una reserva existente\n").reset()));
                    }
                    break;
                case 8:
                    queryExecutor.dropTablasSecundarias();
                    break;
                case 9:
                    menuPrincipal();
                    break;
                case 10:
                    salir();
                    break;
                default:
                    System.out.println(ansi().fg(RED).a("Opción no válida. Por favor, seleccione una opción válida.").reset());
            }
        }
    }

    public void menuCliente() throws IOException {
        isAdmin = false;
        opcionesList = getMenuCliente();

        while (true) {
            System.out.println(ansi().fg(YELLOW).a("╔═══════════════════════════════════════════════════════════════════════════════════════════════════════════════╗\n" +
                    ansi().fg(BLUE).a("\t\t\t\t\t\t\t\t\tMENÚ CLIENTE").reset() + "\n" +
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
                    executeQueries();
                    break;
                case 2:
                    consultarDatosCliente(); // Cosultamos los datos del usuario de la máquina junto a los datos de la proipia máquina sobre la que se ejecuta el programa
                case 3:
                    menuPrincipal();
                case 4:
                    salir();
                    break;
                default:
                    System.out.println(ansi().fg(RED).a("Opción no válida. Por favor, seleccione una opción válida.").reset());
            }
        }
    }

    private void consultarDatosCliente() {
        System.out.println(ansi().fg(YELLOW).a("╔═══════════════════════════════════════════════════════════════════════════════════════════════════════════════╗\n" +
                ansi().fg(BLUE).a("\t\t\t\t\t\t\t\t\tDatos de cliente").reset() + "\n" +
                ansi().fg(YELLOW).a("╠═══════════════════════════════════════════════════════════════════════════════════════════════════════════════╣\n").reset()));
        String nombreUsuario = System.getProperty("user.name");
        String directorioUsuario = System.getProperty("user.home");

        System.out.println(ansi().fg(GREEN).a("==>Datos del Usuario:").reset());
        System.out.println(ansi().fg(GREEN).a("==>Nombre de Usuario: " + nombreUsuario).reset());
        System.out.println(ansi().fg(GREEN).a("==>Directorio del Usuario: " + directorioUsuario).reset());

        String sistemaOperativo = System.getProperty("os.name");
        String versionSistemaOperativo = System.getProperty("os.version");
        String arquitecturaSistema = System.getProperty("os.arch");

        System.out.println(ansi().fg(GREEN).a("==>Datos de la Máquina:").reset());
        System.out.println(ansi().fg(GREEN).a("==>Sistema Operativo: " + sistemaOperativo).reset());
        System.out.println(ansi().fg(GREEN).a("==>Versión del Sistema Operativo: " + versionSistemaOperativo).reset());
        System.out.println(ansi().fg(GREEN).a("==>Arquitectura del Sistema: " + arquitecturaSistema).reset());
    }


    public void executeQueries() throws IOException {

        String clientOrAdmin = isAdmin ? "Admin" : "Cliente";
        opcionesList = getTiposConsultas();

        while (true) {
            // Imprimir las opciones
            System.out.println(ansi().fg(YELLOW).a("╔═══════════════════════════════════════════════════════════════════════════════════════════════════════════════╗\n" +
                    ansi().fg(BLUE).a("\t\t\t\t\t\t\t\t\tMENÚ DE CONSULTAS: "+clientOrAdmin).reset() + "\n" +
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
                    if(isAdmin){
                        menuAdmin();
                    }else{
                        menuCliente();
                    }
                    break;
                case 5:
                    salir();
                    break;
                default:
                    System.out.println(ansi().fg(RED).a("Opción no válida. Por favor, seleccione una opción válida.").reset());
            }
        }


    }

    private void menuConsultasBasicas() throws IOException {
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
                    System.out.print(ansi().fg(BLUE).a("==>Introduce el Id de un paquete: ").reset());
                    input = in.nextLine();
                    try {
                        id = UUID.fromString(input);
                        queryExecutor.getPaqueteDetails(id);
                    } catch (IllegalArgumentException e) {
                        System.out.print((ansi().fg(RED).a("Introduce un Id de un paquete existente\n").reset()));
                    }
                    break;
                case 6:
                    System.out.print(ansi().fg(BLUE).a("==>Introduce el Id de un cliente: ").reset());
                    input = in.nextLine();
                    try {
                        id = UUID.fromString(input);
                        queryExecutor.getReservasByCliente(id);
                    } catch (IllegalArgumentException e) {
                        System.out.print((ansi().fg(RED).a("Introduce un Id de un cliente existente\n").reset()));
                    }
                    break;
                case 7:
                    System.out.print(ansi().fg(BLUE).a("==>Introduce el Id de un destino: ").reset());
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

    private void menuConsultasAvanzadas() throws IOException {
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
                    try {
                        System.out.print(ansi().fg(BLUE).a("==>Introduce un nombre de un paquete: ").reset());
                        input = in.nextLine();
                        queryExecutor.getPaquetesForName(input);
                    }catch(IllegalArgumentException e) {
                        System.out.print((ansi().fg(RED).a("Introduce un nombre de un paquete existente\n").reset()));
                    }
                    break;
                case 2:
                    queryExecutor.createResumenReservasPorPaqueteTable();
                    queryExecutor.updateResumenReservasPorPaquete();
                    queryExecutor.getResumenReservasPorPaqueteTable();
                    break;
                case 3:
                    try {
                        System.out.print(ansi().fg(BLUE).a("==>Introduce el clima de un destino: ").reset());
                        input = in.nextLine();
                        queryExecutor.getAllClientesByDestinoClima(input);
                    }catch(IllegalArgumentException e) {
                        System.out.print((ansi().fg(RED).a("Introduce el clima de un destino existente\n").reset()));
                    }
                    break;
                case 4:
                    queryExecutor.createIndexForReservasClienteFecha();
                    System.out.print(ansi().fg(BLUE).a("==>Introduceel id de un cliente: ").reset());
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
                    System.out.print(ansi().fg(BLUE).a("==>Introduce un pais: ").reset());
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

    private void menuConsultasComplejas() throws IOException {
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

                    System.out.print(ansi().fg(BLUE).a("==>Introduce un correo: ").reset());
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
                    System.out.print(ansi().fg(BLUE).a("==>Introduce el id de un destino: ").reset());
                    input = in.nextLine();
                    System.out.print(ansi().fg(BLUE).a("==>Introduce la duración del paquete: ").reset());
                    int duracion = in.nextInt();
                    in.nextLine(); // Consumir la nueva línea después de leer un entero
                    try {
                        id = UUID.fromString(input);
                        queryExecutor.getPaquetesPorDestinoYDuracion(id, duracion);
                    } catch (IllegalArgumentException e) {
                        System.out.print((ansi().fg(RED).a("Introduce un Id y la duración de un destino existentes\n").reset()));
                    }

                    break;
                case 5:
                    System.out.print(ansi().fg(BLUE).a("==>Introduce el id de un cliente: ").reset());
                    input = in.nextLine();
                    System.out.print(ansi().fg(BLUE).a("==>Introduce el id de un destino: ").reset());
                    String idD = in.nextLine();
                    System.out.print(ansi().fg(BLUE).a("==>Introduce el estado de pago del destino (1 o 0):\n").fg(RED).a("0->NO Pagado\n").fg(BLUE).a("1->Pagado").reset());
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
        opcionesList.add("Volver");
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

    public static List<String> getMenuAdmin() {
        opcionesList = new ArrayList<>();
        opcionesList.add("Introducir registros a la Base de Datos.");
        opcionesList.add("Limpiar Base de Datos.");
        opcionesList.add("Consultar datos.");
        opcionesList.add("Eliminar cliente por ID.");
        opcionesList.add("Eliminar destino por ID.");
        opcionesList.add("Eliminar paquete por ID.");
        opcionesList.add("Eliminar reserva por ID");
        opcionesList.add("Eliminar Tablas secundarias.");
        opcionesList.add("Volver");
        opcionesList.add("Salir");

        return opcionesList;
    }

    public static List<String> getMenuPrincipal() {
        opcionesList = new ArrayList<>();
        opcionesList.add("Admin.");
        opcionesList.add("Cliente.");
        opcionesList.add("Salir");

        return opcionesList;
    }

    public static List<String> getMenuCliente() {
        opcionesList = new ArrayList<>();
        opcionesList.add("Consultar datos.");
        opcionesList.add("Consultar datos de cliente.");
        opcionesList.add("Volver");
        opcionesList.add("Salir");

        return opcionesList;
    }


    private static LocalDate solicitarFecha(String mensaje) {
        do {
            System.out.print(ansi().fg(BLUE).a(mensaje).reset());
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
